/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Security.Cryptography;
using Wv;
using Wv.Extensions;

// An ISchemaBackend that uses a directory on disk as a backing store.
[WvMoniker]
internal class VxDiskSchema : ISchemaBackend
{
    static WvLog log = new WvLog("VxDiskSchema", WvLog.L.Debug2);

    private string exportdir;

    public static void wvmoniker_register()
    {
	WvMoniker<ISchemaBackend>.register("dir",
		  (string m, object o) => new VxDiskSchema(m));
    }
	
    public VxDiskSchema(string _exportdir)
    {
        exportdir = _exportdir;
    }

    public void Dispose()
    {
    }

    //
    // The ISchemaBackend interface
    //

    // Export the current schema to the backing directory, in a format that can
    // be read back later.  
    public VxSchemaErrors Put(VxSchema schema, VxSchemaChecksums sums, 
        VxPutOpts opts)
    {
        bool isbackup = (opts & VxPutOpts.IsBackup) != 0;

        DirectoryInfo dir = new DirectoryInfo(exportdir);
        dir.Create();

        foreach (var p in schema)
        {
            if (!sums.ContainsKey(p.Key))
                throw new ArgumentException("Missing checksum for " + p.Key);

            VxSchemaElement elem = p.Value;
            if (elem.text == null || elem.text == "")
                DropSchema(new string[] {elem.key});
            else
                ExportToDisk(p.Value, sums[p.Key], isbackup);
        }

        // Writing schemas to disk doesn't give us any per-element errors.
        return new VxSchemaErrors();
    }

    public VxSchema Get(IEnumerable<string> keys)
    {
        VxSchema fullschema = new VxSchema();
        VxSchema schema = new VxSchema();

        ReadExportedDir(fullschema, null);

        if (keys == null)
            return fullschema;

        // This is a bit slow and stupid - we could just read only the
        // required keys from disk.  But the key-limiting is mainly for the
        // much slower dbus and database backends, so it's probably not worth
        // fixing.
        foreach (string key in keys)
            schema.Add(key, fullschema[key]);

        if (schema.Count == 0)
            schema = fullschema;
            
        return schema;
    }

    public VxSchemaChecksums GetChecksums()
    {
        VxSchemaChecksums sums = new VxSchemaChecksums();
        ReadExportedDir(null, sums);
        return sums;
    }

    public VxSchemaErrors DropSchema(IEnumerable<string> keys)
    {
        VxSchemaErrors errs = new VxSchemaErrors();

        foreach (string key in keys)
        {
            string fullpath = wv.PathCombine(exportdir, key);
            log.print("Removing {0}\n", fullpath);
            if (File.Exists(fullpath))
                File.Delete(fullpath);
            if (key.StartsWith("Index/"))
            {
                string type, name;
                VxSchema.ParseKey(key, out type, out name);
                if (type != "Index")
                    continue;

                // If it was the last index for a table, remove the empty dir.
                string[] split = wv.PathSplit(name);
                if (split.Length > 0)
                {
                    string table = split[0];
                    string tabpath = wv.PathCombine(exportdir, type, table);
                    // Directory.Delete won't delete non-empty dirs, but we
                    // still check both for safety and to write a sensible
                    // message.
                    if (Directory.GetFileSystemEntries(tabpath).Length == 0)
                    {
                        log.print("Removing empty directory {0}\n", tabpath);
                        Directory.Delete(tabpath);
                    }
                }
            }
        }

        return errs;
    }

    // Note: we ignore the "where" clause and just return everything.
    public string GetSchemaData(string tablename, int seqnum, string where,
		Dictionary<string,string> replaces, List<string> skipfields)
    {
        string datadir = Path.Combine(exportdir, "DATA");
        string filename = wv.fmt("{0}-{1}.sql", seqnum, tablename);
        string fullpath = Path.Combine(datadir, filename);

        return File.ReadAllText(fullpath);
    }

    public void PutSchemaData(string tablename, string text, int seqnum)
    {
        string datadir = Path.Combine(exportdir, "DATA");
        string filename = wv.fmt("{0}-{1}.sql", seqnum, tablename);
        string fullpath = Path.Combine(datadir, filename);

        Directory.CreateDirectory(datadir);
        File.WriteAllBytes(fullpath, text.ToUTF8());
    }

    //
    // Non-ISchemaBackend methods
    //
    
    // Retrieves both the schema and its checksums from exportdir, and puts
    // them into the parameters.
    private void ReadExportedDir(VxSchema schema, VxSchemaChecksums sums)
    {
        DirectoryInfo exportdirinfo = new DirectoryInfo(exportdir);
        if (exportdirinfo.Exists)
        {
            // Read all files that match */* and Index/*/*.
            foreach (DirectoryInfo dir1 in exportdirinfo.GetDirectories())
            {
                if (dir1.Name == "DATA")
                    continue;

                string type = dir1.Name;

                foreach (DirectoryInfo dir2 in dir1.GetDirectories())
                {
                    if (dir2.Name == "DATA" || dir1.Name != "Index")
                        continue;

                    // This is the Index/*/* part
                    foreach (FileInfo file in dir2.GetFiles())
                    {
                        if (!IsFileNameUseful(file.Name))
                            continue;

                        string name = wv.PathCombine(dir2.Name, file.Name);
                        AddFromFile(file.FullName, type, name, schema, sums);
                    }
                }

                // This is the */* part
                foreach (FileInfo file in dir1.GetFiles())
                {
                    if (!IsFileNameUseful(file.Name))
                        continue;

                    AddFromFile(file.FullName, type, file.Name, schema, sums);
                }
            }
        }
    }

    // Static methods

    // We want to ignore hidden files, and backup files left by editors.
    private static bool IsFileNameUseful(string filename)
    {
        return !filename.StartsWith(".") && !filename.EndsWith("~");
    }

    // Adds the contents of extradir to the provided schema and sums.
    // Throws an ArgumentException if the directory contains an entry that
    // already exists in schema or sums.
    public static void AddFromDir(string extradir, VxSchema schema, 
        VxSchemaChecksums sums)
    {
        VxDiskSchema disk = new VxDiskSchema(extradir);

        disk.ReadExportedDir(schema, sums);
    }

    // Reads a file from an on-disk exported schema, and sets the schema
    // element parameter's text field, if the schema element isn't null.
    // Returns a new VxSchemaChecksum object containing the checksum.
    // Returns true if the file passes its MD5 validation.  
    // If it returns false, elem and sum may be set to null.  
    private static bool ReadSchemaFile(string filename, string type, 
        string name, out VxSchemaElement elem, out VxSchemaChecksum sum)
    {
        elem = null;
        sum = null;

        FileInfo fileinfo = new FileInfo(filename);

        // Read the entire file into memory.  C#'s file IO sucks.
        byte[] bytes = File.ReadAllBytes(filename);
        
        // Find the header line
        int ii;
        for (ii = 0; ii < bytes.Length; ii++)
            if (bytes[ii] == '\n')
                break;

        if (ii == bytes.Length)
            return false; 

        // Read the header line
        Encoding utf8 = Encoding.UTF8;
        string header = utf8.GetString(bytes, 0, ii).Replace("\r", "");

        // Skip the newline
        if (bytes[ii] == '\n')
            ii++;

        // Read the body
        string body = utf8.GetString(bytes, ii, bytes.Length - ii);
	//LUKE look at this!!! OMG OMG
	elem = VxSchemaElement.create(type, name, body, false);

        // Parse the header line
        char[] space = {' '};
        string[] headers = header.Split(space, 3);
        if (headers.Length != 3)
            return false;

        string prefix = headers[0];
        string header_md5 = headers[1];
        string dbsum = headers[2];

        if (prefix != "!!SCHEMAMATIC")
            return false;

        // Compute the hash of the rest of the file
        byte[] md5 = MD5.Create().ComputeHash(bytes, ii, 
            (int)fileinfo.Length - ii);
        string content_md5 = md5.ToHex().ToLower();

        IEnumerable<string> sumlist;

        // If the MD5 sums don't match, we want to make it obvious that the
        // database and local file aren't in sync, so we don't load any actual
        // checksums.  
        if (String.Compare(header_md5, content_md5, true) == 0)
        {
            string errctx = wv.fmt("Error while reading file {0}: ", filename);
            sumlist = VxSchemaChecksum.ParseSumString(dbsum, errctx);
        }
        else
        {
            log.print(WvLog.L.Info, "Checksum mismatch for {0}\n", filename);
            sumlist = new List<string>();
        }

        sum = new VxSchemaChecksum(elem.key, sumlist);
        return true;
    }

    // Helper method to load a given on-disk element's schema and checksums
    // into the container objects.
    // Throws an ArgumentException if the schema or sums already contains the
    // given key.
    private static void AddFromFile(string path, string type, string name, 
        VxSchema schema, VxSchemaChecksums sums)
    {
        string key = wv.fmt("{0}/{1}", type, name);

        // schema/sums.Add would throw an exception in this situation anyway, 
        // but it's nice to provide a more helpful error message.
        if (schema != null && schema.ContainsKey(key))
            throw new ArgumentException("Conflicting schema key: " + key);
        if (sums != null && sums.ContainsKey(key))
            throw new ArgumentException("Conflicting sums key: " + key);

        VxSchemaChecksum sum;
        VxSchemaElement elem;
        ReadSchemaFile(path, type, name, out elem, out sum);

        if (schema != null && elem != null)
            schema.Add(key, elem);
        if (sums != null && sum != null)
            sums.Add(key, sum);
    }

    private void ExportToDisk(VxSchemaElement elem, VxSchemaChecksum sum, 
        bool isbackup)
    {
        // Make some kind of attempt to run on Windows.  
        string filename = wv.PathJoin(exportdir, elem.type, elem.name);

        // Make directories
        Directory.CreateDirectory(Path.GetDirectoryName(filename));

        string suffix = "";
        if (isbackup)
        {
            int i = 1;
            while(File.Exists(filename + "-" + i))
                i++;
            suffix = "-" + i;
        }

        filename += suffix;
            
        log.print("Writing {0}\n", filename);
        File.WriteAllBytes(filename, elem.ToStringWithHeader(sum).ToUTF8());
    }

}

