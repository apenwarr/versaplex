using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Security.Cryptography;
using Wv;
using Wv.Extensions;

// An ISchemaBackend that uses a directory on disk as a backing store.
internal class VxDiskSchema : ISchemaBackend
{
    static WvLog log = new WvLog("VxDiskSchema", WvLog.L.Debug2);

    private string exportdir;

    public VxDiskSchema(string _exportdir)
    {
        exportdir = _exportdir;
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

        foreach (KeyValuePair<string,VxSchemaElement> p in schema)
        {
            if (!sums.ContainsKey(p.Key))
                throw new ArgumentException("Missing checksum for " + p.Key);

            VxSchemaElement elem = p.Value;
            if (elem.text == null || elem.text == "")
                DropSchema(elem.type, elem.name);
            else
                p.Value.ExportToDisk(exportdir, sums[p.Key], isbackup);
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

    public void DropSchema(string type, string name)
    {
        if (type == null || name == null)
            return;

        string fullpath = wv.PathCombine(exportdir, type, name);
        log.print("Removing {0}\n", fullpath);
        if (File.Exists(fullpath))
            File.Delete(fullpath);
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
            // Read all files that match */* and */*/*.
            foreach (DirectoryInfo dir1 in exportdirinfo.GetDirectories())
            {
                if (dir1.Name == "DATA")
                    continue;

                string type = dir1.Name;

                foreach (DirectoryInfo dir2 in dir1.GetDirectories())
                {
                    if (dir2.Name == "DATA")
                        continue;

                    // This is the */*/* part
                    foreach (FileInfo file in dir2.GetFiles())
                    {
                        string name = wv.PathCombine(dir2.Name, file.Name);
                        AddFromFile(file.FullName, type, name, schema, sums);
                    }
                }

                // This is the */* part
                foreach (FileInfo file in dir1.GetFiles())
                    AddFromFile(file.FullName, type, file.Name, schema, sums);
            }
        }
    }

    // Static methods

    // Reads a file from an on-disk exported schema, and sets the schema
    // element parameter's text field and the sum parameter's checksums field.
    // If either parameter is null, just loads the other.
    // Returns true if the file passes its MD5 validation.  If it returns
    // false, still sets the element's text field, but the sum parameter
    // will have no sums in it.
    // FIXME: Maybe return new objects in out parameters?
    public static bool ReadSchemaFile(string filename, VxSchemaElement elem, 
        VxSchemaChecksum sum)
    {
        FileInfo fileinfo = new FileInfo(filename);

        // Read the entire file into memory.  C#'s file IO sucks.
        // FIXME: Replace with File.ReadAllBytes
        byte[] bytes = new byte[fileinfo.Length];
        using (FileStream fs = fileinfo.OpenRead())
        {
            fs.Read(bytes, 0, bytes.Length);
        }
        
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
        if (elem != null)
            elem.text = body;

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
        string content_md5 = md5.ToHex();

        // If the MD5 sums don't match, we want to make it obvious that the
        // database and local file aren't in sync, so we don't load any actual
        // checksums.  
        if (header_md5 == content_md5)
        {
            string[] sums = dbsum.Split(' ');
            foreach (string sumstr in sums)
            {
                ulong longsum;
                if (!UInt64.TryParse(sumstr, 
                        System.Globalization.NumberStyles.HexNumber, null, 
                        out longsum))
                {
                    // A bad checksum means the whole file is bad.
                    if (sum != null)
                        sum.checksums.Clear();
                    return false;
                }
                if (sum != null)
                    sum.AddChecksum(longsum);
            }
        }

        return true;
    }

    // Helper method to load a given on-disk element's schema and checksums
    // into the container objects.
    private static void AddFromFile(string path, string type, string name, 
        VxSchema schema, VxSchemaChecksums sums)
    {
        string key = wv.PathCombine(type, name);
        VxSchemaChecksum sum = new VxSchemaChecksum(key);
        VxSchemaElement elem = new VxSchemaElement(type, name, "", false);
        ReadSchemaFile(path, elem, sum);
        if (schema != null)
            schema.Add(key, elem);
        if (sums != null)
            sums.Add(key, sum);
    }
}

