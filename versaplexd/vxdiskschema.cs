using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Security.Cryptography;
using Wv;
using Wv.Extensions;

// An ISchemaBackend that uses a directory on disk as a backing store.
[WvMoniker]
internal class VxDiskSchema : ISchemaBackend
{
    static WvLog log = new WvLog("VxDiskSchema", WvLog.L.Debug2);

    string exportdir;
    Dictionary<string,string> replaces;
    List<string> skipfields;

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
    
    // Extracts the table name and priority out of a path.  E.g. for 
    // "/foo/12345-bar.sql", returns "12345" and "bar" as the priority and 
    // table name.  Returns -1/null if the parse fails.
    public static void ParsePath(string pathname, out int seqnum, 
        out string tablename)
    {
        FileInfo info = new FileInfo(pathname);
        seqnum = -1;
        tablename = null;

        int dashidx = info.Name.IndexOf('-');
        if (dashidx < 0)
            return;

        string pristr = info.Name.Remove(dashidx);
        string rest = info.Name.Substring(dashidx + 1);
        int pri = wv.atoi(pristr);

        if (pri < 0)
            return;

        if (info.Extension.ToLower() == ".sql")
            rest = rest.Remove(rest.ToLower().LastIndexOf(".sql"));
        else
            return;

        seqnum = pri;
        tablename = rest;

        return;
    }
    
    void ReadRules()
    {
        string cmdfile = Path.Combine(exportdir, "data-export.txt");
        if (File.Exists(cmdfile))
        {
            string[] cmd_strings = File.ReadAllLines(cmdfile);
            
            replaces = new Dictionary<string,string>();
            skipfields = new List<string>();
            string tablefield, replacewith;

            foreach (string s in cmd_strings)
            {
                if (s.StartsWith("replace "))
                {
                    //take replace off
                    replacewith = s.Substring(8);
                    
                    //take table.fieldname
                    tablefield = replacewith.Substring(0,
                                        s.IndexOf(" with ")-8).Trim().ToLower();
                    
                    //take the value
                    replacewith = replacewith.Substring(
                                        replacewith.IndexOf(" with ")+6).Trim();
                    if (replacewith.ToLower() == "null")
                        replaces.Add(tablefield,null);
                    else
                        replaces.Add(tablefield,
                                 replacewith.Substring(1,replacewith.Length-2));
                }
                else if (s.StartsWith("skipfield "))
                    skipfields.Add(s.Substring(10).Trim().ToLower());
            }                

        }
    }
    
    public Dictionary<string,string> GetReplaceRules()
    {
        if (replaces == null)
            ReadRules();

        return replaces;
    }
    
    public List<string> GetFieldsToSkip()
    {
        if (skipfields == null)
            ReadRules();
            
        return skipfields;
    }

    public string CsvFix(string tablename, string csvtext)
    {
        List<string> result = new List<string>();
        WvCsv csvhandler = new WvCsv(csvtext);
        string coltype, colsstr;
        List<string> values = new List<string>();
        List<string> cols = new List<string>();
        List<string> allcols = new List<string>();
        int[] fieldstoskip = new int[skipfields.Count];
        
        for (int i=0; i < skipfields.Count; i++)
            fieldstoskip[i] = -1;

        Dictionary<string,string> coltypes = new Dictionary<string,string>();
        VxDiskSchema disk = new VxDiskSchema(exportdir);
        VxSchema schema = disk.Get(null);
        
        Console.WriteLine("Fixing data of table ["+tablename+"]");
        string[] csvcolumns = (string[])csvhandler.GetLine()
                                        .ToArray(Type.GetType("System.String"));
        for (int i=0; i<csvcolumns.Length; i++)
            csvcolumns[i] = csvcolumns[i].ToLower();

        int ii = 0;
        foreach (KeyValuePair<string,VxSchemaElement> p in schema)
        {
            if (!(p.Value is VxSchemaTable))
                continue;
                
            if (((VxSchemaTable)p.Value).name.ToLower() != tablename.ToLower())
                continue;

            foreach (VxSchemaTableElement te in ((VxSchemaTable)p.Value))
            {
                if (te.GetElemType() != "column")
                    continue;
                    
                if (csvcolumns.Contains(te.GetParam("name").ToLower()))
                    coltypes.Add(te.GetParam("name").ToLower(),
                                 te.GetParam("type"));
        
                allcols.Add(te.GetParam("name").ToLower());
                if (csvcolumns.Contains(te.GetParam("name").ToLower()))
                {
                    if (skipfields.Contains(te.GetParam("name").ToLower()))
                        fieldstoskip[skipfields.IndexOf(
                                    te.GetParam("name").ToLower())] = ii;
                    else if (skipfields.Contains(
                                    tablename.ToLower()+"."+
                                    te.GetParam("name").ToLower()))
                        fieldstoskip[skipfields.IndexOf(
                                    tablename.ToLower()+"."+
                                    te.GetParam("name").ToLower())] = ii;
                    else
                        cols.Add(te.GetParam("name"));
                }

                ii++;
            }
        }
        colsstr = "\"" + cols.join("\",\"") + "\"\n";
        
        if (!csvhandler.hasMore())
            return colsstr;
        
        while (csvhandler.hasMore())
        {
            string[] asarray = (string[])csvhandler.GetLine()
                                       .ToArray(Type.GetType("System.String"));

            if (asarray.Length != csvcolumns.Length)
                return "";
                
            values.Clear();
            
            for (int i=0;i<asarray.Length;i++)
            {
                if (Array.IndexOf(fieldstoskip,i)>=0)
                    continue;

                if (replaces.ContainsKey(csvcolumns[i]))
                    asarray[i] = replaces[csvcolumns[i]];
                    
                if (replaces.ContainsKey(tablename.ToLower() + "." +
                                         csvcolumns[i]))
                    asarray[i] = replaces[tablename.ToLower() + "." +
                                          csvcolumns[i]].ToLower();

                if (coltypes.ContainsKey(csvcolumns[i]) && 
                    (coltypes[csvcolumns[i]] != null))
                    coltype = coltypes[csvcolumns[i]];
                else
                    coltype = "";
                    
                if (asarray[i] == null)
                    values.Add("");
                else if ((coltype == "varchar") ||
                         (coltype == "datetime") ||
                         (coltype == "char") ||
                         (coltype == "nchar") ||
                         (coltype == "text"))
                {
                    // Double-quote chars for SQL safety
                    string esc = asarray[i].Replace("\"", "\"\"");
                    
                    //indication that single quotes are already doubled
                    if (esc.IndexOf("''") < 0)
                        esc = esc.Replace("'", "''");
                        
                    if (WvCsv.RequiresQuotes(esc))
                        values.Add('"' + esc + '"');
                    else
                        values.Add(esc);
                }
                else if (coltype == "image")
                {
                    string temp = asarray[i].Replace("\n","");
                    string tmp = "";
                    while (temp.Length > 0)
                    {
                        if (temp.Length > 75)
                        {
                            tmp += temp.Substring(0,76) + "\n";
                            temp = temp.Substring(76);
                        }
                        else
                        {
                            tmp += temp + "\n";
                            break;
                        }
                    }
                    values.Add("\""+tmp+"\"");
                }
                else
                    values.Add(asarray[i]);
            }
            result.Add(values.join(",") + "\n");
        }

        result.Sort(StringComparer.Ordinal);

        return colsstr+result.join("");
    }

    //If there is CSV anywhere, make it SQL statements
    public string Normalize(string text)
    {
        TextReader txt = new StringReader(text);
        StringBuilder result = new StringBuilder();
        string line = "";
        string csvtext = "";
        string tablename = "";
        
        while ((line = txt.ReadLine()) != null)
        {
            if (line.StartsWith("TABLE "))
            {
                csvtext = "";
                tablename = line.Substring(6).Replace(",","").Trim();
                
                //gotta get the CSV part only
                while (!String.IsNullOrEmpty(line = txt.ReadLine()))
                    csvtext += line + "\n";
                
                result.Append("TABLE "+tablename+"\n");
                //Will return CSV part as INSERTs
                result.Append(CsvFix(tablename,csvtext));
            }
            else
                result.Append(line+"\n");
        }
        return result.ToString();
    }
    
    public int FixSchemaData(Dictionary<string,string> replaces, 
                                List<string> skipfields)
    {
        string datadir = Path.Combine(exportdir, "DATA");
        if (!Directory.Exists(datadir))
            return 5; // nothing to do

	var tmpfiles = 
	    from f in Directory.GetFiles(datadir)
	    where !f.EndsWith("~")
	    select f;
	    
        String[] files = tmpfiles.ToArray();

        int seqnum;
        string tablename;

        Array.Sort(files);
        foreach (string file in files)
        {
            string data = File.ReadAllText(file, Encoding.UTF8);
            ParsePath(file, out seqnum, out tablename);

            if (tablename == "ZAP")
                tablename = "";

            log.print("Fixing data from {0}\n", file);
            File.WriteAllBytes(file, Normalize(data).ToUTF8());
        }
	
	return 0;
    }
    
    // Retrieves both the schema and its checksums from exportdir, and puts
    // them into the parameters.
    void ReadExportedDir(VxSchema schema, VxSchemaChecksums sums)
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
    static bool IsFileNameUseful(string filename)
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
    static bool ReadSchemaFile(string filename, string type, 
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

        IEnumerable<ulong> sumlist;

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
            sumlist = new List<ulong>();
        }

        sum = new VxSchemaChecksum(elem.key, sumlist);
        return true;
    }

    // Helper method to load a given on-disk element's schema and checksums
    // into the container objects.
    // Throws an ArgumentException if the schema or sums already contains the
    // given key.
    static void AddFromFile(string path, string type, string name, 
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

    void ExportToDisk(VxSchemaElement elem, VxSchemaChecksum sum, 
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
