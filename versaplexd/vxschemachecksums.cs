using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using System.Security.Cryptography;
using NDesk.DBus;
using Wv;
using Wv.Extensions;

// The checksums for a single database element (table, procedure, etc).
// Can have multiple checksum values - a table has one checksum per column,
// for instance.
// FIXME: Have separate type member?
internal class VxSchemaChecksum
{
    string _name;
    public string name {
        get { return _name; }
    }

    List<ulong> _checksums;
    public List<ulong> checksums {
        get { return _checksums; }
    }

    public VxSchemaChecksum(string newname)
    {
        _name = newname;
        _checksums = new List<ulong>();
    }

    public VxSchemaChecksum(string newname, ulong newchecksum)
    {
        _name = newname;
        _checksums = new List<ulong>();
        AddChecksum(newchecksum);
    }

    // Read a set of checksums from a DBus message into a VxSchemaChecksum.
    public VxSchemaChecksum(MessageReader reader)
    {
        reader.GetValue(out _name);
        _checksums = new List<ulong>();

        // Fill the list
        int size;
        reader.GetValue(out size);
        int endpos = reader.Position + size;
        while (reader.Position < endpos)
        {
            reader.ReadPad(8);
            ulong sum;
            reader.GetValue(out sum);
            AddChecksum(sum);
        }
    }

    // Reads checksums from a file on disk.  If the file is corrupt, will not
    // load any checksums.
    public void ReadChecksum(string filename)
    {
        FileInfo fileinfo = new FileInfo(filename);

        // Read the entire file into memory.  C#'s file IO sucks.
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
            return; 

        // Read the header line
        Encoding utf8 = Encoding.UTF8;
        string header = utf8.GetString(bytes, 0, ii).Replace("\r", "");

        // Parse the header line
        char[] space = {' '};
        string[] elems = header.Split(space, 3);
        if (elems.Length != 3)
            return;

        string prefix = elems[0];
        string header_md5 = elems[1];
        string dbsum = elems[2];

        if (prefix != "!!SCHEMAMATIC")
            return;

        // Compute the hash of the rest of the file
        ii++;
        byte[] md5 = MD5.Create().ComputeHash(bytes, ii, 
            (int)fileinfo.Length - ii);
        string content_md5 = md5.ToHex();

        // If the MD5 sums don't match, we want to make it obvious that the
        // database and local file aren't in sync, so we don't load any actual
        // checksums.  
        if (header_md5 == content_md5)
        {
            string[] sums = dbsum.Split(' ');
            foreach (string sum in sums)
            {
                ulong longsum;
                if (!UInt64.TryParse(sum, 
                        System.Globalization.NumberStyles.HexNumber, null, 
                        out longsum))
                {
                    // A bad checksum means the whole file is bad.
                    _checksums.Clear();
                    return;
                }
                AddChecksum(longsum);
            }
        }

        return;
    }

    public string GetSumString()
    {
        List<string> l = new List<string>();
        foreach (ulong sum in checksums)
            l.Add(sum.ToString("X8"));
        return l.Join(" ");
    }

    private void _WriteSums(MessageWriter writer)
    {
        foreach (ulong sum in _checksums)
            writer.Write(sum);
    }

    // Write the checksum values to DBus
    public void Write(MessageWriter writer)
    {
        writer.Write(typeof(string), name);
        writer.WriteDelegatePrependSize(delegate(MessageWriter w)
            {
                _WriteSums(w);
            }, 8);
    }

    public void AddChecksum(ulong checksum)
    {
        _checksums.Add(checksum);
    }

    // Note: it's unwise to override Object.Equals for mutable classes.
    // Overriding Object.Equals means also overriding Object.GetHashCode,
    // otherwise finding elements in hash tables would break.  But then
    // changing the hash code of an element in a hash table also makes things
    // unhappy.
    public bool IsEqual(VxSchemaChecksum other)
    {
        if (other == null)
            return false;

        if (this.name != other.name)
            return false;

        if (this.checksums.Count != other.checksums.Count)
            return false;

        for (int i = 0; i < this.checksums.Count; i++)
            if (this.checksums[i] != other.checksums[i])
                return false;

        return true;
    }

    public static string GetDbusSignature()
    {
        return "sat";
    }
}

// The checksum values for a set of database elements
internal class VxSchemaChecksums : Dictionary<string, VxSchemaChecksum>
{
    public VxSchemaChecksums()
    {
    }

    // Read an array of checksums from a DBus message.
    public VxSchemaChecksums(MessageReader reader)
    {
        int size;
        reader.GetValue(out size);
        int endpos = reader.Position + size;
        while (reader.Position < endpos)
        {
            reader.ReadPad(8);
            VxSchemaChecksum cs = new VxSchemaChecksum(reader);
            Add(cs.name, cs);
        }
    }

    private void AddChecksumFromFile(string filepath, string key)
    {
        string newkey = key;
        // Internally, we separate elements with '/' regardless of running on
        // Windows or Unix.
        if (Path.DirectorySeparatorChar != '/')
            newkey = key.Replace(Path.DirectorySeparatorChar, '/');
        VxSchemaChecksum cs = new VxSchemaChecksum(newkey);
        cs.ReadChecksum(filepath);
        Add(cs.name, cs);
    }

    public void ReadChecksums(string exportdir)
    {
        DirectoryInfo exportdirinfo = new DirectoryInfo(exportdir);
        if (exportdirinfo.Exists)
        {
            // Read all files that match */* and */*/*.
            foreach (DirectoryInfo dir1 in exportdirinfo.GetDirectories())
            {
                if (dir1.Name == "DATA")
                    continue;

                foreach (DirectoryInfo dir2 in dir1.GetDirectories())
                {
                    if (dir2.Name == "DATA")
                        continue;

                    // This is the */*/* part
                    foreach (FileInfo file in dir2.GetFiles())
                    {
                        AddChecksumFromFile(file.FullName, 
                            wv.PathCombine(dir1.Name, dir2.Name, file.Name));
                    }
                }

                // This is the */* part
                foreach (FileInfo file in dir1.GetFiles())
                {
                    AddChecksumFromFile(file.FullName, 
                        wv.PathCombine(dir1.Name, file.Name));
                }
            }
        }
    }

    private void _WriteChecksums(MessageWriter writer)
    {
        foreach (KeyValuePair<string,VxSchemaChecksum> p in this)
        {
            writer.WritePad(8);
            p.Value.Write(writer);
        }
    }

    // Write the list of checksums to DBus in a(sat) format.
    public void WriteChecksums(MessageWriter writer)
    {
        writer.WriteDelegatePrependSize(delegate(MessageWriter w)
            {
                _WriteChecksums(w);
            }, 8);
    }

    public void Add(string name, ulong checksum)
    {
        if (this.ContainsKey(name))
            this[name].AddChecksum(checksum);
        else
            this.Add(name, new VxSchemaChecksum(name, checksum));
    }

    public static string GetDbusSignature()
    {
        return String.Format("a({0})", VxSchemaChecksum.GetDbusSignature());
    }
}

internal class SchemaTypeComparer: IComparer<string>
{
    enum SchemaTypes
    {
        xmlschema = 100,
        table = 200,
        view = 300,
        index = 400,
        scalarfunction = 1100,
        tablefunction = 1200,
        procedure = 1300,
        trigger = 1400
    }

    private int sort_order(string s)
    {
        string[] parts = s.Split('/');

        int retval;
        try
        {
            bool ignore_case = true;
            retval = Convert.ToInt32(Enum.Parse(typeof(SchemaTypes), 
                parts[0], ignore_case));
        }
        catch (Exception)
        {
            retval = 9999;
        }
        return retval;
    }

    public int Compare(string x, string y)
    {
        int sort_x = sort_order(x);
        int sort_y = sort_order(y);

        if (sort_x != sort_y)
            return sort_x - sort_y;
        else
            return String.Compare(x, y);
    }
}

internal enum VxDiffType
{
    Unchanged = '.',
    Add = '+',
    Remove = '-',
    Change = '*'
}

// FIXME: It might be nicer in the long term to just implement 
// IEnumerable<...> or IDictionary<...> ourselves, and defer to
// an internal member.  But it's a lot of boilerplate code.
internal class VxSchemaChecksumDiff : SortedList<string, VxDiffType>
{
    // Estimate the initial size as being the maximum of either set of 
    // input sums.
    public VxSchemaChecksumDiff(VxSchemaChecksums srcsums, 
        VxSchemaChecksums goalsums):
        base(Math.Max(srcsums.Count, goalsums.Count), new SchemaTypeComparer())
    {
        List<string> keys = srcsums.Keys.Union(goalsums.Keys).ToList();
        keys.Sort(new SchemaTypeComparer());
        foreach (string key in keys)
        {
            if (!srcsums.ContainsKey(key))
                this.Add(key, VxDiffType.Add);
            else if (!goalsums.ContainsKey(key))
                this.Add(key, VxDiffType.Remove);
            else if (!srcsums[key].IsEqual(goalsums[key]))
            {
                if (!this.ContainsKey(key))
                    this.Add(key, VxDiffType.Change);
            }
            else
            {
                //this.Add(key, VxDiffType.Unchanged);
            }
        }
    }

    // Convert a set of diffs to a string of the form:
    // + AddedEntry
    // - RemovedEntry
    // * ChangedEntry
    // . UnchangedEntry
    // The leading characters are taken directly from the enum definition.
    public override string ToString()
    {
        StringBuilder sb = new StringBuilder();
        // Assume around 32 characters per entry.  May be slightly off, but
        // it'll be way better than the default value of 16.
        sb.Capacity = 32 * this.Count;
        foreach (KeyValuePair<string,VxDiffType> p in this)
        {
            sb.AppendLine(((char)p.Value) + " " + p.Key); 
        }
        return sb.ToString();
    }
}
