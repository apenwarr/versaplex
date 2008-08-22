using System;
using System.IO;
using System.Linq;
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

    IEnumerable<ulong> _checksums;
    public IEnumerable<ulong> checksums {
        get { return _checksums; }
    }

    public VxSchemaChecksum(VxSchemaChecksum copy)
    {
        _name = copy.name;
        var list = new List<ulong>();
        foreach (ulong sum in copy.checksums)
            list.Add(sum);
        _checksums = list;
    }

    public VxSchemaChecksum(string newname)
    {
        _name = newname;
        _checksums = new List<ulong>();
    }

    public VxSchemaChecksum(string newname, ulong newchecksum)
    {
        _name = newname;
        var list = new List<ulong>();
        list.Add(newchecksum);
        _checksums = list;
    }

    public VxSchemaChecksum(string newname, IEnumerable<ulong> sumlist)
    {
        _name = newname;
        _checksums = sumlist;
    }

    // Read a set of checksums from a DBus message into a VxSchemaChecksum.
    public VxSchemaChecksum(MessageReader reader)
    {
        reader.GetValue(out _name);

        // Fill the list
        List<ulong> list = new List<ulong>();
        int size;
        reader.GetValue(out size);
        int endpos = reader.Position + size;
        while (reader.Position < endpos)
        {
            reader.ReadPad(8);
            ulong sum;
            reader.GetValue(out sum);
            list.Add(sum);
        }
        _checksums = list;
    }

    public string GetSumString()
    {
        List<string> l = new List<string>();
        foreach (ulong sum in checksums)
            l.Add("0x" + sum.ToString("x8"));
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
        ulong[] sumarr = {checksum};
        _checksums = _checksums.Concat(sumarr);
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

        // FIXME: This can be replaced with Linq's SequenceEquals(), once
        // we're using a version of mono that implements it (>= 1.9).
        ulong[] mysums = this.checksums.ToArray();
        ulong[] theirsums = other.checksums.ToArray();

        if (mysums.Count() != theirsums.Count())
            return false;

        for (int i = 0; i < mysums.Count(); i++)
            if (mysums[i] != theirsums[i])
                return false;

        return true;
    }

    // Given a string containing database sums, returns their parsed version.
    // Leading "0x" prefixes are optional, though all numbers are assumed to
    // be hex.  The sums must be separated by spaces, e.g. "0xdeadbeef badf00d"
    // Prints an error message and ignores any unparseable elements.
    public static IEnumerable<ulong> ParseSumString(string dbsums)
    {
        return ParseSumString(dbsums, null);
    }

    // Acts just like ParseSumString(dbsums), but allows for providing 
    // some context for errors.  If errctx isn't null, it's printed before any
    // error messages produced.  A suitable string might be "Error while 
    // reading file $filename: ".
    public static IEnumerable<ulong> ParseSumString(string dbsums, 
        string errctx)
    {
        if (dbsums == null)
            return new List<ulong>();

        string[] sums = dbsums.Split(' ');
        var sumlist = new List<ulong>();
        foreach (string sumstr in sums)
        {
            // Ignore trailing spaces.
            if (sumstr.Length == 0)
                continue;

            // C#'s hex parser doesn't like 0x prefixes.
            string stripped = sumstr.ToLower();
            if (stripped.StartsWith("0x"))
                stripped = stripped.Remove(0, 2);

            ulong longsum;
            if (UInt64.TryParse(stripped,
                    System.Globalization.NumberStyles.HexNumber, null, 
                    out longsum))
            {
                sumlist.Add(longsum);
            }
            else
            {
                WvLog log = new WvLog("ParseSumString");
                string msg = wv.fmt("Failed to parse database sums '{0}' " + 
                    "due to the malformed element '{1}'.\n", 
                    dbsums, sumstr);
                log.print("{0}{1}", errctx == null ? "" : errctx, msg);
            }
        }
        return sumlist;
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

    public VxSchemaChecksums(VxSchemaChecksums copy)
    {
        foreach (KeyValuePair<string,VxSchemaChecksum> p in copy)
            this.Add(p.Key, new VxSchemaChecksum(p.Value));
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

    public void AddSum(string name, ulong checksum)
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

