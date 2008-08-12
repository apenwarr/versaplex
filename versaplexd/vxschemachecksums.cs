using System;
using System.IO;
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

    public VxSchemaChecksum(VxSchemaChecksum copy)
    {
        _name = copy.name;
        _checksums = new List<ulong>();
        foreach (ulong sum in copy.checksums)
            _checksums.Add(sum);
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

