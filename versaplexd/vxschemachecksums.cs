using System;
using System.Collections.Generic;
using NDesk.DBus;
using Wv;

// FIXME: Have separate type member?
internal struct VxSchemaChecksum
{
    string _name;
    public string name {
        get { return _name; }
    }

    int _checksum;
    public int checksum {
        get { return _checksum; }
    }

    public VxSchemaChecksum(string newname, int newchecksum)
    {
        _name = newname;
        _checksum = newchecksum;
    }

    public VxSchemaChecksum(MessageReader reader)
    {
        reader.GetValue(out _name);
        reader.GetValue(out _checksum);
    }

    public void Write(MessageWriter writer)
    {
        writer.Write(typeof(string), name);
        writer.Write(typeof(int), checksum);
    }

    public static string GetSignature()
    {
        return "si";
    }
}

internal class VxSchemaChecksums : Dictionary<string, VxSchemaChecksum>
{
    public VxSchemaChecksums()
    {
    }

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
            p.Value.Write(writer);
    }

    // Write the list of checksums to DBus in a(si) format
    public void WriteChecksums(MessageWriter writer)
    {
        writer.WriteDelegatePrependSize(delegate(MessageWriter w)
            {
                _WriteChecksums(w);
            }, 8);
    }

    public void Add(string name, int checksum)
    {
        this.Add(name, new VxSchemaChecksum(name, checksum));
    }

    public static string GetSignature()
    {
        return String.Format("a({0})", VxSchemaChecksum.GetSignature());
    }
}
