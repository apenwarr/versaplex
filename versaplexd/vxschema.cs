using System;
using System.Collections.Generic;
using NDesk.DBus;

internal class VxSchemaElement : IComparable
{
    string _name;
    public string name {
        get { return _name; }
    }

    string _type;
    public string type {
        get { return _type; }
    }

    string _text;
    public string text {
        get { return _text; }
        set { _text = value;}
    }
    
    bool _encrypted;
    public bool encrypted {
        get { return _encrypted; }
    }

    public VxSchemaElement(string newname, string newtype, string newtext,
            bool newencrypted)
    {
        _name = newname;
        _type = newtype;
        _encrypted = newencrypted;
        _text = newtext;
    }

    public VxSchemaElement(MessageReader reader)
    {
        reader.GetValue(out _name);
        reader.GetValue(out _type);
        reader.GetValue(out _text);
        byte enc_byte;
        reader.GetValue(out enc_byte);
        _encrypted = enc_byte > 0;
    }

    public void Write(MessageWriter writer)
    {
        writer.Write(typeof(string), name);
        writer.Write(typeof(string), type);
        writer.Write(typeof(string), text);
        byte encb = (byte)(encrypted ? 1 : 0);
        writer.Write(typeof(byte), encb);
    }

    public string GetKey()
    {
        return VxSchema.GetKey(name, type, encrypted);
    }

    public int CompareTo(object obj)
    {
        if (!(obj is VxSchemaElement))
            throw new ArgumentException("object is not a VxSchemaElement");

        VxSchemaElement other = (VxSchemaElement)obj;
        return GetKey().CompareTo(other.GetKey());
    }

    public static string GetSignature()
    {
        return "sssy";
    }
}

// Tables will be special later; for now, they act pretty much identically to
// any other VxSchemaElement.
internal class VxSchemaTable : VxSchemaElement
{
    public VxSchemaTable(string newname, string newtext) :
        base(newname, "Table", newtext, false)
    {
    }
}

// The schema elements for a set of database elements
internal class VxSchema : Dictionary<string, VxSchemaElement>
{
    public VxSchema()
    {
    }

    public VxSchema(MessageReader reader)
    {
        int size;
        reader.GetValue(out size);
        int endpos = reader.Position + size;
        while (reader.Position < endpos)
        {
            reader.ReadPad(8);
            VxSchemaElement elem = new VxSchemaElement(reader);
            Add(elem.GetKey(), elem);
        }
    }

    private void _WriteSchema(MessageWriter writer)
    {
        foreach (KeyValuePair<string,VxSchemaElement> p in this)
        {
            writer.WritePad(8);
            p.Value.Write(writer);
        }
    }

    public void WriteSchema(MessageWriter writer)
    {
        writer.WriteDelegatePrependSize(delegate(MessageWriter w)
            {
                _WriteSchema(w);
            }, 8);
    }

    public void Add(string name, string type, string text, bool encrypted)
    {
        string key = GetKey(name, type, encrypted);
        if (this.ContainsKey(key))
            this[key].text += text;
        else
        {
            if (type == "Table")
                this.Add(key, new VxSchemaTable(name, text));
            else
                this.Add(key, new VxSchemaElement(name, type, text, encrypted));
        }
    }

    public static string GetKey(string name, string type, bool encrypted)
    {
        string enc_str = encrypted ? "-Encrypted" : "";
        return String.Format("{0}{1}/{2}", type, enc_str, name);
    }

    public static string GetSignature()
    {
        return String.Format("a({0})", VxSchemaElement.GetSignature());
    }

}
