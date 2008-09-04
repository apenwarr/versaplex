using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Security.Cryptography;
using NDesk.DBus;
using Wv;
using Wv.Extensions;

[Flags]
public enum VxCopyOpts : int
{
    None = 0,
    DryRun = 0x1,
    ShowProgress = 0x2, 
    ShowDiff = 0x4, 
    Destructive = 0x8,

    Verbose = ShowProgress | ShowDiff,
}

internal class VxSchemaErrors : Dictionary<string, VxSchemaError>
{
    public VxSchemaErrors()
    {
    }

    public VxSchemaErrors(MessageReader reader)
    {
        int size;
        reader.GetValue(out size);

        int endpos = reader.Position + size;
        while (reader.Position < endpos)
        {
            reader.ReadPad(8);
            VxSchemaError err = new VxSchemaError(reader);

            this.Add(err.key, err);
        }
    }

    private void _WriteErrors(MessageWriter writer)
    {
        foreach (KeyValuePair<string,VxSchemaError> p in this)
        {
            writer.WritePad(8);
            p.Value.WriteError(writer);
        }
    }

    // Static so we can properly write an empty array for a null object.
    public static void WriteErrors(MessageWriter writer, VxSchemaErrors errs)
    {
        writer.WriteDelegatePrependSize(delegate(MessageWriter w)
            {
                if (errs != null)
                    errs._WriteErrors(w);
            }, 8);
    }

    public static string GetDbusSignature()
    {
        return String.Format("a({0})", VxSchemaError.GetDbusSignature());
    }
}


internal class VxSchemaElement : IComparable
{
    string _type;
    public string type {
        get { return _type; }
    }

    string _name;
    public string name {
        get { return _name; }
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

    public string key {
        get { return type + "/" + name; }
    }

    public VxSchemaElement(string newtype, string newname, string newtext,
            bool newencrypted)
    {
        _type = newtype;
        _name = newname;
        _encrypted = newencrypted;
        _text = newtext;
    }

    public VxSchemaElement(VxSchemaElement copy)
    {
        _type = copy.type;
        _name = copy.name;
        _text = copy.text;
        _encrypted = copy.encrypted;
    }

    public VxSchemaElement(MessageReader reader)
    {
        reader.GetValue(out _type);
        reader.GetValue(out _name);
        reader.GetValue(out _text);
        byte enc_byte;
        reader.GetValue(out enc_byte);
        _encrypted = enc_byte > 0;
    }

    public void Write(MessageWriter writer)
    {
        writer.Write(typeof(string), type);
        writer.Write(typeof(string), name);
        writer.Write(typeof(string), text);
        byte encb = (byte)(encrypted ? 1 : 0);
        writer.Write(typeof(byte), encb);
    }

    public string GetKey()
    {
        return VxSchema.GetKey(type, name, encrypted);
    }

    // Returns the element's text, along with a header line containing the MD5
    // sum of the text, and the provided database checksums.  This format is
    // suitable for serializing to disk.
    public string ToStringWithHeader(VxSchemaChecksum sum)
    {
        byte[] md5 = MD5.Create().ComputeHash(text.ToUTF8());

        return String.Format("!!SCHEMAMATIC {0} {1} \r\n{2}",
            md5.ToHex().ToLower(), sum.GetSumString(), text);
    }

    public int CompareTo(object obj)
    {
        if (!(obj is VxSchemaElement))
            throw new ArgumentException("object is not a VxSchemaElement");

        VxSchemaElement other = (VxSchemaElement)obj;
        return GetKey().CompareTo(other.GetKey());
    }

    public static string GetDbusSignature()
    {
        return "sssy";
    }
}

// Tables will be special later; for now, they act pretty much identically to
// any other VxSchemaElement.
internal class VxSchemaTable : VxSchemaElement
{
    public VxSchemaTable(string newname, string newtext) :
        base("Table", newname, newtext, false)
    {
    }
}

// The schema elements for a set of database elements
internal class VxSchema : Dictionary<string, VxSchemaElement>
{
    public VxSchema()
    {
    }

    // Convenience method for making single-element schemas
    public VxSchema(VxSchemaElement elem)
    {
        Add(elem.key, elem);
    }

    public VxSchema(VxSchema copy)
    {
        foreach (KeyValuePair<string,VxSchemaElement> p in copy)
            this.Add(p.Key, new VxSchemaElement(p.Value));
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

    // Returns only the elements of the schema that are affected by the diff.
    // If an element is scheduled to be removed, clear its text field.
    // Produces a VxSchema that, if sent to a schema backend's Put, will
    // update the schema as indicated by the diff.
    public VxSchema GetDiffElements(VxSchemaDiff diff)
    {
        VxSchema diffschema = new VxSchema();
        foreach (KeyValuePair<string,VxDiffType> p in diff)
        {
            if (!this.ContainsKey(p.Key))
                throw new ArgumentException("The provided diff does not " + 
                    "match the schema: extra element '" + p.Value + "'");
            if (p.Value == VxDiffType.Remove)
            {
                VxSchemaElement elem = new VxSchemaElement(this[p.Key]);
                elem.text = "";
                diffschema[p.Key] = elem;
            }
            else if (p.Value == VxDiffType.Add || p.Value == VxDiffType.Change)
            {
                diffschema[p.Key] = new VxSchemaElement(this[p.Key]);
            }
        }
        return diffschema;
    }

    public void Add(string type, string name, string text, bool encrypted)
    {
        string key = GetKey(type, name, encrypted);
        if (this.ContainsKey(key))
            this[key].text += text;
        else
        {
            if (type == "Table")
                this.Add(key, new VxSchemaTable(name, text));
            else
                this.Add(key, new VxSchemaElement(type, name, text, encrypted));
        }
    }

    public static string GetKey(string type, string name, bool encrypted)
    {
        string enc_str = encrypted ? "-Encrypted" : "";
        return String.Format("{0}{1}/{2}", type, enc_str, name);
    }

    private static char[] slash = new char[] {'/'};
    public static void ParseKey(string key, out string type, out string name)
    {
        string[] parts = key.Split(slash, 2);
        if (parts.Length != 2)
        {
            type = name = null;
            return;
        }
        type = parts[0];
        name = parts[1];
        return;
    }

    public static string GetDbusSignature()
    {
        return String.Format("a({0})", VxSchemaElement.GetDbusSignature());
    }

    // Make dest look like source.  Only copies the bits that need updating.
    // Note: this is a slightly funny spot to put this method; it really
    // belongs in ISchemaBackend, but you can't put methods in interfaces.
    public static VxSchemaErrors CopySchema(ISchemaBackend source, 
        ISchemaBackend dest)
    {
        return VxSchema.CopySchema(source, dest, VxCopyOpts.None);
    }

    public static VxSchemaErrors CopySchema(ISchemaBackend source, 
        ISchemaBackend dest, VxCopyOpts opts)
    {
        WvLog log = new WvLog("CopySchema");

        if ((opts & VxCopyOpts.ShowProgress) == 0)
            log = new WvLog("CopySchema", WvLog.L.Debug5);

        bool show_diff = (opts & VxCopyOpts.ShowDiff) != 0;
        bool dry_run = (opts & VxCopyOpts.DryRun) != 0;
        bool destructive = (opts & VxCopyOpts.Destructive) != 0;

        log.print("Retrieving schema checksums from source.\n");
        VxSchemaChecksums srcsums = source.GetChecksums();

        log.print("Retrieving schema checksums from dest.\n");
        VxSchemaChecksums destsums = dest.GetChecksums();

        if (srcsums.Count == 0 && destsums.Count != 0)
        {
            log.print("Source index is empty! " + 
                "Refusing to delete entire database.\n");
            return new VxSchemaErrors();
        }

        List<string> names = new List<string>();

        log.print("Computing diff.\n");
        VxSchemaDiff diff = new VxSchemaDiff(destsums, srcsums);

        if (diff.Count == 0)
        {
            log.print("No changes.\n");
            return new VxSchemaErrors();
        }

        if (show_diff)
        {
            log.print("Changes to apply:\n");
            log.print(WvLog.L.Info, diff.ToString());
        }

        log.print("Parsing diff.\n");
        List<string> to_drop = new List<string>();
        foreach (KeyValuePair<string,VxDiffType> p in diff)
        {
            switch (p.Value)
            {
            case VxDiffType.Remove:
                to_drop.Add(p.Key);
                break;
            case VxDiffType.Add:
            case VxDiffType.Change:
                names.Add(p.Key);
                break;
            }
        }

        log.print("Retrieving updated schema.\n");
        VxSchema to_put = source.Get(names);

        if (dry_run)
            return new VxSchemaErrors();

        VxSchemaErrors drop_errs = new VxSchemaErrors();
        VxSchemaErrors put_errs = new VxSchemaErrors();

        // We know at least one of to_drop and to_put must have something in
        // it, otherwise the diff would have been empty.

        if (to_drop.Count > 0)
        {
            log.print("Dropping deleted elements.\n");
            drop_errs = dest.DropSchema(to_drop);
        }

        VxPutOpts putopts = VxPutOpts.None;
        if (destructive)
            putopts |= VxPutOpts.Destructive;
        if (names.Count > 0)
        {
            log.print("Updating and adding elements.\n");
            put_errs = dest.Put(to_put, srcsums, putopts);
        }

        // Combine the two sets of errors.
        foreach (var kvp in drop_errs)
            put_errs.Add(kvp.Key, kvp.Value);

        return put_errs;
    }
}
