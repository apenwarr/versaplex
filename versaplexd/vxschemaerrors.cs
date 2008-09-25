using System;
using System.Text;
using System.Collections.Generic;
using Wv;

internal class VxSchemaError
{
    // The key of the element that had the error
    public string key;
    // The error message
    public string msg;
    // The SQL error number, or -1 if not applicable.
    public int errnum;

    public VxSchemaError(string newkey, string newmsg, int newerrnum)
    {
        key = newkey;
        msg = newmsg;
        errnum = newerrnum;
    }

    public VxSchemaError(VxSchemaError other)
    {
        key = other.key;
        msg = other.msg;
        errnum = other.errnum;
    }

    public VxSchemaError(MessageReader reader)
    {
        key = reader.ReadString();
        msg = reader.ReadString();
        errnum = reader.ReadInt32();
    }

    public void WriteError(MessageWriter writer)
    {
        writer.Write(key);
        writer.Write(msg);
        writer.Write(errnum);
    }

    public override string ToString()
    {
        return String.Format("{0}: {1} ({2})", key, msg, errnum);
    }

    public static string GetDbusSignature()
    {
        return "ssi";
    }
}

internal class VxSchemaErrors : Dictionary<string, List<VxSchemaError>>
{
    public VxSchemaErrors()
    {
    }

    public VxSchemaErrors(MessageReader reader)
    {
        int size = reader.ReadInt32();

        int endpos = reader.Position + size;
        while (reader.Position < endpos)
        {
            reader.ReadPad(8);
            VxSchemaError err = new VxSchemaError(reader);

            this.Add(err.key, err);
        }
    }

    public void Add(string key, VxSchemaError val)
    {
        if (!this.ContainsKey(key))
        {
            var list = new List<VxSchemaError>();
            list.Add(val);
            this.Add(key, list);
        }
        else
            this[key].Add(val);
    }

    public void Add(VxSchemaErrors other)
    {
        foreach (var kvp in other)
        {
            var list = new List<VxSchemaError>();
            foreach (var elem in kvp.Value)
                list.Add(new VxSchemaError(elem));
            this.Add(kvp.Key, list);
        }
    }

    private void _WriteErrors(MessageWriter writer)
    {
        foreach (var kvp in this)
            foreach (VxSchemaError err in kvp.Value)
            {
                writer.WritePad(8);
                err.WriteError(writer);
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

    public override string ToString()
    {
        StringBuilder sb = new StringBuilder();
        foreach (var kvp in this)
            foreach (var err in kvp.Value)
                sb.Append(err.ToString() + "\n");
        return sb.ToString();
    }
}

