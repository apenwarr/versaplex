using System;
using System.Text;
using System.Collections.Generic;
using Wv;
using Wv.Extensions;

internal class VxSchemaError
{
    // The key of the element that had the error
    public string key;
    // The error message
    public string msg;
    // The SQL error number, or -1 if not applicable.
    public int errnum;
    public WvLog.L level;

    // Default to a level of Error; 
    public VxSchemaError(string newkey, string newmsg, int newerrnum)
    {
        key = newkey;
        msg = newmsg;
        errnum = newerrnum;
        level = WvLog.L.Error;
    }

    public VxSchemaError(string newkey, string newmsg, int newerrnum, 
        WvLog.L newlevel)
    {
        key = newkey;
        msg = newmsg;
        errnum = newerrnum;
        level = newlevel;
    }

    public VxSchemaError(VxSchemaError other)
    {
        key = other.key;
        msg = other.msg;
        errnum = other.errnum;
        level = other.level;
    }

    public VxSchemaError(IEnumerable<WvAutoCast> _err)
    {
	var err = _err.GetEnumerator();
        key = err.pop();
        msg = err.pop();
        errnum = err.pop();
        int intlevel = err.pop();
        // Note: C# lets you cast an invalid value to an enum without an
        // exception, we have to check this ourselves.  Default to 
        // Critical (i.e. 0) if someone sends us something unexpected.
        if (Enum.IsDefined(typeof(WvLog.L), intlevel))
            level = (WvLog.L)intlevel;
        else
            level = WvLog.L.Critical;
    }

    public void WriteError(MessageWriter writer)
    {
        writer.Write(key);
        writer.Write(msg);
        writer.Write(errnum);
        writer.Write((int)level);
    }

    public override string ToString()
    {
        return String.Format("{0} {1}: {2} ({3})", key, level, msg, errnum);
    }

    public static string GetDbusSignature()
    {
        return "ssii";
    }
}

internal class VxSchemaErrors : Dictionary<string, List<VxSchemaError>>
{
    public VxSchemaErrors()
    {
    }

    public VxSchemaErrors(IEnumerable<WvAutoCast> errs)
    {
	foreach (var err in errs)
	{
            VxSchemaError e = new VxSchemaError(err);
            this.Add(e.key, e);
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

