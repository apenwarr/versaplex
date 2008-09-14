using System;
using NDesk.DBus;

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
