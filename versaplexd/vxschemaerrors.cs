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
        reader.GetValue(out key);
        reader.GetValue(out msg);
        reader.GetValue(out errnum);
    }

    public void WriteError(MessageWriter writer)
    {
        writer.Write(key);
        writer.Write(msg);
        writer.Write(errnum);
    }

    public static string GetDbusSignature()
    {
        return "ssi";
    }
}
