using System;
using Wv;

// Utility methods for dealing with DBus
class VxDbusUtils
{
    static string DbusConnName = "vx.versaplexd";
    static string DbusInterface = "vx.db";
    static readonly string DbusObjPath = "/db";

    static VxDbusUtils()
    {
    }

    // Fishes an error name and error message out of a DBus message and
    // returns them in an exception.
    public static Exception GetDbusException(Message reply)
    {
        object errname;
        if (!reply.Header.Fields.TryGetValue(FieldCode.ErrorName, out errname))
            return new Exception("Could not find error name in DBus message.");

        object errsig;
        if (!reply.Header.Fields.TryGetValue(FieldCode.Signature, out errsig) || 
                errsig.ToString() != "s")
            return new DbusError(errname.ToString());

        string errmsg = reply.iter().pop();
        return new DbusError(errname.ToString() + ": " + errmsg.ToString());
    }

    // Create a method call using the default connection, object path, and
    // interface
    public static Message CreateMethodCall(Bus bus, 
        string member, string signature)
    {
        return CreateMethodCall(bus, DbusConnName, DbusObjPath, 
            DbusInterface, member, signature);
    }

    public static Message CreateMethodCall(Bus bus, string destination, 
            string path, string iface, string member, string signature)
    {
        Message msg = new Message();
        msg.Header.MessageType = MessageType.MethodCall;
        msg.Header.Flags = HeaderFlag.None;
        msg.Header.Fields[FieldCode.Path] = path;
        msg.Header.Fields[FieldCode.Member] = member;

        if (destination != null && destination != "")
            msg.Header.Fields[FieldCode.Destination] = destination;
        
        if (iface != null && iface != "")
            msg.Header.Fields[FieldCode.Interface] = iface;

        if (signature != null && signature != "")
            msg.Header.Fields[FieldCode.Signature] = new Signature(signature);

        return msg;
    }

}
