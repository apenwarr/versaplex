using System;
using Wv;
using Wv.Extensions;

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
        if (reply.err.e())
            return new Exception("Could not find error name in DBus message.");

	if (!reply.signature.ne() || reply.signature != "s")
            return new DbusError(reply.err);

        string errmsg = reply.iter().pop();
        return new DbusError(wv.fmt("{0}: {1}", reply.err, errmsg));
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
        msg.type = MessageType.MethodCall;
        msg.flags = HeaderFlag.None;
        msg.path = path;
        msg.method = member;
	msg.dest = destination;
        msg.ifc = iface;
	msg.signature = signature;
        return msg;
    }

}
