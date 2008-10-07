using System;
using Wv;
using Wv.Extensions;

// Utility methods for dealing with DBus
class VxDbusUtils
{
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
    public static Message CreateMethodCall(Connection bus, 
        string member, string signature)
    {
        return CreateMethodCall(bus, "vx.versaplexd", "/db", 
				"vx.db", member, signature);
    }

    public static Message CreateMethodCall(Connection bus, string destination, 
            string path, string iface, string member, string signature)
    {
	var msg = new MethodCall(destination, path, iface, member);
	msg.signature = signature;
        return msg;
    }

}
