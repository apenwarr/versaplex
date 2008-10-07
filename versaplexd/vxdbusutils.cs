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
}
