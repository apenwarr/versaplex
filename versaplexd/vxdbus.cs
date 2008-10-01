using System;
using System.Collections.Generic;
using Wv;
using Wv.Extensions;

public static class VxDbus {
    static WvLog log = new WvLog("VxDbus", WvLog.L.Debug1);
    static WvLog smalldump = log.split(WvLog.L.Debug4);
    static WvLog fulldump = log.split(WvLog.L.Debug5);

    public static Message CreateError(string type, string msg, Message cause)
    {
        Message error = new Message();
        error.type = MessageType.Error;
        error.flags = HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;
        error.err = type;
        error.rserial = cause.serial;
	error.dest = cause.sender;

        if (msg != null) {
            error.signature = "s";
            MessageWriter writer = new MessageWriter();
            writer.Write(msg);
            error.Body = writer.ToArray();
        }

        return error;
    }

    public static Message CreateReply(Message call)
    {
        return CreateReply(call, null, null);
    }

    public static Message CreateReply(Message call, string signature,
            MessageWriter body)
    {
        Message reply = new Message();
        reply.type = MessageType.MethodReturn;
        reply.flags = HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;
        reply.rserial = call.serial;
        
	reply.dest = call.sender;
	reply.signature = signature;
	if (signature.ne())
	    reply.Body = body.ToArray();

        return reply;
    }

    public static Message CreateSignal(string destination, string signalname,
					string signature, MessageWriter body)
    {
	// Idea shamelessly stolen from CreateReply method above
	Message signal = new Message();
	signal.type = MessageType.Signal;
	signal.flags = HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;
	// The ObjectPath is required by signals, and is the "source of the
	// signal."  OK then; seems useless to me.
	signal.path = "/db";
	signal.ifc = "vx.db";
	signal.method = signalname;
	signal.dest = destination;
	signal.signature = signature;
	if (signature.ne())
	    signal.Body = body.ToArray();
	return signal;
    }

    public static void MessageDump(string prefix, Message msg)
    {
	if (msg.rserial.HasValue)
	    log.print("MD {0} REPLY#{1}\n",
			prefix, msg.rserial.Value);
	else if (msg.type != MessageType.Signal)
	    log.print("MD {0} #{1} {2}.{3}\n",
			prefix,
			msg.serial, msg.ifc, msg.method);
	else
	    log.print("MD {0} {1}\n", prefix, msg.ifc);

        smalldump.print("Message dump:\n");
        smalldump.print(" endianness={0} ", msg.endian);
        smalldump.print(" t={0} ", msg.type);
        smalldump.print(" blen={0} ", msg.Body==null ? 0 : msg.Body.Length);
        smalldump.print(" ser={0}\n", msg.serial);
        smalldump.print(" flags={0}\n", msg.flags);
	
#if MSG_HEADERDATA_ISNT_REGENERATING_FROM_SCRATCH
        int hdrlen = 0;
	byte[] header = msg.GetHeaderData();
        if (header != null) {
            smalldump.print("Header data:\n");
	    smalldump.print(wv.hexdump(header));
            hdrlen = header.Length;
        } else {
            smalldump.print("No header data encoded\n");
        }

        if (msg.Body != null) {
            fulldump.print("Body data:\n");
	    fulldump.print(wv.hexdump(msg.Body.sub(hdrlen,
						   msg.Body.Length-hdrlen)));
	} else {
            smalldump.print("No body data encoded\n");
        }
#endif
    }
}

public class VxMethodCallRouter {
    WvLog log = new WvLog("VxMethodCallRouter", WvLog.L.Debug3);
    
    private IDictionary<string,VxInterfaceRouter> interfaces
        = new Dictionary<string,VxInterfaceRouter>();

    public void AddInterface(VxInterfaceRouter ir)
    {
        log.print("Adding interface {0}\n", ir.Interface);
        interfaces.Add(ir.Interface, ir);
    }

    public void RemoveInterface(VxInterfaceRouter ir)
    {
        RemoveInterface(ir.Interface);
    }

    public void RemoveInterface(string iface)
    {
        interfaces.Remove(iface);
    }

    public bool RouteMessage(Connection conn, Message call, out Message reply)
    {
        if (call.type != MessageType.MethodCall)
            throw new ArgumentException("Not a method call message");

        reply = null;

        // FIXME: Dbus spec says that interface should be optional so it
        // should search all of the interfaces for a matching method...
        if (call.ifc.e())
            return false; // No interface; ignore it

        log.print("Router interface {0}\n", call.ifc);

        VxInterfaceRouter ir;
        if (!interfaces.TryGetValue(call.ifc, out ir))
            return false; // Interface not found

        log.print("Passing to interface router\n");

        return ir.RouteMessage(conn, call, out reply);
    }
}

public abstract class VxInterfaceRouter {
    public readonly string Interface;

    protected VxInterfaceRouter(string iface)
    {
        Interface = iface;
    }

    // Return value is the response
    protected delegate
        void MethodCallProcessor(Connection conn, Message call,
				 out Message reply);

    protected IDictionary<string,MethodCallProcessor> methods
        = new Dictionary<string,MethodCallProcessor>();

    public bool RouteMessage(Connection conn, Message call, out Message reply)
    {
        if (call.type != MessageType.MethodCall)
            throw new ArgumentException("Not a method call message");

        reply = null;
	string method = call.method;

        MethodCallProcessor processor;
        if (!methods.TryGetValue(method, out processor)) {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    String.Format(
                        "Method name {0} not found on interface {1}",
                        method, Interface), call);

            return true;
        }

        ExecuteCall(processor, conn, call, out reply);

        return true;
    }

    protected virtual void ExecuteCall(MethodCallProcessor processor,
		       Connection conn, Message call, out Message reply)
    {
        try {
            processor(conn, call, out reply);
        } catch (Exception e) {
            reply = VxDbus.CreateError(
                    "vx.db.exception",
                    e.ToString(), call);
        }
    }
}
