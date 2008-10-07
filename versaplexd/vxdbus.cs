using System;
using System.Collections.Generic;
using Wv;
using Wv.Extensions;

public static class VxDbus {
    public static Message CreateError(string type, string msg, Message cause)
    {
	var errmsg = cause.err_reply(type);

        if (msg != null) {
            errmsg.signature = "s";
            MessageWriter writer = new MessageWriter();
            writer.Write(msg);
            errmsg.Body = writer.ToArray();
        }

        return errmsg;
    }

    public static Message CreateReply(Message call)
    {
        return CreateReply(call, null, null);
    }

    public static Message CreateReply(Message call, string signature,
            MessageWriter body)
    {
        var reply = call.reply();
	reply.signature = signature;
	if (signature.ne())
	    reply.Body = body.ToArray();
        return reply;
    }

    public static Message CreateSignal(string destination, string signalname,
					string signature, MessageWriter body)
    {
	var sig = new Signal(destination, "/db", "vx.db", signalname);
	sig.signature = signature;
	if (signature.ne())
	    sig.Body = body.ToArray();
	return sig;
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
