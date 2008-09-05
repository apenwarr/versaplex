using System;
using System.Collections.Generic;
using NDesk.DBus;
using Wv;

public static class VxDbus {
    static WvLog log = new WvLog("VxDbus", WvLog.L.Debug1);
    static WvLog smalldump = log.split(WvLog.L.Debug4);
    static WvLog fulldump = log.split(WvLog.L.Debug5);

    public static Message CreateError(string type, string msg, Message cause)
    {
        Message error = new Message();
        error.Header.MessageType = MessageType.Error;
        error.Header.Flags =
            HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;
        error.Header.Fields[FieldCode.ErrorName] = type;
        error.Header.Fields[FieldCode.ReplySerial] = cause.Header.Serial;

        object sender;
        if (cause.Header.Fields.TryGetValue(FieldCode.Sender, out sender))
            error.Header.Fields[FieldCode.Destination] = sender;

        if (msg != null) {
            error.Signature = new Signature("s");
            MessageWriter writer =
                new MessageWriter(Connection.NativeEndianness);
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
        reply.Header.MessageType = MessageType.MethodReturn;
        reply.Header.Flags =
            HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;
        reply.Header.Fields[FieldCode.ReplySerial] = call.Header.Serial;
        
        object sender;
        if (call.Header.Fields.TryGetValue(FieldCode.Sender, out sender))
            reply.Header.Fields[FieldCode.Destination] = sender;

        if (signature != null && signature != "") {
            reply.Signature = new Signature(signature);
            reply.Body = body.ToArray();
        }

        return reply;
    }

    public static Message CreateSignal(object destination, string signalname,
					string signature, MessageWriter body)
    {
	// Idea shamelessly stolen from CreateReply method above
	Message signal = new Message();
	signal.Header.MessageType = MessageType.Signal;
	signal.Header.Flags =
	    HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;
	// The ObjectPath is required by signals, and is the "source of the
	// signal."  OK then; seems useless to me.
	signal.Header.Fields[FieldCode.Path] = new ObjectPath("/db");
	signal.Header.Fields[FieldCode.Interface] = "vx.db";
	signal.Header.Fields[FieldCode.Member] = signalname;

	if (destination != null)
	    signal.Header.Fields[FieldCode.Destination] = destination;

	if (signature != null && signature != "")
	{
	    signal.Signature = new Signature(signature);
	    signal.Body = body.ToArray();
	}

	return signal;
    }

    public static void MessageDump(string prefix, Message msg)
    {
        Header hdr = msg.Header;
	
	if (hdr.Fields.ContainsKey(FieldCode.ReplySerial))
	    log.print("{0} REPLY#{1}\n",
			prefix,
			hdr.Fields[FieldCode.ReplySerial]);
	else if (hdr.MessageType != MessageType.Signal)
	    log.print("{0} #{1} {2}.{3}\n",
			prefix,
			hdr.Serial,
			hdr.Fields[FieldCode.Interface],
			hdr.Fields[FieldCode.Member]);
	else
	    log.print("{0}\n", hdr.Fields[FieldCode.Interface]);

        smalldump.print("Message dump:\n");
        smalldump.print(" endianness={0} ", hdr.Endianness);
        smalldump.print(" t={0} ", hdr.MessageType);
        smalldump.print(" ver={0} ", hdr.MajorVersion);
        smalldump.print(" blen={0} ", hdr.Length);
        smalldump.print(" ser={0}\n", hdr.Serial);
        smalldump.print(" flags={0}\n", hdr.Flags);
	
        smalldump.print(" Fields\n");
        foreach (KeyValuePair<FieldCode,object> kvp in hdr.Fields) {
            smalldump.print("  - {0}: {1}\n", kvp.Key, kvp.Value);
        }

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
	    fulldump.print(wv.hexdump(msg.Body, hdrlen, msg.Body.Length));
	} else {
            smalldump.print("No body data encoded\n");
        }
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

    public bool RouteMessage(Message call, out Message reply)
    {
        if (call.Header.MessageType != MessageType.MethodCall)
            throw new ArgumentException("Not a method call message");

        reply = null;

        // FIXME: Dbus spec says that interface should be optional so it
        // should search all of the interfaces for a matching method...
        object iface;
        if (!call.Header.Fields.TryGetValue(FieldCode.Interface, out iface))
            return false; // No interface; ignore it

        log.print("Router interface {0}\n", iface);

        VxInterfaceRouter ir;
        if (!interfaces.TryGetValue((string)iface, out ir))
            return false; // Interface not found

        log.print("Passing to interface router\n");

        return ir.RouteMessage(call, out reply);
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
        void MethodCallProcessor(Message call, out Message reply);

    protected IDictionary<string,MethodCallProcessor> methods
        = new Dictionary<string,MethodCallProcessor>();

    public bool RouteMessage(Message call, out Message reply)
    {
        if (call.Header.MessageType != MessageType.MethodCall)
            throw new ArgumentException("Not a method call message");

        reply = null;

        object method;
        if (!call.Header.Fields.TryGetValue(FieldCode.Member, out method))
            return false; // No method 

        MethodCallProcessor processor;
        if (!methods.TryGetValue((string)method, out processor)) {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    String.Format(
                        "Method name {0} not found on interface {1}",
                        method, Interface), call);

            return true;
        }

        ExecuteCall(processor, call, out reply);

        return true;
    }

    protected virtual void ExecuteCall(MethodCallProcessor processor,
            Message call, out Message reply)
    {
        try {
            processor(call, out reply);
        } catch (Exception e) {
            reply = VxDbus.CreateError(
                    "vx.db.exception",
                    e.ToString(), call);
        }
    }
}
