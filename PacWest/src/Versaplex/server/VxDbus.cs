using System;
using System.Collections.Generic;
using NDesk.DBus;

namespace versabanq.Versaplex.Dbus {

public static class VxDbus {
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

        /*
    public static Message CreateReply(Message call)
    {
        Message reply = new Message();
        reply.Header.MessageType = MessageType.Error;
        reply.Header.Flags =
            HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;
        reply.Header.Fields[FieldCode.ErrorName] = type;
        reply.Header.Fields[FieldCode.ReplySerial] = cause.Header.Serial;

        string sender;
        if (cause.Header.Fields.TryGetValue(FieldCode.Sender, out sender))
            reply.Header.Fields[FieldCode.Destination] = sender;

        return error;
    }
        */
}

public class VxMethodCallRouter {
    private IDictionary<string,VxInterfaceRouter> interfaces
        = new Dictionary<string,VxInterfaceRouter>();

    public void AddInterface(VxInterfaceRouter ir)
    {
        Console.WriteLine("Adding interface {0}", ir.Interface);
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

        Console.WriteLine("Router interface {0}", iface);

        VxInterfaceRouter ir;
        if (!interfaces.TryGetValue((string)iface, out ir))
            return false; // Interface not found

        Console.WriteLine("Passing to interface router");

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

    public virtual bool RouteMessage(Message call, out Message reply)
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

        try {
            processor(call, out reply);
        } catch (Exception e) {
            reply = VxDbus.CreateError(
                    "com.versabanq.versaplex.DBus.Error.Exception",
                    e.ToString(), call);
        }

        return true;
    }
}

public struct VxDbusDateTime {
    private long seconds;
    private int microseconds;

    public long Seconds {
        get { return seconds; }
        set { seconds = value; }
    }

    public int Microseconds {
        get { return microseconds; }
        set { microseconds = value; }
    }

    public DateTime DateTime {
        get {
            return new DateTime(seconds*10000000 + microseconds*10);
        }
    }

    public VxDbusDateTime(DateTime dt)
    {
        seconds = (dt.Ticks + EpochOffset.Ticks) / 10000000;
        microseconds = (int)(((dt.Ticks + EpochOffset.Ticks) / 10) % 1000000);
    }

    private static readonly DateTime Epoch = new DateTime(1970, 1, 1);
    private static readonly TimeSpan EpochOffset = DateTime.MinValue - Epoch;
}

public struct VxDbusDbResult {
    private bool nullity;
    private object data;

    public bool Nullity {
        get { return nullity; }
        set { nullity = value; }
    }

    public object Data {
        get { return data; }
        set { data = value; }
    }
}

}
