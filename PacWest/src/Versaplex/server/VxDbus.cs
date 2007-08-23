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

    public static void MessageDump(Message msg)
    {
        Header hdr = msg.Header;

        Console.WriteLine("Message dump:");
        Console.WriteLine(" endianness {0}", hdr.Endianness);
        Console.WriteLine(" type {0}", hdr.MessageType);
        Console.WriteLine(" flags {0}", hdr.Flags);
        Console.WriteLine(" version {0}", hdr.MajorVersion);
        Console.WriteLine(" body length {0}", hdr.Length);
        Console.WriteLine(" serial {0}", hdr.Serial);
        Console.WriteLine(" Fields");

        foreach (KeyValuePair<FieldCode,object> kvp in hdr.Fields) {
            Console.WriteLine("  - {0}: {1}", kvp.Key, kvp.Value);
        }

        int hdrlen = 0;
        if (msg.HeaderData != null) {
            Console.WriteLine("Header data:");
            HexDump(msg.HeaderData);
            hdrlen = msg.HeaderData.Length;
        } else {
            Console.WriteLine("No header data encoded");
        }

        if (msg.Body != null) {
            Console.WriteLine("Body data:");
            HexDump(msg.Body, hdrlen);
        } else {
            Console.WriteLine("No header data encoded");
        }
    }

    public static void HexDump(byte[] data)
    {
        HexDump(data, 0);
    }

    public static void HexDump(byte[] data, int startoffset)
    {
        // This is overly complicated so that the body and header can be printed
        // separately yet still show the proper alignment
 
        int rowoffset = startoffset & (~0xf);
        int coloffset = startoffset & 0xf;

        int cnt = rowoffset;
        for (int i=0; i < data.Length; cnt += 16) {
            Console.Write("{0} ", cnt.ToString("x4"));

            int co=0;
            if (coloffset > 0 && i == 0) {
                for (int j=0; j < coloffset; j++)
                    Console.Write("   ");

                co=coloffset;
            }

            // Print out the hex digits
            for (int j=0; j < 8-co && i+j < data.Length; j++)
                Console.Write("{0} ", data[i+j].ToString("x2"));

            Console.Write(" ");

            for (int j=8-co; j < 16-co && i+j < data.Length; j++)
                Console.Write("{0} ", data[i+j].ToString("x2"));

            // extra space if incomplete line
            if (i + 16-co > data.Length) {
                for (int j = data.Length - i; j < 16-co; j++)
                    Console.Write("   ");
            }

            if (co > 0) {
                for (int j=0; j < co; j++)
                    Console.Write(" ");
            }

            for (int j=0; j < 16-co && i+j < data.Length; j++) {
                if (31 < data[i+j] && data[i+j] < 127) {
                    Console.Write((char)data[i+j]);
                } else {
                    Console.Write('.');
                }
            }

            Console.WriteLine("");

            i += 16-co;
        }
    }
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
                    "com.versabanq.versaplex.exception",
                    e.ToString(), call);
        }
    }
}

}
