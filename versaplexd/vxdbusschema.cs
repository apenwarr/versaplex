using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using NDesk.DBus;

// An ISchemaBackend that uses a DBus connection to Versaplex as a backing
// store.
internal class VxDbusSchema : ISchemaBackend
{
    Bus bus;

    public VxDbusSchema()
    {
        if (Address.Session == null)
            throw new Exception ("DBUS_SESSION_BUS_ADDRESS not set");
        AddressEntry aent = AddressEntry.Parse(Address.Session);
        DodgyTransport trans = new DodgyTransport();
        trans.Open(aent);
        bus = new Bus(trans);
    }

    // If you've already got a Bus you'd like to use.
    public VxDbusSchema(Bus _bus)
    {
        bus = _bus;
    }

    // 
    // The ISchema interface
    //

    // Note: this implementation ignores the sums.
    public VxSchemaErrors Put(VxSchema schema, VxSchemaChecksums sums, 
        VxPutOpts opts)
    {
        Message call = CreateMethodCall("PutSchema", 
            String.Format("{0}i", VxSchema.GetDbusSignature()));

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        schema.WriteSchema(writer);
        writer.Write(typeof(int), (int)opts);
        call.Body = writer.ToArray();

        Message reply = call.Connection.SendWithReplyAndBlock(call);

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        case MessageType.Error:
        {
            object replysig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had no signature.");

            if (replysig == null)
                throw new Exception("D-Bus reply had null signature");

            // Some unexpected error
            if (replysig.ToString() == "s")
                throw VxDbusUtils.GetDbusException(reply);

            if (replysig.ToString() != "a(ssi)")
                throw new Exception("D-Bus reply had invalid signature: " +
                    replysig);

            MessageReader reader = new MessageReader(reply);
            VxSchemaErrors errors = new VxSchemaErrors(reader);

            return errors;
        }
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    // Utility API so you can say Get("foo").
    public VxSchema Get(params string[] keys)
    {
        Message call = CreateMethodCall("GetSchema", "as");

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        if (keys == null)
            keys = new string[0];

        writer.Write(typeof(string[]), (Array)keys);
        call.Body = writer.ToArray();

        Message reply = call.Connection.SendWithReplyAndBlock(call);

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had no signature");

            if (replysig == null || replysig.ToString() != "a(sssy)")
                throw new Exception("D-Bus reply had invalid signature: " +
                    replysig);

            MessageReader reader = new MessageReader(reply);
            VxSchema schema = new VxSchema(reader);
            return schema;
        }
        case MessageType.Error:
            throw VxDbusUtils.GetDbusException(reply);
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    public VxSchema Get(IEnumerable<string> keys)
    {
        if (keys == null)
            keys = new string[0];
        return Get(keys.ToArray());
    }

    public VxSchemaChecksums GetChecksums()
    {
        Message call = CreateMethodCall("GetSchemaChecksums", "");

        Message reply = call.Connection.SendWithReplyAndBlock(call);

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had no signature");

            if (replysig == null || replysig.ToString() != "a(sat)")
                throw new Exception("D-Bus reply had invalid signature: " +
                    replysig);

            MessageReader reader = new MessageReader(reply);
            VxSchemaChecksums sums = new VxSchemaChecksums(reader);
            return sums;
        }
        case MessageType.Error:
            throw VxDbusUtils.GetDbusException(reply);
        default:
            throw new Exception("D-Bus response was not a method return or " +
                    "error");
        }
    }

    // A method exported over DBus but not exposed in ISchemaBackend
    public void DropSchema(string type, string name)
    {
        Message call = CreateMethodCall("DropSchema", "ss");

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        writer.Write(typeof(string), type);
        writer.Write(typeof(string), name);
        call.Body = writer.ToArray();

        Message reply = call.Connection.SendWithReplyAndBlock(call);

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had unexpected signature" + 
                    replysig);

            return;
        }
        case MessageType.Error:
            throw VxDbusUtils.GetDbusException(reply);
        default:
            throw new Exception("D-Bus response was not a method return or "
                    + "error");
        }
    }
    
    //
    // Non-ISchemaBackend methods
    //

    // A method exported over DBus but not exposed in ISchemaBackend
    public string GetSchemaData(string tablename)
    {
        Message call = CreateMethodCall("GetSchemaData", "s");

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        writer.Write(typeof(string), tablename);
        call.Body = writer.ToArray();

        Message reply = call.Connection.SendWithReplyAndBlock(call);

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had no signature");

            if (replysig == null || replysig.ToString() != "s")
                throw new Exception("D-Bus reply had invalid signature: " +
                    replysig);

            MessageReader reader = new MessageReader(reply);
            return reader.ReadString();
        }
        case MessageType.Error:
            throw VxDbusUtils.GetDbusException(reply);
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    // A method exported over DBus but not exposed in ISchemaBackend
    public void PutSchemaData(string tablename, string text)
    {
        Message call = CreateMethodCall("PutSchemaData", "ss");

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        writer.Write(tablename);
        writer.Write(text);
        call.Body = writer.ToArray();

        Message reply = call.Connection.SendWithReplyAndBlock(call);

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had unexpected signature" + 
                    replysig);

            return;
        }
        case MessageType.Error:
            throw VxDbusUtils.GetDbusException(reply);
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    // Use our Bus object to create a method call.
    public Message CreateMethodCall(string member, string signature)
    {
        return VxDbusUtils.CreateMethodCall(bus, member, signature);
    }
}


