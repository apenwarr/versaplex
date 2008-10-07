using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using Wv;
using Wv.Extensions;

// An ISchemaBackend that uses a DBus connection to Versaplex as a backing
// store.
[WvMoniker]
internal class VxDbusSchema : ISchemaBackend
{
    Connection bus;

    public static void wvmoniker_register()
    {
	WvMoniker<ISchemaBackend>.register("vx",
		  (string m, object o) => new VxDbusSchema(m));
    }
	
    public VxDbusSchema()
	: this((string)null)
    {
    }

    public VxDbusSchema(string bus_moniker)
    {
	if (bus_moniker.e())
	    bus = Connection.session_bus;
	else
	    bus = new Connection(bus_moniker);
    }

    // If you've already got a Bus you'd like to use.
    public VxDbusSchema(Connection _bus)
    {
        bus = _bus;
    }
    
    static Message methodcall(string method, string signature)
    {
        return new MethodCall("vx.versaplexd", "/db", 
			      "vx.db", method, signature);
    }

    // 
    // The ISchema interface
    //

    // Note: this implementation ignores the sums.
    public VxSchemaErrors Put(VxSchema schema, VxSchemaChecksums sums, 
        VxPutOpts opts)
    {
        Message call = methodcall("PutSchema", 
            String.Format("{0}i", VxSchema.GetDbusSignature()));

        MessageWriter writer = new MessageWriter();

        schema.WriteSchema(writer);
        writer.Write((int)opts);
        call.Body = writer.ToArray();

        Message reply = bus.send_and_wait(call);
	if (reply.signature == VxSchemaErrors.GetDbusSignature())
	    return new VxSchemaErrors(reply.iter().pop());
	else
	    reply.check(VxSchemaErrors.GetDbusSignature());
	return null;
    }

    // Utility API so you can say Get("foo").
    public VxSchema Get(params string[] keys)
    {
        Message call = methodcall("GetSchema", "as");

        MessageWriter writer = new MessageWriter();

        if (keys == null)
            keys = new string[0];

        writer.WriteArray(4, keys, (w2, k) => {
	    w2.Write(k);
	});
        call.Body = writer.ToArray();

        Message reply = bus.send_and_wait(call);
	reply.check("a(sssy)");
	VxSchema schema = new VxSchema(reply.iter().pop());
	return schema;
    }

    public VxSchema Get(IEnumerable<string> keys)
    {
        if (keys == null)
            keys = new string[0];
        return Get(keys.ToArray());
    }

    public VxSchemaChecksums GetChecksums()
    {
        Message call = methodcall("GetSchemaChecksums", "");

        Message reply = bus.send_and_wait(call);
	reply.check("a(sat)");
	VxSchemaChecksums sums = new VxSchemaChecksums(reply);
	return sums;
    }

    public VxSchemaErrors DropSchema(IEnumerable<string> keys)
    {
        if (keys == null)
            keys = new string[0];
        return DropSchema(keys.ToArray());
    }

    // A method exported over DBus but not exposed in ISchemaBackend
    public VxSchemaErrors DropSchema(params string[] keys)
    {
        Message call = methodcall("DropSchema", "as");

        MessageWriter writer = new MessageWriter();

	writer.WriteArray(4, keys, (w2, k) => {
	    w2.Write(k);
	});
        call.Body = writer.ToArray();

        Message reply = bus.send_and_wait(call);
	if (reply.signature == VxSchemaErrors.GetDbusSignature())
	    return new VxSchemaErrors(reply.iter().pop());
	else
	    reply.check(VxSchemaErrors.GetDbusSignature());
	return null;
    }
    
    public string GetSchemaData(string tablename, int seqnum, string where)
    {
        Message call = methodcall("GetSchemaData", "ss");

        MessageWriter writer = new MessageWriter();

        if (where == null)
            where = "";

        writer.Write(tablename);
        writer.Write(where);
        call.Body = writer.ToArray();

        Message reply = bus.send_and_wait(call);
	reply.check("s");
	return reply.iter().pop();
    }

    public void PutSchemaData(string tablename, string text, int seqnum)
    {
        Message call = methodcall("PutSchemaData", "ss");

        MessageWriter writer = new MessageWriter();

        writer.Write(tablename);
        writer.Write(text);
        call.Body = writer.ToArray();

        Message reply = bus.send_and_wait(call);
	reply.check("");
    }
}


