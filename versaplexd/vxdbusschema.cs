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
    WvDbus bus;

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
	    bus = WvDbus.session_bus;
	else
	    bus = new WvDbus(bus_moniker);
    }

    // If you've already got a Bus you'd like to use.
    public VxDbusSchema(WvDbus _bus)
    {
        bus = _bus;
    }
    
    static WvDbusMsg methodcall(string method, string signature)
    {
        return new WvDbusCall("vx.versaplexd", "/db", 
			      "vx.db", method, signature);
    }

    // 
    // The ISchema interface
    //

    // Note: this implementation ignores the sums.
    public VxSchemaErrors Put(VxSchema schema, VxSchemaChecksums sums, 
        VxPutOpts opts)
    {
        WvDbusMsg call = methodcall("PutSchema", 
            String.Format("{0}i", VxSchema.GetDbusSignature()));

        WvDbusWriter writer = new WvDbusWriter();

        schema.WriteSchema(writer);
        writer.Write((int)opts);
        call.Body = writer.ToArray();

        WvDbusMsg reply = bus.send_and_wait(call);
	if (reply.signature == VxSchemaErrors.GetDbusSignature())
	    return new VxSchemaErrors(reply.iter().pop());
	else
	    reply.check(VxSchemaErrors.GetDbusSignature());
	return null;
    }

    // Utility API so you can say Get("foo").
    public VxSchema Get(params string[] keys)
    {
        WvDbusMsg call = methodcall("GetSchema", "as");

        WvDbusWriter writer = new WvDbusWriter();

        if (keys == null)
            keys = new string[0];

        writer.WriteArray(4, keys, (w2, k) => {
	    w2.Write(k);
	});
        call.Body = writer.ToArray();

        WvDbusMsg reply = bus.send_and_wait(call);
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
        WvDbusMsg call = methodcall("GetSchemaChecksums", "");

        WvDbusMsg reply = bus.send_and_wait(call);
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
        WvDbusMsg call = methodcall("DropSchema", "as");

        WvDbusWriter writer = new WvDbusWriter();

	writer.WriteArray(4, keys, (w2, k) => {
	    w2.Write(k);
	});
        call.Body = writer.ToArray();

        WvDbusMsg reply = bus.send_and_wait(call);
	if (reply.signature == VxSchemaErrors.GetDbusSignature())
	    return new VxSchemaErrors(reply.iter().pop());
	else
	    reply.check(VxSchemaErrors.GetDbusSignature());
	return null;
    }
    
    public string GetSchemaData(string tablename, int seqnum, string where)
    {
        WvDbusMsg call = methodcall("GetSchemaData", "ss");

        WvDbusWriter writer = new WvDbusWriter();

        if (where == null)
            where = "";

        writer.Write(tablename);
        writer.Write(where);
        call.Body = writer.ToArray();

        WvDbusMsg reply = bus.send_and_wait(call);
	reply.check("s");
	return reply.iter().pop();
    }

    public void PutSchemaData(string tablename, string text, int seqnum)
    {
        WvDbusMsg call = methodcall("PutSchemaData", "ss");

        WvDbusWriter writer = new WvDbusWriter();

        writer.Write(tablename);
        writer.Write(text);
        call.Body = writer.ToArray();

        WvDbusMsg reply = bus.send_and_wait(call);
	reply.check("");
    }
}


