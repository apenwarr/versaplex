#include "wvtest.cs.h"

using System;
using System.Linq;
using Wv;
using Wv.Test;

[TestFixture]
class DbusTest
{
    [Test]
    public void message_read_write()
    {
	byte[] msgdata;
	
	// write
	{
	    Message m = new Message();
	    m.Signature = new Signature("is");
	    MessageWriter w = new MessageWriter();
	    w.Write(42);
	    w.Write("hello world");
	    m.Body = w.ToArray();
	    
	    var buf = new WvBuf();
	    buf.put(m.GetHeaderData());
	    buf.put(m.Body);
	    msgdata = buf.getall();
	}
	
	// read
	{
	    Message m = new Message();
	    m.Body = msgdata;
	    MessageReader r = new MessageReader(m);
	    m.Header = (Header)r.ReadStruct(typeof(Header));
	    WVPASSEQ(r.ReadInt32(), 42);
	    WVPASSEQ(r.ReadString(), "hello world");
	}
    }

    [Test]
    public void send_receive()
    {
        Bus bus = new Bus(Address.Session);
	WVPASS("got bus");
	
	Message m = new Message();
	m.Signature = new Signature("su");
	m.Header.MessageType = MessageType.MethodCall;
	m.ReplyExpected = true;
	m.Header.Fields[FieldCode.Destination] = "org.freedesktop.DBus";
	m.Header.Fields[FieldCode.Path] = new ObjectPath("/org/freedesktop/DBus");
	m.Header.Fields[FieldCode.Interface] = "org.freedesktop.DBus";
	m.Header.Fields[FieldCode.Member] = "RequestName";
	MessageWriter w = new MessageWriter();
	w.Write("all.t.cs");
	w.Write(0);
	m.Body = w.ToArray();
	
	uint serial = bus.Send(m);
	
	Message reply;
	bool got_reply = false;
	for (int i = 0; i < 50; i++)
	{
	    reply = bus.ReadMessage();
	    if (reply == null)
	    {
		wv.sleep(100);
		continue;
	    }
	    
	    wv.print("<< #{0}\n", reply.Header.Serial);
	    wv.print("{0}\n", wv.hexdump(reply.Body));
	    
	    if (!reply.Header.Fields.ContainsKey(FieldCode.ReplySerial)
		|| (uint)reply.Header.Fields[FieldCode.ReplySerial] != serial)
	    {
		WVPASS("skipping unwanted serial");
		continue;
	    }
	    
	    uint rserial = (uint)reply.Header.Fields[FieldCode.ReplySerial];
	    wv.print("ReplySerial is: {0} (want {1})\n", rserial, serial);
	    WVPASSEQ(rserial, serial);
	    got_reply = true;
	    
	    MessageReader r = new MessageReader(reply);
	    int rv = r.ReadInt32();
	    WVPASSEQ(rv, 1);
	    
	    break;
	}
	
	WVPASS(got_reply);
	
	WVPASS(bus.NameHasOwner("all.t.cs"));
	WVFAIL(bus.NameHasOwner("all.t.cs.nonexist"));
    }
    
    public static void Main()
    {
	WvTest.DoMain();
    }
}
