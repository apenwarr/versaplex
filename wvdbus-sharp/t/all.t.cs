#include "wvtest.cs.h"

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Wv;
using Wv.Extensions;
using Wv.Test;

[TestFixture]
class DbusTest
{
    struct Stupid
    {
	public string s;
    }
    
    [Test]
    public void message_read_write()
    {
	WvBytes msgdata, content;
	
	// write
	{
	    WvDbusMsg m = new WvDbusMsg();
	    m.rserial = 0xf00f;
	    m.signature = "yisaxaxva(s)";
	    m.sender = "booga";
	    WvDbusWriter w = new WvDbusWriter();
	    w.Write((byte)42);
	    w.Write(42);
	    w.Write("hello world");
	      // empty array test
	    w.WriteArray(8, new Int64[] { }, (w2, i) => {
		w2.Write(i);
	    });
	      // nonempty array test
	    w.WriteArray(8, new Int64[] { 0x42, 0x43, 0x44 }, (w2, i) => {
		w2.Write(i);
	    });
	    
	    // variant
	    w.WriteSig("s");
	    w.Write("VSTRING");
	    
	    w.WriteArray(8, new string[] { "a", "aaa", "aaaaa" }, (w2, i) => {
		w2.Write(i);
	    });
	    m.Body = w.ToArray();
	    
	    var buf = new WvBuf();
	    buf.put(m.GetHeaderData());
	    buf.put(m.Body);
	    content = m.Body;
	    msgdata = buf.getall();
	}
	
	WVPASS("wrote");
	wv.print("message:\n{0}\n", wv.hexdump(msgdata));
	
	// new-style read
	{
	    WvDbusMsg m = new WvDbusMsg();
	    m.Body = (byte[])msgdata;
	    m.SetHeaderData(msgdata);
	    m.Body = (byte[])content;
	    
	    var i = m.iter();
	    WVPASSEQ(i.pop(), 42);
	    WVPASSEQ(i.pop(), 42);
	    WVPASSEQ(i.pop(), "hello world");

	    // empty list
	    WVPASSEQ(i.pop().Count(), 0);
	    
	    
	    // nonempty list
	    var it = i.pop();
	    var a = it.ToArray<WvAutoCast>();
	    WVPASSEQ(a.Length, 3);
            WVPASSEQ(a[2], 0x44);
	    
	    foreach (long v in it)
		wv.print("value: {0:x}\n", v);
	    
	    WVPASSEQ(i.pop(), "VSTRING");

	    var a2 = i.pop().ToArray();
	    WVPASSEQ(a2.Length, 3);
	    WVPASSEQ(a2[2].autocast().join(""), "aaaaa");
	}
    }

    [Test]
    public void send_receive()
    {
        WvDbus bus = new WvDbus(WvDbus.session_bus_address);
	WVPASS("got bus");
	
	WvDbusMsg m = new WvDbusMsg();
	m.signature = "su";
	m.type = Wv.Dbus.MType.MethodCall;
	m.ReplyExpected = true;
	m.dest = "org.freedesktop.DBus";
	m.path = "/org/freedesktop/DBus";
	m.ifc = "org.freedesktop.DBus";
	m.method = "RequestName";
	WvDbusWriter w = new WvDbusWriter();
	w.Write("all.t.cs");
	w.Write(0);
	m.Body = w.ToArray();
	
	uint serial = bus.send(m);
	
	WvDbusMsg reply;
	bool got_reply = false;
	for (int i = 0; i < 50; i++)
	{
	    reply = bus.readmessage(-1);
	    if (reply == null)
		continue;
	    
	    wv.print("<< #{0}\n", reply.serial);
	    wv.print("{0}\n", wv.hexdump(reply.Body));
	    
	    if (!reply.rserial.HasValue || reply.rserial.Value != serial)
	    {
		WVPASS("skipping unwanted serial");
		continue;
	    }
	    
	    uint rserial = reply.rserial.Value;
	    wv.print("ReplySerial is: {0} (want {1})\n", rserial, serial);
	    WVPASSEQ(rserial, serial);
	    got_reply = true;
	    
	    int rv = reply.iter().pop();
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
