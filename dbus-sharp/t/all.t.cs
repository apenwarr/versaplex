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
    public void create_bus()
    {
        new Bus(Address.Session);
	WVPASS(true);
    }
    
    public static void Main()
    {
	WvTest.DoMain();
    }
}
