using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Mono.Unix;
using Wv;
using Wv.Extensions;

namespace Wv
{
    public class Transport
    {
	public WvBufStream stream;
	
	public void WriteCred()
	{
	    stream.write(new byte[] { 0 });
	}
	
	public Transport(WvUrl entry)
	{
	    WvStream s;
	    if (entry.method == "unix")
	    {
		if (entry.path.ne())
		    s = new WvUnix(entry.path);
		else
		    throw new Exception("No path specified for UNIX transport");
	    }
	    else if (entry.method == "tcp")
	    {
		string host = entry.host.or("127.0.0.1");
		int port = entry.port.or(5555);
		s = new WvTcp(host, (ushort)port);
	    }
	    else
		throw new Exception(String.Format("Unknown connection method {0}",
						  entry.method));
	    stream = new WvBufStream(s);
	}
	
	public Transport(string address)
	    : this(Address.Parse(address))
	{
	}
	
	public void wait(int msec_timeout)
	{
	    stream.wait(msec_timeout, true, false);
	}
	
	public int read(WvBytes b)
	{
	    if (!stream.isok)
		wv.printerr("STREAM ERROR: {0}\n", stream.err);
	    wv.assert(stream.isok);
	    return stream.read(b);
	}
	
	public void write(WvBytes b)
	{
	    if (!stream.isok)
		wv.printerr("STREAM ERROR: {0}\n", stream.err);
	    wv.assert(stream.isok);
	    int written = stream.write(b);
	    wv.assert(written == b.len); 
	}
    }
}
