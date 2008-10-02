using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Mono.Unix;
using Wv;
using Wv.Extensions;

class DodgyTransport : Wv.Transports.Transport
{
    public WvStream stream;
    
    // This has to be a separate function so we can delay JITting it until
    // we're sure it's mono.
    string MonoAuthString()
    {
	try { //will work in Mono on Linux.
	    return UnixUserInfo.GetRealUserId().ToString();
	} catch { return "WIN32"; }
    }
    
    public override string AuthString()
    {
	if (Wv.wv.IsMono())
	    return MonoAuthString();
	else
	    return "WIN32"; // FIXME do something better?
    }

    public override void WriteCred()
    {
        stream.write(new byte[] { 0 });
    }

    public DodgyTransport(AddressEntry entry)
    {
	WvStream s;
	if (entry.Method == "unix")
	{
	    string path = entry.Properties.tryget("path");
	    string abstr = entry.Properties.tryget("abstract");
	    
	    if (path.ne())
		s = new WvUnix(path);
	    else if (abstr.ne())
		s = new WvUnix("@" + abstr);
	    else
		throw new Exception("No path specified for UNIX transport");
	}
	else if (entry.Method == "tcp")
	{
	    string host = entry.Properties.tryget("host", "127.0.0.1");
	    string port = entry.Properties.tryget("port", "5555");
	    s = new WvTcp(host, (ushort)port.atoi());
	}
	else
	    throw new Exception(String.Format("Unknown connection method {0}",
					      entry.Method));
	stream = new WvOutBufStream(s);
    }
    
    public override void wait(int msec_timeout)
    {
	stream.wait(msec_timeout, true, false);
    }
	    
    public override int read(WvBytes b)
    {
	if (!stream.isok)
	    wv.printerr("STREAM ERROR: {0}\n", stream.err);
	wv.assert(stream.isok);
	return stream.read(b);
    }
    
    public override void write(WvBytes b)
    {
	if (!stream.isok)
	    wv.printerr("STREAM ERROR: {0}\n", stream.err);
	wv.assert(stream.isok);
	int written = stream.write(b);
	wv.assert(written == b.len); 
    }
}
