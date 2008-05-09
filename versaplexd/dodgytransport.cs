using System;
using System.Net;
using System.Net.Sockets;
using NDesk.DBus;
using Mono.Unix;

class DodgyTransport : NDesk.DBus.Transports.Transport
{
    static bool IsMono()
    {
	return Type.GetType("Mono.Runtime") != null;
    }
    
    // This has to be a separate function so we can delay JITting it until
    // we're sure it's mono.
    string MonoAuthString()
    {
	return UnixUserInfo.GetRealUserId().ToString();
    }
    
    public override string AuthString()
    {
	if (IsMono())
	    return MonoAuthString();
	else
	    return "WIN32"; // FIXME do something better?
    }

    public override void WriteCred()
    {
        Stream.WriteByte(0);
    }

    public override void Open(AddressEntry entry)
    {
	if (entry.Method == "unix")
	{
	    string path;
	    bool abstr;

	    if (entry.Properties.TryGetValue("path", out path))
		abstr = false;
	    else if (entry.Properties.TryGetValue("abstract", out path))
		abstr = true;
	    else
		throw new Exception("No path specified for UNIX transport");

	    if (abstr)
		socket = OpenAbstractUnix(path);
	    else
		socket = OpenPathUnix(path);
	}
	else if (entry.Method == "tcp")
	{
	    string host = "127.0.0.1";
	    string port = "5555";
	    entry.Properties.TryGetValue("host", out host);
	    entry.Properties.TryGetValue("port", out port);
	    socket = OpenTcp(host, Int32.Parse(port));
	}
	else
	    throw new Exception(String.Format("Unknown connection method {0}",
					      entry.Method));
	
        socket.Blocking = true;
        SocketHandle = (long)socket.Handle;
        Stream = new NetworkStream(socket);
    }

    protected VxNotifySocket OpenAbstractUnix(string path)
    {
        AbstractUnixEndPoint ep = new AbstractUnixEndPoint(path);
        VxNotifySocket client = new VxNotifySocket(AddressFamily.Unix,
                SocketType.Stream, 0);
        client.Connect(ep);
        return client;
    }

    public VxNotifySocket OpenPathUnix(string path) 
    {
        UnixEndPoint ep = new UnixEndPoint(path);
        VxNotifySocket client = new VxNotifySocket(AddressFamily.Unix,
                SocketType.Stream, 0);
        client.Connect(ep);
        return client;
    }
    
    public VxNotifySocket OpenTcp(string host, int port)
    {
	IPHostEntry hent = Dns.GetHostEntry(host);
	IPAddress ip = hent.AddressList[0];
        IPEndPoint ep = new IPEndPoint(ip, port);
        VxNotifySocket client = new VxNotifySocket(AddressFamily.InterNetwork,
						   SocketType.Stream, 0);
        client.Connect(ep);
        return client;
    }

    protected VxNotifySocket socket;
    public VxNotifySocket Socket {
        get { return socket; }
    }
}
