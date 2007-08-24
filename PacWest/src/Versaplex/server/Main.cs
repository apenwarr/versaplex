using System;
using System.Net;
using System.Net.Sockets;
using Mono.Unix;
using NDesk.DBus;
using org.freedesktop.DBus;
using versabanq.Versaplex.Server;
using versabanq.Versaplex.Dbus;
using versabanq.Versaplex.Dbus.Db;

public static class Versaplex
{
    static Connection.MessageHandler oldhandler = null;
    static VxMethodCallRouter msgrouter = new VxMethodCallRouter();

    private static void DataReady(object sender, object cookie)
    {
        // FIXME: This may require special handling for padding between
        // messages: it hasn't been a problem so far, but should be addressed

        VxBufferStream vxbs = (VxBufferStream)sender;

        Connection conn = (Connection)cookie;

        if (vxbs.BufferPending == 0) {
            Console.WriteLine("??? DataReady but nothing to read");
            return;
        }

        // XXX: Ew.
        byte[] buf = new byte[vxbs.BufferPending];
        vxbs.Read(buf, 0, buf.Length);
        vxbs.BufferAmount = conn.ReceiveBuffer(buf, 0, buf.Length);
    }

    private static void NoMoreData(object sender, object cookie)
    {
        Console.WriteLine(
                "***********************************************************\n"+
                "************ D-bus connection closed by server ************\n"+
                "***********************************************************");

        VxBufferStream vxbs = (VxBufferStream)sender;
        vxbs.Close();

        VxEventLoop.Shutdown();
    }

    private static void MessageReady(Message msg)
    {
        // FIXME: This should really queue things to be run from the thread
        // pool and then the response would be sent back through the action
        // queue
        Console.WriteLine("MessageReady");

        VxDbus.MessageDump(msg);

        switch (msg.Header.MessageType) {
            case MessageType.MethodCall:
            {
                Message reply;
                if (msgrouter.RouteMessage(msg, out reply)) {
                    if (reply == null) {
                        // FIXME: Do something if this happens, maybe?
                        Console.WriteLine("Empty reply from RouteMessage");
                    } else {
                        // XXX: Should this be done further down rather than
                        // passing the reply out here?
                        msg.Connection.Send(reply);
                    }
                    return;
                }
            
                break;
            }
        }

        // FIXME: This is hacky. But it covers stuff I don't want to deal with
        // yet.
        oldhandler(msg);
    }

    public static void Main()
    {
        msgrouter.AddInterface(VxDbInterfaceRouter.Instance);

        Console.WriteLine("Connecting to '{0}'", Address.Session);
	if (Address.Session == null)
	    throw new Exception("DBUS_SESSION_BUS_ADDRESS not set");
        AddressEntry aent = AddressEntry.Parse(Address.Session);

        DodgyTransport trans = new DodgyTransport();
        trans.Open(aent);

        Bus conn = new Bus(trans);

        string myNameReq = "com.versabanq.versaplex";
        RequestNameReply rnr = conn.RequestName(myNameReq,
                NameFlag.DoNotQueue);

        switch (rnr) {
            case RequestNameReply.PrimaryOwner:
                Console.WriteLine("Name registered, ready");
                break;
            default:
                Console.WriteLine("Register name result: " + rnr.ToString());
                return;
        }

        VxBufferStream vxbs = new VxBufferStream(trans.Socket);
        conn.Transport.Stream = vxbs;
        conn.ns = conn.Transport.Stream;
        vxbs.Cookie = conn;
        vxbs.DataReady += DataReady;
        vxbs.NoMoreData += NoMoreData;
        vxbs.BufferAmount = 16;

        oldhandler = conn.OnMessage;
        conn.OnMessage = MessageReady;

#if false
        // XXX: Shutdown after 5 minutes
        // You probably don't want to keep this
        VxEventLoop.AddEvent(new TimeSpan(0, 5, 0),
                delegate() {
                    VxEventLoop.Shutdown();
                });
#endif

        VxEventLoop.Run();

        Console.WriteLine("Done!");
    }
}

class DodgyTransport : NDesk.DBus.Transports.Transport
{
    public override string AuthString()
    {
        long uid = UnixUserInfo.GetRealUserId();
        return uid.ToString();
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
