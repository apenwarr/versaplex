using System;
using System.Net.Sockets;
using Mono.Unix;
using NDesk.DBus;
using org.freedesktop.DBus;
using versabanq.Versaplex.Server;
using versabanq.Versaplex.Dbus.Db;

public static class Versaplex
{
    public static void DataReady(object sender, object cookie)
    {
        VxBufferStream vxbs = (VxBufferStream)sender;

        Console.WriteLine(cookie.GetType().ToString());

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

    public static void Main()
    {
        Console.WriteLine("Connecting to " + Address.Session);
        AddressEntry aent = AddressEntry.Parse(Address.Session);

        DodgyTransport trans = new DodgyTransport();
        trans.Open(aent);

        Bus conn = new Bus(trans);

        ObjectPath myOpath = new ObjectPath ("/com/versabanq/versaplex/db");
        string myNameReq = "com.versabanq.versaplex.db";

        VxDb dbapi = VxDb.Instance;
        conn.Register(myNameReq, myOpath, dbapi);

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
        vxbs.BufferAmount = 16;

        VxEventLoop.AddEvent(new TimeSpan(0, 0, 14),
                delegate() {
                    Console.WriteLine("5");
                });
        VxEventLoop.AddEvent(new TimeSpan(0, 0, 1),
                delegate() {
                    Console.WriteLine("1");
                });
        VxEventLoop.AddEvent(new TimeSpan(0, 0, 10),
                delegate() {
                    Console.WriteLine("3");
                });
        VxEventLoop.AddEvent(new TimeSpan(0, 0, 5),
                delegate() {
                    Console.WriteLine("2");
                });
        VxEventLoop.AddEvent(new TimeSpan(0, 0, 12),
                delegate() {
                    Console.WriteLine("4");
                });
        VxEventLoop.AddEvent(new TimeSpan(0, 5, 0),
                delegate() {
                    VxEventLoop.Shutdown();
                });

        VxEventLoop.Run();
    }
}

class DodgyTransport : NDesk.DBus.Transports.Transport
{
    public override void Open (AddressEntry entry)
    {
        string path;
        bool abstr;

        if (entry.Properties.TryGetValue ("path", out path))
            abstr = false;
        else if (entry.Properties.TryGetValue ("abstract", out path))
            abstr = true;
        else
            throw new Exception ("No path specified for UNIX transport");

        Open(path, abstr);
    }

    public override string AuthString ()
    {
        long uid = UnixUserInfo.GetRealUserId ();
        return uid.ToString ();
    }

    public override void WriteCred ()
    {
        Stream.WriteByte (0);
    }

    public void Open (string path, bool @abstract)
    {
        if (@abstract)
            socket = OpenAbstractUnix (path);
        else
            socket = OpenUnix (path);

        socket.Blocking = true;
        SocketHandle = (long)socket.Handle;
        Stream = new NetworkStream (socket);
    }

    protected VxNotifySocket OpenAbstractUnix (string path)
    {
        AbstractUnixEndPoint ep = new AbstractUnixEndPoint (path);
        VxNotifySocket client = new VxNotifySocket(AddressFamily.Unix,
                SocketType.Stream, 0);
        client.Connect (ep);
        return client;
    }

    public VxNotifySocket OpenUnix (string path) 
    {
        UnixEndPoint ep = new UnixEndPoint (path);
        VxNotifySocket client = new VxNotifySocket(AddressFamily.Unix,
                SocketType.Stream, 0);
        client.Connect (ep);
        return client;
    }

    protected VxNotifySocket socket;
    public VxNotifySocket Socket {
        get { return socket; }
    }
}
