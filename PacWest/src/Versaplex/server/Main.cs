using System;
using System.Net.Sockets;
using Mono.Unix;
using NDesk.DBus;
using versabanq.Versaplex.Server;

public static class Versaplex
{
    public static void DataReady(object sender, object cookie)
    {
        VxBufferStream vxbs = (VxBufferStream)sender;
        Connection conn = (Connection)cookie;

        if (vxbs.BufferPending == 0) {
            Console.WriteLine("??? DataReady but nothing to read");
            return;
        }

        // XXX: Ew.
        byte[] buf = new byte[vxbs.BufferPending];
        vxbs.Read(buf, 0, buf.Length);
        conn.ReceiveBuffer(buf, 0, buf.Length);

        vxbs.BufferAmount = conn.NonBlockIterate();
    }

    public static void Main()
    {
        VxNotifySocket dbus_socket = new VxNotifySocket (AddressFamily.Unix,
                SocketType.Stream, 0);

        Console.WriteLine("Connecting to " + Address.Session);
        AddressEntry aent = AddressEntry.Parse(Address.Session);

        NDesk.DBus.Transports.Transport trans = new DodgyTransport();
        trans.Open(aent);

        Connection conn = Connection.Open(trans);

        using (VxBufferStream vxbs = new VxBufferStream(dbus_socket)) {
            conn.Transport.Stream = new VxBufferStream(dbus_socket);
            conn.ns = conn.Transport.Stream;
            vxbs.Cookie = conn;
            vxbs.DataReady += DataReady;
            vxbs.BufferAmount = 16;
        }

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
}
