using System;
using System.Net;
using System.Net.Sockets;
using Wv;

namespace Wv
{
    public class WvSockStream : WvStream
    {
	Socket _sock;
	protected Socket sock {
	    get {
		return _sock;
	    }
	    set {
		_sock = value;
		if (_sock != null)
		    _sock.Blocking = false;
	    }
	}

	public override bool isok { 
	    get { return (sock != null) && base.isok; }
	}

	public WvSockStream(Socket sock)
	{
	    this.sock = sock;
	}

	public override EndPoint localaddr {
	    get {
		if (!isok)
		    return null;
		return sock.LocalEndPoint;
	    }
	}

	public override EndPoint remoteaddr {
	    get {
		if (!isok)
		    return null;
		return sock.RemoteEndPoint;
	    }
	}

	public override int read(byte[] buf, int offset, int len)
	{
	    if (!isok)
		return 0;

	    try
	    {
		int ret = sock.Receive(buf, offset, len, 0);
		if (ret <= 0) // EOF
		{
		    nowrite();
		    noread();
		    return 0;
		}
		else
		    return ret;
	    }
	    catch (SocketException e)
	    {
		if (e.ErrorCode != 10004) // EINTR is normal when non-blocking
		    err = e;
		return 0; // non-blocking, so interruptions are normal
	    }
	}

	public override int write(byte[] buf, int offset, int len)
	{
	    if (!isok)
		return 0;

	    int ret = sock.Send(buf, offset, len, 0);
	    if (ret < 0) // Error
	    {
		err = new Exception("Write error"); // FIXME
		return 0;
	    }
	    else
		return ret;
	}
	
	public override event Action onreadable {
	    add { base.onreadable += value;
		  ev.onreadable(sock, do_readable); }
	    remove { base.onreadable -= value;
		     if (can_onreadable) ev.onreadable(sock, null); }
	}

	public override event Action onwritable {
	    add { base.onwritable += value;
		  ev.onwritable(sock, do_writable); }
	    remove { base.onwritable -= value;
		     if (can_onwritable) ev.onwritable(sock, null); }
	}

	void tryshutdown(SocketShutdown sd)
	{
	    try
	    {
		sock.Shutdown(sd);
	    }
	    catch (SocketException)
	    {
		// ignore
	    }
	}

	public override void noread()
	{
	    base.noread();
	    if (sock != null)
		tryshutdown(SocketShutdown.Receive);
	    ev.onreadable(sock, null);
	}

	public override void nowrite()
	{
	    base.nowrite();
	    if (sock != null)
		tryshutdown(SocketShutdown.Send);
	    ev.onwritable(sock, null);
	}

	public override void close()
	{
	    base.close();
	    if (sock != null)
	    {
		tryshutdown(SocketShutdown.Both);
		sock.Close();
		((IDisposable)sock).Dispose();
		sock = null;
	    }
	}
    }

    public class WvTcp : WvSockStream
    {
        public WvTcp(string remote) : base(null)
	{
	    try
	    {
		IPHostEntry ipe = Dns.GetHostEntry(remote);
		IPEndPoint ipep = new IPEndPoint(ipe.AddressList[0], 80);
		Socket sock = new Socket(AddressFamily.InterNetwork,
					 SocketType.Stream,
					 ProtocolType.Tcp);
		sock.Connect(ipep);
		this.sock = sock;
	    }
	    catch (Exception e)
	    {
		err = e;
	    }
	}
    }

}
