using System;
using System.Net;
using System.Net.Sockets;

namespace Wv
{
    public class WvStream
    {
	public WvStream()
	{
	}

	bool isopen = true;
	public virtual bool isok { get { return isopen; } }

	Exception _err;
	public Exception err {
	    get {
		return _err;
	    }
	    protected set {
		if (_err == null) // remember the *first* error
		{
		    _err = value;
		    close();
		}
	    }
	}

	public virtual void close()
	{
	    flush(-1);
	    canread = canwrite = false;
	    isopen = false;
	    onreadable(null);
	    onwritable(null);
	}

	public virtual EndPoint localaddr()
	{
	    return null;
	}

	public virtual EndPoint remoteaddr()
	{
	    return null;
	}

	public virtual int read(byte[] buf, int offset, int len)
	{
	    return 0;
	}

	// for convenience
	public byte[] read(int len)
	{
	    byte[] bytes = new byte[len];
	    int got = read(bytes, 0, len);
	    if (got < len)
	    {
		byte[] ret = new byte[got];
		Array.Copy(bytes, 0, ret, 0, got);
		return ret;
	    }
	    else
		return bytes;
	}

	public virtual int write(byte[] buf, int offset, int len)
	{
	    return len; // lie: we "wrote" all the bytes to nowhere
	}

	public int write(byte[] buf)
	{
	    return write(buf, 0, buf.Length);
	}

	public virtual void onreadable(Action a)
	{
	    // never readable
	}

	public virtual void onwritable(Action a)
	{
	    // never writable
	}

	bool canread = true, canwrite = true;

	public virtual void noread()
	{
	    canread = false;
	    maybe_autoclose();
	}

	public virtual void nowrite()
	{
	    canwrite = false;
	    maybe_autoclose();
	}

	public void maybe_autoclose()
	{
	    if (!canread && !canwrite)
		close();
	}

	public virtual bool flush(int msec_timeout)
	{
	    return true; // no buffer
	}

	// FIXME: assumes the write will succeed!  Should only be available
	// on streams with an outbuf.
	public void print(string fmt, params object[] args)
	{
	    write(string.Format(fmt, args).ToUTF8());
	}

	public void print(string fmt)
	{
	    write(fmt.ToUTF8());
	}
    }

    public class WvSockStream: WvStream
    {
	IWvEventer ev;
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

	public override bool isok { get { return (sock != null) && base.isok; } }

	public WvSockStream(IWvEventer ev, Socket sock)
	{
	    this.ev = ev;
	    this.sock = sock;
	}

	public override EndPoint localaddr()
	{
	    if (!!isok)
		return null;

	    return sock.LocalEndPoint;
	}

	public override EndPoint remoteaddr()
	{
	    return sock.RemoteEndPoint;
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

	public override void onreadable(Action a)
	{
	    ev.onreadable(sock, a);
	}

	public override void onwritable(Action a)
	{
	    ev.onwritable(sock, a);
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
	}

	public override void nowrite()
	{
	    base.nowrite();
	    if (sock != null)
		tryshutdown(SocketShutdown.Send);
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

    public class WvTcp: WvSockStream
    {
        public WvTcp(IWvEventer ev, string remote) : base(ev, null)
	{
	    IPHostEntry ipe = Dns.GetHostEntry(remote);
	    IPEndPoint ipep = new IPEndPoint(ipe.AddressList[0], 80);
	    Socket sock = new Socket(AddressFamily.InterNetwork,
				     SocketType.Stream,
				     ProtocolType.Tcp);
	    sock.Connect(ipep);
	    this.sock = sock;
	}
    }
}
