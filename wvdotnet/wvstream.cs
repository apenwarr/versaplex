using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Wv.Extensions;

namespace Wv
{
    public interface IWvStream
    {
	bool isok { get; }
	Exception err { get; }
	EndPoint localaddr();
	EndPoint remoteaddr();
	
	int read(byte[] buf, int offset, int len);
	int write(byte[] buf, int offset, int len);
	bool flush(int msec_timeout);
	
	event Action onreadable;
	event Action onwritable;
	event Action onclose;
	
	void close();
	void noread();
	void nowrite();
    }
    
    public class WvStream : IWvStream
    {
	public WvStream()
	{
	}

	event Action _onreadable, _onwritable, _onclose;
	
	protected bool can_readable { get { return _onreadable != null; } }
	protected bool can_writable { get { return _onwritable != null; } }
	
	protected void do_readable() 
	    { if (can_readable) _onreadable(); }
	protected void do_writable() 
	    { if (can_writable) _onwritable(); }
	protected void do_close()
	    { if (_onclose != null) _onclose(); }
	
	public virtual event Action onreadable { 
	    add    { _onreadable += value; }
	    remove { _onreadable -= value; }
	}
	public virtual event Action onwritable { 
	    add    { _onwritable += value; }
	    remove { _onwritable -= value; }
	}
	public virtual event Action onclose { 
	    add    { _onclose += value; }
	    remove { _onclose -= value; }
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
	    if (isopen)
	    {
		isopen = false;
		if (_onclose != null) _onclose();
	    }
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
	
	public int read(byte[] buf)
	{
	    return read(buf, 0, buf.Length);
	}

	// for convenience.  Note: always returns non-null, but the returned
	// array size might be zero.
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

	// WARNING: assumes the write() will succeed!  Use only on WvStreams
	// with a write buffer.
	public void print(string fmt, params object[] args)
	{
	    print((object)wv.fmt(fmt, args));
	}

	// WARNING: assumes the write() will succeed!  Use only on WvStreams
	// with a write buffer.
	public virtual void print(object o)
	{
	    byte[] b = o.ToUTF8();
	    int n = write(b);
	    wv.assert(n == b.Length,
		      "Don't use print() on an unbuffered WvStream!");
	}
    }
    
    public class WvFile : WvStream
    {
	FileStream fs;
	
	public WvFile(string filename)
	{
	    fs = new FileStream(filename, FileMode.Open, FileAccess.Read);
	}
	
	public override void close()
	{
	    fs.Close();
	    fs.Dispose();
	    fs = null;
	    base.close();
	}
	
	public override int read(byte[] buf, int offset, int len)
	{
	    return fs.Read(buf, offset, len);
	}
	
	public override int write(byte[] buf, int offset, int len)
	{
	    fs.Write(buf, offset, len);
	    return len;
	}
    }

    public class WvSockStream : WvStream
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
	
	public override event Action onreadable {
	    add { base.onreadable += value;
		  ev.onreadable(sock, do_readable); }
	    remove { base.onreadable -= value;
		     if (can_readable) ev.onreadable(sock, null); }
	}

	public override event Action onwritable {
	    add { base.onwritable += value;
		  ev.onwritable(sock, do_writable); }
	    remove { base.onwritable -= value;
		     if (can_writable) ev.onwritable(sock, null); }
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
        public WvTcp(IWvEventer ev, string remote) : base(ev, null)
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
