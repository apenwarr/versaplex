using System;
using System.Net;
using Wv.Extensions;

namespace Wv
{
    public interface IWvStream: IDisposable
    {
	bool isok { get; }
	Exception err { get; }
	EndPoint localaddr { get; }
	EndPoint remoteaddr { get; }
	
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
	public static IWvEventer ev = new WvEventer();
	
	public static void run()
	{
	    ev.run();
	}
	
	public WvStream()
	{
	}
	
	~WvStream()
	{
	    wv.assert(false, "A WvStream was not close()d/Dispose()d.");
	}
	
	public void Dispose()
	{
	    close();
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
	public virtual Exception err {
	    get {
		return _err;
	    }
	    set {
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
	    GC.SuppressFinalize(this);
	}

	public virtual EndPoint localaddr {
	    get { return null; }
	}

	public virtual EndPoint remoteaddr {
	    get { return null; }
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
    
    // Wraps a WvStream in another WvStream, allowing us to override some
    // behaviour.  By default, a WvStreamClone just passes everything through
    // to the inner stream.
    public class WvStreamClone : WvStream
    {
	protected WvStream inner = null;
	bool hasinner { get { return inner != null; } }
	
	public WvStreamClone(WvStream inner)
	{
	    setinner(inner);
	}
	
	public void setinner(WvStream inner)
	{
	    if (inner != this.inner)
	    {
		if (hasinner)
		{
		    this.inner.onreadable -= do_readable;
		    this.inner.onwritable -= do_writable;
		    this.inner.onclose -= do_close;
		}
		this.inner = inner;
		if (hasinner)
		{
		    if (can_readable) this.inner.onreadable += do_readable;
		    if (can_writable) this.inner.onwritable += do_writable;
		    this.inner.onclose += do_close;
		}
	    }
	}
	
	public override bool isok { 
	    get { return base.isok && hasinner && inner.isok; }
	}
	
	public override Exception err {
	    get { 
		return hasinner ? inner.err : base.err;
	    }
	    set { 
		if (hasinner)
		    inner.err = value; 
		else 
		    base.err = value;
	    }
	}
	
	public override EndPoint localaddr {
	    get { return hasinner ? inner.localaddr : base.localaddr; }
	}
	public override EndPoint remoteaddr {
	    get { return hasinner ? inner.localaddr : base.localaddr; }
	}
	
	public override int read(byte[] buf, int offset, int len)
	{
	    if (hasinner)
		return inner.read(buf, offset, len);
	    else
		return 0; // 0 bytes read
	}
	
	public override int write(byte[] buf, int offset, int len)
	{
	    if (hasinner)
		return inner.write(buf, offset, len);
	    else
		return 0; // 0 bytes written
	}
	
	public override bool flush(int msec_timeout)
	{
	    if (hasinner)
		return inner.flush(msec_timeout);
	    else
		return true;
	}
	
	// We only want to register our callback with the inner stream if
	// we *have* a callback, and then only once.  Otherwise the stream
	// might start listening for read when we don't have any readable
	// handlers, resulting in it spinning forever.
	public override event Action onreadable {
	    add { if (!can_readable) inner.onreadable += do_readable;
		  onreadable += value; }
	    remove { onreadable -= value;
		     if (!can_readable) inner.onreadable -= do_readable; }
	}
	public override event Action onwritable {
	    add { if (!can_writable) inner.onwritable += do_writable;
		  onwritable += value; }
	    remove { onwritable -= value;
		     if (!can_writable) inner.onwritable -= do_writable; }
	}
	
    }
    
    // Adds an input buffer to a WvStream.
    public class WvInBufStream : WvStreamClone
    {
	WvBuf inbuf = new WvBuf();
	
	public WvInBufStream(WvStream inner) : base(inner)
	{
	}
	
	public override int read(byte[] buf, int offset, int len)
	{
	    if (inbuf.used > 0)
	    {
		int max = inbuf.used > len ? len : inbuf.used;
		Array.Copy(inbuf.get(max), 0, buf, offset, len);
		return max;
	    }
	    else
		return base.read(buf, offset, len);
	}
	
	public string getline(char splitchar)
	{
	    while (inbuf.strchr(splitchar) <= 0)
	    {
		byte[] b = read(4096);
		if (b == null)
		    return null;
		inbuf.put(b);
	    }
	    
	    // if we get here, there's a splitchar in the buffer
	    return inbuf.get(inbuf.strchr(splitchar)).FromUTF8();
	}
    }
    
}
