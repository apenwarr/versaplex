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
	
	public static void runonce()
	{
	    ev.runonce();
	}
	
	public static void runonce(int msec_timeout)
	{
	    ev.runonce(msec_timeout);
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

	bool is_readable = false, is_writable = false;
	event Action _onreadable, _onwritable, _onclose;
	
	protected bool can_onreadable { get { return _onreadable != null; } }
	protected bool can_onwritable { get { return _onwritable != null; } }
	
	protected void do_readable() 
	{
	    if (can_onreadable)
	    {
		is_readable = false;
		_onreadable(); 
	    }
	}
	
	protected void do_writable() 
	{
	    if (can_onwritable)
	    {
		is_writable = false;
		_onwritable();
	    }
	}
	
	protected void do_close()
	{
	    if (_onclose != null)
		_onclose(); 
	}
	
	object pr_obj = new object();
	protected void post_readable()
	{
	    is_readable = true;
	    ev.addpending(pr_obj, do_readable);
	}
	
	object pw_obj = new object();
	protected void post_writable()
	{
	    is_writable = true;
	    ev.addpending(pw_obj, do_writable);
	}
	
	public virtual event Action onreadable { 
	    add    { _onreadable += value; if (is_readable) post_readable(); }
	    remove { _onreadable -= value; }
	}
	public virtual event Action onwritable { 
	    add    { _onwritable += value; if (is_writable) post_writable(); }
	    remove { _onwritable -= value; }
	}
	public virtual event Action onclose { 
	    add    { _onclose += value; }
	    remove { _onclose -= value; }
	}
	
	bool isopen = true;
	public virtual bool isok { get { return isopen && err == null; } }

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
	    ev.delpending(pr_obj);
	    ev.delpending(pw_obj);
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
		    if (can_onreadable) this.inner.onreadable += do_readable;
		    if (can_onwritable) this.inner.onwritable += do_writable;
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
	
	public override void noread()
	{
	    base.noread();
	    if (hasinner)
		inner.noread();
	}

	public override void nowrite()
	{
	    base.nowrite();
	    if (hasinner)
		inner.nowrite();
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
	    add { if (!can_onreadable) inner.onreadable += do_readable;
		  base.onreadable += value; }
	    remove { base.onreadable -= value;
		     if (!can_onreadable) inner.onreadable -= do_readable; }
	}
	public override event Action onwritable {
	    add { if (!can_onwritable) inner.onwritable += do_writable;
		  base.onwritable += value; }
	    remove { base.onwritable -= value;
		     if (!can_onwritable) inner.onwritable -= do_writable; }
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
		Array.Copy(inbuf.get(max), 0, buf, offset, max);
		post_readable();
		return max;
	    }
	    else
		return base.read(buf, offset, len);
	}
	
	public string getline(char splitchar)
	{
	    while (isok && inbuf.strchr(splitchar) <= 0)
	    {
		byte[] b = inner.read(4096);
		// Console.WriteLine("got {0} bytes", b.Length);
		if (b.Length == 0)
		    return null;
		inbuf.put(b);
	    }
	    
	    // if we get here, there's a splitchar in the buffer
	    post_readable(); // not stalled yet!
	    return inbuf.get(inbuf.strchr(splitchar)).FromUTF8();
	}
    }
    
}
