using System;
using System.IO;

namespace Wv
{
    public class WvStreamStream : WvStream
    {
	System.IO.Stream inner;
	bool hasinner { get { return inner != null; } }
	
	public WvStreamStream(System.IO.Stream inner)
	{
	    this.inner = inner;
	    if (hasinner)
	    {
		if (!inner.CanWrite)
		    nowrite();
		else
		    post_writable();
		if (!inner.CanRead)
		    noread();
		else
		{
		    lock(readlock) start_reading();
		}
	    }
	}
	
	public override bool isok { 
	    get { return hasinner && base.isok; }
	}
	
	object readlock = new object();
	byte[] inbuf = new byte[4096];
	int in_ofs = 0, in_left = 0;
	bool got_eof = false;
	
	void start_reading()
	{
	    //Console.WriteLine("starting...");
	    if (got_eof)
	    {
		//Console.WriteLine("eof close!");
		noread();
		nowrite();
		return;
	    }
	    
	    in_ofs = 0;
	    in_left = 0;
	    try {
		inner.BeginRead(inbuf, 0, inbuf.Length,
				delegate(IAsyncResult ar) {
				    lock (readlock)
				    {
					in_ofs = 0;
					in_left = inner.EndRead(ar);
					//Console.WriteLine("ending... {0}", in_left);
					if (in_left == 0)
					    got_eof = true;
					post_readable();
				    }
				},
				null);
	    } finally {} /* catch (Exception e) {
		err = e;
	    }*/
	}
	
	public override int read(byte[] buf, int offset, int len)
	{
	    //Console.WriteLine("read() request");
	    lock (readlock)
	    {
		if (in_left > 0)
		{
		    int max = in_left <= len ? in_left : len;
		    Array.Copy(inbuf, in_ofs, buf, offset, max);
		    in_ofs += max;
		    in_left -= max;
		    //Console.WriteLine("left: {0}", in_left);
		    if (in_left > 0)
			post_readable();
		    else
			start_reading();
		    return max;
		}
		else
		{
		    start_reading();
		    return 0;
		}
	    }
	}
	
	public override int write(byte[] buf, int offset, int len)
	{
	    if (!isok) return 0;
	    try {
		inner.BeginWrite(buf, offset, len,
				 delegate(IAsyncResult ar) {
				     inner.EndWrite(ar);
				     post_writable();
				 },
				 null);
	    } catch (Exception e) {
		err = e;
		return 0;
	    }
	    
	    return len;
	}
	
	public override bool flush(int msec_timeout)
	{
	    // FIXME: how to implement msec_timeout?
	    if (hasinner) inner.Flush();
	    return base.flush(msec_timeout);
	}
	
	public override void close()
	{
	    base.close();
	    if (hasinner)
		inner.Dispose();
	    inner = null;
	}
    }
    
    public class _WvFile : WvStreamStream
    {
        public _WvFile(string filename)
	    : base(new FileStream(filename, FileMode.Open, FileAccess.Read))
	{
	}
    }
    
    public class WvFile : WvInBufStream
    {
	public WvFile(string filename)
	    : base(new _WvFile(filename))
	{
	}
    }
}
