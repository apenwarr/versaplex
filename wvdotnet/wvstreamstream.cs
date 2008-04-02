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
		if (!inner.CanRead)
		    noread();
	    }
	}
	
	public override bool isok { 
	    get { return hasinner && base.isok; }
	}
	
	object readlock = new object();
	byte[] inbuf = new byte[4096];
	int in_ofs = 0, in_left = 0;
	bool got_eof = false;
	
	public override int read(byte[] buf, int offset, int len)
	{
	    lock (readlock)
	    {
		if (in_left > 0)
		{
		    int max = in_left <= len ? in_left : len;
		    Array.Copy(inbuf, in_ofs, buf, offset, max);
		    in_ofs += max;
		    in_left -= max;
		    return max;
		}
		else // start a read for next time
		{
		    if (got_eof)
		    {
			noread();
			nowrite();
			return 0;
		    }
		    
		    in_ofs = 0;
		    in_left = 0;
		    try {
			inner.BeginRead(inbuf, 0, inbuf.Length,
					delegate(IAsyncResult ar) {
					    lock (readlock)
					    {
						wv.assert(inbuf == null);
						in_ofs = 0;
						in_left = inner.EndRead(ar);
						if (in_left == 0)
						    got_eof = true;
					    }
					},
					null);
		    } catch (Exception e) {
			err = e;
		    }
		    
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
