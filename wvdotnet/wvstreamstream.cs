/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
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
	
	public override bool ok { 
	    get { return hasinner && base.ok; }
	}
	
	object readlock = new object();
	WvBytes inbuf = new byte[4096];
	int in_ofs = 0, in_left = 0;
	bool got_eof = false;
	
	void start_reading()
	{
	    //wv.printerr("starting...\n");
	    if (got_eof)
	    {
		//wv.printerr("eof close!\n");
		noread();
		nowrite();
		return;
	    }
	    
	    in_ofs = 0;
	    in_left = 0;
	    try {
		inner.BeginRead(inbuf.bytes, inbuf.start, inbuf.len,
				delegate(IAsyncResult ar) {
				    lock (readlock)
				    {
					in_ofs = 0;
					in_left = inner.EndRead(ar);
					//wv.printerr("ending... {0}\n", in_left);
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
	
	public override int read(WvBytes b)
	{
	    lock (readlock)
	    {
		if (in_left > 0)
		{
		    int max = in_left <= b.len ? in_left : b.len;
		    b.put(0, inbuf.sub(in_ofs, max));
		    in_ofs += max;
		    in_left -= max;
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
	
	public override int write(WvBytes b)
	{
	    if (!ok) return 0;
	    try {
		inner.BeginWrite(b.bytes, b.start, b.len,
				 delegate(IAsyncResult ar) {
				     inner.EndWrite(ar);
				     inner.Flush();
				     post_writable();
				 },
				 null);
	    } catch (Exception e) {
		err = e;
		return 0;
	    }
	    
	    return b.len;
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
	static FileStream openstream(string filename, string modestring)
	{
	    FileMode mode;
	    FileAccess access;
	    bool truncate = false;
	    
	    if (modestring.Length == 0)
		throw new ArgumentException
		  ("WvFile: modestring cannot be empty", "modestring");
	    
	    char first = modestring[0];
	    bool plus = (modestring.Length > 1 && modestring[1] == '+');
		
	    switch (first)
	    {
	    case 'r':
		mode = FileMode.Open;
		access = plus ? FileAccess.ReadWrite : FileAccess.Read;
		break;
	    case 'w':
		mode = FileMode.OpenOrCreate;
		access = plus ? FileAccess.ReadWrite : FileAccess.Write;
		truncate = true;
		break;
	    case 'a':
		mode = FileMode.Append;
		access = plus ? FileAccess.ReadWrite : FileAccess.Write;
		break;
	    default:
		throw new ArgumentException
		    (wv.fmt("WvFile: modestring '{0}' must start " +
			    "with r, w, or a", modestring),
		     "modestring");
	    }
	    
	    var f = new FileStream(filename, mode, access, FileShare.ReadWrite);
	    if (truncate)
		f.SetLength(0);
	    return f;
	}
	
        public _WvFile(string filename, string modestring)
	    : base(openstream(filename, modestring))
	{
	}
    }
    
    public class WvFile : WvInBufStream
    {
	public WvFile(string filename, string modestring)
	    : base(new _WvFile(filename, modestring))
	{
	}
    }
}
