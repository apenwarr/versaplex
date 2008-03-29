using System;
using System.IO;
using System.Threading;
using Wv.Extensions;

namespace Wv
{
    public class WvLog : WvStream
    {
	public enum L {
	    Critical = 0,
	    Error,
	    Warning,
	    Notice,
	    Info,
	    Debug, Debug1=Debug,
	    Debug2,
	    Debug3,
	    Debug4,
	    Debug5,
	};
	
	static L _maxlevel = L.Info;
	public static L maxlevel { 
	    get { return _maxlevel; }
	    set { _maxlevel = value; }
	}
	static Stream outstr = Console.OpenStandardError();
	
	WvBuf outbuf = new WvBuf();
	byte[] header;
	L level;
	
	private WvLog(byte[] header, L level)
	{
	    this.header = header;
	    this.level = level;
	}
	
	public WvLog(string name, L level)
	    : this(String.Format("<{0}> ", name).ToUTF8(), level)
	    { }
	
	public WvLog(string name)
	    : this(name, L.Info)
	    { }

	public override int write(byte[] buf, int offset, int len)
	{
	    if (level > maxlevel)
		return len; // pretend it's written
	    
	    outbuf.put(buf, (uint)offset, (uint)len);
	    uint i;
	    while ((i = outbuf.strchr('\n')) > 0)
	    {
		outstr.Write(header, 0, header.Length);
		byte[] b = outbuf.get(i);
		outstr.Write(b, 0, b.Length);
	    }
	    return len;
	}
	
	public void print(L level, object s)
	{
	    L old = this.level;
	    this.level = level;
	    print(s);
	    this.level = old;
	}
	
	public void print(L level, string fmt, params object[] args)
	{
	    print(level, (object)String.Format(fmt, args));
	}
	
	public override void print(object o)
	{
	    if (level > maxlevel)
		return;
	    base.print(o);
	}

	public WvLog split(L level)
	{
	    return new WvLog(header, level);
	}
    }
}

