using System;
using System.IO;

namespace Wv
{
    public class WvDelayedString
    {
	Func<string> a;
	
	public WvDelayedString(Func<string> a)
	{
	    this.a = a;
	    a.ToString();
	}
	
	public override string ToString()
	{
	    return a();
	}
    }

    public partial class wv
    {
	public static string _hexdump(byte[] data,
				      int startoffset, int maxlen)
	{
	    WvBuf buf = new WvBuf();
	    
	    // This is overly complicated so that the body and header of
	    // the same buffer can be printed separately yet still show the
	    // proper alignment
 	    
	    int length = data.Length > maxlen ? maxlen : data.Length;
	    int rowoffset = startoffset & (~0xf);
	    int coloffset = startoffset & 0xf;
	    
	    int cnt = rowoffset;
	    for (int i=0; i < length; cnt += 16) {
		buf.put("{0} ", cnt.ToString("x4"));
		
		int co=0;
		if (coloffset > 0 && i == 0) {
		    for (int j=0; j < coloffset; j++)
			buf.put("   ");
		    
		    co=coloffset;
		}
		
		// Print out the hex digits
		for (int j=0; j < 8-co && i+j < length; j++)
		    buf.put("{0} ", data[i+j].ToString("x2"));
		
		buf.put(" ");
		
		for (int j=8-co; j < 16-co && i+j < length; j++)
		    buf.put("{0} ", data[i+j].ToString("x2"));
		
		// extra space if incomplete line
		if (i + 16-co > length) {
		    for (int j = length - i; j < 16-co; j++)
			buf.put("   ");
		}
		
		if (co > 0) {
		    for (int j=0; j < co; j++)
			buf.put(" ");
		}
		
		for (int j=0; j < 16-co && i+j < length; j++) {
		    if (31 < data[i+j] && data[i+j] < 127) {
			buf.put((char)data[i+j]);
		    } else {
			buf.put('.');
		    }
		}
		
		buf.put("\n");
		
		i += 16-co;
	    }
	
	    return buf.getstr();
	}
	
	public static object hexdump(byte[] data,
				     int startoffset, int maxlen)
	{
	    return new WvDelayedString(
			   () => _hexdump(data, startoffset, maxlen));
	}
	    
	public static object hexdump(byte[] data)
	{
	    return hexdump(data, 0, data.Length);
	}
    }
    
    
    public class WvLog: WvStream
    {
	public enum Level {
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
	
	public static Level maxlevel { get; set; }
	static Stream outstr = Console.OpenStandardError();
	
	WvBuf outbuf = new WvBuf();
	byte[] header;
	Level level;
	
	private WvLog(byte[] header, Level level)
	{
	    this.header = header;
	    this.level = level;
	}
	
	public WvLog(string name, Level level)
	    : this(String.Format("<{0}> ", name).ToUTF8(), level)
	    { }
	
	public WvLog(string name)
	    : this(name, Level.Info)
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
	
	public void print(Level level, string s)
	{
	    Level old = this.level;
	    this.level = level;
	    print(s);
	    this.level = old;
	}
	
	public void print(Level level, string fmt, params object[] args)
	{
	    print(level, String.Format(fmt, args));
	}
	
	public WvLog split(Level level)
	{
	    return new WvLog(header, level);
	}
    }
}

