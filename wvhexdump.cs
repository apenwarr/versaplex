using System;
using System.Linq;

namespace Wv
{
    public class WvDelayedString
    {
	Func<string> a;
	
	public WvDelayedString(Func<string> a)
	{
	    this.a = a;
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
}
