using System;
using System.Text;
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
	public static string _hexdump(WvBytes b)
	{
	    byte[] data = b.bytes;
	    int startoffset = b.start;
	    int maxlen = b.start + b.len;
	    
	    if (data == null)
		return "(nil)";
	    
            var sb = new StringBuilder();
	    
	    // This is overly complicated so that the body and header of
	    // the same buffer can be printed separately yet still show the
	    // proper alignment
 	    
	    int length = data.Length > maxlen ? maxlen : data.Length;
	    int rowoffset = startoffset & (~0xf);
	    int coloffset = startoffset & 0xf;
	    
            // 16 bytes of input turns into 70 bytes of output, plus newline, 
            int linelen = 71;
            // If we have to deal with more than 2^16 bytes of data, the
            // leading count number will take more space
            if (length > 1<<16)
                linelen += 4;

            // Add an extra line since we might have partial lines or such.
            int result_size_est = ((length / 16) + 1) * linelen; 
            // Note: it's important to set the right capacity when dealing 
            // with large quantities of data.
            sb.EnsureCapacity(result_size_est);

	    int cnt = rowoffset;
	    for (int i=0; i < length; cnt += 16) {
		sb.Append(cnt.ToString("x4")).Append(" ");
		
		int co=0;
		if (coloffset > 0 && i == 0) {
		    for (int j=0; j < coloffset; j++)
			sb.Append("   ");
		    
		    co=coloffset;
		}
		
		// Print out the hex digits
		for (int j=0; j < 8-co && i+j < length; j++)
		    sb.Append(data[i+j].ToString("x2")).Append(" ");
		
		sb.Append(" ");
		
		for (int j=8-co; j < 16-co && i+j < length; j++)
		{
		    if (i+j < 0) continue;
		    sb.Append(data[i+j].ToString("x2")).Append(" ");
		}
		
		// extra space if incomplete line
		if (i + 16-co > length) {
		    for (int j = length - i; j < 16-co; j++)
			sb.Append("   ");
		}
		
		if (co > 0) {
		    for (int j=0; j < co; j++)
			sb.Append(" ");
		}
		
		for (int j=0; j < 16-co && i+j < length; j++) {
		    if (31 < data[i+j] && data[i+j] < 127) {
			sb.Append((char)data[i+j]);
		    } else {
			sb.Append('.');
		    }
		}
		
		sb.Append("\n");
		
		i += 16-co;
	    }
	
	    return sb.ToString();
	}
	
	public static object hexdump(WvBytes b)
	{
	    return new WvDelayedString(() => _hexdump(b));
	}
    }
}
