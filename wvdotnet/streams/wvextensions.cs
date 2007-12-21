using System;
using System.Collections;
using System.Text;

public static class WvContExtensions
{
    public static Action ToAction(this IEnumerator ie)
    {
	return new Action(delegate() { 
	    ie.MoveNext();
	});
    }
    
    public static Action ToAction(this IEnumerable aie)
    {
	bool must_reset = false;
	IEnumerator ie = aie.GetEnumerator();
	return new Action(delegate() { 
	    if (must_reset)
		ie = aie.GetEnumerator();
	    must_reset = !ie.MoveNext();
	});
    }
}


public static class WvStreamExtensions
{
    public static byte[] ToUTF8(this Object o)
    {
	return Encoding.UTF8.GetBytes(o.ToString());
    }
    
    public static string FromUTF8(this byte[] bytes)
    {
	return Encoding.UTF8.GetString(bytes);
    }
    
    // FIXME: assumes the write will succeed!  Should only be available on
    // streams with an outbuf.
    public static void print(this WvStream s, string fmt,
			     params object[] args)
    {
	s.write(string.Format(fmt, args).ToUTF8());
    }
    
    public static void print(this WvStream s, string fmt)
    {
	s.write(fmt.ToUTF8());
    }
}

