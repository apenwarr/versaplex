using System;
using System.Collections;
using System.Text;

namespace Wv.Extensions
{
    public static class ExceptionHelper
    {
	public static string Short(this Exception e)
	{
	    if (e == null)
		return "Success";
	    else
		return e.Message;
	}
    }
    
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
    }
}
