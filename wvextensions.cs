using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace Wv.Extensions
{
    public static class StreamHelper
    {
	public static void Write(this Stream s, byte[] buffer)
	{
	    s.Write(buffer, 0, buffer.Length);
	}
    }
    
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
	
	public static string ToHex(this byte[] bytes)
	{
	    StringBuilder sb = new StringBuilder();
	    foreach (byte b in bytes)
		sb.Append(b.ToString("X2"));
	    return sb.ToString();
	}
    }
    
    public static class DictExtensions
    {
	public static string getstr<T1,T2>(this Dictionary<T1,T2> dict,
					   T1 key)
	{
	    if (dict.ContainsKey(key))
		return dict[key].ToString();
	    else
		return "";
	}
	
	public static bool has<T1,T2>(this Dictionary<T1,T2> dict,
					   T1 key)
	{
	    return dict.ContainsKey(key);
	}
    }
    
    public static class DataExtensions
    {
	public static IEnumerable<T2> map<T1,T2>(this IEnumerable<T1> list,
					  Func<T1,T2> f)
	{
	    foreach (T1 t in list)
		yield return f(t);
	}
	
	public static string[] ToStringArray<T>(this IEnumerable<T> l)
	{
	    List<string> tmp = new List<string>();
	    foreach (T t in l)
		tmp.Add(t.ToString());
	    return tmp.ToArray();
	}
	
	public static T only<T>(this IEnumerable<T> l)
	    where T: class
	{
	    foreach (T t in l)
		return t;
	    return (T)null;
	}
	
	public static int atoi(this object o)
	{
	    return wv.atoi(o);
	}

	public static double atod(this object o)
	{
	    return wv.atod(o);
	}
	
	public static WvAutoCast[] ToWvAutoCasts(this IDataRecord r)
	{
	    int max = r.FieldCount;
	    
	    object[] oa = new object[max];
	    r.GetValues(oa);
	    
	    WvAutoCast[] a = new WvAutoCast[max];
	    for (int i = 0; i < max; i++)
		a[i] = new WvAutoCast(oa[i]);
	    
	    return a;
	}

	public static IEnumerable<WvAutoCast[]>
	    ToWvAutoReader(this IDataReader e)
	{
	    while (e.Read())
		yield return e.ToWvAutoCasts();
	}
    }
}
