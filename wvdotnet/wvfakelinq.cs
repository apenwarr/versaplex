using System;
using System.Collections;
using System.Collections.Generic;

namespace Wv.FakeLinq
{
    public delegate void WvAction();
    public delegate void WvAction<T1>(T1 t1);
    public delegate void WvAction<T1,T2>(T1 t1, T2 t2);
    public delegate void WvAction<T1,T2,T3>(T1 t1, T2 t2, T3 t3);
    public delegate R WvFunc<R>();
    public delegate R WvFunc<T1,R>(T1 t1);
    public delegate R WvFunc<T1,T2,R>(T1 t1, T2 t2);
    public delegate R WvFunc<T1,T2,T3,R>(T1 t1, T2 t2, T3 t3);
    
    public static class WvFakeLinq
    {
	public static bool Contains(this IEnumerable ie, object o)
	{
	    foreach (object t in ie)
		if (t == o)
		    return true;
	    return false;
	}
	
	public static IEnumerable<T> Where<T>(this IEnumerable<T> ie,
					      WvFunc<T,bool> f)
	{
	    foreach (T t in ie)
		if (f(t))
		    yield return t;
	}
	
	public static List<T> ToList<T>(this IEnumerable<T> ie)
	{
	    List<T> l = new List<T>();
	    foreach(T t in ie)
		l.Add(t);
	    return l;
	}
	
	public static T[] ToArray<T>(this IEnumerable<T> ie)
	{
	    List<T> l = ie.ToList();
	    T[] a = new T[l.Count];
	    for (int i = 0; i < a.Length; i++)
		a[i] = l[i];
	    return a;
	}
	
	public static IEnumerable<U> Select<T,U>(this IEnumerable<T> ie,
						 WvFunc<T,U> f)
	{
	    foreach (T t in ie)
		yield return f(t);
	}
	
	public static U Aggregate<T,U>(this IEnumerable<T> ie,
			       WvFunc<U,T,U> f)
	{
	    U x = default(U);
	    foreach (T t in ie)
		x = f(x, t);
	    return x;
	}
	
	public static int Sum(this IEnumerable<int> ie)
	{
	    return Aggregate(ie, (int prev, int cur) => (prev+cur));
	}
	
	public static long Sum(this IEnumerable<long> ie)
	{
	    return Aggregate(ie, (long prev, long cur) => (prev+cur));
	}
	
	public static int Count<T>(this IEnumerable<T> ie)
	{
	    return Aggregate(ie, (int prev, T cur) => (prev+1));
	}
	
	public static IEnumerable<U> Cast<U>(this IEnumerable ie)
	{
	    foreach (U u in ie)
		yield return u;
	}
	
	public static T First<T>(this IEnumerable<T> ie)
	{
	    foreach (T t in ie)
		return t;
	    return default(T);
	}
	
	public static IEnumerable<T> Union<T>(this IEnumerable<T> a,
					      IEnumerable<T> b)
	{
	    Dictionary<T,T> d = new Dictionary<T,T>();
	    foreach (T t in a)
		d[t] = t;
	    foreach (T t in b)
		d[t] = t;
	    return d.Keys;
	}
	
	public static T ElementAt<T>(this IEnumerable<T> ie, int at)
	{
	    int x = 0;
	    foreach (T t in ie)
		if (x++ == at)
		    return t;
	    return default(T);
	}
    }
}
