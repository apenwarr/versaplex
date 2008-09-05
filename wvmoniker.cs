using System;
using System.Collections.Generic;
using Wv;
using Wv.Extensions;

namespace Wv
{
    public class WvMoniker<T>
    {
	static List<WvMoniker<T>> registry = new List<WvMoniker<T>>();
	string prefix;
	Func<string,object,T> func;
	
	public WvMoniker(string prefix, Func<string,object,T> func)
	{
	    this.prefix = prefix;
	    this.func = func;
	    registry.Add(this);
	}
	
	// probably nobody will ever call this
	public void unregister()
	{
	    registry.Remove(this);
	}
	
	public static WvMoniker<T> find(string prefix)
	{
	    foreach (WvMoniker<T> m in registry)
		if (m.prefix == prefix)
		    return m;
	    return null;
	}
	
	public static T create(string moniker, object o)
	{
	    int pos = moniker.IndexOf(':');
	    string prefix, suffix;
	    if (pos >= 0)
	    {
		prefix = moniker.Substring(0, pos);
		suffix = moniker.Substring(pos+1);
	    }
	    else
	    {
		prefix = moniker;
		suffix = "";
	    }
	    
	    WvMoniker<T> m = find(prefix);
	    if (m == null)
		return default(T);
	    else
		return m.func(suffix, o);
	}
	
	public static T create(string moniker)
	{
	    return create(moniker, null);
	}
    }
}
