using System;
using System.Net.Sockets;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

public interface IWvEventer
{
    void onreadable(Socket s, Action a);
    void onwritable(Socket s, Action a);
    void addtimeout(Object cookie, DateTime t, Action a);
    void deltimeout(Object cookie);
}

public class WvEventer : IWvEventer
{
    Dictionary<Socket, Action> 
	r = new Dictionary<Socket, Action>(),
        w = new Dictionary<Socket, Action>();
    
    class TimeAction
    {
	public DateTime t;
	public Action a;
	
	public TimeAction(DateTime t, Action a)
	{
	    this.t = t;
	    this.a = a;
	}
    }
    Dictionary<Object, TimeAction>
	ta = new Dictionary<Object, TimeAction>();
    
    public WvEventer()
    {
    }
    
    public void onreadable(Socket s, Action a)
    {
	if (s == null) return;
	r.Remove(s);
	if (a != null)
	    r.Add(s, a);
    }
    
    public void onwritable(Socket s, Action a)
    {
	if (s == null) return;
	w.Remove(s);
	if (a != null)
	    w.Add(s, a);
    }
    
    public void addtimeout(Object cookie, DateTime t, Action a)
    {
	ta.Remove(cookie);
	if (a != null)
	    ta.Add(cookie, new TimeAction(t, a));
    }
	
    public void deltimeout(Object cookie)
    {
	ta.Remove(cookie);
    }
    
    public void run()
    {
	IList<Socket> rlist = r.Keys.ToList();
	IList<Socket> wlist = w.Keys.ToList();
	IList<TimeAction> talist = ta.Values.ToList();
	TimeAction first 
	    = new TimeAction(DateTime.Now
			     + TimeSpan.FromMilliseconds(1000000), null);
	
	foreach (TimeAction t in talist)
	    if (t.t < first.t)
		first = t;
	
	TimeSpan timeout = first.t - DateTime.Now;
	if (timeout < TimeSpan.Zero)
	    timeout = TimeSpan.Zero;
	
	if (rlist.Count == 0 && wlist.Count == 0)
	{
	    // Socket.Select throws an exception if all lists are empty;
	    // idiots.
	    if (timeout > TimeSpan.Zero)
		Thread.Sleep((int)timeout.TotalMilliseconds);
	}
	else
	{
	    Socket.Select((IList)rlist, (IList)wlist, null,
			  (int)timeout.TotalMilliseconds * 1000);
	    DateTime now = DateTime.Now;
	    foreach (Socket s in rlist)
		r[s]();
	    foreach (Socket s in wlist)
		w[s]();
	    foreach (Object cookie in ta.Keys)
	    {
		TimeAction t = ta[cookie];
		if (t.t <= now)
		{
		    t.a();
		    ta.Remove(cookie);
		}
	    }
	}
    }
}

