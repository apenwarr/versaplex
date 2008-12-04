/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
using System;
using System.Net.Sockets;
using System.Collections;
using System.Collections.Generic;
using Wv.FakeLinq;
using System.Threading;

public interface IWvEventer
{
    void runonce(int msec_timeout);
    void runonce();
    void onreadable(Socket s, WvAction a);
    void onwritable(Socket s, WvAction a);
    void addpending(Object cookie, WvAction a);
    void delpending(Object cookie);
    void addtimeout(Object cookie, DateTime t, WvAction a);
    void deltimeout(Object cookie);
}

public class WvEventer : IWvEventer
{
    // CAREFUL! The 'pending' structure might be accessed from other threads!
    Dictionary<object, WvAction> 
	pending = new Dictionary<object, WvAction>();
    
    Dictionary<Socket, WvAction> 
	r = new Dictionary<Socket, WvAction>(),
        w = new Dictionary<Socket, WvAction>();
    
    class TimeAction
    {
	public DateTime t;
	public WvAction a;
	
	public TimeAction(DateTime t, WvAction a)
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
    
    public void onreadable(Socket s, WvAction a)
    {
	if (s == null) return;
	r.Remove(s);
	if (a != null)
	    r.Add(s, a);
    }
    
    public void onwritable(Socket s, WvAction a)
    {
	if (s == null) return;
	w.Remove(s);
	if (a != null)
	    w.Add(s, a);
    }
    
    public void addtimeout(Object cookie, DateTime t, WvAction a)
    {
	ta.Remove(cookie);
	if (a != null)
	    ta.Add(cookie, new TimeAction(t, a));
    }
    
    // NOTE: 
    // This is the only kind of event you can enqueue from a thread other
    // than the one doing runonce()!
    // It will run your WvAction in the runonce() thread on the next pass.
    public void addpending(Object cookie, WvAction a)
    {
	lock(pending)
	{
	    pending.Remove(cookie);
	    pending.Add(cookie, a);
	}
    }
    
    public void delpending(Object cookie)
    {
	lock(pending)
	{
	    pending.Remove(cookie);
	}
    }
	
    public void deltimeout(Object cookie)
    {
	ta.Remove(cookie);
    }
    
    public void runonce()
    {
	runonce(-1);
    }
    
    public void runonce(int msec_timeout)
    {
	// Console.WriteLine("Pending: {0}", pending.Count);
	
	IList<Socket> rlist = r.Keys.ToList();
	IList<Socket> wlist = w.Keys.ToList();
	IList<TimeAction> talist = ta.Values.ToList();
	if (msec_timeout < 0)
	    msec_timeout = 1000000;
	TimeAction first 
	    = new TimeAction(DateTime.Now
			     + TimeSpan.FromMilliseconds(msec_timeout), null);
	
	foreach (TimeAction t in talist)
	    if (t.t < first.t)
		first = t;
	
	TimeSpan timeout = first.t - DateTime.Now;
	if (timeout < TimeSpan.Zero)
	    timeout = TimeSpan.Zero;
	
	lock(pending)
	{
	    if (pending.Count > 0)
		timeout = TimeSpan.Zero;
	}
	
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
	}
	
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
	
	WvAction[] nowpending;
	lock(pending)
	{
	    nowpending = pending.Values.ToArray();
	    pending.Clear();
	}
	// Console.WriteLine("NowPending: {0}", nowpending.Length);
	foreach (WvAction a in nowpending)
	    a();
    }
}

