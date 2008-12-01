using System;
using System.Collections.Generic;
using Wv;
using Wv.NDesk.Options;
using Wv.Extensions;

public static class VersaMain
{
    static void ShowHelp()
    {
	wv.printerr
	    ("Usage: versaplexd [-v] [-b dbus-moniker]\n" +
	     "                  [-l listen-moniker]\n" +
	     "                  [-c config-file]\n" +
	     "                  [-p port]\n");
	Environment.Exit(1);
    }
    
    public static int Main(string[] args)
    {
	try
	{
	    WvLog.L verbose = WvLog.L.Info;
	    string bus = null;
	    string cfgfile = null;
	    var listeners = new List<string>();
	    new OptionSet()
		.Add("v|verbose",
		     delegate(string v) { ++verbose; })
		.Add("b=|bus=", 
		     delegate(string v) { bus = v; })
		.Add("c=|config=",
		     delegate(string v) { cfgfile = v; })
		.Add("l=|listen=",
		     delegate(string v) { listeners.Add(v); })
		.Add("p=|port=",
		     delegate(string v) {
			 bus = "tcp:host=127.0.0.1,port=" + v;
			 listeners.Add("tcp:0.0.0.0:" + v);
		     })
		.Add("?|h|help",
		     delegate(string v) { ShowHelp(); })
		.Parse(args);
	
	    WvLog.maxlevel = (WvLog.L)verbose;
	
	    if (bus.e())
		bus = WvDbus.session_bus_address;
	    
	    if (bus.e())
	    {
		wv.printerr
		("DBUS_SESSION_BUS_ADDRESS not set and no -b option given.\n");
		ShowHelp();
	    }
	    
	    return Versaplexd.Go(cfgfile, bus, listeners.ToArray());
	}
	catch (Exception e)
	{
            wv.printerr("versaplexd: {0}\n", e.Message);
            return 99;
	}
    }
}

