// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Text;
using System.Collections.Generic;
using Wv.Extensions;

namespace Wv
{
    public class BadAddressException : Exception
    {
	public BadAddressException(string fmt, params object[] args)
	    : base(wv.fmt(fmt, args))
	    { }
    }
    
    public class Address
    {
	// Standard D-Bus monikers:
	//   unix:path=whatever,guid=whatever
	//   unix:abstract=whatever,guid=whatever
	//   tcp:host=whatever,port=whatever
	// Non-standard:
	//   wv:wvstreams_moniker
	public static WvUrl Parse(string s)
	{
	    string[] parts = s.Split(new char[] { ':' }, 2);

	    if (parts.Length < 2)
		throw new BadAddressException("No colon found in '{0}'", s);

	    string method = parts[0];
	    string user = null, pass = null, host = null, path = null;
	    int port = 0;
	    
	    if (method == "wv")
		path = parts[1];
	    else
	    {
		foreach (string prop in parts[1].Split(new char[] { ',' }, 2))
		{
		    string[] propa = prop.Split(new char[] { '=' }, 2);
		    
		    if (propa.Length < 2)
			throw new BadAddressException("No '=' found in '{0}'",
						      prop);
		    
		    string name = propa[0];
		    string value = propa[1];
		    
		    if (name == "path")
			path = value;
		    else if (name == "abstract")
			path = "@" + value;
		    else if (name == "guid")
			pass = value;
		    else if (name == "host")
			host = value;
		    else if (name == "port")
			port = value.atoi();
		    // else ignore it silently; extra properties might be used
		    // in newer versions for backward compatibility
		}
	    }

	    return new WvUrl(method, user, pass, host, port, path);
	}
	
	const string SYSTEM_BUS_ADDRESS = "unix:path=/var/run/dbus/system_bus_socket";
	public static string System
	{
	    get {
		string addr = wv.getenv("DBUS_SYSTEM_BUS_ADDRESS");

		if (addr.e())
		    addr = SYSTEM_BUS_ADDRESS;

		return addr;
	    }
	}

	public static string Session
	{
	    get {
		return wv.getenv("DBUS_SESSION_BUS_ADDRESS");
	    }
	}
    }
}
