// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.IO;

namespace Wv.Transports
{
    public abstract class Transport
    {
	public static Transport Create(AddressEntry ae)
	{
	    switch (ae.Method)
	    {
	    case "tcp":
		return new SocketTransport(ae);
	    case "unix":
		return new UnixNativeTransport(ae);
	    default:
		throw new NotSupportedException
		    ("Transport method \"" + ae.Method 
		     + "\" not supported");
	    }
	}

	public Connection Connection;

	//TODO: design this properly

	//this is just a temporary solution
	public Stream Stream;
	public abstract string AuthString();
	public abstract void WriteCred();
    }
}
