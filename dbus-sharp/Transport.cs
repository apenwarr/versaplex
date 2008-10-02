// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using Wv;

namespace Wv.Transports
{
    public abstract class Transport
    {
	public static Transport Create(AddressEntry ae)
	{
	    return new DodgyTransport(ae);
	}

	public Connection Connection;

	public virtual void wait(int msec_timeout)
	{
	    // by default, no need to wait, since read/write are blocking
	    // anyway
	    return; 
	}
	
	public abstract void write(WvBytes b);
	public abstract int read(WvBytes b);
	public abstract string AuthString();
	public abstract void WriteCred();
    }
}
