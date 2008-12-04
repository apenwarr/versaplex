/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
using System;
using System.Runtime.InteropServices;
 
public class WvDBusServer : IDisposable
{
    [DllImport("wvdbusd.dll")]
    static extern void wvdbusd_start();

    [DllImport("wvdbusd.dll")]
    static extern void wvdbusd_stop();

    [DllImport("wvdbusd.dll", CharSet=CharSet.Ansi)]
    static extern void wvdbusd_listen(string moniker);

    [DllImport("wvdbusd.dll")]
    static extern void wvdbusd_runonce();
    
    [DllImport("wvdbusd.dll")]
    static extern int wvdbusd_check();
    
    public static int check()
    {
	return wvdbusd_check();
    }
    
    public WvDBusServer()
    {
	wvdbusd_start();
    }
    
    public void Dispose()
    {
	wvdbusd_stop();
    }
    
    public void listen(string moniker)
    {
	wvdbusd_listen(moniker);
    }
    
    public void runonce()
    {
	wvdbusd_runonce();
    }
}

