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

