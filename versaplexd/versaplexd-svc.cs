using System;
using System.Collections.Generic;
using Wv;
using Wv.NDesk.Options;
using Wv.Extensions;

public static class VersaService
{
    static void ShowHelp()
    {
	wv.printerr("Usage: versaplexd-svc <-i | -u>\n" +
		    "        -i: install as a Windows service" +
		    "        -u: uninstall Windows service");
	Environment.Exit(1);
    }
    
    public static int Main(string[] args)
    {
	try
	{
	    WvLog.maxlevel = WvLog.L.Info;
	    return Versaplexd.Go("versaplexd.ini",
				 "tcp:host=127.0.0.1,port=5561",
				 new string[] { "tcp:0.0.0.0:5561" });
	}
	catch (Exception e)
	{
            wv.printerr("versaplex: {0}\n", e.Message);
            return 99;
	}
    }
}

