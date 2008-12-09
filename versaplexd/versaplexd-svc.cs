/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.IO;
using System.ServiceProcess;
using System.Threading;
using Wv;
using Wv.NDesk.Options;
using Wv.Extensions;


[RunInstaller(true)]
public class VersaServiceInstaller : Installer
{
    public static bool auth = false;
    ServiceProcessInstaller spi;
    ServiceInstaller si; 

    public VersaServiceInstaller()
    {
	spi = new ServiceProcessInstaller(); 
	si = new ServiceInstaller(); 

	// if auth, leave these alone and the ServiceInstaller should prompt
	// for authorization.
	if (!auth)
	{
	    spi.Account = ServiceAccount.LocalSystem; 
	    spi.Password = null;
	    spi.Username = null;
	}

	si.ServiceName = "Versaplex";
	si.Description = "Provides a secure connection to SQL Server";
	si.StartType = ServiceStartMode.Automatic;
	
	Installers.AddRange(new Installer[] { si, spi });
    }
}


public class VersaService : ServiceBase
{
    WvLog log = new WvLog("versaplexd-svc", WvLog.L.Info);
    Thread t = null;
    
    public VersaService()
    {
	WvLog.recv = new WvLogFile(wv.PathJoin(VersaMain.mydir,
					       "versaplex.log"),
				   "a");
	log.print("Initializing.\n");
    }
    
    void go()
    {
	try
	{
	    Versaplexd.Go(VersaMain.config_path,
			  "tcp:host=127.0.0.1,port=5561",
			  new string[] { "tcp:0.0.0.0:5561" });
	}
	catch (Exception e)
	{
	    log.print(WvLog.L.Error, "Exception: {0}\n", e.Message);
	}
    }
    
    protected override void OnStart(string[] args)
    { 
	log.print("Starting.\n");
	wv.assert(t == null);
	t = new Thread(go);
	t.Start();
    }
    
    protected override void OnStop()
    { 
	log.print("Stopping...\n");
	wv.assert(t != null);
	Versaplexd.want_to_die = true;
	
	t.Join();
	log.print("Stopped.\n");
    }
    
    protected override void OnContinue()
    {
	log.print("Continuing.");
    }
}


public static class VersaMain
{
    static void ShowHelp()
    {
	wv.printerr("Usage: versaplexd-svc <-i | -u>\n" +
		    "        -i: install as a Windows service\n" +
		    "        -u: uninstall Windows service\n" +
		    "        -A: prompt for account/password info\n");
	Environment.Exit(1);
    }
    
    static void Uninstall()
    {
	using (var inst 
	       = new AssemblyInstaller(typeof(VersaService).Assembly,
				       new string[] { "/logfile" }))
	{
	    inst.UseNewContext = true;
	    inst.Uninstall(new Hashtable());
	}
    }
    
    static void Install()
    {
	// We might already be installed, so uninstall first.  But we might
	// *not* be installed, which is an error, so ignore it.
	try { Uninstall(); } catch { }
	
	using (var inst 
	       = new AssemblyInstaller(typeof(VersaService).Assembly,
				       new string[] { "/logfile" }))
	{
	    inst.UseNewContext = true;

	    IDictionary state = new Hashtable();
	    try
	    {
		inst.Install(state);
		inst.Commit(state);
		(new ServiceController("Versaplex")).Start();
	    }
	    catch
	    {
		try { inst.Rollback(state); } catch { }
		throw;
	    }
	}
    }

    internal static string mydir 
	= Path.GetDirectoryName(typeof(VersaMain).Assembly.Location);
    internal static string config_path 
	= wv.PathJoin(mydir, "versaplexd.ini");
    
    public static int Main(string[] args)
    {
	try
	{
	    bool install = false, uninstall = false;
	    new OptionSet()
		.Add("i|install",
		     delegate(string v) { install = true; })
		.Add("u|uninstall", 
		     delegate(string v) { uninstall = true; })
		.Add("A|authorize",
		     delegate(string v) { VersaServiceInstaller.auth = true; })
		.Add("?|h|help",
		     delegate(string v) { ShowHelp(); })
		.Parse(args);
	    
	    if (install)
	    {
		
		if (!File.Exists(config_path))
		    throw new Exception(wv.fmt("Config file missing: {0}",
					       config_path));
		Install();
	    }
	    else if (uninstall)
		Uninstall();
	    else
		ServiceBase.Run(new ServiceBase[] { new VersaService() });
	    return 0;
	}
	catch (Exception e)
	{
            wv.printerr("versaplex: {0}\n", e.Message);
            return 99;
	}
    }
}
