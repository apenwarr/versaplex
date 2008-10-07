using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Linq;
using Wv;
using Wv.Extensions;
using Wv.NDesk.Options;

public static class VersaMain
{
    static WvLog log = new WvLog("Versaplex");
    static VxDbusRouter msgrouter = new VxDbusRouter();
    static WvDBusServer dbusserver;
    static Thread dbusserver_thread = null;
    static ManualResetEvent thread_ready = new ManualResetEvent(false);
    public static bool want_to_die = false;
    
    public static Connection conn;
    static Queue<Action> action_queue = new Queue<Action>();

    static bool MessageReady(Connection conn, Message msg)
    {
        // FIXME: This should really queue things to be run from the thread
        // pool and then the response would be sent back through the action
        // queue
        log.print(WvLog.L.Debug4, "MessageReady\n");

        switch (msg.type)
	{
	case MessageType.MethodCall:
	    if (msg.ifc == "vx.db")
	    {
		action_queue.Enqueue(() => {
		    Message reply;
		    if (msgrouter.route(conn, msg, out reply))
		    {
			if (reply == null) {
			    // FIXME: Do something if this happens, maybe?
			    log.print("Empty reply from RouteMessage\n");
			} else {
			    // XXX: Should this be done further down rather than
			    // passing the reply out here?
			    conn.send(reply);
			}
		    }
		});
		return true;
	    }
	    return false;
	    
	default:
	    log.print(WvLog.L.Warning,
		      "Unexpected DBus message received: #{0} {1}->{2} {3}:{4}.{5}\n",
		        msg.serial, msg.sender, msg.dest,
		      msg.path, msg.ifc, msg.method);
	    return false;
        }
    }
    
    static void _StartDBusServerThread(string[] monikers)
    {
	using (dbusserver = new WvDBusServer())
	{
	    foreach (string m in monikers)
		dbusserver.listen(m);
	    thread_ready.Set();
	    while (!want_to_die)
		dbusserver.runonce();
	}
    }
    
    static void StartDBusServerThread(string[] monikers)
    {
	if (monikers.Length == 0) return;
	thread_ready.Reset();
	dbusserver_thread = new Thread(() => _StartDBusServerThread(monikers));
	dbusserver_thread.Start();
	thread_ready.WaitOne();
    }
    
    static void StopDBusServerThread()
    {
	want_to_die = true;
	if (dbusserver_thread != null)
	    dbusserver_thread.Join();
    }
    
    static void ShowHelp()
    {
	Console.Error.WriteLine
	    ("Usage: versaplexd [-v] [-b dbus-moniker]\n" +
	     "                  [-l listen-moniker]\n" +
	     "                  [-c config-file]");
	Environment.Exit(1);
    }
    
    public static int Main(string[] args)
    {
	WvLog.L verbose = WvLog.L.Info;
	string bus = null;
	string cfgfile = "versaplexd.ini";
	var listeners = new List<string>();
	new OptionSet()
	    .Add("v|verbose", delegate(string v) { ++verbose; })
	    .Add("b=|bus=", delegate(string v) { bus = v; })
		.Add("c=|config=", delegate(string v) { cfgfile = v; })
	    .Add("l=|listen=", delegate(string v) { listeners.Add(v); })
	    .Add("?|h|help", delegate(string v) { ShowHelp(); })
	    .Parse(args);
	
	WvLog.maxlevel = (WvLog.L)verbose;
	
	StartDBusServerThread(listeners.ToArray());

	bool cfgfound = false;

	if (File.Exists(cfgfile))
		cfgfound = true;
        else if (File.Exists("/etc/versaplexd.ini"))
	{
	    log.print("Using /etc/versaplexd.ini for configuration.\n");
	    cfgfound = true;
	    cfgfile = "/etc/versaplexd.ini";
	}

	if (cfgfound == true) {
		VxSqlPool.SetIniFile(cfgfile);
	} else {
		throw new Exception(wv.fmt(
			"Could not find config file '{0}',\n" +
			"and /etc/versaplexd.ini does not exist",
					   cfgfile));
	}
	
	if (bus == null)
	    bus = Connection.session_bus_address;

	if (bus == null)
	{
	    log.print
		("DBUS_SESSION_BUS_ADDRESS not set and no -b option given.\n");
	    ShowHelp();
	}
	
        log.print("Connecting to '{0}'\n", bus);
        conn = new Connection(bus);

        string myNameReq = "vx.versaplexd";
        RequestNameReply rnr = conn.RequestName(myNameReq,
                NameFlag.DoNotQueue);

        switch (rnr) {
            case RequestNameReply.PrimaryOwner:
                log.print("Name registered, ready\n");
                break;
            default:
                log.print("Register name result: \n" + rnr.ToString());
                StopDBusServerThread();
                return 2;
        }

        conn.stream.onclose += () => { 
	    log.print(
	      "***********************************************************\n"+
	      "************ D-bus connection closed by server ************\n"+
	      "***********************************************************\n");
	    want_to_die = true;
	};

	conn.handlers.Insert(0, (m) => MessageReady(conn, m));
	
	while (!want_to_die)
	{
	    log.print(WvLog.L.Debug2, "Event loop.\n");
	    WvStream.runonce(-1);
	    while (action_queue.Count > 0)
		action_queue.Dequeue()();
	}

	StopDBusServerThread();
        log.print("Done!\n");
	return 0;
    }
}

