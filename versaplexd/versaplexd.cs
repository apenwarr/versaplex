using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Linq;
using Wv;
using Wv.Extensions;
using Wv.NDesk.Options;

public class VxActionTriple
{
    public VxActionTriple(WvDbus _conn, WvDbusMsg _src, Action _action)
    {
	this.conn = _conn;
	this.src = _src;
	this.action = _action;
    }

    public WvDbus conn;
    public WvDbusMsg src;
    public Action action;
}

public static class VersaMain
{
    static WvLog log = new WvLog("Versaplex");
    static VxDbusRouter msgrouter = new VxDbusRouter();
    static WvDBusServer dbusserver;
    static Thread dbusserver_thread = null;
    static ManualResetEvent thread_ready = new ManualResetEvent(false);
    public static bool want_to_die = false;
    
    public static WvDbus conn;
    static List<VxActionTriple> action_queue = new List<VxActionTriple>();

    static Object action_mutex = new Object();
    static VxActionTriple curaction = null;

    private static void HandleCancelQuery(WvDbus conn, WvDbusMsg msg)
    {
	log.print(WvLog.L.Debug4, "Received CancelQuery request\n");
	//FIXME:  Should I be in yet another thread?
	Action perform = null;
	if (msg.signature != "u" && msg.signature != "s")
	{
	    //accept 's' signatures for Perl DBus, which is stupid and can't
	    //send me 'u' parameters, even though the api accepts them.
	    log.print(WvLog.L.Debug4, "CancelQuery:  bad signature {0}\n",
			msg.signature);
	    perform = () => {
		conn.send(msg.err_reply(
			    "org.freedesktop.DBus.Error.UnknownMethod",
			    "No overload of {0} has signature '{1}'",
			    "CancelQuery", msg.signature));
	    };
	}
	else
	{
	    var it = msg.iter();
	    uint tokill;
	    if (msg.signature == "s")
	    {
		log.print(WvLog.L.Debug4,
			    "CancelQuery: converting arg from string\n");
		string temps = it.pop();
		tokill = Convert.ToUInt32(temps);
	    }
	    else
		tokill = it.pop();

	    log.print(WvLog.L.Debug4,
			"CancelQuery: try killing msg id {0}\n", tokill);

	    lock (action_mutex)
	    {
		if (curaction != null && curaction.conn == conn &&
		    curaction.src.serial == tokill)
		{
		    log.print(WvLog.L.Debug4,
				"CancelQuery: killing current action!\n");
			    
		    WvSqlRows_IDataReader.Cancel();
		    curaction = null;
		}
		else
		{
		    log.print(WvLog.L.Debug4,
				"CancelQuery: traversing action queue...\n");
			    
		    //Traverse the action queue, killing stuff
		    foreach (VxActionTriple t in action_queue)
			if (t.conn == conn && t.src.serial == tokill)
			{
			    log.print(WvLog.L.Debug4,
					"CancelQuery: found culprit, killing.\n");
			    //action_queue.Remove(t);
			    //FIXME:  What message should we really put here?
			    t.action = () => {
			    	conn.send(t.src.err_reply("vx.db.sqlerror",
					    "This message got canceled"));
			    };
			    break;
			}
		}
	    }

	    //Pointless return to make Perl happy.
	    perform = () => {
		WvDbusWriter writer = new WvDbusWriter();
		writer.Write("Cancel");
		conn.send(msg.reply("s").write(writer));
	    };
	    log.print(WvLog.L.Debug4, "CancelQuery:  complete\n");
	}

	//FIXME:  It's not clear whether for just add operations, in conjuction
	//with RemoveAt(0) going on in the otherthread, we need a mutex.
	action_queue.Add(new VxActionTriple(conn, msg, perform));
    }

    static bool WvDbusMsgReady(WvDbus conn, WvDbusMsg msg)
    {
        // FIXME: This should really queue things to be run from the thread
        // pool and then the response would be sent back through the action
        // queue
        log.print(WvLog.L.Debug4, "WvDbusMsgReady\n");

        switch (msg.type)
	{
	case Wv.Dbus.MType.MethodCall:
	    if (msg.ifc == "vx.db")
	    {
		if (msg.path == "/db" && msg.method == "CancelQuery")
		    HandleCancelQuery(conn, msg);
		else //not 'CancelQuery'
		{
		    //FIXME:  It's not clear whether for just add operations,
		    //in conjuction with RemoveAt(0) going on in the other
		    //thread, we need a mutex.
		    action_queue.Add(new VxActionTriple(conn, msg,
		    () => {
			WvDbusMsg reply;
			if (msgrouter.route(conn, msg, out reply))
			{
			    if (reply == null) {
				// FIXME: Do something if this happens, maybe?
				log.print("Empty reply from RouteWvDbusMsg\n");
			    } else {
				// XXX: Should this be done further down rather
				// than passing the reply out here?
				conn.send(reply);
			    }
			}
		    }));
		}
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
	wv.assert(WvDBusServer.check() == 42, "wvdbusd.dll test failed");
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
        try {
            return _Main(args);
        }
        catch (Exception e) {
            wv.printerr("versaplexd: {0}\n", e.Message);
            return 99;
        }
	finally {
	    StopDBusServerThread();
	}
    }
    
    static int _Main(string[] args)
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
	    throw new Exception
		(wv.fmt("Could not find config file '{0}',\n" +
			"and /etc/versaplexd.ini does not exist",
			cfgfile));
	}
	
	if (bus == null)
	    bus = WvDbus.session_bus_address;

	if (bus == null)
	{
	    log.print
		("DBUS_SESSION_BUS_ADDRESS not set and no -b option given.\n");
	    ShowHelp();
	}
	
        log.print("Connecting to '{0}'\n", bus);
        conn = new WvDbus(bus);

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

	conn.handlers.Insert(0, (m) => WvDbusMsgReady(conn, m));
	
	while (!want_to_die)
	{
	    log.print(WvLog.L.Debug2, "Event loop.\n");
	    WvStream.runonce(-1);
	    while (action_queue.Count > 0)
	    {
		log.print(WvLog.L.Debug2, "Action queue.\n");
		lock (action_mutex)
		{
		    curaction = action_queue.First();
		    action_queue.RemoveAt(0);
		}
		curaction.action();
		lock (action_mutex)
		{
		    curaction = null;
		}
		log.print(WvLog.L.Debug2, "Action queue element done.\n");
	    }
	}

	StopDBusServerThread();
        log.print("Done!\n");
	return 0;
    }
}

