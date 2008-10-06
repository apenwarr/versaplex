// Copyright 2006 Alp Toker <alp@atoker.com>
// Copyright 2007 Versabanq (Adrian Dewhurst <adewhurst@versabanq.com>)
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Wv.Extensions;

namespace Wv
{
    using Authentication;

    [Flags]
    public enum NameFlag : uint
    {
        None = 0,
        AllowReplacement = 0x1,
        ReplaceExisting = 0x2,
        DoNotQueue = 0x4,
    }

    public enum RequestNameReply : uint
    {
        PrimaryOwner = 1,
        InQueue,
        Exists,
        AlreadyOwner,
    }

    public enum ReleaseNameReply : uint
    {
        Released = 1,
        NonExistent,
        NotOwner,
    }

    public enum StartReply : uint
    {
	Success = 1,    // service was successfully started
        AlreadyRunning, // connection already owns the given name
    }

    public class Connection
    {
	static readonly string DBusName = "org.freedesktop.DBus";
	static readonly string DBusPath = "/org/freedesktop/DBus";

	WvLog log = new WvLog("DBus");
	public WvBufStream stream { get; private set; }
	public bool ok { get { return stream.ok; } }

	public Connection(string address)
	{
	    handlers = new List<Func<Message,bool>>();
	    handlers.Add(default_handler);
	    stream = make_stream(address);
	    stream.onreadable += () => {
		handlemessages(0);
	    };
	    
	    // write the credential byte (needed for passing unix uids over
	    // the unix domain socket, but anyway, part of the protocol
	    // spec).
	    stream.write(new byte[] { 0 });
	    
	    // Run the authentication phase
	    SaslProcess auth = new ExternalAuthClient(this, stream);
	    auth.Run();
	    
            unique_name = CallDBusMethod("Hello");
	}

	static WvBufStream make_stream(string address)
	{
	    WvStream s;
	    WvUrl url = address_to_url(address);
	    if (url.method == "unix")
	    {
		if (url.path.ne())
		    s = new WvUnix(url.path);
		else
		    throw new Exception("No path specified for UNIX transport");
	    }
	    else if (url.method == "tcp")
	    {
		string host = url.host.or("127.0.0.1");
		int port = url.port.or(5555);
		s = new WvTcp(host, (ushort)port);
	    }
	    else
		throw new Exception(wv.fmt("Unknown connection method {0}",
					   url.method));
	    return new WvBufStream(s);
	}
	
	uint serial = 0;
	uint GenerateSerial()
	{
	    return ++serial;
	}

	public Message send_and_wait(Message msg)
	{
	    Message reply = null;
	    
	    send(msg, (r) => { reply = r; });
	    
	    while (reply == null && ok)
		handlemessage(-1);
	    
	    return reply;
	}

	public void send(Message msg, Action<Message> replyaction)
	{
	    msg.ReplyExpected = true;
	    msg.serial = send(msg);
	    rserial_to_action[msg.serial] = replyaction;
	}

	public uint send(Message msg)
	{
	    msg.serial = GenerateSerial();
	    
	    byte[] HeaderData = msg.GetHeaderData();

	    long msgLength = HeaderData.Length + (msg.Body != null ? msg.Body.Length : 0);
	    if (msgLength > Protocol.MaxMessageLength)
		throw new Exception("Message length " + msgLength + " exceeds maximum allowed " + Protocol.MaxMessageLength + " bytes");
	    
	    log.print(WvLog.L.Debug3, "Sending!\n");
	    log.print(WvLog.L.Debug4, "Header:\n{0}",
	                wv.hexdump(HeaderData));
	    log.print(WvLog.L.Debug5, "Body:\n{0}",
		        wv.hexdump(msg.Body));

	    stream.write(HeaderData);
	    if (msg.Body != null && msg.Body.Length != 0)
		stream.write(msg.Body);
	    
	    return msg.serial;
	}
	
	WvBuf inbuf = new WvBuf();
	
	int entrycount = 0;
	void readbytes(int max, int msec_timeout)
	{
	    entrycount++;
	    wv.assert(entrycount == 1);
	    
	    log.print(WvLog.L.Debug5, "Reading: have {0} of needed {1}\n",
		      inbuf.used, max);
	    int needed = max - inbuf.used;
	    if (needed > 0)
	    {
		if (stream.wait(msec_timeout, true, false))
		{
		    WvBytes b = inbuf.alloc(needed);
		    int got = stream.read(b);
		    inbuf.unalloc(needed-got);
		}
	    }
	    entrycount--;
	}
	
	/**
	 * You shouldn't use this, as it bypasses normal message processing.
	 * Add a message handler instead.
	 *
	 * It exists because it's useful in our unit tests.
	 */
	public Message readmessage(int msec_timeout)
	{
	    foreach (int remain in wv.until(msec_timeout))
	    {
		readbytes(16, remain);
		if (inbuf.used < 16)
		    continue;
		
		int needed = Message.bytes_needed(inbuf.peek(16));
		readbytes(needed, remain);
		if (inbuf.used < needed)
		    continue;
		
		return new Message(inbuf.get(needed));
	    }
	    
	    return null;
	}
	
	bool handlemessage(int msec_timeout)
	{
	    var m = readmessage(msec_timeout);
	    if (m != null)
	    {
		// use ToArray() here in case the list changes while
		// we're iterating
		foreach (var handler in handlers.ToArray())
		    if (handler(m))
			return true;
		
		// if we get here, there was a message but nobody could
		// handle it.  That's weird because our default handler
		// should handle *everything*.
		wv.assert(false, "No default message handler?!");
		return true;
	    }
	    return false;
	}
	
	public void handlemessages(int msec_timeout)
	{
	    while (handlemessage(msec_timeout) && ok)
		;
	}
	
	public List<Func<Message,bool>> handlers { get; private set; }

	bool default_handler(Message msg)
	{
	    if (msg == null)
		return false;

	    if (msg.rserial.HasValue)
	    {
		Action<Message> raction 
		    = rserial_to_action.tryget(msg.rserial.Value);
		if (raction != null)
		{
		    raction(msg);
		    return true;
		}
	    }

	    switch (msg.type)
	    {
	    case MessageType.Error:
		//TODO: better exception handling
		string errMsg = String.Empty;
		if (msg.signature.StartsWith("s")) {
		    errMsg = msg.iter().pop();
		}
		Console.Error.WriteLine
		    ("Remote Error: Signature='" + msg.signature
		     + "' " + msg.err + ": " + errMsg);
		return true;
	    case MessageType.Signal:
	    case MessageType.MethodCall:
		// nothing to do with these by default, so give an error
		if (msg.ReplyExpected)
		{
		    var r = msg.err_reply
			("org.freedesktop.DBus.Error.UnknownMethod", 
			 "Unknown dbus method '{0}'.'{1}'",
			 msg.ifc, msg.method);
		    send(r);
		}
		return true;
	    case MessageType.Invalid:
	    default:
		throw new Exception("Invalid message received: MessageType='" + msg.type + "'");
	    }
	}

	Dictionary<uint,Action<Message>> rserial_to_action
	    = new Dictionary<uint,Action<Message>>();

	// Standard D-Bus monikers:
	//   unix:path=whatever,guid=whatever
	//   unix:abstract=whatever,guid=whatever
	//   tcp:host=whatever,port=whatever
	// Non-standard:
	//   wv:wvstreams_moniker
	internal static WvUrl address_to_url(string s)
	{
	    string[] parts = s.Split(new char[] { ':' }, 2);

	    if (parts.Length < 2)
		throw new Exception(wv.fmt("No colon found in '{0}'", s));

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
			throw new Exception(wv.fmt("No '=' found in '{0}'",
						   prop));
		    
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
	
	const string SYSTEM_BUS_ADDRESS 
	    = "unix:path=/var/run/dbus/system_bus_socket";
	public static string system_bus_address
	{
	    get {
		string addr = wv.getenv("DBUS_SYSTEM_BUS_ADDRESS");

		if (addr.e())
		    addr = SYSTEM_BUS_ADDRESS;

		return addr;
	    }
	}

	public static string session_bus_address
	{
	    get {
		return wv.getenv("DBUS_SESSION_BUS_ADDRESS");
	    }
	}

	// FIXME: might as well cache this
	public static Connection session_bus {
	    get {
		if (session_bus_address.e())
		    throw new Exception("DBUS_SESSION_BUS_ADDRESS not set");
		return new Connection(session_bus_address);
	    }
	}
	
        WvAutoCast CallDBusMethod(string method)
        {
            return CallDBusMethod(method, "", new byte[0]);
        }

        WvAutoCast CallDBusMethod(string method, string param)
        {
            MessageWriter w = new MessageWriter();
            w.Write(param);

            return CallDBusMethod(method, "s", w.ToArray());
        }

        WvAutoCast CallDBusMethod(string method, string p1, uint p2)
        {
            MessageWriter w = new MessageWriter();
            w.Write(p1);
            w.Write(p2);

            return CallDBusMethod(method, "su", w.ToArray());
        }

        WvAutoCast CallDBusMethod(string method, string sig, 
            byte[] body)
        {
            Message m = new Message();
            m.signature = sig;
            m.type = MessageType.MethodCall;
            m.ReplyExpected = true;
            m.dest = DBusName;
            m.path = DBusPath;
            m.ifc = DBusName;
            m.method = method;
            m.Body = body;

            Message reply = send_and_wait(m);

            var i = reply.iter();
            return i.pop();
        }

	public string GetUnixUserName(string name)
	{
	    return CallDBusMethod("GetConnectionUnixUserName", name);
	}

	public ulong GetUnixUser(string name)
	{
            return CallDBusMethod("GetUnixUser", name);
	}

	public string GetCert(string name)
	{
            return CallDBusMethod("GetCert", name);
	}

	public string GetCertFingerprint(string name)
	{
            return CallDBusMethod("GetCertFingerprint", name);
	}

	public RequestNameReply RequestName(string name)
	{
	    return RequestName(name, NameFlag.None);
	}

	public RequestNameReply RequestName(string name, NameFlag flags)
	{
            WvAutoCast reply = CallDBusMethod("RequestName", name, (uint)flags);
            return (RequestNameReply)(uint)reply;
	}

	public ReleaseNameReply ReleaseName(string name)
	{
            return (ReleaseNameReply)(uint)CallDBusMethod("ReleaseName", name);
	}

	public bool NameHasOwner(string name)
	{
            return CallDBusMethod("NameHasOwner", name);
	}

	public StartReply StartServiceByName(string name)
	{
	    return StartServiceByName(name, 0);
	}

	public StartReply StartServiceByName(string name, uint flags)
	{
            var retval = CallDBusMethod("StartServiceByName", name, flags);
            return (StartReply)(uint)retval;
	}

	public void AddMatch(string rule)
	{
            CallDBusMethod("AddMatch", rule);
	}

	public void RemoveMatch(string rule)
	{
            CallDBusMethod("RemoveMatch", rule);
	}

	string unique_name;
    }
}
