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
    using Transports;

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
	WvLog log = new WvLog("DBusConn");
	public Transport transport;

	// FIXME: There should be a better way to hack in a socket
	// created elsewhere
	public Connection()
	{
	    OnMessage = HandleMessage;
	}

	public Connection(Transport transport)
	{
	    OnMessage = HandleMessage;
	    this.transport = transport;
	    transport.Connection = this;
	    
	    Authenticate();
	    Register();
	}

	public Connection(string address)
	{
	    OnMessage = HandleMessage;
	    OpenPrivate(address);
	    Authenticate();
	    Register();
	}

	//should we do connection sharing here?
	public static Connection Open(string address)
	{
	    Connection conn = new Connection();
	    conn.OpenPrivate(address);
	    conn.Authenticate();

	    return conn;
	}

	public static Connection Open(Transport transport)
	{
	    Connection conn = new Connection(transport);
	    conn.Authenticate();

	    return conn;
	}

	internal void OpenPrivate(string address)
	{
	    if (address == null)
		throw new ArgumentNullException("address");

	    AddressEntry[] entries = Address.Parse(address);
	    if (entries.Length == 0)
		throw new Exception("No addresses were found");

	    //TODO: try alternative addresses if needed
	    AddressEntry entry = entries[0];

	    transport = Transport.Create(entry);
	}

	internal void Authenticate()
	{
	    if (transport != null)
		transport.WriteCred();

	    SaslProcess auth = new ExternalAuthClient(this);
	    auth.Run();
	    isAuthenticated = true;
	}

	bool isAuthenticated = false;
	internal bool IsAuthenticated
	{
	    get {
		return isAuthenticated;
	    }
	}

	uint serial = 0;
	uint GenerateSerial()
	{
	    return ++serial;
	}

	public Message SendWithReplyAndBlock(Message msg)
	{
	    Message reply = null;
	    
	    SendWithReply(msg, (r) => { reply = r; });
	    
	    while (reply == null)
		HandleMessage(ReadMessage());
	    
	    return reply;
	}

	internal void SendWithReply(Message msg, Action<Message> replyaction)
	{
	    msg.ReplyExpected = true;
	    msg.serial = GenerateSerial();
	    rserial_to_action[msg.serial] = replyaction;
	    WriteMessage(msg);
	}

	public uint Send(Message msg)
	{
	    msg.serial = GenerateSerial();
	    WriteMessage(msg);
	    return msg.serial;
	}

	internal void WriteMessage(Message msg)
	{
	    byte[] HeaderData = msg.GetHeaderData();

	    long msgLength = HeaderData.Length + (msg.Body != null ? msg.Body.Length : 0);
	    if (msgLength > Protocol.MaxMessageLength)
		throw new Exception("Message length " + msgLength + " exceeds maximum allowed " + Protocol.MaxMessageLength + " bytes");
	    
	    log.print(WvLog.L.Debug3, "Sending!\n");
	    log.print(WvLog.L.Debug4, "Header:\n{0}",
	                wv.hexdump(HeaderData));
	    log.print(WvLog.L.Debug5, "Body:\n{0}",
		        wv.hexdump(msg.Body));

	    transport.write(HeaderData);
	    if (msg.Body != null && msg.Body.Length != 0)
		transport.write(msg.Body);
	}
	
	WvBuf inbuf = new WvBuf();
	
	void readbytes(int max)
	{
	    log.print(WvLog.L.Debug5, "Reading: have {0} of needed {1}\n",
		      inbuf.used, max);
	    int needed = max - inbuf.used;
	    if (needed <= 0) return;
	    
	    WvBytes b = inbuf.alloc(needed);
	    int got = transport.read(b);
	    inbuf.unalloc(needed-got);
	}
	
	Message _ReadMessage()
	{
	    if (inbuf.used < 16)
		return null;
	    int needed = Message.bytes_needed(inbuf.peek(16));
	    if (inbuf.used < needed)
		return null;
	    return new Message(inbuf.get(needed));
	}
	
	// FIXME: a blocking Read isn't particularly useful
	public Message ReadMessage()
	{
	    while (inbuf.used < 16)
		readbytes(16);
	    if (inbuf.used < 16)
		return null;
	    int needed = Message.bytes_needed(inbuf.peek(16));
	    while (inbuf.used < needed)
		readbytes(needed);
	    if (inbuf.used < needed)
		return null;
	    return _ReadMessage();
	}
	
	void handlemessages()
	{
	    Message m;
	    while ((m = _ReadMessage()) != null)
		OnMessage(m);
	}
	
	// used only for versaplexd - FIXME remove it eventually
	public int DoBytes(WvBytes b)
	{
	    inbuf.put(b);
	    handlemessages();
	    
	    if (inbuf.used < 16)
		return 16;
	    int needed = Message.bytes_needed(inbuf.peek(16));
	    return needed - inbuf.used;
	}

	// hacky
	public delegate void MessageHandler(Message msg);
	public MessageHandler OnMessage;

	internal void HandleMessage(Message msg)
	{
	    if (msg == null)
		return;

	    if (msg.rserial.HasValue)
	    {
		Action<Message> raction 
		    = rserial_to_action.tryget(msg.rserial.Value);
		if (raction != null)
		{
		    raction(msg);
		    return;
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
		break;
	    case MessageType.Signal:
	    case MessageType.MethodCall:
		// nothing to do with these by default
		break;
	    case MessageType.Invalid:
	    default:
		throw new Exception("Invalid message received: MessageType='" + msg.type + "'");
	    }
	}

	Dictionary<uint,Action<Message>> rserial_to_action
	    = new Dictionary<uint,Action<Message>>();

	static Connection()
	{
	    if (BitConverter.IsLittleEndian)
		NativeEndianness = EndianFlag.Little;
	    else
		NativeEndianness = EndianFlag.Big;
	}

	public static readonly EndianFlag NativeEndianness;
	static readonly string DBusName = "org.freedesktop.DBus";
	static readonly string DBusPath = "/org/freedesktop/DBus";

        private WvAutoCast CallDBusMethod(string method)
        {
            return CallDBusMethod(method, "", new byte[0]);
        }

        private WvAutoCast CallDBusMethod(string method, string param)
        {
            MessageWriter w = new MessageWriter();
            w.Write(param);

            return CallDBusMethod(method, "s", w.ToArray());
        }

        private WvAutoCast CallDBusMethod(string method, string p1, uint p2)
        {
            MessageWriter w = new MessageWriter();
            w.Write(p1);
            w.Write(p2);

            return CallDBusMethod(method, "su", w.ToArray());
        }

        private WvAutoCast CallDBusMethod(string method, string sig, 
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

            Message reply = SendWithReplyAndBlock(m);

            var i = reply.iter();
            return i.pop();
        }

	void Register()
	{
	    if (unique_name != null)
		throw new Exception("Bus already has a unique name");

            unique_name = CallDBusMethod("Hello");
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

	string unique_name = null;
	public string UniqueName
	{
	    get {
		return unique_name;
	    }
	    set {
		if (unique_name != null)
		    throw new Exception("Unique name can only be set once");
		unique_name = value;
	    }
	}
    }
}
