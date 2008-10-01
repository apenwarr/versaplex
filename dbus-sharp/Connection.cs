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

    public class Connection
    {
	//TODO: reconsider this field
	public Stream ns = null;

	Transport transport;
	public Transport Transport {
	    get {
		return transport;
	    }
	    set {
		transport = value;
	    }
	}

	// FIXME: There should be a better way to hack in a socket
	// created elsewhere
	public Connection() {
	    OnMessage = HandleMessage;
	}

	internal Connection(Transport transport)
	{
	    OnMessage = HandleMessage;
	    this.transport = transport;
	    transport.Connection = this;

	    //TODO: clean this bit up
	    ns = transport.Stream;
	}

	//should this be public?
	internal Connection(string address)
	{
	    OnMessage = HandleMessage;
	    OpenPrivate(address);
	    Authenticate();
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

	    //TODO: clean this bit up
	    ns = transport.Stream;
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
	    
	    wv.print("Sending! Header:\n{0}\nBody:\n{1}\n",
		     wv.hexdump(HeaderData), 
		     wv.hexdump(msg.Body));

	    ns.Write(HeaderData, 0, HeaderData.Length);
	    if (msg.Body != null && msg.Body.Length != 0)
		ns.Write(msg.Body, 0, msg.Body.Length);
	}
	
	WvBuf inbuf = new WvBuf();
	
	void readbytes(int max)
	{
	    int needed = max - inbuf.used;
	    if (needed <= 0) return;
	    
	    WvBytes b = inbuf.alloc(needed);
	    int got = ns.Read(b.bytes, b.start, b.len);
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
	    while (inbuf.used < 16 && ns.CanRead)
		readbytes(16);
	    if (inbuf.used < 16)
		return null;
	    int needed = Message.bytes_needed(inbuf.peek(16));
	    while (inbuf.used < needed && ns.CanRead)
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

	internal Dictionary<MatchRule,Delegate> Handlers = new Dictionary<MatchRule,Delegate>();

	//these look out of place, but are useful
	internal protected virtual void AddMatch(string rule)
	{
	}

	internal protected virtual void RemoveMatch(string rule)
	{
	}

	static Connection()
	{
	    if (BitConverter.IsLittleEndian)
		NativeEndianness = EndianFlag.Little;
	    else
		NativeEndianness = EndianFlag.Big;
	}

	public static readonly EndianFlag NativeEndianness;
    }
}
