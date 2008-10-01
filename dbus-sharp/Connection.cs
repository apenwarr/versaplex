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

	object writeLock = new object();
	internal void WriteMessage(Message msg)
	{
	    byte[] HeaderData = msg.GetHeaderData();

	    long msgLength = HeaderData.Length + (msg.Body != null ? msg.Body.Length : 0);
	    if (msgLength > Protocol.MaxMessageLength)
		throw new Exception("Message length " + msgLength + " exceeds maximum allowed " + Protocol.MaxMessageLength + " bytes");
	    
	    wv.print("Sending! Header:\n{0}\nBody:\n{1}\n",
		     wv.hexdump(HeaderData), 
		     wv.hexdump(msg.Body));

	    lock (writeLock) {
		ns.Write(HeaderData, 0, HeaderData.Length);
		if (msg.Body != null && msg.Body.Length != 0)
		    ns.Write(msg.Body, 0, msg.Body.Length);
	    }
	}

	// Given the first 16 bytes of the header, returns the full
	// header and body lengths (including the 16 bytes of the
	// header already read) Positions the stream after
	// execution at the point where it began
	internal static void GetMessageSize(Stream s, out int headerSize,
					    out int bodySize)
	{
	    int read;

	    byte[] buf = new byte[16];
	    read = s.Read(buf, 0, 16);

	    s.Seek(-read, SeekOrigin.Current);

	    if (read != 16)
		throw new Exception("Header read length mismatch: "
				    + read + " of expected " + "16");

	    // the real header signature is: yyyyuua{yv}
	    // However, we only care about the first 16 bytes, and we know
	    // that the a{yv} starts with an unsigned int that's the number
	    // of bytes in the array, which is what we *really* want to know.
	    // So we lie about the signature here.
	    var it = new WvDBusIter((EndianFlag)buf[0], "yyyyuuu", buf)
		.GetEnumerator();

	    it.pop();
	    it.pop();
	    it.pop();

	    byte version = it.pop();

	    if (version < Protocol.MinVersion || version > Protocol.MaxVersion)
		throw new NotSupportedException
		    ("Protocol version '"
		     + version.ToString() + "' is not supported");

	    uint blen = it.pop(); // body length
	    it.pop(); // serial
	    uint hlen = it.pop(); // remaining header length

	    if (blen > Int32.MaxValue || hlen > Int32.MaxValue)
		throw new NotImplementedException
		    ("Long messages are not yet supported");

	    bodySize = (int)blen;
	    headerSize = Protocol.Padded((int)hlen, 8) + 16;
	}

	internal Message BuildMessage(Stream s,
				       int headerSize, int bodySize)
	{
	    if (s.Length-s.Position < headerSize + bodySize)
		throw new Exception("Buffer is not header + body sizes");

	    Message msg = new Message();

	    int len;

	    byte[] header = new byte[headerSize];
	    len = s.Read(header, 0, headerSize);

	    if (len != headerSize)
		throw new Exception("Read length mismatch: "
				    + len + " of expected " + headerSize);

	    msg.SetHeaderData(header);

	    if (bodySize != 0) {
		byte[] body = new byte[bodySize];
		len = s.Read(body, 0, bodySize);

		if (len != bodySize)
		    throw new Exception("Read length mismatch: "
					+ len + " of expected " + bodySize);

		msg.Body = body;
	    }

	    return msg;
	}

	public Message ReadMessage()
	{
	    byte[] header;
	    byte[] body = null;

	    //16 bytes is the size of the fixed part of the header
	    byte[] hbuf = new byte[16];
	    int read = ns.Read(hbuf, 0, 16);

	    if (read == 0)
		return null;

	    if (read != 16)
		throw new Exception("Header read length mismatch: " + read + " of expected " + "16");

	    var it = new WvDBusIter((EndianFlag)hbuf[0], "yyyyuuu", hbuf)
		.GetEnumerator();
	    
	    it.pop();
	    it.pop();
	    it.pop();
	    
	    byte version = it.pop();

	    if (version < Protocol.MinVersion || version > Protocol.MaxVersion)
		throw new NotSupportedException
		    ("Protocol version '" + version.ToString() 
		     + "' is not supported");

	    uint bodyLength = it.pop();
	    it.pop(); // serial
	    uint headerLength = it.pop();

	    int bodyLen = (int)bodyLength;
	    int toRead = (int)headerLength;

	    // fixup to include the padding following the header
	    toRead = Protocol.Padded(toRead, 8);

	    long msgLength = toRead + bodyLen;
	    if (msgLength > Protocol.MaxMessageLength)
		throw new Exception
		    ("Message length " + msgLength 
		     + " exceeds maximum allowed " 
		     + Protocol.MaxMessageLength + " bytes");

	    header = new byte[16 + toRead];
	    Array.Copy(hbuf, header, 16);

	    read = ns.Read(header, 16, toRead);

	    if (read != toRead)
		throw new Exception("Message header length mismatch: " + read + " of expected " + toRead);

	    //read the body
	    if (bodyLen != 0) {
		body = new byte[bodyLen];

		int numRead = 0;
		int lastRead = -1;
		while (numRead < bodyLen && lastRead != 0)
		{
		    lastRead = ns.Read(body, numRead, bodyLen - numRead);
		    numRead += lastRead;
		}

		if (numRead != bodyLen)
		    throw new Exception(String.Format(
						       "Message body size mismatch: numRead={0}, bodyLen={1}",
						       numRead, bodyLen));
	    }

	    Message msg = new Message();
	    msg.Body = body;
	    msg.SetHeaderData(header);

	    return msg;
	}

	// hacky
	public delegate void MessageHandler(Message msg);
	public MessageHandler OnMessage;

	MemoryStream msgbuf = new MemoryStream();
	public long ReceiveBuffer(byte[] buf, int offset, int count)
	{
	    msgbuf.Seek(0, SeekOrigin.End);
	    msgbuf.Write(buf, offset, count);

	    msgbuf.Seek(0, SeekOrigin.Begin);

	    long left = msgbuf.Length;
	    long want = 0;

	    while (left >= 16) {
		int headerSize, bodySize;
		GetMessageSize(msgbuf, out headerSize, out bodySize);

		if (left >= headerSize + bodySize) {
		    Message msg = BuildMessage(msgbuf, headerSize,
					       bodySize);
		    OnMessage(msg);
		    left -= headerSize + bodySize;
		}
		else {
		    want = headerSize + bodySize - left;
		    break;
		}
	    }

	    if (left > 0 && msgbuf.Length != left) {
		byte[] tmp = new byte[left];

		msgbuf.Read(tmp, 0, tmp.Length);

		msgbuf.SetLength(tmp.Length);

		msgbuf.Seek(0, SeekOrigin.Begin);
		msgbuf.Write(tmp, 0, tmp.Length);
	    }
	    else if (left == 0) {
		msgbuf.SetLength(0);
	    }

	    if (want > 0)
		return want;

	    return 16 - left;
	}

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
		Error error = new Error(msg);
		string errMsg = String.Empty;
		if (msg.Signature.Value.StartsWith("s")) {
		    errMsg = msg.iter().pop();
		}
		Console.Error.WriteLine("Remote Error: Signature='" + msg.Signature.Value + "' " + error.ErrorName + ": " + errMsg);
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
