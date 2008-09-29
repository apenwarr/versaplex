// Copyright 2006 Alp Toker <alp@atoker.com>
// Copyright 2007 Versabanq (Adrian Dewhurst <adewhurst@versabanq.com>)
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
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

	//Interlocked.Increment() handles the overflow condition for uint correctly, so it's ok to store the value as an int but cast it to uint
	int serial = 0;
	uint GenerateSerial()
	{
	    //return ++serial;
	    return (uint)Interlocked.Increment(ref serial);
	}

	public Message SendWithReplyAndBlock(Message msg)
	{
	    PendingCall pending = SendWithReply(msg);
	    return pending.Reply;
	}

	internal PendingCall SendWithReply(Message msg)
	{
	    msg.ReplyExpected = true;
	    msg.Header.Serial = GenerateSerial();

	    //TODO: throttle the maximum number of concurrent PendingCalls
	    PendingCall pending = new PendingCall(this);
	    pendingCalls[msg.Header.Serial] = pending;

	    WriteMessage(msg);

	    return pending;
	}

	public uint Send(Message msg)
	{
	    msg.Header.Serial = GenerateSerial();

	    WriteMessage(msg);

	    return msg.Header.Serial;
	}

	object writeLock = new object();
	internal void WriteMessage(Message msg)
	{
	    byte[] HeaderData = msg.GetHeaderData();

	    long msgLength = HeaderData.Length + (msg.Body != null ? msg.Body.Length : 0);
	    if (msgLength > Protocol.MaxMessageLength)
		throw new Exception("Message length " + msgLength + " exceeds maximum allowed " + Protocol.MaxMessageLength + " bytes");

	    lock (writeLock) {
		ns.Write(HeaderData, 0, HeaderData.Length);
		if (msg.Body != null && msg.Body.Length != 0)
		    ns.Write(msg.Body, 0, msg.Body.Length);
	    }
	}

	Queue<Message> Inbound = new Queue<Message>();

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
	    msg.Connection = this;

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

	    int read;

	    //16 bytes is the size of the fixed part of the header
	    byte[] hbuf = new byte[16];
	    read = ns.Read(hbuf, 0, 16);

	    if (read == 0)
		return null;

	    if (read != 16)
		throw new Exception("Header read length mismatch: " + read + " of expected " + "16");

	    EndianFlag endianness = (EndianFlag)hbuf[0];
	    MessageReader reader = new MessageReader(endianness, hbuf);

	    //discard the endian byte as we've already read it
	    reader.ReadByte();

	    //discard message type and flags, which we don't care about here
	    reader.ReadByte();
	    reader.ReadByte();

	    byte version = reader.ReadByte();

	    if (version < Protocol.MinVersion || version > Protocol.MaxVersion)
		throw new NotSupportedException("Protocol version '" + version.ToString() + "' is not supported");

	    if (Protocol.Verbose)
		if (version != Protocol.Version)
		    Console.Error.WriteLine("Warning: Protocol version '" + version.ToString() + "' is not explicitly supported but may be compatible");

	    uint bodyLength = reader.ReadUInt32();
	    //discard serial
	    reader.ReadUInt32();
	    uint headerLength = reader.ReadUInt32();

	    int bodyLen = (int)bodyLength;
	    int toRead = (int)headerLength;

	    //we fixup to include the padding following the header
	    toRead = Protocol.Padded(toRead, 8);

	    long msgLength = toRead + bodyLen;
	    if (msgLength > Protocol.MaxMessageLength)
		throw new Exception("Message length " + msgLength + " exceeds maximum allowed " + Protocol.MaxMessageLength + " bytes");

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
	    msg.Connection = this;
	    msg.Body = body;
	    msg.SetHeaderData(header);

	    return msg;
	}

	//temporary hack
	internal void DispatchSignals()
	{
	    lock (Inbound) {
		while (Inbound.Count != 0) {
		    Message msg = Inbound.Dequeue();
		    HandleSignal(msg);
		}
	    }
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
		    DispatchSignals();

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

	internal Thread mainThread = Thread.CurrentThread;

	//temporary hack
	public void Iterate()
	{
	    mainThread = Thread.CurrentThread;

	    //Message msg = Inbound.Dequeue();
	    Message msg = ReadMessage();
	    HandleMessage(msg);
	    DispatchSignals();
	}

	internal void HandleMessage(Message msg)
	{
	    //TODO: support disconnection situations properly and move this check elsewhere
	    if (msg == null)
		throw new ArgumentNullException("msg", "Cannot handle a null message; maybe the bus was disconnected");

	    {
		object field_value;
		if (msg.Header.Fields.TryGetValue(FieldCode.ReplySerial, out field_value)) {
		    uint reply_serial = (uint)field_value;
		    PendingCall pending;

		    if (pendingCalls.TryGetValue(reply_serial, out pending)) {
			if (pendingCalls.Remove(reply_serial))
			    pending.Reply = msg;

			return;
		    }

		    //we discard reply messages with no corresponding PendingCall
		    if (Protocol.Verbose)
			Console.Error.WriteLine("Unexpected reply message received: MessageType='" + msg.Header.MessageType + "', ReplySerial=" + reply_serial);

		    return;
		}
	    }

	    switch (msg.Header.MessageType) {
	    case MessageType.MethodCall:
		MethodCall method_call = new MethodCall(msg);
		HandleMethodCall(method_call);
		break;
	    case MessageType.Signal:
		lock (Inbound)
		    Inbound.Enqueue(msg);
		break;
	    case MessageType.Error:
		//TODO: better exception handling
		Error error = new Error(msg);
		string errMsg = String.Empty;
		if (msg.Signature.Value.StartsWith("s")) {
		    errMsg = msg.iter().pop();
		}
		Console.Error.WriteLine("Remote Error: Signature='" + msg.Signature.Value + "' " + error.ErrorName + ": " + errMsg);
		break;
	    case MessageType.Invalid:
	    default:
		throw new Exception("Invalid message received: MessageType='" + msg.Header.MessageType + "'");
	    }
	}

	Dictionary<uint,PendingCall> pendingCalls = new Dictionary<uint,PendingCall>();

	//this might need reworking with MulticastDelegate
	internal void HandleSignal(Message msg)
	{
	    Signal signal = new Signal(msg);

	    //TODO: this is a hack, not necessary when MatchRule is complete
	    MatchRule rule = new MatchRule();
	    rule.MessageType = MessageType.Signal;
	    rule.Interface = signal.Interface;
	    rule.Member = signal.Member;
	    rule.Path = signal.Path;

	    Delegate dlg;
	    if (Handlers.TryGetValue(rule, out dlg)) {
		//dlg.DynamicInvoke(GetDynamicValues(msg));

		MethodInfo mi = dlg.Method;
		//signals have no return value
		dlg.DynamicInvoke(MessageHelper.GetDynamicValues(msg, mi.GetParameters()));

	    }
	    else {
		//TODO: how should we handle this condition? sending an Error may not be appropriate in this case
		if (Protocol.Verbose)
		    Console.Error.WriteLine("Warning: No signal handler for " + signal.Member);
	    }
	}

	internal Dictionary<MatchRule,Delegate> Handlers = new Dictionary<MatchRule,Delegate>();

	//very messy
	internal void MaybeSendUnknownMethodError(MethodCall method_call)
	{
	    Message msg = MessageHelper.CreateUnknownMethodError(method_call);
	    if (msg != null)
		Send(msg);
	}

	//not particularly efficient and needs to be generalized
	internal void HandleMethodCall(MethodCall method_call)
	{
	    //TODO: Ping and Introspect need to be abstracted and moved somewhere more appropriate once message filter infrastructure is complete

	    //FIXME: these special cases are slightly broken for the case where the member but not the interface is specified in the message
	    if (method_call.Interface == "org.freedesktop.DBus.Peer" && method_call.Member == "Ping") {
		Message reply = MessageHelper.ConstructReply(method_call);
		Send(reply);
		return;
	    }

	    if (method_call.Interface == "org.freedesktop.DBus.Introspectable" && method_call.Member == "Introspect") {
		Introspector intro = new Introspector();
		intro.root_path = method_call.Path;
		intro.WriteStart();

		//FIXME: do this properly
		//this is messy and inefficient
		List<string> linkNodes = new List<string>();
		int depth = method_call.Path.Decomposed.Length;
		foreach (ObjectPath pth in RegisteredObjects.Keys) {
		    if (pth.Value == (method_call.Path.Value)) {
			ExportObject exo = (ExportObject)RegisteredObjects[pth];
			intro.WriteType(exo.obj.GetType());
		    }
		    else {
			for (ObjectPath cur = pth ; cur != null ; cur = cur.Parent) {
			    if (cur.Value == method_call.Path.Value) {
				string linkNode = pth.Decomposed[depth];
				if (!linkNodes.Contains(linkNode)) {
				    intro.WriteNode(linkNode);
				    linkNodes.Add(linkNode);
				}
			    }
			}
		    }
		}

		intro.WriteEnd();

		Message reply = MessageHelper.ConstructReply(method_call, intro.xml);
		Send(reply);
		return;
	    }

	    BusObject bo;
	    if (RegisteredObjects.TryGetValue(method_call.Path, out bo)) {
		ExportObject eo = (ExportObject)bo;
		eo.HandleMethodCall(method_call);
	    }
	    else {
		MaybeSendUnknownMethodError(method_call);
	    }
	}

	Dictionary<ObjectPath,BusObject> RegisteredObjects = new Dictionary<ObjectPath,BusObject>();

	public object GetObject(Type type, string bus_name, ObjectPath path)
	{
	    // if the requested type is an interface, we can implement it
	    // efficiently otherwise we fall back to using a transparent
	    // proxy
	    if (type.IsInterface) {
		return BusObject.GetObject(this, bus_name, path, type);
	    }
	    else {
		if (Protocol.Verbose)
		    Console.Error.WriteLine("Warning: Note that MarshalByRefObject use is not recommended; for best performance, define interfaces");

		BusObject busObject = new BusObject(this, bus_name, path);
		DProxy prox = new DProxy(busObject, type);
		return prox.GetTransparentProxy();
	    }
	}

	public T GetObject<T>(string bus_name, ObjectPath path)
	{
	    return (T)GetObject(typeof(T), bus_name, path);
	}

	public void Register(ObjectPath path, object obj)
	{
	    ExportObject eo = new ExportObject(this, path, obj);
	    eo.Registered = true;

	    RegisteredObjects[path] = eo;
	}

	public object Unregister(ObjectPath path)
	{
	    BusObject bo;

	    if (!RegisteredObjects.TryGetValue(path, out bo))
		throw new Exception("Cannot unregister " + path + " as it isn't registered");

	    RegisteredObjects.Remove(path);

	    ExportObject eo = (ExportObject)bo;
	    eo.Registered = false;

	    return eo.obj;
	}

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
