// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Reflection;

namespace NDesk.DBus
{
	using Authentication;
	using Transports;

	public class Connection
	{
		//TODO: reconsider this field
		Stream ns = null;

		Transport transport;
		internal Transport Transport {
			get {
				return transport;
			} set {
				transport = value;
			}
		}

		protected Connection () {}

		internal Connection (Transport transport)
		{
			this.transport = transport;
			transport.Connection = this;

			//TODO: clean this bit up
			ns = transport.Stream;
		}

		//should this be public?
		internal Connection (string address)
		{
			OpenPrivate (address);
			Authenticate ();
		}

		/*
		bool isConnected = false;
		public bool IsConnected
		{
			get {
				return isConnected;
			}
		}
		*/

		//should we do connection sharing here?
		public static Connection Open (string address)
		{
			Connection conn = new Connection ();
			conn.OpenPrivate (address);
			conn.Authenticate ();

			return conn;
		}

		internal void OpenPrivate (string address)
		{
			if (address == null)
				throw new ArgumentNullException ("address");

			AddressEntry[] entries = Address.Parse (address);
			if (entries.Length == 0)
				throw new Exception ("No addresses were found");

			//TODO: try alternative addresses if needed
			AddressEntry entry = entries[0];

			transport = Transport.Create (entry);

			//TODO: clean this bit up
			ns = transport.Stream;
		}

		void Authenticate ()
		{
			if (transport != null)
				transport.WriteCred ();

			SaslClient auth = new SaslClient (this);
			auth.Run ();
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
		uint GenerateSerial ()
		{
			//return ++serial;
			return (uint)Interlocked.Increment (ref serial);
		}

		internal Message SendWithReplyAndBlock (Message msg)
		{
			uint id = SendWithReply (msg);

			Message retMsg;

			//TODO: this isn't fully thread-safe but works much of the time
			while (!replies.TryGetValue (id, out retMsg))
				HandleMessage (ReadMessage ());

			replies.Remove (id);

			//FIXME: we should dispatch signals and calls on the main thread
			DispatchSignals ();

			return retMsg;
		}

		internal uint SendWithReply (Message msg)
		{
			msg.ReplyExpected = true;
			return Send (msg);
		}

		internal uint Send (Message msg)
		{
			msg.Header.Serial = GenerateSerial ();

			msg.WriteHeader ();

			WriteMessage (msg);

			//Outbound.Enqueue (msg);
			//temporary
			//Flush ();

			return msg.Header.Serial;
		}

		internal void WriteMessage (Message msg)
		{
			ns.Write (msg.HeaderData, 0, msg.HeaderData.Length);
			if (msg.Body != null && msg.Body.Length != 0)
				ns.Write (msg.Body, 0, msg.Body.Length);
		}

		Queue<Message> Inbound = new Queue<Message> ();
		/*
		Queue<Message> Outbound = new Queue<Message> ();

		public void Flush ()
		{
			//should just iterate the enumerator here
			while (Outbound.Count != 0) {
				Message msg = Outbound.Dequeue ();
				WriteMessage (msg);
			}
		}

		public bool ReadWrite (int timeout_milliseconds)
		{
			//TODO

			return true;
		}

		public bool ReadWrite ()
		{
			return ReadWrite (-1);
		}

		public bool Dispatch ()
		{
			//TODO
			Message msg = Inbound.Dequeue ();
			//HandleMessage (msg);

			return true;
		}

		public bool ReadWriteDispatch (int timeout_milliseconds)
		{
			//TODO
			return Dispatch ();
		}

		public bool ReadWriteDispatch ()
		{
			return ReadWriteDispatch (-1);
		}
		*/

		internal Message ReadMessage ()
		{
			//FIXME: fix reading algorithm to work in one step
			//this code is a bit silly and inefficient
			//hopefully it's at least correct and avoids polls for now

			int read;

			byte[] buf = new byte[16];
			read = ns.Read (buf, 0, 16);

			if (read == 0)
				return null;

			if (read != 16)
				throw new Exception ("Header read length mismatch: " + read + " of expected " + "16");

			MemoryStream ms = new MemoryStream ();

			ms.Write (buf, 0, 16);

			EndianFlag endianness = (EndianFlag)buf[0];
			MessageReader reader = new MessageReader (endianness, buf);

			//discard the endian byte as we've already read it
			byte tmp;
			reader.GetValue (out tmp);

			//discard message type and flags, which we don't care about here
			reader.GetValue (out tmp);
			reader.GetValue (out tmp);

			byte version;
			reader.GetValue (out version);

			if (version < Protocol.MinVersion || version > Protocol.MaxVersion)
				throw new NotSupportedException ("Protocol version '" + version.ToString () + "' is not supported");

			if (Protocol.Verbose)
				if (version != Protocol.Version)
					Console.Error.WriteLine ("Warning: Protocol version '" + version.ToString () + "' is not explicitly supported but may be compatible");

			uint bodyLength, serial, headerLength;
			reader.GetValue (out bodyLength);
			reader.GetValue (out serial);
			reader.GetValue (out headerLength);

			//TODO: remove this limitation
			if (bodyLength > Int32.MaxValue || headerLength > Int32.MaxValue)
				throw new NotImplementedException ("Long messages are not yet supported");

			int bodyLen = (int)bodyLength;
			int toRead = (int)headerLength;

			toRead = Protocol.Padded ((int)toRead, 8);

			buf = new byte[toRead];

			read = ns.Read (buf, 0, toRead);

			if (read != toRead)
				throw new Exception ("Read length mismatch: " + read + " of expected " + toRead);

			ms.Write (buf, 0, buf.Length);

			Message msg = new Message ();
			msg.Connection = this;
			msg.HeaderData = ms.ToArray ();

			//read the body
			if (bodyLen != 0) {
				//FIXME
				//msg.Body = new byte[(int)msg.Header->Length];
				byte[] body = new byte[bodyLen];

				//int len = ns.Read (msg.Body, 0, msg.Body.Length);
				int len = ns.Read (body, 0, bodyLen);

				//if (len != msg.Body.Length)
				if (len != bodyLen)
					throw new Exception ("Message body size mismatch");

				//msg.Body = new MemoryStream (body);
				msg.Body = body;
			}

			//this needn't be done here
			msg.ParseHeader ();

			return msg;
		}

		//temporary hack
		void DispatchSignals ()
		{
			lock (Inbound) {
				while (Inbound.Count != 0) {
					Message msg = Inbound.Dequeue ();
					HandleSignal (msg);
				}
			}
		}

		//temporary hack
		public void Iterate ()
		{
			//Message msg = Inbound.Dequeue ();
			Message msg = ReadMessage ();
			HandleMessage (msg);
			DispatchSignals ();
		}

		internal void HandleMessage (Message msg)
		{
			//TODO: support disconnection situations properly and move this check elsewhere
			if (msg == null)
				throw new ArgumentNullException ("msg", "Cannot handle a null message; maybe the bus was disconnected");

			{
				//TODO: don't store replies unless they are expected (right now all replies are expected as we don't support NoReplyExpected)
				object reply_serial;
				if (msg.Header.Fields.TryGetValue (FieldCode.ReplySerial, out reply_serial)) {
					replies[(uint)reply_serial] = msg;
					return;
				}
			}

			switch (msg.Header.MessageType) {
				case MessageType.MethodCall:
					MethodCall method_call = new MethodCall (msg);
					HandleMethodCall (method_call);
					break;
				case MessageType.Signal:
					//HandleSignal (msg);
					lock (Inbound)
						Inbound.Enqueue (msg);
					break;
				case MessageType.Error:
					//TODO: better exception handling
					Error error = new Error (msg);
					string errMsg = String.Empty;
					if (msg.Signature.Value.StartsWith ("s")) {
						MessageReader reader = new MessageReader (msg);
						reader.GetValue (out errMsg);
					}
					//throw new Exception ("Remote Error: Signature='" + msg.Signature.Value + "' " + error.ErrorName + ": " + errMsg);
					//if (Protocol.Verbose)
					Console.Error.WriteLine ("Remote Error: Signature='" + msg.Signature.Value + "' " + error.ErrorName + ": " + errMsg);
					break;
				case MessageType.Invalid:
				default:
					throw new Exception ("Invalid message received: MessageType='" + msg.Header.MessageType + "'");
			}
		}

		Dictionary<uint,Message> replies = new Dictionary<uint,Message> ();

		//this might need reworking with MulticastDelegate
		internal void HandleSignal (Message msg)
		{
			Signal signal = new Signal (msg);

			//TODO: this is a hack, not necessary when MatchRule is complete
			MatchRule rule = new MatchRule ();
			rule.MessageType = MessageType.Signal;
			rule.Interface = signal.Interface;
			rule.Member = signal.Member;
			rule.Path = signal.Path;

			Delegate dlg;
			if (Handlers.TryGetValue (rule, out dlg)) {
				//dlg.DynamicInvoke (GetDynamicValues (msg));

				MethodInfo mi = dlg.Method;
				//signals have no return value
				dlg.DynamicInvoke (MessageHelper.GetDynamicValues (msg, mi.GetParameters ()));

			} else {
				//TODO: how should we handle this condition? sending an Error may not be appropriate in this case
				if (Protocol.Verbose)
					Console.Error.WriteLine ("Warning: No signal handler for " + signal.Member);
			}
		}

		internal Dictionary<MatchRule,Delegate> Handlers = new Dictionary<MatchRule,Delegate> ();

		//very messy
		internal void MaybeSendUnknownMethodError (MethodCall method_call)
		{
			Message msg = MessageHelper.CreateUnknownMethodError (method_call);
			if (msg != null)
				Send (msg);
		}

		//not particularly efficient and needs to be generalized
		internal void HandleMethodCall (MethodCall method_call)
		{
			//TODO: Ping and Introspect need to be abstracted and moved somewhere more appropriate once message filter infrastructure is complete

			//FIXME: these special cases are slightly broken for the case where the member but not the interface is specified in the message
			if (method_call.Interface == "org.freedesktop.DBus.Peer" && method_call.Member == "Ping") {
				object[] pingRet = new object[0];
				Message reply = MessageHelper.ConstructReplyFor (method_call, pingRet);
				Send (reply);
				return;
			}

			if (method_call.Interface == "org.freedesktop.DBus.Introspectable" && method_call.Member == "Introspect") {
				Introspector intro = new Introspector ();
				intro.root_path = method_call.Path;
				intro.WriteStart ();

				//FIXME: do this properly
				//this is messy and inefficient
				List<string> linkNodes = new List<string> ();
				int depth = method_call.Path.Decomposed.Length;
				foreach (ObjectPath pth in RegisteredObjects.Keys) {
					if (pth.Value == (method_call.Path.Value)) {
						ExportObject exo = (ExportObject)RegisteredObjects[pth];
						intro.WriteType (exo.obj.GetType ());
					} else {
						for (ObjectPath cur = pth ; cur != null ; cur = cur.Parent) {
							if (cur.Value == method_call.Path.Value) {
								string linkNode = pth.Decomposed[depth];
								if (!linkNodes.Contains (linkNode)) {
									intro.WriteNode (linkNode);
									linkNodes.Add (linkNode);
								}
							}
						}
					}
				}

				intro.WriteEnd ();

				object[] introRet = new object[1];
				introRet[0] = intro.xml;
				Message reply = MessageHelper.ConstructReplyFor (method_call, introRet);
				Send (reply);
				return;
			}

			BusObject bo;
			if (RegisteredObjects.TryGetValue (method_call.Path, out bo)) {
				ExportObject eo = (ExportObject)bo;
				eo.HandleMethodCall (method_call);
			} else {
				MaybeSendUnknownMethodError (method_call);
			}
		}

		Dictionary<ObjectPath,BusObject> RegisteredObjects = new Dictionary<ObjectPath,BusObject> ();

		//FIXME: this shouldn't be part of the core API
		//that also applies to much of the other object mapping code
		//it should cache proxies and objects, really

		//inspired by System.Activator
		public object GetObject (Type type, string bus_name, ObjectPath path)
		{
			BusObject busObject = new BusObject (this, bus_name, path);
			DProxy prox = new DProxy (busObject, type);

			object obj = prox.GetTransparentProxy ();

			return obj;
		}

		/*
		public object GetObject (Type type, string bus_name, ObjectPath path)
		{
			return BusObject.GetObject (this, bus_name, path, type);
		}
		*/

		public T GetObject<T> (string bus_name, ObjectPath path)
		{
			return (T)GetObject (typeof (T), bus_name, path);
		}

		public void Register (string bus_name, ObjectPath path, object obj)
		{
			ExportObject eo = new ExportObject (this, bus_name, path, obj);
			eo.Registered = true;

			//TODO: implement some kind of tree data structure or internal object hierarchy. right now we are ignoring the name and putting all object paths in one namespace, which is bad
			RegisteredObjects[path] = eo;
		}

		public object Unregister (string bus_name, ObjectPath path)
		{
			//TODO: make use of bus_name

			BusObject bo;

			if (!RegisteredObjects.TryGetValue (path, out bo))
				throw new Exception ("Cannot unregister " + path + " as it isn't registered");

			RegisteredObjects.Remove (path);

			ExportObject eo = (ExportObject)bo;
			eo.Registered = false;

			return eo.obj;
		}

		//these look out of place, but are useful
		internal protected virtual void AddMatch (string rule)
		{
		}

		internal protected virtual void RemoveMatch (string rule)
		{
		}

		static Connection ()
		{
			if (BitConverter.IsLittleEndian)
				NativeEndianness = EndianFlag.Little;
			else
				NativeEndianness = EndianFlag.Big;
		}

		internal static readonly EndianFlag NativeEndianness;
	}
}
