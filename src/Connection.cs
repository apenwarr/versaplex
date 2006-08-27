// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details

using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;

using System.Runtime.InteropServices;

//using Console = System.Diagnostics.Trace;

namespace NDesk.DBus
{
	public partial class Connection
	{
		//unix:path=/var/run/dbus/system_bus_socket
		const string SYSTEM_BUS = "/var/run/dbus/system_bus_socket";

		public Socket sock = null;
		Stream ns = null;
		Transport transport;

		public Connection ()
		{
			string sessAddr = System.Environment.GetEnvironmentVariable ("DBUS_SESSION_BUS_ADDRESS");
			//string sysAddr = System.Environment.GetEnvironmentVariable ("DBUS_SYSTEM_BUS_ADDRESS");
			bool abstr;
			string path;

			Address.Parse (sessAddr, out abstr, out path);

			//not really correct
			if (path == null)
				path = SYSTEM_BUS;

			transport = new UnixTransport (path, abstr);

			sock = transport.socket;

			sock.Blocking = true;
			//ns = new UnixStream ((int)sock.Handle);
			ns = new NetworkStream (sock);

			Authenticate ();
		}

		uint serial = 0;
		public uint GenerateSerial ()
		{
			return ++serial;
		}




		public Message SendWithReplyAndBlock (Message msg)
		{
			uint id = SendWithReply (msg);

			Message retMsg = WaitForReplyTo (id);

			return retMsg;
		}

		public uint SendWithReply (Message msg)
		{
			msg.ReplyExpected = true;
			return Send (msg);
		}

		public uint Send (Message msg)
		{
			msg.Serial = GenerateSerial ();

			WriteMessage (msg);

			//Outbound.Enqueue (msg);
			//temporary
			//Flush ();

			return msg.Serial;
		}

		//could be cleaner
		protected void WriteMessage (Message msg)
		{
			ns.Write (msg.HeaderData, 0, msg.HeaderSize);
			if (msg.Body != null) {
				//ns.Write (msg.Body, 0, msg.BodySize);
				msg.Body.WriteTo (ns);
			}
		}

		protected Queue<Message> Inbound = new Queue<Message> ();
		protected Queue<Message> Outbound = new Queue<Message> ();

		public void Flush ()
		{
			//should just iterate the enumerator here
			while (Outbound.Count != 0) {
				Message msg = Outbound.Dequeue ();
				WriteMessage (msg);
			}
		}

		public unsafe Message ReadMessage ()
		{
			//FIXME: fix reading algorithm to work in one step

			byte[] buf = new byte[1024];

			//ns.Read (buf, 0, buf.Length);
			ns.Read (buf, 0, 16);

			//Console.WriteLine ("");
			//Console.WriteLine ("Header:");

			Message msg = new Message ();

			fixed (byte* pbuf = buf) {
				msg.Header = (DHeader*)pbuf;
				//Console.WriteLine (msg.MessageType);
				//System.Console.WriteLine ("Length: " + msg.Header->Length);
				//System.Console.WriteLine ("Header Length: " + msg.Header->HeaderLength);
			}

			int toRead = 0;
			toRead += Message.Padded ((int)msg.Header->HeaderLength, 8);

			//System.Console.WriteLine ("toRead: " + toRead);

			int read;

			read = ns.Read (buf, 16, toRead);

			if (read != toRead)
				System.Console.Error.WriteLine ("Read length mismatch: " + read + " of expected " + toRead);

			msg.HeaderData = buf;

			/*
			System.Console.WriteLine ("Len: " + msg.Header->Length);
			System.Console.WriteLine ("HLen: " + msg.Header->HeaderLength);
			System.Console.WriteLine ("toRead: " + toRead);
			*/
			//read the body
			if (msg.Header->Length != 0) {
				//FIXME
				//msg.Body = new byte[(int)msg.Header->Length];
				byte[] body = new byte[(int)msg.Header->Length];

				//int len = ns.Read (msg.Body, 0, msg.Body.Length);
				int len = ns.Read (body, 0, body.Length);

				//if (len != msg.Body.Length)
				if (len != body.Length)
					System.Console.Error.WriteLine ("Message body size mismatch");

				msg.Body = new MemoryStream (body);
			}

			//this needn't be done here
			Message.IsReading = true;
			msg.ParseHeader ();
			Message.IsReading = false;

			return msg;
		}

		//this is just a start
		//needs to be done properly
		protected Message WaitForReplyTo (uint id)
		{
			//Message msg = Inbound.Dequeue ();
			//HandleMessage (msg);

			Message msg;

			while ((msg = ReadMessage ()) != null) {
				switch (msg.MessageType) {
					case MessageType.Invalid:
						break;
					case MessageType.MethodCall:
						break;
					case MessageType.MethodReturn:
					case MessageType.Error:
						if (msg.ReplySerial == id)
							return msg;
						break;
					case MessageType.Signal:
						HandleMessage (msg);
						break;
				}
			}

			return null;
		}


		//temporary convenience method
		public void HandleMessage (Message msg)
		{
			string val;
			Message.GetValue (msg.Body, out val);

			Console.WriteLine ("Signal out value: " + val);

			if (Handlers.ContainsKey (msg.Member)) {
				Delegate dlg = Handlers[msg.Member];
				org.freedesktop.DBus.NameAcquiredHandler handler = dlg as org.freedesktop.DBus.NameAcquiredHandler;
				handler (val);
			}
		}

		public Dictionary<string,Delegate> Handlers = new Dictionary<string,Delegate> ();
	}

	//hacky dummy debug console
	internal static class Console
	{
		public static void WriteLine ()
		{
			WriteLine (String.Empty);
		}

		public static void WriteLine (string line)
		{
			//Write (line + "\n");
		}

		public static void WriteLine (object line)
		{
			//Write (line.ToString () + "\n");
		}

		public static void Write (string text)
		{
		}
	}
}
