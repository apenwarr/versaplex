// Copyright 2006 Alp Toker <alp@atoker.com>
// Copyright 2007 Versabanq (Adrian Dewhurst <adewhurst@versabanq.com>)
// This software is made available under the MIT License
// See COPYING for details

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Globalization;

namespace NDesk.DBus.Authentication
{
	enum ClientState
	{
		WaitingForData,
		WaitingForOK,
		WaitingForReject,
		Terminate
	}

	enum ServerState
	{
		WaitingForAuth,
		WaitingForData,
		WaitingForBegin,
		Terminate,
		DoneAuth
	}

	public class ExternalAuthClient : SaslProcess
	{
		public override bool SupportNonBlocking { get { return false; } }

		bool done = false;
		public override bool Done { get { return done; } }
		bool ok = false;
		public override bool OK { get { return ok; } }

		public ExternalAuthClient (Connection conn) : base(conn)
		{
		}
		
		public override void Run ()
		{
			//NetworkStream ns = new NetworkStream (sock);
			//UnixStream ns = new UnixStream ((int)sock.Handle);
			//StreamReader sr = new StreamReader (conn.Transport.Stream, Encoding.ASCII);
			//StreamWriter sw = new StreamWriter (conn.Transport.Stream, Encoding.ASCII);
			StreamReader sr = new StreamReader (conn.ns, Encoding.ASCII);
			StreamWriter sw = new StreamWriter (conn.ns, Encoding.ASCII);

			sw.NewLine = "\r\n";
			//sw.AutoFlush = true;

			string str = conn.Transport.AuthString ();
			byte[] bs = Encoding.ASCII.GetBytes (str);

			string authStr = ToHex (bs);

			sw.WriteLine ("AUTH EXTERNAL {0}", authStr);
			sw.Flush ();

			string ok_rep = sr.ReadLine ();

			string[] parts;
			parts = ok_rep.Split (' ');

			if (parts.Length < 1 || parts[0] != "OK") {
				done = true;
				throw new Exception ("Authentication error: AUTH EXTERNAL was not OK");
			}

			/*
			string guid = parts[1];
			byte[] guidData = FromHex (guid);
			uint unixTime = BitConverter.ToUInt32 (guidData, 0);
			Console.Error.WriteLine ("guid: " + guid + ", " + "unixTime: " + unixTime + " (" + UnixToDateTime (unixTime) + ")");
			*/

			sw.WriteLine ("BEGIN");
			sw.Flush ();

			done = true;
			ok = true;
		}
	}

	public enum SaslMechResponse {
		Continue,
		OK,
		Reject
	}

	public abstract class SaslProcess {
		protected Connection conn;

		public abstract bool SupportNonBlocking { get; }
		public abstract bool Done { get; }
		public abstract bool OK { get; }

		public virtual void Run()
		{
			while (ProcessLine()) {
			}
		}

		public virtual bool ProcessLine()
		{
			throw new NotImplementedException();
		}

		protected SaslProcess(Connection conn)
		{
			this.conn = conn;
		}

		protected static string GetLine (Stream s)
		{
			const int MAX_BUFFER = 16384; // From real dbus client

			// FIXME: There must be a better way to do this that
			// doesn't run the risk of eating bytes after the
			// BEGIN\r\n

			// XXX: This is just generally horrible and
			// inefficient. Sorry.

			Encoding e = Encoding.ASCII;
			StringBuilder sb = new StringBuilder();

			while (sb.Length < MAX_BUFFER) {
				int r = s.ReadByte();

				// End of stream... no line to get
				if (r < 0)
					return null;

				byte[] b = new byte[1];
				b[0] = (byte)r;

				sb.Append(e.GetString(b));

				// Look for \r\n
				if (sb.Length >= 2) {
					// For some reason I can't use
					// sb.Chars[i] (mono problem?)
					string str = sb.ToString();

					if (str.EndsWith("\r\n"))
						return str;
				}
			}

			// Line shouldn't be this big
			return null;
		}

		protected static void PutLine (Stream s, string line)
		{
			Encoding e = Encoding.ASCII;

			byte[] outbuf = e.GetBytes(line);
			s.Write(outbuf, 0, outbuf.Length);
		}

		//From Mono.Unix.Native.NativeConvert
		//should these methods use long or (u)int?
		public static DateTime UnixToDateTime (long time)
		{
			DateTime LocalUnixEpoch = new DateTime (1970, 1, 1);
			TimeSpan LocalUtcOffset = TimeZone.CurrentTimeZone.GetUtcOffset (DateTime.UtcNow);
			return LocalUnixEpoch.AddSeconds ((double) time + LocalUtcOffset.TotalSeconds);
		}

		public static long DateTimeToUnix (DateTime time)
		{
			DateTime LocalUnixEpoch = new DateTime (1970, 1, 1);
			TimeSpan LocalUtcOffset = TimeZone.CurrentTimeZone.GetUtcOffset (DateTime.UtcNow);
			TimeSpan unixTime = time.Subtract (LocalUnixEpoch) - LocalUtcOffset;

			return (long) unixTime.TotalSeconds;
		}

		//From Mono.Security.Cryptography
		//Modified to output lowercase hex
		static public string ToHex (byte[] input)
		{
			if (input == null)
				return null;

			StringBuilder sb = new StringBuilder (input.Length * 2);
			foreach (byte b in input) {
				sb.Append (b.ToString ("x2", CultureInfo.InvariantCulture));
			}
			return sb.ToString ();
		}

		//From Mono.Security.Cryptography
		static private byte FromHexChar (char c)
		{
			if ((c >= 'a') && (c <= 'f'))
				return (byte) (c - 'a' + 10);
			if ((c >= 'A') && (c <= 'F'))
				return (byte) (c - 'A' + 10);
			if ((c >= '0') && (c <= '9'))
				return (byte) (c - '0');
			throw new ArgumentException ("Invalid hex char");
		}

		//From Mono.Security.Cryptography
		static public byte[] FromHex (string hex)
		{
			if (hex == null)
				return null;
			if ((hex.Length & 0x1) == 0x1)
				throw new ArgumentException ("Length must be a multiple of 2");

			byte[] result = new byte [hex.Length >> 1];
			int n = 0;
			int i = 0;
			while (n < result.Length) {
				result [n] = (byte) (FromHexChar (hex [i++]) << 4);
				result [n++] += FromHexChar (hex [i++]);
			}
			return result;
		}
	}

	public abstract class SaslAuthCtx
        {
		protected Connection conn;

		protected SaslAuthCtx (Connection conn)
		{
			this.conn = conn;
		}

		public abstract SaslMechResponse Data (byte[] response,
				out byte[] challenge);

                public abstract bool Accepted { get; }

		public virtual void Aborted()
		{
		}

		public virtual void Completed()
		{
		}
	}

	public delegate SaslMechResponse SaslAuthCtxFactory (byte[] initialData,
			out SaslAuthCtx ctx, out byte[] challenge);

	public sealed class SaslServer : SaslProcess {
		private ServerState state = ServerState.WaitingForAuth;
		private SaslAuthCtx authctx = null;

		public override bool SupportNonBlocking { get { return true; } }

		public override bool Done {
			get {
				return (state == ServerState.Terminate
						|| state == ServerState.DoneAuth);
			}
		}

		public override bool OK {
			get { return state == ServerState.DoneAuth; }
		}

		private IDictionary<string,SaslAuthCtxFactory> mechs;

		public SaslServer (IDictionary<string,SaslAuthCtxFactory> mechs,
				Connection conn) : base(conn)
		{
			if (mechs.Keys.Count == 0)
				throw new Exception("SaslServer requires at "
						+"least one authentication "
						+"mechanism");

			this.mechs = new Dictionary<string,SaslAuthCtxFactory>(mechs);
		}

		public override bool ProcessLine()
		{
			string line = GetLine(conn.Transport.Stream);

			if (line == null) {
				// Didn't get a complete line
				state = ServerState.Terminate;
				return true;
			}

			if (line.Substring(line.Length - 2) != "\r\n") {
				// Actually, this isn't a big deal at the
				// moment because GetLine ensures that there
				// is a \r\n, so this block will never run.
				//
				// FIXME
				// Log("Line didn't end with CRLF");
				state = ServerState.Terminate;
				return true;
			}

			line = line.Substring(0, line.Length - 2);

			string[] tokens = line.Split(' ');

			string response = null;

			if (tokens.Length < 1) {
				response = "ERROR No command provided";
			} else switch (state) {
			case ServerState.WaitingForAuth:
				ProcessWaitAuth(tokens, out response);
				break;
			case ServerState.WaitingForData:
				ProcessWaitData(tokens, out response);
				break;
			case ServerState.WaitingForBegin:
				ProcessWaitBegin(tokens, out response);
				break;
			case ServerState.Terminate:
			case ServerState.DoneAuth:
				throw new Exception("No further authentication"
						+" processing should be done");
			}

			if (response != null) {
				PutLine(conn.Transport.Stream, response + "\r\n");
				return false;
			} else switch (state) {
			case ServerState.Terminate:
			case ServerState.DoneAuth:
				return true;
			default:
				throw new Exception("Auth mechanism "
						+"didn't provide a "
						+"response");
			}
		}

		private void ProcessWaitAuth(string[] tokens,
				out string response)
		{
			if (authctx != null)
				throw new Exception("Expected no authctx");

			byte[] outData = null;

			switch (tokens[0]) {
			case "AUTH":
			{
				SaslMechResponse rv;

				switch (tokens.Length) {
				case 1:
					rv = SaslMechResponse.Reject;
					break;
				case 2:
					rv = Auth(tokens[1], null, out outData);
					break;
				case 3:
				{
					byte[] initialData;
					try {
						initialData = FromHex(tokens[2]);
					} catch (ArgumentException) {
						response = "ERROR Invalid "
							+ "initial response "
							+ "data";
						state = ServerState.WaitingForAuth;
						return;
					}

					rv = Auth(tokens[1], initialData,
							out outData);
					break;
				}
				default:
					response = "ERROR Invalid AUTH command";
					state = ServerState.WaitingForAuth;
					return;
				}

				switch (rv) {
				case SaslMechResponse.Continue:
					response = "DATA " + ToHex(outData);
					state = ServerState.WaitingForData;
					return;
				case SaslMechResponse.OK:
					response = "OK";
					state = ServerState.WaitingForBegin;
					return;
				case SaslMechResponse.Reject:
					response = CreateRejection();
					state = ServerState.WaitingForAuth;
					return;
				default:
					throw new Exception("Invalid SASL Mechanism "
							+"return value");
				}
			}
			case "BEGIN":
				response = null;
				state = ServerState.Terminate;
				return;
			case "ERROR":
				response = CreateRejection();
				state = ServerState.WaitingForAuth;
				return;
			default:
				response = "ERROR Invalid command";
				state = ServerState.WaitingForAuth;
				return;
			}
		}

		private void ProcessWaitData(string[] tokens,
				out string response)
		{
			if (authctx == null)
				throw new Exception("Expected to have an authctx");

			if (authctx.Accepted)
				throw new Exception("Expected authentication to not be completed");

			switch (tokens[0]) {
			case "DATA":
			{
				if (tokens.Length != 2) {
					response = "ERROR Invalid DATA command";
					state = ServerState.WaitingForData;
					return;
				}

				byte[] inData;
				try {
					inData = FromHex(tokens[1]);
				} catch (ArgumentException) {
					response = "ERROR Invalid response data";
					state = ServerState.WaitingForData;
					return;
				}

				byte[] outData = null;
				SaslMechResponse rv = authctx.Data(inData, out outData);

				switch (rv) {
				case SaslMechResponse.OK:
					response = "OK";
					state = ServerState.WaitingForBegin;
					return;

				case SaslMechResponse.Continue:
					response = "DATA " + ToHex(outData);
					state = ServerState.WaitingForData;
					return;

				case SaslMechResponse.Reject:
					authctx = null;

					response = CreateRejection();
					state = ServerState.WaitingForAuth;
					return;
				default:
					throw new Exception("Invalid SASL Mechanism "
							+"return value");
				}
			}

			case "CANCEL":
			case "ERROR":
				authctx.Aborted();
				authctx = null;

				response = CreateRejection();
				state = ServerState.WaitingForAuth;
				return;

			case "BEGIN":
				response = null;
				state = ServerState.Terminate;
				return;

			default:
				response = "ERROR Invalid command";
				state = ServerState.WaitingForData;
				return;
			}
		}

		private void ProcessWaitBegin(string[] tokens,
				out string response)
		{
			if (authctx == null)
				throw new Exception("Expected to have an authctx");

			if (authctx.Accepted)
				throw new Exception("Expected authentication to be completed");

			switch (tokens[0]) {
			case "ERROR":
			case "CANCEL":
				authctx.Aborted();
				authctx = null;

				response = CreateRejection();
				state = ServerState.WaitingForAuth;
				return;

			case "BEGIN":
				authctx.Completed();

				response = null;
				state = ServerState.DoneAuth;
				return;

			default:
				response = "ERROR Invalid command";
				state = ServerState.WaitingForBegin;
				break;
			}
		}

		private SaslMechResponse Auth(string mech,
				byte[] initialData, out byte[] responseData)
		{
			authctx = null;

			SaslAuthCtxFactory factory;

			if (!mechs.TryGetValue(mech, out factory)) {
				responseData = null;
				return SaslMechResponse.Reject;
			}

			SaslAuthCtx ctx;
			SaslMechResponse response;

			response = factory(initialData, out ctx, out responseData);
			
			if (response != SaslMechResponse.Reject)
				authctx = ctx;

			return response;
		}

		private string CreateRejection()
		{
			StringBuilder sb = new StringBuilder("REJECT ");

			foreach (string key in mechs.Keys) {
				sb.AppendFormat(" {0}", key);
			}

			return sb.ToString();
		}
	}
}
