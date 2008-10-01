// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;

namespace Wv.Transports
{
	class SocketTransport : Transport
	{
		protected Socket socket;

	        internal SocketTransport(AddressEntry entry)
		{
			string host, portStr;
			int port;

			if (!entry.Properties.TryGetValue ("host", out host))
				throw new Exception ("No host specified");

			if (!entry.Properties.TryGetValue ("port", out portStr))
				throw new Exception ("No port specified");

			if (!Int32.TryParse (portStr, out port))
				throw new Exception ("Invalid port: \"" + port + "\"");

			Open (host, port);
		}

	        void Open (string host, int port)
		{
			//TODO: use Socket directly
			TcpClient client = new TcpClient (host, port);
			Stream = client.GetStream ();
		}

		void Open (Socket socket)
		{
			this.socket = socket;

			socket.Blocking = true;
			Stream = new NetworkStream (socket);
		}

	        public override void WriteCred ()
		{
			Stream.WriteByte (0);
		}

		public override string AuthString ()
		{
			return String.Empty;
		}
	}
}
