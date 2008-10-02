// Copyright 2006 Alp Toker <alp@atoker.com>
// Copyright 2007 Versabanq (Adrian Dewhurst <adewhurst@versabanq.com>)
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Globalization;
using Wv;
using Wv.Extensions;

namespace Wv.Authentication
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
	
	string getline()
	{
	    var buf = new WvBuf();
	    
	    while (true)
	    {
		var b = buf.alloc(1);
		conn.transport.wait(-1);
		int got = conn.transport.read(b);
		if (got == 0 || b[0] == (byte)'\n')
		{
		    var obuf = buf.getall();
		    if (obuf[obuf.len-1] == '\r')
			obuf = obuf.sub(0, obuf.len-1);
		    return obuf.FromUTF8();
		}
	    }
	}
	
	void writestr(string s)
	{
	    conn.transport.write(s.ToUTF8());
	}
	
	void writestr(string fmt, params object[] args)
	{
	    writestr(wv.fmt(fmt, args));
	}

	public override void Run()
	{
	    string str = conn.transport.AuthString();
	    byte[] bs = Encoding.ASCII.GetBytes(str);
	    string authStr = ToHex(bs);

	    writestr("AUTH EXTERNAL {0}\r\n", authStr);

	    string ok_rep = getline();

	    string[] parts = ok_rep.Split(' ');

	    if (parts.Length < 1 || parts[0] != "OK") {
		done = true;
		throw new Exception ("Authentication error: AUTH EXTERNAL was not OK: \"" + ok_rep + "\"");
	    }

	    writestr("BEGIN\r\n");
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

}
