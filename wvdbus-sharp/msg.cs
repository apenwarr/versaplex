// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Mono;
using Wv.Extensions;

namespace Wv
{
    public class WvDbusMsg
    {
	public static readonly Dbus.Endian NativeEndianness;
	public Dbus.Endian endian { get; private set; }
	public Dbus.MType type = Dbus.MType.MethodCall;
	public Dbus.MFlag flags = Dbus.MFlag.NoReplyExpected;
	public uint serial { get; internal set; }
	public uint? rserial = null;
	public string path = null;
	public string ifc = null;
	public string method = null;
	public string err = null;
	public string sender = null;
	public string dest = null;
	public string signature = null;

	public WvBytes bytes;
	public byte[] Body;

	static WvDbusMsg()
	{
	    if (BitConverter.IsLittleEndian)
		NativeEndianness = Dbus.Endian.Little;
	    else
		NativeEndianness = Dbus.Endian.Big;
	}

	public WvDbusMsg()
	{
	    endian = NativeEndianness;
	}
	
	public WvDbusMsg(WvBytes b)
	{
	    int hlen, blen;
	    _bytes_needed(b, out hlen, out blen);
	    
	    wv.assert(b.len == hlen+blen);
	    
	    SetHeaderData(b);
	    Body = b.sub(hlen, blen).ToArray();
	}
	
	public WvDbusMsg reply(string signature)
	{
	    WvDbusMsg reply = new WvDbusMsg();
	    reply.type = Dbus.MType.MethodReturn;
	    reply.flags = Dbus.MFlag.NoReplyExpected | Dbus.MFlag.NoAutoStart;
	    reply.rserial = this.serial;
	    reply.dest = this.sender;
	    reply.signature = signature;
	    return reply;
	}
	
	public WvDbusMsg reply()
	{
	    return reply(null);
	}
	
	public WvDbusMsg err_reply(string errcode)
	{
	    return err_reply(errcode, null);
	}

	public WvDbusMsg err_reply(string errcode, string errstr)
	{
	    WvDbusMsg r = reply();
	    r.type = Dbus.MType.Error;
	    r.err = errcode;
	    if (errstr.ne())
	    {
		r.signature = "s";
		var w = new WvDbusWriter();
		w.Write(errstr);
		r.Body = w.ToArray();
	    }
	    return r;
	}
	
	public WvDbusMsg err_reply(string errcode,
				 string fmt, params object[] args)
	{
	    return err_reply(errcode, wv.fmt(fmt, args));
	}

	public bool ReplyExpected
	{
	    get {
		return (flags & Dbus.MFlag.NoReplyExpected)
		          == Dbus.MFlag.None;
	    }
	    set {
		if (value)
		    flags &= ~Dbus.MFlag.NoReplyExpected;
		else
		    flags |= Dbus.MFlag.NoReplyExpected;
	    }
	}
	
	// Tries to guess how many bytes you'll need into inbuf before you can
	// read an entire message.  The buffer needs to have at least 16
	// bytes before you can start.
	static void _bytes_needed(WvBytes b, out int hlen, out int blen)
	{
	    wv.assert(b.len >= 16);

	    // the real header signature is: yyyyuua{yv}
	    // However, we only care about the first 16 bytes, and we know
	    // that the a{yv} starts with an unsigned int that's the number
	    // of bytes in the array, which is what we *really* want to know.
	    // So we lie about the signature here.
	    var it = new WvDbusIter((Dbus.Endian)b[0], "yyyyuuu", b.sub(0,16))
		.GetEnumerator();

	    it.pop();
	    it.pop();
	    it.pop();

	    byte version = it.pop();

	    if (version < Dbus.Protocol.MinVersion || version > Dbus.Protocol.MaxVersion)
		throw new NotSupportedException
		    ("Dbus.Protocol version '"
		     + version.ToString() + "' is not supported");

	    uint _blen = it.pop(); // body length
	    it.pop(); // serial
	    uint _hlen = it.pop(); // remaining header length

	    if (_blen > Int32.MaxValue || _hlen > Int32.MaxValue)
		throw new NotImplementedException
		    ("Long messages are not yet supported");

	    hlen = 16 + Dbus.Protocol.Padded((int)_hlen, 8);
	    blen = (int)_blen;
	}
	
	public static int bytes_needed(WvBytes b)
	{
	    int hlen, blen;
	    _bytes_needed(b, out hlen, out blen);
	    return hlen + blen;
	}

	public void SetHeaderData(WvBytes data)
	{
	    var it = new WvDbusIter((Dbus.Endian)data[0], "yyyyuua{yv}", data)
		.GetEnumerator();

	    endian   = (Dbus.Endian)(byte)it.pop();
	    type     = (Dbus.MType)(byte)it.pop();
	    flags    = (Dbus.MFlag)(byte)it.pop();
	    it.pop(); // version
	    it.pop(); // length
	    serial = it.pop();
	    foreach (var _f in it.pop())
	    {
		var f = _f.GetEnumerator();
	        Dbus.Field c = (Dbus.Field)(byte)f.pop();
		var v = f.pop();
		switch (c)
		{
		case Dbus.Field.Sender:
		    sender = v;
		    break;
		case Dbus.Field.Destination:
		    dest = v;
		    break;
		case Dbus.Field.ReplySerial:
		    rserial = v;
		    break;
		case Dbus.Field.Signature:
		    signature = v;
		    break;
		case Dbus.Field.Path:
		    path = v;
		    break;
		case Dbus.Field.Interface:
		    ifc = v;
		    break;
		case Dbus.Field.Member:
		    method = v;
		    break;
		case Dbus.Field.ErrorName:
		    err = v;
		    break;
		default:
		    break; // unknown field code, ignore
		}
	    }
	}
	
	void wws(WvDbusWriter w, Dbus.Field c, string sig, string val)
	{
	    w.Write((byte)c);
	    w.WriteSig(sig);
	    w.Write(val);
	}
	
	void wwu(WvDbusWriter w, Dbus.Field c, string sig, uint val)
	{
	    w.Write((byte)c);
	    w.WriteSig(sig);
	    w.Write(val);
	}
	
	void wwsig(WvDbusWriter w, Dbus.Field c, string sig, string val)
	{
	    w.Write((byte)c);
	    w.WriteSig(sig);
	    w.WriteSig(val);
	}
	
	// Header format is: yyyyuua{yv}
	public byte[] GetHeaderData()
	{
	    WvDbusWriter w = new WvDbusWriter();
	    
	    w.Write((byte)endian);
	    w.Write((byte)type);
	    w.Write((byte)flags);
	    w.Write((byte)Dbus.Protocol.Version);
	    w.Write(Body != null ? (uint)Body.Length : 0);
	    w.Write((uint)serial);
	    
	    // This two-step process is a little convoluted because of the
	    // way the WriteArray function needs to work.  That, in turn,
	    // is convoluted because the alignment of an array is complicated:
	    // there's different padding for zero-element arrays than for
	    // nonzero-element arrays, and WriteArray does that for us, 
	    // which means it needs to know in advance how many elements
	    // are in our array.
	    
	    var l = new List<Dbus.Field>();
	    if (sender.ne())
		l.Add(Dbus.Field.Sender);
	    if (dest.ne())
		l.Add(Dbus.Field.Destination);
	    if (rserial.HasValue)
		l.Add(Dbus.Field.ReplySerial);
	    if (signature.ne())
		l.Add(Dbus.Field.Signature);
	    if (path.ne())
		l.Add(Dbus.Field.Path);
	    if (ifc.ne())
		l.Add(Dbus.Field.Interface);
	    if (method.ne())
		l.Add(Dbus.Field.Member);
	    if (err.ne())
		l.Add(Dbus.Field.ErrorName);

	    w.WriteArray(8, l, (w2, i) => {
		switch (i)
		{
		case Dbus.Field.Sender:
		    wws(w2, i, "s", sender);
		    break;
		case Dbus.Field.Destination:
		    wws(w2, i, "s", dest);
		    break;
		case Dbus.Field.ReplySerial:
		    wv.assert(rserial.Value != 0);
		    wwu(w2, i, "u", rserial.Value);
		    break;
		case Dbus.Field.Signature:
		    wwsig(w2, i, "g", signature);
		    break;
		case Dbus.Field.Path:
		    wws(w2, i, "o", path);
		    break;
		case Dbus.Field.Interface:
		    wws(w2, i, "s", ifc);
		    break;
		case Dbus.Field.Member:
		    wws(w2, i, "s", method);
		    break;
		case Dbus.Field.ErrorName:
		    wws(w2, i, "s", err);
		    break;
		default:
		    break; // unknown field code, ignore
		}
	    });
	    
	    w.WritePad(8); // the header is *always* a multiple of 8
	    return w.ToArray();
	}
	
	public WvDbusMsg write(WvDbusWriter w)
	{
	    Body = w.ToArray();
	    return this;
	}
	
	public WvDbusIter iter()
	{
	    return new WvDbusIter(endian, signature, Body);
	}
	
	public static implicit operator WvDbusIter(WvDbusMsg m)
	{
	    return m.iter();
	}
	
	public WvDbusMsg check(string testsig)
	{
	    if (type == Dbus.MType.Error || err.ne())
	    {
		if (signature.ne())
		    throw new WvDbusError(wv.fmt("{0}: {1}", 
						 err, iter().Join(",")));
		else
		    throw new WvDbusError(err + ": Unknown error");
	    }
	    
	    if (testsig.e() && signature.ne())
		throw new WvDbusError
		    (wv.fmt("Expected empty message, got '{0}'", signature));
	    else if (testsig.ne() && signature.e())
		throw new WvDbusError
		    (wv.fmt("Expected '{0}', got empty message", testsig));
	    else if (signature.ne() && testsig.ne() && signature != testsig)
		throw new WvDbusError(wv.fmt("Expected '{0}', got '{1}'",
						 testsig, signature));
	    return this;
	}
    }
    
    public class WvDbusCall : WvDbusMsg
    {
	public WvDbusCall(string dest, string path, string ifc, string method,
			  string signature)
	{
	    this.type = Dbus.MType.MethodCall;
	    this.dest = dest;
	    this.path = path;
	    this.ifc = ifc;
	    this.method = method;
	    this.signature = signature;
	    
	    // This shouldn't be needed, since send() will set it if you
	    // have a replyaction.  But sometimes people don't actually plan
	    // to look at the reply, in which case the server will get a
	    // weird security error; so we have to set this to true even if
	    // we don't intend to see the reply.
	    this.ReplyExpected = true;
	}
	
	public WvDbusCall(string dest, string path, string ifc, string method)
	    : this(dest, path, ifc, method, null)
	{
	}
    }
    
    public class WvDbusSignal : WvDbusMsg
    {
	public WvDbusSignal(string dest, string path,
			    string ifc, string method,
			    string signature)
	{
	    this.type = Dbus.MType.Signal;
	    this.flags = Dbus.MFlag.NoReplyExpected | Dbus.MFlag.NoAutoStart;
	    this.dest = dest;
	    this.path = path;
	    this.ifc = ifc;
	    this.method = method;
	    this.signature = signature;
	}
	
	public WvDbusSignal(string dest, string path,
			    string ifc, string method)
	    : this(dest, path, ifc, method, null)
	{
	}
    }
}
