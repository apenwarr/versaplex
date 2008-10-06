// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections.Generic;
using System.IO;
using Mono;
using Wv.Extensions;

namespace Wv
{
    public class Message
    {
	public static readonly EndianFlag NativeEndianness;
	public EndianFlag endian { get; private set; }
	public MessageType type = MessageType.MethodCall;
	public HeaderFlag flags = HeaderFlag.NoReplyExpected;
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

	static Message()
	{
	    if (BitConverter.IsLittleEndian)
		NativeEndianness = EndianFlag.Little;
	    else
		NativeEndianness = EndianFlag.Big;
	}

	public Message()
	{
	    endian = NativeEndianness;
	}
	
	public Message(WvBytes b)
	{
	    int hlen, blen;
	    _bytes_needed(b, out hlen, out blen);
	    
	    wv.assert(b.len == hlen+blen);
	    
	    SetHeaderData(b);
	    Body = b.sub(hlen, blen).ToArray();
	}
	
	public Message reply()
	{
	    Message reply = new Message();
	    reply.type = MessageType.MethodReturn;
	    reply.flags = HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;
	    reply.rserial = this.serial;
	    reply.dest = this.sender;
	    return reply;
	}
	
	public Message err_reply(string errcode)
	{
	    return err_reply(errcode, null);
	}

	public Message err_reply(string errcode, string errstr)
	{
	    Message r = reply();
	    r.type = MessageType.Error;
	    r.err = err;
	    if (errstr.ne())
	    {
		r.signature = "s";
		var w = new MessageWriter();
		w.Write(errstr);
		r.Body = w.ToArray();
	    }
	    return r;
	}
	
	public Message err_reply(string errcode,
				 string fmt, params object[] args)
	{
	    return err_reply(errcode, wv.fmt(fmt, args));
	}

	public bool ReplyExpected
	{
	    get {
		return (flags & HeaderFlag.NoReplyExpected)
		          == HeaderFlag.None;
	    }
	    set {
		if (value)
		    flags &= ~HeaderFlag.NoReplyExpected;
		else
		    flags |= HeaderFlag.NoReplyExpected;
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
	    var it = new WvDBusIter((EndianFlag)b[0], "yyyyuuu", b.sub(0,16))
		.GetEnumerator();

	    it.pop();
	    it.pop();
	    it.pop();

	    byte version = it.pop();

	    if (version < Protocol.MinVersion || version > Protocol.MaxVersion)
		throw new NotSupportedException
		    ("Protocol version '"
		     + version.ToString() + "' is not supported");

	    uint _blen = it.pop(); // body length
	    it.pop(); // serial
	    uint _hlen = it.pop(); // remaining header length

	    if (_blen > Int32.MaxValue || _hlen > Int32.MaxValue)
		throw new NotImplementedException
		    ("Long messages are not yet supported");

	    hlen = 16 + Protocol.Padded((int)_hlen, 8);
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
	    var it = new WvDBusIter((EndianFlag)data[0], "yyyyuua{yv}", data)
		.GetEnumerator();

	    endian   = (EndianFlag)(byte)it.pop();
	    type     = (MessageType)(byte)it.pop();
	    flags    = (HeaderFlag)(byte)it.pop();
	    it.pop(); // version
	    it.pop(); // length
	    serial = it.pop();
	    foreach (var _f in it.pop())
	    {
		var f = _f.GetEnumerator();
	        FieldCode c = (FieldCode)(byte)f.pop();
		var v = f.pop();
		switch (c)
		{
		case FieldCode.Sender:
		    sender = v;
		    break;
		case FieldCode.Destination:
		    dest = v;
		    break;
		case FieldCode.ReplySerial:
		    rserial = v;
		    break;
		case FieldCode.Signature:
		    signature = v;
		    break;
		case FieldCode.Path:
		    path = v;
		    break;
		case FieldCode.Interface:
		    ifc = v;
		    break;
		case FieldCode.Member:
		    method = v;
		    break;
		case FieldCode.ErrorName:
		    err = v;
		    break;
		default:
		    break; // unknown field code, ignore
		}
	    }
	}
	
	void wws(MessageWriter w, FieldCode c, string sig, string val)
	{
	    w.Write((byte)c);
	    w.WriteSig(sig);
	    w.Write(val);
	}
	
	void wwu(MessageWriter w, FieldCode c, string sig, uint val)
	{
	    w.Write((byte)c);
	    w.WriteSig(sig);
	    w.Write(val);
	}
	
	void wwsig(MessageWriter w, FieldCode c, string sig, string val)
	{
	    w.Write((byte)c);
	    w.WriteSig(sig);
	    w.WriteSig(val);
	}
	
	// Header format is: yyyyuua{yv}
	public byte[] GetHeaderData()
	{
	    MessageWriter w = new MessageWriter();
	    
	    w.Write((byte)endian);
	    w.Write((byte)type);
	    w.Write((byte)flags);
	    w.Write((byte)Protocol.Version);
	    w.Write(Body != null ? (uint)Body.Length : 0);
	    w.Write((uint)serial);
	    
	    // This two-step process is a little convoluted because of the
	    // way the WriteArray function needs to work.  That, in turn,
	    // is convoluted because the alignment of an array is complicated:
	    // there's different padding for zero-element arrays than for
	    // nonzero-element arrays, and WriteArray does that for us, 
	    // which means it needs to know in advance how many elements
	    // are in our array.
	    
	    var l = new List<FieldCode>();
	    if (sender.ne())
		l.Add(FieldCode.Sender);
	    if (dest.ne())
		l.Add(FieldCode.Destination);
	    if (rserial.HasValue)
		l.Add(FieldCode.ReplySerial);
	    if (signature.ne())
		l.Add(FieldCode.Signature);
	    if (path.ne())
		l.Add(FieldCode.Path);
	    if (ifc.ne())
		l.Add(FieldCode.Interface);
	    if (method.ne())
		l.Add(FieldCode.Member);
	    if (err.ne())
		l.Add(FieldCode.ErrorName);

	    w.WriteArray(8, l, (w2, i) => {
		switch (i)
		{
		case FieldCode.Sender:
		    wws(w2, i, "s", sender);
		    break;
		case FieldCode.Destination:
		    wws(w2, i, "s", dest);
		    break;
		case FieldCode.ReplySerial:
		    wv.assert(rserial.Value != 0);
		    wwu(w2, i, "u", rserial.Value);
		    break;
		case FieldCode.Signature:
		    wwsig(w2, i, "g", signature);
		    break;
		case FieldCode.Path:
		    wws(w2, i, "o", path);
		    break;
		case FieldCode.Interface:
		    wws(w2, i, "s", ifc);
		    break;
		case FieldCode.Member:
		    wws(w2, i, "s", method);
		    break;
		case FieldCode.ErrorName:
		    wws(w2, i, "s", err);
		    break;
		default:
		    break; // unknown field code, ignore
		}
	    });
	    
	    w.WritePad(8); // the header is *always* a multiple of 8
	    return w.ToArray();
	}
	
	// FIXME: this whole Message class is junk, so this will presumably
	// migrate elsewhere eventually.
	public WvDBusIter iter()
	{
	    return new WvDBusIter(endian, signature, Body);
	}
    }
}
