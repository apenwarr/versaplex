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
	
	public byte[] Body;

	public Message()
	{
	    endian = Connection.NativeEndianness;
	}

	public Signature Signature
	{
	    get {
		return signature.e() 
		    ? Signature.Empty : new Signature(signature);
	    }
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
		wv.print("code:{0} type:{1} val:'{2}'\n",
			 c, v.inner.GetType(), v);
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
	    wv.print("Writing: type={0}/{3}, code={1}, val='{2}'\n",
		     val.GetType(), c, val, typeof(string));
	    w.Write((byte)c);
	    w.Write(new Signature(sig));
	    w.Write(val);
	}
	
	void wwu(MessageWriter w, FieldCode c, string sig, uint val)
	{
	    wv.print("Writing: type={0}/{3}, code={1}, val='{2}'\n",
		     val.GetType(), c, val, typeof(uint));
	    w.Write((byte)c);
	    w.Write(new Signature(sig));
	    w.Write(val);
	}
	
	void wwsig(MessageWriter w, FieldCode c, string sig, Signature val)
	{
	    wv.print("Writing: type={0}/{3}, code={1}, val='{2}'\n",
		     val.GetType(), c, val, typeof(Signature));
	    w.Write((byte)c);
	    w.Write(new Signature(sig));
	    w.Write(val);
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
		    wwsig(w2, i, "g", Signature);
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
