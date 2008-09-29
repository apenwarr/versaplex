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
	public Header Header;
	public Connection Connection;
	public byte[] Body;

	public Message()
	{
	    Header.Endianness = Connection.NativeEndianness;
	    Header.MessageType = MessageType.MethodCall;
	    Header.Flags = HeaderFlag.NoReplyExpected;
	    Header.MajorVersion = Protocol.Version;
	    Header.Fields = new Dictionary<FieldCode,object>();
	}

	public Signature Signature
	{
	    get {
		object o;
		if (Header.Fields.TryGetValue(FieldCode.Signature, out o))
		    return new Signature((string)o);
		else
		    return Signature.Empty;
	    }
	    set {
		if (value == Signature.Empty)
		    Header.Fields.Remove(FieldCode.Signature);
		else
		    Header.Fields[FieldCode.Signature] = value;
	    }
	}

	public bool ReplyExpected
	{
	    get {
		return (Header.Flags & HeaderFlag.NoReplyExpected)
		          == HeaderFlag.None;
	    }
	    set {
		if (value)
		    Header.Flags &= ~HeaderFlag.NoReplyExpected;
		else
		    Header.Flags |= HeaderFlag.NoReplyExpected;
	    }
	}
	
	struct SillyType
	{
	    public FieldCode Code;
	    public object Value;
	}

	public void SetHeaderData(byte[] data)
	{
	    var it = new WvDBusIter((EndianFlag)data[0], "yyyyuua{yv}", data)
		.GetEnumerator();

	    Header h;
	    h.Endianness   = (EndianFlag)(byte)it.pop();
	    h.MessageType  = (MessageType)(byte)it.pop();
	    h.Flags        = (HeaderFlag)(byte)it.pop();
	    h.MajorVersion = it.pop();
	    h.Length       = it.pop();
	    h.Serial       = it.pop();
	    h.Fields = new Dictionary<FieldCode,object>();
	    foreach (var _f in it.pop())
	    {
		var f = _f.GetEnumerator();
	        FieldCode c = (FieldCode)(byte)f.pop();
		object o = f.pop().inner;
		wv.print("code:{0} type:{1} val:'{2}'\n",
			 c, o.GetType(), o);
		h.Fields[c] = o;
	    }
	    Header = h;
	}

	// Header format is: yyyyuua{yv}
	public byte[] GetHeaderData()
	{
	    if (Body != null)
		Header.Length = (uint)Body.Length;

	    MessageWriter writer = new MessageWriter(Header.Endianness);
	    
	    Header h = Header;
	    writer.Write((byte)h.Endianness);
	    writer.Write((byte)h.MessageType);
	    writer.Write((byte)h.Flags);
	    writer.Write((byte)h.MajorVersion);
	    writer.Write((uint)h.Length);
	    writer.Write((uint)h.Serial);
	
	    writer.WriteArray(8, h.Fields, (w2, i) => {
		w2.Write((byte)i.Key);
		w2.WriteVariant(i.Value.GetType(), i.Value);
	    });
	    
	    writer.WritePad(8); // the header is *always* a multiple of 8
	    return writer.ToArray();
	}
	
	// FIXME: this whole Message class is junk, so this will presumably
	// migrate elsewhere eventually.
	public WvDBusIter iter()
	{
	    byte[] data = Body;
	    return new WvDBusIter(Header.Endianness, Header.Signature, data);
	}
    }
}
