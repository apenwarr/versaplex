// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections.Generic;
using System.IO;

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
		    return (Signature)o;
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

	public void SetHeaderData(byte[] data)
	{
	    EndianFlag endianness = (EndianFlag)data[0];
	    MessageReader reader = new MessageReader(endianness, data);

	    Header = (Header)reader.ReadStruct(typeof(Header));
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
	    
	    {
		MessageWriter w2 = new MessageWriter(Header.Endianness);
		
		foreach (var i in h.Fields)
		{
		    w2.WritePad(8);
		    w2.Write((byte)i.Key);
		    w2.WriteVariant(i.Value.GetType(), i.Value);
		}
		
		byte[] a = w2.ToArray();
		writer.Write((uint)a.Length);
		writer.stream.Write(a, 0, a.Length);
	    }
	    writer.CloseWrite();

	    return writer.ToArray();
	}
    }
}
