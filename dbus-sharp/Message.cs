// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections.Generic;
using System.IO;
using Mono;

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
	
	struct SillyType
	{
	    public FieldCode Code;
	    public object Value;
	}

	// Header format is: yyyyuua{yv}
	public void SetHeaderData(byte[] data)
	{
	    EndianFlag endianness = (EndianFlag)data[0];
	    MessageReader reader = new MessageReader(endianness, data);

	    Header h;
	    h.Endianness = (EndianFlag)reader.ReadByte();
	    h.MessageType = (MessageType)reader.ReadByte();
	    h.Flags = (HeaderFlag)reader.ReadByte();
	    h.MajorVersion = reader.ReadByte();
	    h.Length = reader.ReadUInt32();
	    h.Serial = reader.ReadUInt32();
	    h.Fields = new Dictionary<FieldCode,object>();
	    reader.ReadArrayFunc(8, (r) => {
	        FieldCode c = (FieldCode)r.ReadByte();
		object o = r.ReadVariant();
		h.Fields[c] = o;
	    });
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
	public WvDBusIter open()
	{
	    DataConverter conv = Header.Endianness==EndianFlag.Little 
		    ? DataConverter.LittleEndian : DataConverter.BigEndian;
	    
	    byte[] data = Body;
	    
	    wv.print("Decoding message:\n{0}\n", wv.hexdump(data));
	    
	    return new WvDBusIter(conv, Header.Signature,
				  data, 0, data.Length);
	}
    }
}
