// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Reflection;
using Mono;

namespace Wv
{
    public class MessageWriter
    {
	DataConverter conv;
	EndianFlag endianness;
	MemoryStream stream;

	public MessageWriter()
	{
	    endianness = Connection.NativeEndianness;
	    if (endianness == EndianFlag.Little)
		conv = DataConverter.LittleEndian;
	    else
	        conv = DataConverter.BigEndian;
	    
	    stream = new MemoryStream();
	}

	public byte[] ToArray()
	{
	    //TODO: mark the writer locked or something here
	    return stream.ToArray();
	}
	
	void p(int align, byte[] b)
	{
	    WritePad(align);
	    stream.Write(b, 0, b.Length);
	}

	public void Write(byte val)
	{
	    stream.WriteByte(val);
	}

	public void Write(bool val)
	{
	    Write((uint)(val ? 1 : 0));
	}

	public void Write(short val)
	{
	    p(2, conv.GetBytes(val));
	}

	public void Write(ushort val)
	{
	    p(2, conv.GetBytes(val));
	}

	public void Write(int val)
	{
	    p(4, conv.GetBytes(val));
	}

	public void Write(uint val)
	{
	    p(4, conv.GetBytes(val));
	}

	public void Write(long val)
	{
	    p(8, conv.GetBytes(val));
	}

	public void Write(ulong val)
	{
	    p(8, conv.GetBytes(val));
	}

	public void Write(float val)
	{
	    p(4, conv.GetBytes(val));
	}

	public void Write(double val)
	{
	    p(8, conv.GetBytes(val));
	}

	public void Write(string val)
	{
	    byte[] utf8_data = Encoding.UTF8.GetBytes(val);
	    Write((uint)utf8_data.Length);
	    stream.Write(utf8_data, 0, utf8_data.Length);
	    WriteNull();
	}

	public void Write(Signature val)
	{
	    byte[] ascii_data = val.GetBuffer();

	    if (ascii_data.Length > Protocol.MaxSignatureLength)
		throw new Exception
		    ("Signature length "
		     + ascii_data.Length + " exceeds maximum allowed "
		     + Protocol.MaxSignatureLength + " bytes");

	    Write((byte)ascii_data.Length);
	    stream.Write(ascii_data, 0, ascii_data.Length);
	    WriteNull();
	}

	public void Write(Type type, object val)
	{
	    if (type == typeof(void))
		return;

	    if (type.IsArray) {
		xWriteArray(val, type.GetElementType());
	    }
	    else if (type == typeof(Signature)) {
		Write((Signature)val);
	    }
	    else if (type == typeof(object)) {
		WriteV(val);
	    }
	    else if (type == typeof(string)) {
		Write((string)val);
	    }
	    else if (type.IsGenericType 
		     && (type.GetGenericTypeDefinition() 
			   == typeof(IDictionary<,>) 
			 || type.GetGenericTypeDefinition() 
			   == typeof(Dictionary<,>))) {
		Type[] genArgs = type.GetGenericArguments();
		IDictionary idict = (IDictionary)val;
		WriteFromDict(genArgs[0], genArgs[1], idict);
	    }
	    else if (!type.IsPrimitive && !type.IsEnum) {
		WriteValueType(val, type);
	    }
	    else {
		Write(Signature.TypeToDType(type), val);
	    }
	}

	//helper method, should not be used as it boxes needlessly
	public void Write (DType dtype, object val)
	{
	    switch (dtype)
	    {
	    case DType.Byte:
		Write((byte)val);
		break;
	    case DType.Boolean:
		Write((bool)val);
		break;
	    case DType.Int16:
		Write((short)val);
		break;
	    case DType.UInt16:
		Write((ushort)val);
		break;
	    case DType.Int32:
		Write((int)val);
		break;
	    case DType.UInt32:
		Write((uint)val);
		break;
	    case DType.Int64:
		Write((long)val);
		break;
	    case DType.UInt64:
		Write((ulong)val);
		break;
	    case DType.Single:
		Write((float)val);
		break;
	    case DType.Double:
		Write((double)val);
		break;
	    case DType.String:
		Write((string)val);
		break;
	    case DType.ObjectPath:
		Write((string)val);
		break;
	    case DType.Signature:
		Write((Signature)val);
		break;
	    case DType.Variant:
		WriteV((object)val);
		break;
	    default:
		throw new Exception("Unhandled D-Bus type: " + dtype);
	    }
	}

	//variant
	public void WriteV(object val)
	{
	    if (val == null)
		throw new NotSupportedException("Cannot send null variant");

	    Type type = val.GetType();
	    WriteVariant(type, val);
	}

	public void WriteVariant(Type type, object val)
	{
	    Signature sig = Signature.GetSig(type);
	    Write(sig);
	    Write(type, val);
	}

	//this requires a seekable stream for now
	public void xWriteArray(object obj, Type elemType)
	{
	    Array val = (Array)obj;

	    //TODO: more fast paths for primitive arrays
	    if (elemType == typeof (byte)) {
		if (val.Length > Protocol.MaxArrayLength)
		    throw new Exception
		        ("Array length " + val.Length 
			 + " exceeds maximum allowed " 
			 + Protocol.MaxArrayLength + " bytes");

		Write((uint)val.Length);
		stream.Write((byte[])val, 0, val.Length);
		return;
	    }

	    long origPos = stream.Position;
	    Write((uint)0);

	    //advance to the alignment of the element
	    WritePad(Protocol.GetAlignment(Signature.TypeToDType (elemType)));

	    long startPos = stream.Position;

	    foreach (object elem in val)
		Write(elemType, elem);

	    long endPos = stream.Position;
	    uint ln = (uint)(endPos - startPos);
	    stream.Position = origPos;

	    if (ln > Protocol.MaxArrayLength)
		throw new Exception
		    ("Array length " + ln + " exceeds maximum allowed "
		     + Protocol.MaxArrayLength + " bytes");

	    Write(ln);
	    stream.Position = endPos;
	}
	
	static byte[] zeroes = new byte[8] { 0,0,0,0,0,0,0,0 };
	public void WriteArray<T>(int align,
				  IEnumerable<T> list,
				  Action<MessageWriter,T> doelement)
	{
	    var tmp = new MessageWriter();
	    
	    // after the arraylength, we'll be aligned to size 4, but that
	    // might not be enough, so maybe we need to fix it up.
	    WritePad(4);
	    
	    int startpad = (int)(stream.Position+4) % 8;
	    tmp.stream.Write(zeroes, 0, startpad);
			     
	    int first = -1;
	    foreach (T i in list)
	    {
		tmp.WritePad(align);
		if (first < 0)
		    first = (int)tmp.stream.Position;
		doelement(tmp, i);
	    }
	    
	    byte[] a = tmp.ToArray();
	    Write((uint)(a.Length - first));
	    stream.Write(a, startpad, a.Length - startpad);
	}
	
	public void WriteFromDict(Type keyType, Type valType, IDictionary val)
	{
	    long origPos = stream.Position;
	    Write ((uint)0);

	    //advance to the alignment of the element
	    //WritePad (Protocol.GetAlignment (Signature.TypeToDType (type)));
	    WritePad (8);

	    long startPos = stream.Position;

	    foreach (System.Collections.DictionaryEntry entry in val)
	    {
		WritePad (8);

		Write (keyType, entry.Key);
		Write (valType, entry.Value);
	    }

	    long endPos = stream.Position;
	    uint ln = (uint)(endPos - startPos);
	    stream.Position = origPos;

	    if (ln > Protocol.MaxArrayLength)
		throw new Exception ("Dict length " + ln + " exceeds maximum allowed " + Protocol.MaxArrayLength + " bytes");

	    Write (ln);
	    stream.Position = endPos;
	}

	public void WriteValueType(object val, Type type)
	{
	    MethodInfo mi = TypeImplementer.GetWriteMethod (type);
	    mi.Invoke (null, new object[] {this, val});
	}

	public void WriteNull()
	{
	    stream.WriteByte(0);
	}

	public void WritePad(int alignment)
	{
	    int needed = Protocol.PadNeeded ((int)stream.Position, alignment);
	    for (int i = 0 ; i != needed ; i++)
		stream.WriteByte (0);
	}

	public delegate void WriterDelegate(MessageWriter w);
	public void WriteDelegatePrependSize(WriterDelegate wd, int alignment)
	{
	    WritePad(4);

	    long sizepos = stream.Position;

	    Write((int)0);

	    WritePad(alignment);

	    long counter = stream.Position;

	    wd(this);

	    long endpos = stream.Position;

	    stream.Position = sizepos;
	    Write((int)(endpos-counter));

	    stream.Position = endpos;
	}
    }
}
