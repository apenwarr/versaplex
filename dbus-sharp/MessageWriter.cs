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
using Wv.Extensions;

namespace Wv
{
    public class MessageWriter
    {
	readonly DataConverter conv;
	readonly EndianFlag endianness;
	WvBuf buf = new WvBuf(1024);

	public MessageWriter()
	{
	    endianness = Connection.NativeEndianness;
	    if (endianness == EndianFlag.Little)
		conv = DataConverter.LittleEndian;
	    else
	        conv = DataConverter.BigEndian;
	}

	public byte[] ToArray()
	{
	    return (byte[])buf.peekall();
	}
	
	void p(int align, WvBytes b)
	{
	    WritePad(align);
	    buf.put(b);
	}

	public void Write(byte val)
	{
	    buf.put(val);
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
	    byte[] b = Encoding.UTF8.GetBytes(val);
	    Write((uint)b.Length);
	    buf.put(b);
	    WriteNull();
	}

	public void Write(Signature val)
	{
	    byte[] b = val.GetBuffer();

	    if (b.Length > Protocol.MaxSignatureLength)
		throw new Exception
		    ("Signature length "
		     + b.Length + " exceeds maximum allowed "
		     + Protocol.MaxSignatureLength + " bytes");

	    Write((byte)b.Length);
	    buf.put(b);
	    WriteNull();
	}

	public void Write(Type type, object val)
	{
	    if (type == typeof(void))
		return;

	    if (type.IsArray) {
		wv.assert(false);
		// xWriteArray(val, type.GetElementType());
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
		wv.assert(false);
		//WriteFromDict(genArgs[0], genArgs[1], idict);
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
	
	static byte[] zeroes = new byte[8] { 0,0,0,0,0,0,0,0 };
	public void WriteArray<T>(int align,
				  IEnumerable<T> list,
				  Action<MessageWriter,T> doelement)
	{
	    // after the arraylength, we'll be aligned to size 4, but that
	    // might not be enough, so maybe we need to fix it up.
	    WritePad(4);
	    
	    var tmp = new MessageWriter();
	    
	    int startpad = (int)(buf.used+4) % 8;
	    tmp.buf.put(zeroes.sub(0, startpad));
	    tmp.WritePad(align);
	    int first = tmp.buf.used;
	    
	    foreach (T i in list)
	    {
		tmp.WritePad(align);
		doelement(tmp, i);
	    }
	    
	    byte[] a = tmp.ToArray();
	    
	    // the length word excludes all padding...
	    Write((uint)(a.Length - first));
	    
	    // ...but we have to copy all the bytes *including* padding
	    buf.put(a.sub(startpad, a.Length - startpad));
	}
	
	public void WriteValueType(object val, Type type)
	{
	    MethodInfo mi = TypeImplementer.GetWriteMethod (type);
	    mi.Invoke (null, new object[] {this, val});
	}
	
	public void Write(WvBytes b)
	{
	    Write((int)b.len);
	    buf.put(b);
	}
	
	public void Write(byte[] b)
	{
	    Write((WvBytes)b);
	}

	public void WriteNull()
	{
	    buf.put((byte)0);
	}

	public void WritePad(int alignment)
	{
	    int needed = Protocol.PadNeeded(buf.used, alignment);
	    buf.put(zeroes.sub(0, needed));
	}
    }
}
