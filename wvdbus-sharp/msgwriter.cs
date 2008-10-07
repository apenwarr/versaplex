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
    public class WvDbusWriter
    {
	readonly DataConverter conv;
	readonly Dbus.Endian endianness;
	WvBuf buf = new WvBuf(1024);

	public WvDbusWriter()
	{
	    endianness = WvDbusMsg.NativeEndianness;
	    if (endianness == Dbus.Endian.Little)
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

	public void WriteSig(string val)
	{
	    byte[] b = val.ToUTF8();

	    if (b.Length > Dbus.Protocol.MaxSignatureLength)
		throw new Exception
		    ("Signature length "
		     + b.Length + " exceeds maximum allowed "
		     + Dbus.Protocol.MaxSignatureLength + " bytes");

	    Write((byte)b.Length);
	    buf.put(b);
	    WriteNull();
	}

	static byte[] zeroes = new byte[8] { 0,0,0,0,0,0,0,0 };
	public void WriteArray<T>(int align,
				  IEnumerable<T> list,
				  Action<WvDbusWriter,T> doelement)
	{
	    // after the arraylength, we'll be aligned to size 4, but that
	    // might not be enough, so maybe we need to fix it up.
	    WritePad(4);
	    
	    var tmp = new WvDbusWriter();
	    
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
	    int needed = Dbus.Protocol.PadNeeded(buf.used, alignment);
	    buf.put(zeroes.sub(0, needed));
	}
    }
}
