// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using Mono;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Wv.Extensions;

namespace Wv
{
    public class WvDbusIter 
	: WvDbusIterBase, IEnumerator<WvAutoCast>, IEnumerable<WvAutoCast>
    {
	int sigpos;
	WvAutoCast cur;
	protected WvLog log = new WvLog("DbusIter");
	
	internal WvDbusIter(DataConverter conv, string sig, WvBytes b)
	    : base(conv, sig, b)
	{
	    log.print(WvLog.L.Debug3, "Iterating! (sig={0}, bytes={1}-{2})\n",
	                 sig, b.start, b.len);
	    log.print(WvLog.L.Debug5, wv.hexdump(b));
	    Reset();
	}
	
	internal WvDbusIter(Dbus.Endian e, string sig, WvBytes b)
	    : this(parse_endian_byte((byte)e), sig, b)
	{
	}
	
	static DataConverter parse_endian_byte(byte e)
	{
	    if (e == (byte)Dbus.Endian.Little)
		return DataConverter.LittleEndian;
	    else if (e == (byte)Dbus.Endian.Big)
		return DataConverter.BigEndian;
	    else
		throw new ArgumentException
		    (wv.fmt("value '0x{0:x}' must be 'l' or 'B'", e), "e");
	}
	
	// IEnumerable
	public IEnumerator<WvAutoCast> GetEnumerator()
	{
	    return new WvDbusIter(conv, sig, data.sub(start, end-start));
	}
	IEnumerator System.Collections.IEnumerable.GetEnumerator()
	{
	    return new WvDbusIter(conv, sig, data.sub(start, end-start));
	}
	
	// IEnumerator
	public void Reset()
	{
	    sigpos = 0;
	    _Reset();
	}
	
	// IEnumerator
	public bool MoveNext()
	{
	    if (sigpos >= sig.Length || atend())
	    {
		cur = new WvAutoCast(null);
		return false;
	    }
	    else
	    {
		string sub = subsig(sig, sigpos);
		cur = new WvAutoCast(getone(sub));
		sigpos += sub.Length;
		return true;
	    }
	}
	
	// IEnumerator
	public void Dispose()
	{
	}
	
	public WvAutoCast Current 
	    { get { return cur; } }
	object System.Collections.IEnumerator.Current 
	    { get { return cur; } }
	
	public WvAutoCast pop()
	{
	    MoveNext();
	    return Current;
	}
    }
    
    class WvDbusIter_Array 
	: WvDbusIterBase, IEnumerator<WvAutoCast>, IEnumerable<WvAutoCast>
    {
	WvAutoCast cur;
	
	internal WvDbusIter_Array(DataConverter conv, string sig, WvBytes b)
	    : base(conv, sig, b)
	{
	    Reset();
	}
	
	// IEnumerable
	public IEnumerator<WvAutoCast> GetEnumerator()
	{
	    return new WvDbusIter_Array(conv, sig, data.sub(start, end-start));
	}
	IEnumerator System.Collections.IEnumerable.GetEnumerator()
	{
	    return new WvDbusIter_Array(conv, sig, data.sub(start, end-start));
	}
	
	// IEnumerator
	public void Reset()
	{
	    _Reset();
	}
	
	// IEnumerator
	public bool MoveNext()
	{
	    if (atend())
		return false;
	    else
	    {
		cur = new WvAutoCast(getone(sig));
		return true;
	    }
	}
	
	// IEnumerator
	public void Dispose()
	{
	}
	
	public WvAutoCast Current 
	    { get { return cur; } }
	object System.Collections.IEnumerator.Current 
	    { get { return cur; } }
	
	public WvAutoCast pop()
	{
	    MoveNext();
	    return Current;
	}
    }
    
    public class WvDbusIterBase
    {
	internal DataConverter conv;
	protected string sig;
	protected byte[] data;
	protected int start, end, pos;
	
	internal WvDbusIterBase(DataConverter conv, string sig, WvBytes b)
	{
	    this.conv = conv;
	    this.sig = sig;
	    this.data = b.bytes;
	    this.start = b.start;
	    this.end = b.start + b.len;
	}
	
	protected void _Reset()
	{
	    pos = start;
	}
	
	protected bool atend()
	{
	    return pos >= end;
	}
	
	protected static string subsig(string sig, int offset)
	{
	    Dbus.DType dtype = (Dbus.DType)sig[offset];
	    switch (dtype)
	    {
	    case Dbus.DType.Array:
		return "a" + subsig(sig, offset+1);
	    case Dbus.DType.StructBegin:
	    case Dbus.DType.DictEntryBegin:
		{
		    int depth = 0, i;
		    for (i = offset; i < sig.Length; i++)
		    {
			Dbus.DType c = (Dbus.DType)sig[i];
			if (c == Dbus.DType.StructBegin 
			       || c == Dbus.DType.DictEntryBegin)
			    depth++;
			else if (c == Dbus.DType.StructEnd 
				 || c == Dbus.DType.DictEntryEnd)
			{
			    depth--;
			    if (depth <= 0) break;
			}
		    }
		    
		    wv.assert(depth==0,
			      wv.fmt("Mismatched brackets in '{0}'", sig));
		    return sig.Substring(offset, i-offset+1);
		}
	    default:
		return sig.Substring(offset, 1);
	    }
	}
	
	protected object getone(string sig)
	{
	    Dbus.DType dtype = (Dbus.DType)sig[0];
	    
	    switch (dtype)
	    {
	    case Dbus.DType.Byte:
		return ReadByte();
	    case Dbus.DType.Boolean:
		return ReadBoolean();
	    case Dbus.DType.Int16:
		return ReadInt16();
	    case Dbus.DType.UInt16:
		return ReadUInt16();
	    case Dbus.DType.Int32:
		return ReadInt32();
	    case Dbus.DType.UInt32:
		return ReadUInt32();
	    case Dbus.DType.Int64:
		return ReadInt64();
	    case Dbus.DType.UInt64:
		return ReadUInt64();
	    case Dbus.DType.Single:
		return ReadSingle();
	    case Dbus.DType.Double:
		return ReadDouble();
	    case Dbus.DType.String:
	    case Dbus.DType.ObjectPath:
		return ReadString();
	    case Dbus.DType.Signature:
		return ReadSignature();
	    case Dbus.DType.Variant:
		return ReadVariant();
	    case Dbus.DType.Array:
		return ReadArray(subsig(sig, 1));
	    case Dbus.DType.StructBegin:
	    case Dbus.DType.DictEntryBegin:
		return ReadStruct(sig);
	    default:
		throw new Exception("Unhandled D-Bus type: " + dtype);
	    }
	}
	
	void pad(int align)
	{
	    int pad = (align - (pos % align)) % align;
	    int upto = pos + pad;

	    for (; pos < upto; pos++)
		if (data[pos] != 0)
		    throw new Exception
		         (wv.fmt("Read non-zero byte at position 0x{0:x} "
				 + "while expecting padding", pos));
	}
	
	int _advance(int amt)
	{
	    int oldpos = pos;
	    pos += amt;
	    wv.assert(pos <= end, "Oops, decoded past end of buffer!");
	    return oldpos;
	}
	
	int advance(int amt)
	{
	    pad(amt);
	    return _advance(amt);
	}
	
	void ReadNull()
	{
	    advance(1);
	}
	
	byte ReadByte()
	{
	    return data[pos++];
	}

	bool ReadBoolean()
	{
	    uint intval = ReadUInt32();

	    switch (intval) {
	    case 0:
		return false;
	    case 1:
		return true;
	    default:
		throw new Exception("Read value " + intval + " at position " + pos + " while expecting boolean (0/1)");
	    }
	}

	short ReadInt16()
	{
	    return conv.GetInt16(data, advance(2));
	}

	ushort ReadUInt16()
	{
	    return (UInt16)ReadInt16();
	}

	int ReadInt32()
	{
	    return conv.GetInt32(data, advance(4));
	}

	uint ReadUInt32()
	{
	    return (UInt32)ReadInt32();
	}

	long ReadInt64()
	{
	    return conv.GetInt64(data, advance(8));
	}

	ulong ReadUInt64()
	{
	    return (UInt64)ReadInt64();
	}

	float ReadSingle()
	{
	    return conv.GetFloat(data, advance(4));
	}

	double ReadDouble()
	{
	    return conv.GetDouble(data, advance(8));
	}
	
	int ReadLength()
	{
	    uint len = ReadUInt32();
	    if (len > Int32.MaxValue)
		throw new Exception(wv.fmt("Invalid string length ({0})", len));
	    return (int)len;
	}

	string ReadString()
	{
	    int len = ReadLength();
	    string val = Encoding.UTF8.GetString(data, pos, len);
	    _advance(len);
	    ReadNull();
	    return val;
	}

	string ReadSignature()
	{
	    int len = ReadByte();
	    return Encoding.UTF8.GetString(data, _advance(len+1), len);
	}
	
	object ReadVariant()
	{
	    string vsig = ReadSignature();
	    return getone(vsig);
	}
	
	IEnumerable<WvAutoCast> ReadArray(string subsig)
	{
	    int len = ReadLength();
	    pad(Dbus.Protocol.GetAlignment((Dbus.DType)subsig[0]));
	    var x = new WvDbusIter_Array(conv, subsig, data.sub(pos, len));
	    _advance(len);
	    return x;
	}
	
	IEnumerable<WvAutoCast> ReadStruct(string structsig)
	{
	    Dbus.DType first = (Dbus.DType)structsig[0];
	    Dbus.DType last  = (Dbus.DType)structsig[structsig.Length-1];
	    
	    if (first == Dbus.DType.StructBegin)
		wv.assert(last == Dbus.DType.StructEnd, "No matching ')'");
	    else if (first == Dbus.DType.DictEntryBegin)
		wv.assert(last == Dbus.DType.DictEntryEnd, "No matching '}'");
	    else
		wv.assert(false,
			  wv.fmt("ReadStruct called for unknown type '{0}'",
				 (char)first));
	    
	    structsig = structsig.Substring(1, structsig.Length-2);
	    pad(8); // structs are always 8-padded
	    
	    List<WvAutoCast> list = new List<WvAutoCast>();
	    for (int subpos = 0; subpos < structsig.Length; )
	    {
		string sub = subsig(structsig, subpos);
		list.Add(new WvAutoCast(getone(sub)));
		subpos += sub.Length;
	    }
	    
	    return list;
	}
    }
}
