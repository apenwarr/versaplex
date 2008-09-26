// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Linq;
using Mono;
using Wv.Extensions;

namespace Wv
{
    public class WvDBusIter : WvDBusIterBase, IEnumerator<WvAutoCast>
    {
	int sigpos;
	WvAutoCast cur;
	
	public static WvDBusIter open(Message m)
	{
	    DataConverter conv = m.Header.Endianness==EndianFlag.Little 
		    ? DataConverter.LittleEndian : DataConverter.BigEndian;
	    
	    byte[] data = m.Body;
	    return new WvDBusIter(conv, m.Header.Signature,
				  data, 0, data.Length);
	}
	
	internal WvDBusIter(DataConverter conv, string sig,
			    byte[] data, int start, int end)
	    : base(conv, sig, data, start, end)
	{
	    Reset();
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
		cur = new WvAutoCast(getone(subsig(sig, sigpos)));
		sigpos++;
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
	
	public WvAutoCast getnext()
	{
	    MoveNext();
	    return Current;
	}
    }
    
    class WvDBusIter_Array : WvDBusIterBase, IEnumerator<WvAutoCast>
    {
	int pos;
	WvAutoCast cur;
	
	internal WvDBusIter_Array(DataConverter conv, string sig,
				  byte[] data, int start, int end)
	    : base(conv, sig, data, start, end)
	{
	    Reset();
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
	
	public WvAutoCast getnext()
	{
	    MoveNext();
	    return Current;
	}
    }
    
    public class WvDBusIterBase
    {
	DataConverter conv;
	protected string sig;
	byte[] data;
	int start, end, pos;
	
	internal WvDBusIterBase(DataConverter conv, string sig,
				byte[] data, int start, int end)
	{
	    this.conv = conv;
	    this.sig = sig;
	    this.data = data;
	    this.start = start;
	    this.end = end;
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
	    return sig.Substring(offset);
	}
	
	protected object getone(string sig)
	{
	    DType dtype = (DType)sig[0];
	    switch (dtype)
	    {
	    case DType.Byte:
		return ReadByte();
	    case DType.Boolean:
		return ReadBoolean();
	    case DType.Int16:
		return ReadInt16();
	    case DType.UInt16:
		return ReadUInt16();
	    case DType.Int32:
		return ReadInt32();
	    case DType.UInt32:
		return ReadUInt32();
	    case DType.Int64:
		return ReadInt64();
	    case DType.UInt64:
		return ReadUInt64();
	    case DType.Single:
		return ReadSingle();
	    case DType.Double:
		return ReadDouble();
	    case DType.String:
	    case DType.ObjectPath:
		return ReadString();
	    case DType.Signature:
		return ReadSignature();
	    case DType.Array:
		return ReadArray(subsig(sig, 1));
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
		         (wv.fmt("Read non-zero byte at position {0} "
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
	    pos += len;
	    ReadNull();
	    return val;
	}

	string ReadSignature()
	{
	    int len = ReadByte();
	    return Encoding.UTF8.GetString(data, advance(len+1), len);
	}
	
	IEnumerator<WvAutoCast> ReadArray(string subsig)
	{
	    int len = ReadLength();
	    var x = new WvDBusIter_Array(conv, subsig,
					 data, pos, pos+len);
	    advance(len);
	    return x;
	}
    }
    
    public class MessageReader
    {
	DataConverter conv;
	byte[] data;
	int pos = 0;
	Message message;
	
	public MessageReader(EndianFlag endianness, byte[] data)
	{
	    this.data = data;
	    this.conv = endianness==EndianFlag.Little 
		? DataConverter.LittleEndian : DataConverter.BigEndian;
	    
	    if (data == null)
		throw new ArgumentNullException("data");
	}

        public MessageReader(Message message) 
	    : this(message.Header.Endianness, message.Body)
	{
	    if (message == null)
		throw new ArgumentNullException("message");

	    this.message = message;
	}
	
	public object ReadValue(Type type)
	{
	    if (type == typeof(void))
		return null;

	    if (type.IsArray) {
		return ReadArray(type.GetElementType());
	    }
	    else if (type == typeof(ObjectPath)) {
		return ReadObjectPath();
	    }
	    else if (type == typeof(Signature)) {
		return ReadSignature();
	    }
	    else if (type == typeof(object)) {
		return ReadVariant();
	    }
	    else if (type == typeof(string)) {
		return ReadString();
	    }
	    else if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IDictionary<,>)) {
		Type[] genArgs = type.GetGenericArguments();
		Type dictType = Mapper.GetGenericType(typeof(Dictionary<,>), genArgs);
		IDictionary idict = (IDictionary)Activator.CreateInstance(dictType, new object[0]);
		GetValueToDict(genArgs[0], genArgs[1], idict);
		return idict;
	    }
	    else if (Mapper.IsPublic(type)) {
		return GetObject(type);
	    }
	    else if (!type.IsPrimitive && !type.IsEnum) {
		return ReadStruct(type);
	    }
	    else {
		object val;
		DType dtype = Signature.TypeToDType(type);
		val = ReadValue(dtype);

		if (type.IsEnum)
		    val = Enum.ToObject(type, val);

		return val;
	    }
	}

	//helper method, should not be used generally
	public object ReadValue(DType dtype)
	{
	    switch (dtype)
	    {
	    case DType.Byte:
		return ReadByte();

	    case DType.Boolean:
		return ReadBoolean();

	    case DType.Int16:
		return ReadInt16();

	    case DType.UInt16:
		return ReadUInt16();

	    case DType.Int32:
		return ReadInt32();

	    case DType.UInt32:
		return ReadUInt32();

	    case DType.Int64:
		return ReadInt64();

	    case DType.UInt64:
		return ReadUInt64();

	    case DType.Single:
		return ReadSingle();

	    case DType.Double:
		return ReadDouble();

	    case DType.String:
		return ReadString();

	    case DType.ObjectPath:
		return ReadObjectPath();

	    case DType.Signature:
		return ReadSignature();

	    case DType.Variant:
		return ReadVariant();

	    default:
		throw new Exception("Unhandled D-Bus type: " + dtype);
	    }
	}

	public object GetObject(Type type)
	{
	    ObjectPath path = ReadObjectPath();

	    return message.Connection.GetObject(type, (string)message.Header.Fields[FieldCode.Sender], path);
	}

	public byte ReadByte()
	{
	    return data[pos++];
	}

	public bool ReadBoolean()
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

	public short ReadInt16()
	{
	    ReadPad(2);
	    pos += 2;
	    return conv.GetInt16(data, pos-2);
	}

	public ushort ReadUInt16()
	{
	    return (UInt16)ReadInt16();
	}

	public int ReadInt32()
	{
	    ReadPad(4);
	    pos += 4;
	    return conv.GetInt32(data, pos-4);
	}

	public uint ReadUInt32()
	{
	    return (UInt32)ReadInt32();
	}

	public long ReadInt64()
	{
	    ReadPad(8);
	    pos += 8;
	    return conv.GetInt64(data, pos-8);
	}

	public ulong ReadUInt64()
	{
	    return (UInt64)ReadInt64();
	}

	public float ReadSingle()
	{
	    ReadPad(4);
	    pos += 4;
	    return conv.GetFloat(data, pos-4);
	}

        public double ReadDouble()
	{
	    ReadPad(8);
	    pos += 8;
	    return conv.GetDouble(data, pos-8);
	}

	public string ReadString()
	{
	    uint _ln = ReadUInt32();
	    if (_ln > Int32.MaxValue)
		throw new Exception(wv.fmt("Invalid string length ({0})", _ln));
	    int ln = (int)_ln;

	    string val = Encoding.UTF8.GetString(data, pos, ln);
	    pos += ln;
	    ReadNull();
	    return val;
	}

	public ObjectPath ReadObjectPath()
	{
	    return new ObjectPath(ReadString());
	}

	public Signature ReadSignature()
	{
	    int ln = ReadByte();

	    byte[] sigData = new byte[ln];
	    Array.Copy(data, pos, sigData, 0, ln);
	    pos += ln;
	    ReadNull();

	    return new Signature(sigData);
	}

	public object ReadVariant()
	{
	    return ReadVariant(ReadSignature());
	}

	object ReadVariant(Signature sig)
	{
	    return ReadValue(sig.ToType());
	}

	//not pretty or efficient but works
	public void GetValueToDict(Type keyType, Type valType, IDictionary val)
	{
	    uint ln = ReadUInt32();

	    if (ln > Protocol.MaxArrayLength)
		throw new Exception("Dict length " + ln + " exceeds maximum allowed " + Protocol.MaxArrayLength + " bytes");

	    //advance to the alignment of the element
	    //ReadPad (Protocol.GetAlignment (Signature.TypeToDType (type)));
	    ReadPad(8);

	    int endPos = pos + (int)ln;

	    //while (stream.Position != endPos)
	    while (pos < endPos)
	    {
		ReadPad(8);

		val.Add(ReadValue(keyType), ReadValue(valType));
	    }

	    if (pos != endPos)
		throw new Exception("Read pos " + pos + " != ep " + endPos);
	}
	
	int GetAlignment(Type t)
	{
	    return Protocol.GetAlignment(Signature.TypeToDType(t));
	}

	public Array ReadArray(Type type)
	{
	    wv.printerr("ReadArray({0}) @ 0x{1:x}\n", type, pos);
	    
	    uint _ln = ReadUInt32();
	    if (_ln > Protocol.MaxArrayLength)
		throw new Exception(wv.fmt("Array length {0} is > {1} bytes",
					   _ln, Protocol.MaxArrayLength));
	    int ln = (int)_ln;
	    int end = pos + ln;

	    // advance to the alignment of the element
	    int align = GetAlignment(type);
	    
	    var a = new ArrayList();
	    while (pos < end)
	    {
		ReadPad(align);
		a.Add(ReadValue(type));
	    }

	    return a.ToArray(type);
	}
	
	public T[] ReadArray<T>()
	{
	    return (T[])ReadArray(typeof(T));
	}
	
	public void ReadArrayFunc(int align, Action<MessageReader> action)
	{
	    uint _ln = ReadUInt32();
	    if (_ln > Protocol.MaxArrayLength)
		throw new Exception(wv.fmt("Array length {0} is > {1} bytes",
					   _ln, Protocol.MaxArrayLength));
	    int ln = (int)_ln;
	    int end = pos + ln;

	    while (pos < end)
	    {
		ReadPad(align);
		action(this);
	    }
	}

	//struct
	//probably the wrong place for this
	//there might be more elegant solutions
	public object ReadStruct(Type type)
	{
	    wv.printerr("pos={0}, type={1}\n", pos, type);
	    ReadPad(8);

	    object val = Activator.CreateInstance(type);

	    FieldInfo[] fis = type.GetFields(BindingFlags.Public
			     | BindingFlags.NonPublic | BindingFlags.Instance);

	    foreach (System.Reflection.FieldInfo fi in fis)
		fi.SetValue(val, ReadValue(fi.FieldType));

	    wv.printerr("  done: pos={0}\n", pos);
	    return val;
	}

	public void ReadNull()
	{
	    if (data[pos] != 0)
		throw new Exception("Read non-zero byte at position " + pos + " while expecting null terminator");
	    pos++;
	}

	public void ReadPad(int alignment)
	{
	    for (int endPos = Protocol.Padded(pos, alignment) ; pos != endPos ; pos++)
		if (data[pos] != 0)
		    throw new Exception("Read non-zero byte at position " + pos + " while expecting padding");
	}
    }
}
