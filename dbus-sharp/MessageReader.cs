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

namespace Wv
{
    public class MessageReader
    {
	EndianFlag endianness;
	byte[] data;
	int pos = 0;
	Message message;

	public int Position {
	    get { return pos; }
	}

	public MessageReader (EndianFlag endianness, byte[] data)
	{
	    if (data == null)
		throw new ArgumentNullException ("data");

	    this.endianness = endianness;
	    this.data = data;
	}

        public MessageReader (Message message) : this (message.Header.Endianness, message.Body)
	{
	    if (message == null)
		throw new ArgumentNullException ("message");

	    this.message = message;
	}
	
	public object ReadValue(Type type)
	{
	    if (type == typeof (void))
		return null;

	    if (type.IsArray) {
		return ReadArray(type.GetElementType());
	    }
	    else if (type == typeof (ObjectPath)) {
		return ReadObjectPath ();
	    }
	    else if (type == typeof (Signature)) {
		return ReadSignature ();
	    }
	    else if (type == typeof (object)) {
		return ReadVariant ();
	    }
	    else if (type == typeof (string)) {
		return ReadString ();
	    }
	    else if (type.IsGenericType && type.GetGenericTypeDefinition () == typeof (IDictionary<,>)) {
		Type[] genArgs = type.GetGenericArguments ();
		Type dictType = Mapper.GetGenericType (typeof (Dictionary<,>), genArgs);
		IDictionary idict = (IDictionary)Activator.CreateInstance(dictType, new object[0]);
		GetValueToDict (genArgs[0], genArgs[1], idict);
		return idict;
	    }
	    else if (Mapper.IsPublic (type)) {
		return GetObject (type);
	    }
	    else if (!type.IsPrimitive && !type.IsEnum) {
		return ReadStruct (type);
	    }
	    else {
		object val;
		DType dtype = Signature.TypeToDType (type);
		val = ReadValue (dtype);

		if (type.IsEnum)
		    val = Enum.ToObject (type, val);

		return val;
	    }
	}
	
	//helper method, should not be used generally
	public object ReadValue (DType dtype)
	{
	    switch (dtype)
	    {
	    case DType.Byte:
		return ReadByte ();

	    case DType.Boolean:
		return ReadBoolean ();

	    case DType.Int16:
		return ReadInt16 ();

	    case DType.UInt16:
		return ReadUInt16 ();

	    case DType.Int32:
		return ReadInt32 ();

	    case DType.UInt32:
		return ReadUInt32 ();

	    case DType.Int64:
		return ReadInt64 ();

	    case DType.UInt64:
		return ReadUInt64 ();

	    case DType.Single:
		return ReadSingle ();

	    case DType.Double:
		return ReadDouble ();

	    case DType.String:
		return ReadString ();

	    case DType.ObjectPath:
		return ReadObjectPath ();

	    case DType.Signature:
		return ReadSignature ();

	    case DType.Variant:
		return ReadVariant ();

	    default:
		throw new Exception ("Unhandled D-Bus type: " + dtype);
	    }
	}

	public object GetObject (Type type)
	{
	    ObjectPath path = ReadObjectPath ();

	    return message.Connection.GetObject (type, (string)message.Header.Fields[FieldCode.Sender], path);
	}

	public byte ReadByte ()
	{
	    return data[pos++];
	}

	public bool ReadBoolean ()
	{
	    uint intval = ReadUInt32 ();

	    switch (intval) {
	    case 0:
		return false;
	    case 1:
		return true;
	    default:
		throw new Exception ("Read value " + intval + " at position " + pos + " while expecting boolean (0/1)");
	    }
	}

	unsafe protected void MarshalUShort (byte *dst)
	{
	    ReadPad (2);

	    if (endianness == Connection.NativeEndianness) {
		dst[0] = data[pos + 0];
		dst[1] = data[pos + 1];
	    }
	    else {
		dst[0] = data[pos + 1];
		dst[1] = data[pos + 0];
	    }

	    pos += 2;
	}

	unsafe public short ReadInt16 ()
	{
	    short val;

	    MarshalUShort ((byte*)&val);

	    return val;
	}

	unsafe public ushort ReadUInt16 ()
	{
	    ushort val;

	    MarshalUShort ((byte*)&val);

	    return val;
	}

	unsafe protected void MarshalUInt (byte *dst)
	{
	    ReadPad (4);

	    if (endianness == Connection.NativeEndianness) {
		dst[0] = data[pos + 0];
		dst[1] = data[pos + 1];
		dst[2] = data[pos + 2];
		dst[3] = data[pos + 3];
	    }
	    else {
		dst[0] = data[pos + 3];
		dst[1] = data[pos + 2];
		dst[2] = data[pos + 1];
		dst[3] = data[pos + 0];
	    }

	    pos += 4;
	}

	unsafe public int ReadInt32 ()
	{
	    int val;

	    MarshalUInt ((byte*)&val);

	    return val;
	}

	unsafe public uint ReadUInt32 ()
	{
	    uint val;

	    MarshalUInt ((byte*)&val);

	    return val;
	}

	unsafe protected void MarshalULong (byte *dst)
	{
	    ReadPad (8);

	    if (endianness == Connection.NativeEndianness) {
		for (int i = 0; i < 8; ++i)
		    dst[i] = data[pos + i];
	    }
	    else {
		for (int i = 0; i < 8; ++i)
		    dst[i] = data[pos + (7 - i)];
	    }

	    pos += 8;
	}

	unsafe public long ReadInt64 ()
	{
	    long val;

	    MarshalULong ((byte*)&val);

	    return val;
	}

	unsafe public ulong ReadUInt64 ()
	{
	    ulong val;

	    MarshalULong ((byte*)&val);

	    return val;
	}

	unsafe public float ReadSingle ()
	{
	    float val;

	    MarshalUInt ((byte*)&val);

	    return val;
	}

	unsafe public double ReadDouble ()
	{
	    double val;

	    MarshalULong ((byte*)&val);

	    return val;
	}

	public string ReadString ()
	{
	    uint ln = ReadUInt32 ();

	    string val = Encoding.UTF8.GetString (data, pos, (int)ln);
	    pos += (int)ln;
	    ReadNull ();

	    return val;
	}

	public ObjectPath ReadObjectPath ()
	{
	    //exactly the same as string
	    return new ObjectPath (ReadString ());
	}

	public Signature ReadSignature ()
	{
	    byte ln = ReadByte ();

	    if (ln > Protocol.MaxSignatureLength)
		throw new Exception("Signature length " + ln + " exceeds maximum allowed " + Protocol.MaxSignatureLength + " bytes");

	    byte[] sigData = new byte[ln];
	    Array.Copy (data, pos, sigData, 0, (int)ln);
	    pos += (int)ln;
	    ReadNull ();

	    return new Signature (sigData);
	}

	public object ReadVariant ()
	{
	    return ReadVariant (ReadSignature ());
	}

	object ReadVariant (Signature sig)
	{
	    return ReadValue (sig.ToType ());
	}

	//not pretty or efficient but works
	public void GetValueToDict (Type keyType, Type valType, System.Collections.IDictionary val)
	{
	    uint ln = ReadUInt32 ();

	    if (ln > Protocol.MaxArrayLength)
		throw new Exception ("Dict length " + ln + " exceeds maximum allowed " + Protocol.MaxArrayLength + " bytes");

	    //advance to the alignment of the element
	    //ReadPad (Protocol.GetAlignment (Signature.TypeToDType (type)));
	    ReadPad (8);

	    int endPos = pos + (int)ln;

	    //while (stream.Position != endPos)
	    while (pos < endPos)
	    {
		ReadPad (8);

		val.Add (ReadValue (keyType), ReadValue (valType));
	    }

	    if (pos != endPos)
		throw new Exception ("Read pos " + pos + " != ep " + endPos);
	}
	
	int GetAlignment(Type t)
	{
	    wv.printerr("GetAlignment: {0}/{1}/{2}\n",
			t, t.IsPrimitive, t.IsEnum);
/*	    if (!t.IsPrimitive && !t.IsEnum)
		return 8;
	    else*/
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
	    int oldpos = pos;
	    int end = pos + ln;

	    // advance to the alignment of the element
	    int align = GetAlignment(type);
	    ReadPad(align);
	    end -= (pos-oldpos);
 
	    var a = new ArrayList();
	    while (pos < end)
		a.Add(ReadValue(type));

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
	    int oldpos = pos;
	    int end = pos + ln;

	    // advance to the alignment of the element
	    ReadPad(align);
	    end -= (pos-oldpos);
 
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
		throw new Exception ("Read non-zero byte at position " + pos + " while expecting null terminator");
	    pos++;
	}

	public void ReadPad (int alignment)
	{
	    for (int endPos = Protocol.Padded (pos, alignment) ; pos != endPos ; pos++)
		if (data[pos] != 0)
		    throw new Exception ("Read non-zero byte at position " + pos + " while expecting padding");
	}
    }
}
