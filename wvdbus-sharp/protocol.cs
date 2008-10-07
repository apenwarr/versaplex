// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections.Generic;
using Wv.Extensions;

namespace Wv
{
    namespace Dbus
    {
        public enum MType : byte
	{
	    Invalid      = 0,
	    MethodCall   = 1,
	    MethodReturn = 2,
	    Error        = 3,
	    Signal       = 4,
	}

        public enum Field : byte
	{
	    Invalid     = 0,
	    Path        = 1,
	    Interface   = 2,
	    Member      = 3,
	    ErrorName   = 4,
	    ReplySerial = 5,
	    Destination = 6,
	    Sender      = 7,
	    Signature   = 8,
	}
	
        public enum Endian : byte
	{
	    Little = (byte)'l',
	    Big = (byte)'B',
	}

	[Flags]
        public enum MFlag : byte
	{
	    None            = 0x00,
	    NoReplyExpected = 0x01,
	    NoAutoStart     = 0x02,
	}

        public enum DType : byte
	{
	    Invalid        = (byte)'\0',

	    Byte           = (byte)'y',
	    Boolean        = (byte)'b',
	    Int16          = (byte)'n',
	    UInt16         = (byte)'q',
	    Int32          = (byte)'i',
	    UInt32         = (byte)'u',
	    Int64          = (byte)'x',
	    UInt64         = (byte)'t',
	    Single         = (byte)'f',
	    Double         = (byte)'d',
	    String         = (byte)'s',
	    ObjectPath     = (byte)'o',
	    Signature      = (byte)'g',
		
	    Array          = (byte)'a',
	    Variant        = (byte)'v',

	    Struct         = (byte)'r',
	    StructBegin    = (byte)'(',
	    StructEnd      = (byte)')',

	    DictEntry      = (byte)'e',
	    DictEntryBegin = (byte)'{',
	    DictEntryEnd   = (byte)'}',
	}
    
	static class Protocol
	{
	    //protocol versions that we support
	    public const byte MinVersion = 0;
	    public const byte Version = 1;
	    public const byte MaxVersion = Version;
	    
	    public const uint MaxMessageLength = 134217728; //2 to the 27th power
	    public const uint MaxArrayLength = 67108864; //2 to the 26th power
	    public const uint MaxSignatureLength = 255;
	    public const uint MaxArrayDepth = 32;
	    public const uint MaxStructDepth = 32;
	    
	    public static int PadNeeded (int pos, int alignment)
	    {
		int pad = pos % alignment;
		pad = pad == 0 ? 0 : alignment - pad;
		
		return pad;
	    }
	    
	    public static int Padded (int pos, int alignment)
	    {
		int pad = pos % alignment;
		if (pad != 0)
		    pos += alignment - pad;
		
		return pos;
	    }
	    
	    public static int GetAlignment(DType dtype)
	    {
		switch (dtype) {
		case DType.Byte:
		    return 1;
		case DType.Boolean:
		    return 4;
		case DType.Int16:
		case DType.UInt16:
		    return 2;
		case DType.Int32:
		case DType.UInt32:
		    return 4;
		case DType.Int64:
		case DType.UInt64:
		    return 8;
		case DType.Single:
		    return 4;
		case DType.Double:
		    return 8;
		case DType.String:
		    return 4;
		case DType.ObjectPath:
		    return 4;
		case DType.Signature:
		    return 1;
		case DType.Array:
		    return 4;
		case DType.Struct:
		case DType.StructBegin:
		case DType.StructEnd:
		case DType.DictEntry:
		case DType.DictEntryBegin:
		case DType.DictEntryEnd:
		    return 8;
		case DType.Variant:
		    return 1;
		case DType.Invalid:
		default:
		    throw new Exception("Cannot determine alignment of " + dtype);
		}
	    }
	}
    }
    
    public class WvDbusError : Exception
    {
        public WvDbusError() : base()
	{
	}
	    
	public WvDbusError(string msg) : base(msg)
	{
	}
	    
        public WvDbusError(string msg, Exception inner) : base(msg, inner)
	{
	}
    }
}
