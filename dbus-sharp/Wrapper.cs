// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections.Generic;
using System.IO;

namespace Wv
{
    //TODO: complete and use these wrapper classes
    //not sure exactly what I'm thinking but there seems to be sense here

    // FIXME: signature sending/receiving is currently ambiguous in this code
    // 
    // FIXME: in fact, these classes are totally broken and end up doing
    // no-op, do not use without understanding the problem
    class MethodCall
    {
	public Message msg = new Message ();

	public MethodCall (string path, string @interface, string member, string destination, string signature)
	{
	    msg.type = MessageType.MethodCall;
	    msg.ReplyExpected = true;
	    msg.path = path;
	    if (@interface != null)
		msg.ifc = @interface;
	    msg.method = member;
	    msg.dest = destination;
	    msg.signature = signature;
	}

	public MethodCall (Message msg)
	{
	    this.msg = msg;
	    Path = msg.path;
	    Interface = msg.ifc;
	    Member = msg.method;
	    Destination = msg.dest;
	    Sender = msg.sender;
	    Signature = msg.Signature;
	}

	public string Path;
	public string Interface;
	public string Member;
	public string Destination;
	public string Sender;
	public Signature Signature;
    }

    class MethodReturn
    {
	public Message msg = new Message ();

	public MethodReturn (uint reply_serial)
	{
	    msg.type = MessageType.MethodReturn;
	    msg.flags = HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;
	    msg.rserial = reply_serial;
	}

	public MethodReturn (Message msg)
	{
	    this.msg = msg;
	    ReplySerial = msg.rserial.Value;
	}

	public uint ReplySerial;
    }

    class Error
    {
	public Message msg = new Message ();

	public Error (string error_name, uint reply_serial)
	{
	    msg.type = MessageType.Error;
	    msg.flags = HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;
	    msg.err = error_name;
	    msg.rserial = reply_serial;
	}

	public Error (Message msg)
	{
	    this.msg = msg;
	    ErrorName = msg.err;
	    ReplySerial = msg.rserial.Value;
	}

	public string ErrorName;
	public uint ReplySerial;
	//public Signature Signature;
    }

    class Signal
    {
	public Message msg = new Message ();

	public Signal (string path, string @interface, string member)
	{
	    msg.type = MessageType.Signal;
	    msg.flags = HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;
	    msg.path = path;
	    msg.ifc = @interface;
	    msg.method = member;
	}

	public Signal (Message msg)
	{
	    this.msg = msg;
	    Path = msg.path;
	    Interface = msg.ifc;
	    Member = msg.method;
	    Sender = msg.sender;
	}

	public string Path;
	public string Interface;
	public string Member;
	public string Sender;
    }
}
