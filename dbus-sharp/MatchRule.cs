// Copyright 2007 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Text;
using System.Collections.Generic;

namespace Wv
{
    class MatchRule
    {
	public MessageType? MessageType;
	public string Interface;
	public string Member;
	public string Path;
	public string Sender;
	public string Destination;
	public readonly SortedDictionary<int,string> Args = new SortedDictionary<int,string> ();

	public MatchRule ()
	{
	}

	void Append (StringBuilder sb, string key, string value)
	{
	    if (sb.Length != 0)
		sb.Append (",");

	    sb.Append (key + "='");
	    sb.Append (value);
	    sb.Append ("'");
	}

	void AppendArg (StringBuilder sb, int index, string value)
	{
	    Append (sb, "arg" + index, value);
	}

	public override bool Equals (object o)
	{
	    MatchRule r = o as MatchRule;

	    if (r == null)
		return false;

	    if (r.MessageType != MessageType)
		return false;

	    if (r.Interface != Interface)
		return false;

	    if (r.Member != Member)
		return false;

	    if (r.Path != Path)
		return false;

	    if (r.Sender != Sender)
		return false;

	    if (r.Destination != Destination)
		return false;

	    //FIXME: do args

	    return true;
	}

	public override int GetHashCode ()
	{
	    //FIXME: not at all optimal
	    return ToString ().GetHashCode ();
	}

	public static string MessageTypeToString(Wv.MessageType mtype)
	{
	    switch (mtype)
	    {
	    case Wv.MessageType.MethodCall:
		return "method_call";
	    case Wv.MessageType.MethodReturn:
		return "method_return";
	    case Wv.MessageType.Error:
		return "error";
	    case Wv.MessageType.Signal:
		return "signal";
	    case Wv.MessageType.Invalid:
		return "invalid";
	    default:
		throw new Exception("Bad MessageType: " + mtype);
	    }
	}

	public override string ToString ()
	{
	    StringBuilder sb = new StringBuilder ();

	    if (MessageType != null)
		Append (sb, "type", MessageTypeToString ((MessageType)MessageType));

	    if (Interface != null)
		Append (sb, "interface", Interface);

	    if (Member != null)
		Append (sb, "member", Member);

	    if (Path != null)
		Append (sb, "path", Path);

	    if (Sender != null)
		Append (sb, "sender", Sender);

	    if (Destination != null)
		Append (sb, "destination", Destination);

	    if (Args != null) {
		foreach (KeyValuePair<int,string> pair in Args)
		    AppendArg (sb, pair.Key, pair.Value);
	    }

	    return sb.ToString ();
	}

	//this is useful as a Predicate<Message> delegate
	public bool Matches (Message msg)
	{
	    if (MessageType != null)
		if (msg.type != MessageType)
		    return false;
	    if (Interface != null)
		if (msg.ifc != Interface)
		    return false;
	    if (Member != null)
		if (msg.method != Member)
			return false;
	    if (Path != null)
		if (msg.path != Path)
			return false;
	    if (Sender != null)
		if (msg.sender != Sender)
			return false;
	    if (Destination != null)
		if (msg.dest != Destination)
			return false;
	    return true;
	}
    }
}
