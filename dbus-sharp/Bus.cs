// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections.Generic;
using org.freedesktop.DBus;

namespace Wv
{
    using Transports;

    public sealed class Bus : Connection
    {
	static Bus systemBus = null;
	public static Bus System
	{
	    get {
		if (systemBus == null) {
		    try {
			if (Address.StarterBusType == "system")
			    systemBus = Starter;
			else
			    systemBus = Bus.Open(Address.System);
		    }
		    catch (Exception e) {
			throw new Exception("Unable to open the system message bus.", e);
		    }
		}

		return systemBus;
	    }
	}

	static Bus sessionBus = null;
	public static Bus Session
	{
	    get {
		if (sessionBus == null) {
		    try {
			if (Address.StarterBusType == "session")
			    sessionBus = Starter;
			else
			    sessionBus = Bus.Open(Address.Session);
		    }
		    catch (Exception e) {
			throw new Exception("Unable to open the session message bus.", e);
		    }
		}

		return sessionBus;
	    }
	}

	//TODO: parsing of starter bus type, or maybe do this another way
	static Bus starterBus = null;
	public static Bus Starter
	{
	    get {
		if (starterBus == null) {
		    try {
			starterBus = Bus.Open(Address.Starter);
		    }
		    catch (Exception e) {
			throw new Exception("Unable to open the starter message bus.", e);
		    }
		}

		return starterBus;
	    }
	}

	//TODO: use the guid, not the whole address string
	//TODO: consider what happens when a connection has been closed
	static Dictionary<string,Bus> buses = new Dictionary<string,Bus>();

	public static new Bus Open(string address)
	{
	    if (address == null)
		throw new ArgumentNullException("address");

	    if (buses.ContainsKey(address))
		return buses[address];

	    Bus bus = new Bus(address);
	    buses[address] = bus;

	    return bus;
	}

	static readonly string DBusName = "org.freedesktop.DBus";
	static readonly string DBusPath = "/org/freedesktop/DBus";

        public Bus(string address) : base(address)
	{
	    Register();
	}

        public Bus(Transport trans) : base(trans)
	{
	    Authenticate();

	    Register();
	}

        private WvAutoCast CallDBusMethod(string method)
        {
            return CallDBusMethod(method, "", new byte[0]);
        }

        private WvAutoCast CallDBusMethod(string method, string param)
        {
            MessageWriter w = new MessageWriter();
            w.Write(param);

            return CallDBusMethod(method, "s", w.ToArray());
        }

        private WvAutoCast CallDBusMethod(string method, string p1, uint p2)
        {
            MessageWriter w = new MessageWriter();
            w.Write(p1);
            w.Write(p2);

            return CallDBusMethod(method, "su", w.ToArray());
        }

        private WvAutoCast CallDBusMethod(string method, string sig, 
            byte[] body)
        {
            Message m = new Message();
            m.signature = sig;
            m.type = MessageType.MethodCall;
            m.ReplyExpected = true;
            m.dest = DBusName;
            m.path = DBusPath;
            m.ifc = DBusName;
            m.method = method;
            m.Body = body;

            Message reply = SendWithReplyAndBlock(m);

            var i = reply.iter();
            return i.pop();
        }

	void Register()
	{
	    if (unique_name != null)
		throw new Exception("Bus already has a unique name");

            unique_name = CallDBusMethod("Hello");
	}

	public string GetUnixUserName(string name)
	{
	    return CallDBusMethod("GetConnectionUnixUserName", name);
	}

	public ulong GetUnixUser(string name)
	{
            return CallDBusMethod("GetUnixUser", name);
	}

	public string GetCert(string name)
	{
            return CallDBusMethod("GetCert", name);
	}

	public string GetCertFingerprint(string name)
	{
            return CallDBusMethod("GetCertFingerprint", name);
	}

	public RequestNameReply RequestName(string name)
	{
	    return RequestName(name, NameFlag.None);
	}

	public RequestNameReply RequestName(string name, NameFlag flags)
	{
            WvAutoCast reply = CallDBusMethod("RequestName", name, (uint)flags);
            return (RequestNameReply)(uint)reply;
	}

	public ReleaseNameReply ReleaseName(string name)
	{
            return (ReleaseNameReply)(uint)CallDBusMethod("ReleaseName", name);
	}

	public bool NameHasOwner(string name)
	{
            return CallDBusMethod("NameHasOwner", name);
	}

	public StartReply StartServiceByName(string name)
	{
	    return StartServiceByName(name, 0);
	}

	public StartReply StartServiceByName(string name, uint flags)
	{
            var retval = CallDBusMethod("StartServiceByName", name, flags);
            return (StartReply)(uint)retval;
	}

	internal protected override void AddMatch(string rule)
	{
            CallDBusMethod("AddMatch", rule);
	}

	internal protected override void RemoveMatch(string rule)
	{
            CallDBusMethod("RemoveMatch", rule);
	}

	string unique_name = null;
	public string UniqueName
	{
	    get {
		return unique_name;
	    }
	    set {
		if (unique_name != null)
		    throw new Exception("Unique name can only be set once");
		unique_name = value;
	    }
	}
    }
}
