// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details
//
using System;
using System.Collections.Generic;
using Wv;

namespace org.freedesktop.DBus
{
    [Flags]
    public enum NameFlag : uint
    {
	None = 0,
	AllowReplacement = 0x1,
	ReplaceExisting = 0x2,
	DoNotQueue = 0x4,
    }

    public enum RequestNameReply : uint
    {
	PrimaryOwner = 1,
	InQueue,
	Exists,
	AlreadyOwner,
    }

    public enum ReleaseNameReply : uint
    {
	Released = 1,
	NonExistent,
	NotOwner,
    }

    public enum StartReply : uint
    {
	//The service was successfully started.
	Success = 1,
	//A connection already owns the given name.
	AlreadyRunning,
    }
}
