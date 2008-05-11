using System;
using System.Runtime.Serialization;

// FIXME: This should go in an assembly rather than being pasted from
// Versaplex/server/VxDbus.cs

class DbusError : Exception {
    public DbusError()
        : base()
    {
    }
    
    public DbusError(string msg)
        : base(msg)
    {
    }

    public DbusError(SerializationInfo si, StreamingContext sc)
        : base(si, sc)
    {
    }

    public DbusError(string msg, Exception inner)
        : base(msg, inner)
    {
    }
}
