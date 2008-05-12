using System;
using System.Runtime.Serialization;

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
