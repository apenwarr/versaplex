using System;
using System.Net.Sockets;

public class VxNotifySocket : VxSocket
{
    public VxNotifySocket(AddressFamily addressFamily,
            SocketType socketType,
            ProtocolType protocolType)
        : base(addressFamily, socketType, protocolType)
    {
    }

    public VxNotifySocket(SocketInformation socketInformation)
        : base(socketInformation)
    {
    }

    public delegate bool ReadyHandler(object sender);
    public event ReadyHandler ReadReady = null;
    public event ReadyHandler WriteReady = null;

    public override bool OnReadable()
    {
        if (ReadReady != null)
            return ReadReady(this);

        throw new InvalidOperationException("No readable handler is registered");
    }

    public override bool OnWritable()
    {
        if (WriteReady != null)
            return WriteReady(this);

        throw new InvalidOperationException("No writable handler is registered");
    }
}
