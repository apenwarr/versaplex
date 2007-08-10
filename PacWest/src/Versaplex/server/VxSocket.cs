using System.Net.Sockets;

namespace versabanq.Versaplex.Server {

public abstract class VxSocket : Socket
{
    public VxSocket(AddressFamily addressFamily,
            SocketType socketType,
            ProtocolType protocolType)
        : base(addressFamily, socketType, protocolType)
    {
    }

    public VxSocket(SocketInformation socketInformation)
        : base(socketInformation)
    {
    }

    public virtual bool OnReadable()
    {
        return false;
    }

    public virtual bool OnWritable()
    {
        return false;
    }
}

}
