using System.IO;
using System.Collections.Generic;

namespace versabanq.Versaplex.Server {

public class VxBufferStream : Stream
{
    private bool closed = false;

    virtual public bool CanRead { get { return true; } }
    virtual public bool CanWrite { get { return true; } }
    virtual public bool CanSeek { get { return false; } }

    virtual public long Length {
        get { throw new NotSupportedException(); }
    }
    virtual public long Position {
        get { throw new NotSupportedException(); }
        set { throw new NotSupportedException(); }
    }

    private object cookie;
    public object Cookie {
        get { return cookie; }
        set { cookie = value; }
    }

    public delegate void DataReadyHandler(object sender, object cookie);
    public event DataReadyHandler DataReady;

    private VxNotifySocket sock;

    // Maximum value for rbuf_used to take; run the DataReady event when this
    // has filled
    private int rbuf_size = 0;

    private Buffer rbuf = null;
    private Buffer wbuf = null;

    public VxBufferStream(VxNotifySocket sock)
    {
        sock.Blocking = false;
        sock.KeepAlive = true;
        sock.ReadReady = OnReadable;
        sock.WriteReady = OnWritable;

        this.sock = sock;
    }

    public int BufferAmount {
        get { return rbuf_size; }
        set {
            if (value < 0)
                throw new ArgumentOutOfRangeException(
                        "BufferAmount must be nonnegative");

            rbuf_size = value;

            if (rbuf.Size >= rbuf_size) {
                ReadWaiting = false;

                if (rbuf.Size > 0) {
                    EventLoop.AddAction(new VxEvent(
                                delegate() {
                                    DataReady(this, cookie);
                                }));
                }
            } else {
                ReadWaiting = true;
            }
        }
    }

    public int BufferPending {
        get { return rbuf.Size; }
    }

    public bool IsDataReady {
        get { return rbuf_size <= rbuf.Size; }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) {
            Flush();

            rbuf.Clear();
            ReadWaiting = false;

            wbuf.Clear();
            WriteWaiting = false;

            if (sock != null) {
                sock.Close();
                sock = null;
            }

            cookie = null;
        }

        closed = true;

        Stream.Dispose(disposing);
    }

    public override void Flush()
    {
        if (closed)
            throw new ObjectDisposedException("Stream is closed");

        if (wbuf.Size == 0)
            return;

        try {
            sock.Blocking = true;

            do {
                int amt = sock.Send(wbuf.FilledBufferList);

                wbuf.Discard(amt);
            } while (wbuf.Size > 0);

        } finally {
            sock.Blocking = false;
        }
    }

    public override int Read(
            [InAttribute] [OutAttribute] byte[] buffer,
            int offset,
            int count)
    {
        if (closed)
            throw new ObjectDisposedException("Stream is closed");

        return rbuf.Retrieve(buffer, offset, count);
    }

    public override int ReadByte()
    {
        if (closed)
            throw new ObjectDisposedException("Stream is closed");

        return rbuf.RetrieveByte();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength()
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        if (closed)
            throw new ObjectDisposedException("Stream is closed");

        wbuf.Append(buffer, offset, count);
    }

    public override void WriteByte(byte value)
    {
        if (closed)
            throw new ObjectDisposedException("Stream is closed");

        wbuf.AppendByte(value);
    }

    private bool read_waiting = false;
    private bool ReadWaiting {
        get { return read_waiting; }
        set {
            if (!read_waiting && value) {
                EventLoop.RegisterRead(sock);
            } else if (read_waiting && !value) {
                EventLoop.UnregisterRead(sock);
            }

            read_waiting = value;
        }
    }

    private bool write_waiting = false;
    private bool WriteWaiting {
        get { return write_waiting; }
        set {
            if (!write_waiting && value) {
                EventLoop.RegisterWrite(sock);
            } else if (write_waiting && !value) {
                EventLoop.UnregisterWrite(sock);
            }

            write_waiting = value;
        }
    }

    private bool OnReadable()
    {
        const int READSZ = 16384;

        try {
            byte[] data = new byte[READSZ];

            while (rbuf.Size < rbuf_size) {
                int amt = sock.Recv(data);
                rbuf.Append(data, 0, amt);
            }
        } catch (SocketException e) {
            if (e.ErrorCode != SocketError.WouldBlock) {
                throw e;
            }
        }

        if (rbuf.Size >= rbuf_size) {
            DataReady(this, cookie);
        }

        // Don't use ReadWaiting to change this since the return value will
        // determine the fate in the event loop, far more efficiently than
        // generic removal
        read_waiting = rbuf.Size < rbuf_size;
        return read_waiting;
    }

    private bool OnWritable()
    {
        try {
            int amt = sock.Send(wbuf.FilledBufferList);

            wbuf.Discard(amt);
        } catch (SocketException e) {
            if (e.ErrorCode != SocketError.WouldBlock) {
                throw e;
            }
        }

        // Don't use WriteWaiting to change this since the return value will
        // determine the fate in the event loop, far more efficiently than
        // generic removal
        write_waiting = wbuf.Size > 0;
        return write_waiting;
    }

    private class Buffer {
        private const int BUFCHUNKSZ = 16384;

        private int buf_start = 0;
        private int buf_end = BUFCHUNKSZ;

        private LinkedList<byte[]> buf = new LinkedList<byte[]>();

        // Number of bytes that can be retrieved
        public int Size
        {
            get {
                switch (buf.Count) {
                    case 0:
                        return 0;
                    default:
                        return buf.Count * BUFCHUNKSZ - buf_start - LastLeft;
                }
            }
        }

        // Number of bytes between buf_start and end of first buffer
        private int FirstUsed
        {
            get {
                switch (buf.Count) {
                    case 0:
                        return 0;
                    case 1:
                        return buf_end - buf_start;
                    default:
                        return BUFCHUNKSZ - buf_start;
                }
            }
        }

        // Number of bytes between buf_end and end of last buffer
        private int LastLeft
        {
            get {
                switch (buf.Count) {
                    case 0:
                        return 0;
                    default:
                        return BUFCHUNKSZ - buf_end;
                }
            }
        }

        // For the Socket.Send(IList<ArraySegment<byte>>) overload
        public IList<ArraySegment<byte>> FilledBufferList
        {
            get {
                if (Size == 0) {
                    throw new InvalidOperationException(
                            "Attempt to get buffer list of empty Buffer");
                }

                while (FirstUsed == 0) {
                    Contract();
                }

                IList<ArraySegment<byte>> outlist
                    = new List<ArraySegment<byte>>(buf.Count);

                LinkedListNode node = buf.First;

                outlist.Add(new ArraySegment<byte>(node.Value, buf_start,
                            FirstUsed));
                node = node.Next;

                if (node != null) {
                    while (node.Next != null) {
                        outlist.Add(new ArraySegment<byte>(node.Value, 0,
                                    BUFCHUNKSZ));
                        node = node.Next;
                    }

                    if (buf_end > 0) {
                        outlist.Add(new ArraySegment<byte>(node.Value, 0,
                                    buf_end));
                    }
                }

                return outlist;
            }
        }

        public void Append(byte[] buffer, int offset, int count)
        {
            if (offset+count > buffer.Length)
                throw new ArgumentException(
                        "offset+count is larger than buffer length");
            if (buffer == null)
                throw new ArgumentNullException("buffer is null");
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset is negative");
            if (count < 0)
                throw new ArgumentOutOfRangeException("count is negative");
            

            int sofar = 0;

            while (sofar < count) {
                while (LastLeft == 0)
                    Expand();

                int amt = Math.Min(count, LastLeft);

                Array.ConstrainedCopy(buffer, sofar+offset, buf.Last.Value,
                        buf_end, amt);

                buf_end += amt;
                sofar += amt;
            }
        }

        public void AppendByte(byte data)
        {
            while (LastLeft == 0)
                Expand();

            buf.Last.Value[buf_end] = data;

            buf_end++;
        }

        public int Retrieve(byte[] buffer, int offset, int count)
        {
            if (offset+count > buffer.Length)
                throw new ArgumentException(
                        "offset + count larger than buffer length");
            if (buffer == null)
                throw new ArgumentNullException("buffer is null");
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset is negative");
            if (count < 0)
                throw new ArgumentOutOfRangeException("count is negative");

            if (count > Size) {
                return -1;
            }

            int sofar = 0;

            while (sofar < count) {
                while (FirstUsed == 0) {
                    // buf.Count > 0 will be true because we know from above
                    // that buffer.Length <= original size. Thus if buf becomes
                    // completely emptied, then sofar >= buffer.Length and
                    // the loop would not have repeated.
                    Contract();
                }

                int amt = Math.Min(count - sofar, FirstUsed);

                Array.ConstrainedCopy(buf.First.Value, buf_start, buffer,
                        sofar+offset, amt);

                buf_start += amt;
                sofar += amt;
            }

            OptimizeBuf();
        }

        public int RetrieveByte()
        {
            if (Size == 0)
                return -1;

            while (FirstUsed == 0)
                Contract();

            int result = buf.First.Value[buf_start];

            buf_start++;
            OptimizeBuf();

            return result;
        }

        public void Discard(int amt)
        {
            if (amt < 0)
                throw new ArgumentOutOfRangeException("amount is negative");

            int cursize = FirstUsed;

            while (amt > cursize) {
                Contract();
                amt -= cursize;

                cursize = FirstUsed;
            }

            buf_start += amt;

            OptimizeBuf();
        }

        public void Clear()
        {
            buf.Clear();
            buf_start = 0;
            buf_end = BUFCHUNKSZ;
        }

        private void OptimizeBuf()
        {
            // If we finished reading the first buffer, contract it
            while (buf.Count > 1 && FirstUsed == 0) {
                Contract();
            }

            // Slight optimization for if the buffer is completely drained
            // to possibly avoid extra expansions later on
            if (buf_start == buf_end && buf.Count == 1) {
                buf_start = 0;
                buf_end = 0;
            }
        }
        
        private void Expand()
        {
            buf.AddLast(new byte[BUFCHUNKSZ]);
            buf_end = 0;
        }

        private void Contract()
        {
            buf.RemoveFirst();
            buf_start = 0;
        }
    }
}

}
