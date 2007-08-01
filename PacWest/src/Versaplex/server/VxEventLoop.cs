using System.Threading;
using System.Net.Sockets;
using System.Collections.Generic;

namespace versabanq.Versaplex.Server {

public static class VxEventLoop {
    // Public members
    public static void Run()
    {
        if (!Monitor.TryEnter(run_lock)) {
            throw new InvalidOperationException("Only one thread can run "
                    +"the event loop");
        }

        try {

            keep_going = true;

            while (keep_going)
                SinglePass();

            // One final chance to clear everything out
            HandleActions();

        } finally {
            Monitor.Exit(run_lock);
        }
    }

    public static void AddEvent(IFutureVxEvent e)
    {
        AddAction(new VxEvent(
                    delegate() {
                        events.Enqueue(e.When, e);
                    }));
    }

    public static void AddEvent(DateTime when, EmptyCallback cb)
    {
        AddEvent(when, new FutureVxEvent(cb, when));
    }

    public static void AddEvent(TimeSpan delta, EmptyCallback cb)
    {
        DateTime when = DateTime.Now + delta;

        AddEvent(when, new FutureVxEvent(cb, when));
    }

    // XXX: Should there be a RemoveEvent? Kind of awkward to do.

    public static void RegisterRead(VxSocket c)
    {
        AddAction(new VxEvent(
                    delegate() {
                        // XXX: This should maybe verify that c is not already
                        // in readlist
                        readlist.Add(c);
                    }));
    }

    // XXX This is terribly inefficient. Use only when there is no other choice.
    public static void UnregisterRead(VxSocket c)
    {
        AddAction(new VxEvent(
                    delegate() {
                        Unregister(readlist, c);
                    }));
    }

    public static void RegisterWrite(VxSocket c)
    {
        AddAction(new VxEvent(
                    delegate() {
                        // XXX: This should maybe verify that c is not already
                        // in writelist
                        writelist.Add(c);
                    }));
    }

    // XXX This is terribly inefficient. Use only when there is no other choice.
    public static void UnregisterWrite(VxSocket c)
    {
        AddAction(new VxEvent(
                    delegate() {
                        Unregister(writelist, c);
                    }));
    }

    public static void Shutdown()
    {
        AddAction(new VxEvent(
                    delegate() {
                        keep_going = false;
                    }));
    }

    public static void AddAction(IVxEvent e)
    {
        lock (actions) {
            actions.Enqueue(e);
        }

        SendNotification();
    }

    // Private members
    static Queue<IVxEvent> actions;

    // This is terribly inefficient (O(n)) for Unregister{Read,Write}()
    //
    // But the normal way to remove something from the list should be to have
    // the callback request its own unregistration, which is O(1)
    //
    // The only glaring problem with this is that it doesn't catch the same
    // socket being added twice.
    static LinkedList<VxSocket> readlist = new LinkedList<VxSocket>();
    static LinkedList<VxSocket> writelist = new LinkedList<VxSocket>();

    static JMBucknall.Containers.PriorityQueue events =
        new JMBucknall.Containers.PriorityQueue(false);

    static Socket notify_socket_sender;
    static Socket notify_socket_receiver;

    static object run_lock = new object();
    static bool keep_going = false;

    static VxEventLoop()
    {
        Socket[] socks;
        socks = NotifyPair();
        notify_socket_sender = socks[0];
        notify_socket_receiver = socks[1];
    }

    private static Socket[] NotifyPair()
    {
        // When polling, wait 1 second, which should be plenty
        const int POLLTIME = 1000000;

        Socket[] socks = new Socket[2];

        socks[0] = new Socket(AddressFamily.InterNetwork, SocketType.Stream,
                ProtocolType.Tcp);

        try {
            socks[0].Blocking = false;
            socks[0].NoDelay = true;
            socks[0].KeepAlive = true;
            socks[0].SendBufferSize = 1;

            using (Socket listener = new Socket(AddressFamily.InterNetwork,
                        SocketType.Stream, ProtocolType.Tcp)) {
                listener.Blocking = false;

                listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
                listener.Listen(1);

                // Start the connection
                try {
                    socks[0].Connect(listener.LocalEndPoint);
                } catch (SocketException e) {
                    if (e.SocketErrorCode == SocketError.WouldBlock) {
                        // This is expected
                    } else {
                        throw new Exception(
                                "Failed to connect notification sockets", e);
                    }
                }

                // Wait for it to go through
                if (!listener.Poll(POLLTIME, SelectMode.SelectRead)) {
                    throw new Exception(
                            "Failed to receive notification socket connection");
                }

                // Do the accepting
                try {
                    socks[1] = listener.Accept();
                } catch (SocketException e) {
                    throw new Exception("Failed to accept notification socket "
                            +"connection", e);
                }
            }

            try {
                socks[1].ReceiveBufferSize = 1;
                socks[1].KeepAlive = true;

                // Wait for the connection to be detected by both sides
                if (!socks[0].Poll(POLLTIME, SelectMode.SelectWrite)) {
                    throw new Exception("Problem connecting notification "
                            +"socket connection");
                }

                // Check for race conditions
                if (!socks[0].LocalEndPoint.Equals(socks[1].RemoteEndPoint)
                        || !socks[0].RemoteEndPoint.Equals(
                            socks[1].LocalEndPoint)) {
                    throw new Exception("Notification socket connected to "
                            +"incorrect endpoint");
                }

                // Make the connection simplex
                socks[0].Shutdown(SocketShutdown.Receive);
                socks[1].Shutdown(SocketShutdown.Send);
            } catch (Exception e) {
                socks[1].Dispose();
                throw e;
            }
        } catch (Exception e) {
            socks[0].Dispose();
            throw e;
        }

        return socks;
    }

    // XXX This is terribly inefficient. Use only when there is no other choice.
    private static void Unregister(LinkedList<VxSocket> l, VxSocket c)
    {
        // Instead of using l.Remove(c), check that _all_ references
        // are cleaned up.

        LinkedListNode<VxSocket> n = l.First;

        uint hits = 0;

        while (n != null) {
            if (n.Value == c) {
                hits++;
            }

            LinkedListNode<VxSocket> next = n.Next;

            l.Remove(n);
            n = next;
        }

        if (hits > 1) {
            // TODO: Some sort of log message warning about possible bugs,
            // instead of croaking
            throw new Exception("list corruption: socket appeared "
                    + hits + " times");
        }

        if (hits == 0) {
            throw new InvalidOperationException(
                    "socket was not in list");
        }
    }

    private static void SinglePass()
    {
        IList<Socket> readers = new List<Socket>();
        IList<Socket> writers = new List<Socket>();

        // Set up sockets
        readers.Add(notify_socket_receiver);

        foreach (VxSocket r in readlist) {
            readers.Add(r.Socket);
        }
        foreach (VxSocket w in writelist) {
            writers.Add(w.Socket);
        }

        // Set up events
        int waittime = -1;
        if (events.Count > 0) {
            DateTime next = NextEventTime;

            TimeSpan sleeptime = next - DateTime.Now;

            if (sleeptime <= 0) {
                sleeptime = TimeSpan.Zero;
            }

            waittime = sleeptime.Ticks / 10;
        }

        // Do Select()
        Select(readers, writers, null, waittime);

        // Process socket activity

        // - Writing first
        foreach (Socket s in writers) {
            LinkedListNode<VxSocket> node = writelist.First;

            while (node != null) {
                VxSocket w = node.Value;

                if (w.Socket == s) {
                    if (!w.OnWritable()) {
                        writelist.Remove(node);
                    }
                    break;
                }

                node = node.Next;
            }
        }

        // - Then reading
        foreach (Socket s in readers) {
            if (s == notify_socket_receiver) {
                HandleActions();
                break;
            }

            LinkedListNode<VxSocket> node = readlist.First;

            while (node != null) {
                VxSocket r = node.Value;

                if (r.Socket == s) {
                    if (!r.OnReadable()) {
                        readlist.Remove(node);
                    }
                    break;
                }

                node = node.Next;
            }
        }

        // Process events
        DateTime now = DateTime.Now;

        while (events.Count > 0 && NextEventTime <= now) {
            IVxEvent nextevent = (IVxEvent)events.Dequeue();

            nextevent.Run();
        }
    }

    private static DateTime NextEventTime
    {
        get {
            return (DateTime)events.RootPriority;
        }
    }

    private static void SendNotification()
    {
        try {
            notify_socket_sender.Send('\0');
        } catch (SocketException e) {
            if (e.SocketErrorCode == SocketError.WouldBlock) {
                // Ignore. This only happens if there are already pending
                // notifications anyway.
                return;
            } else {
                throw e;
            }
        }
    }

    private static void HandleActions()
    {
        const int BUFSZ = 16; // Should be plenty. The send/receive buffers are
                              // supposed to just be 1 byte anyway.
        byte[] buffer = new byte[BUFSZ];
        int rcvd;

        // Drain the receive socket
        do {
            try {
                rcvd = notify_socket_sender.Receive(buffer);
            } catch (SocketException e) {
                if (e.SocketErrorCode == SocketError.WouldBlock) {
                    // We've read everything.
                    break;
                } else {
                    throw e;
                }
            }
        } while (rcvd >= BUFSZ);

        // Get the list of things to do right now so that
        // 1. The lock is not held while we process the action events
        // 2. Action events can't add to the action queue for this pass, so the
        //    sockets can't be starved
        IVxEvent[] actions_now;
        lock (actions) {
            if (actions.Count == 0) {
                return;
            }

            actions_now = actions.ToArray();
            actions.Clear();
        }

        foreach (IVxEvent e in actions_now) {
            switch (e.Context) {
                case EventContext.MainThread:
                    e.Run();
                    break;
                case EventContext.ThreadPool:
                    ThreadPool.QueueUserWorkItem(
                            delegate(object state) {
                                e.Run();
                            });
                    break;
            }
        }
    }
}

}
