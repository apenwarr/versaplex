using System;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Collections;
using System.Collections.Generic;
using Wv;

public static class VxEventLoop {
    static WvLog log = new WvLog("VxEventLoop", WvLog.L.Debug3);
    // Public members
    public static void Run()
    {
        if (!Monitor.TryEnter(run_lock)) {
            throw new InvalidOperationException("Only one thread can run "
                    +"the event loop");
        }

        if (eventloop_running) {
            Monitor.Exit(run_lock);

            throw new InvalidOperationException("Can not recursively call "
                    +"the event loop");
        }

        try {
            eventloop_running = true;
            keep_going = true;

            while (keep_going && !VersaMain.want_to_die)
                SinglePass();

            // One final chance to clear everything out
            HandleActions();

        } finally {
            eventloop_running = false;
            Monitor.Exit(run_lock);
        }
    }

    public static void AddEvent(IFutureVxEvent e)
    {
        AddAction(new VxEvent(
                    delegate() {
                        events.Enqueue(e, e.When);
                    }));
    }

    public static void AddEvent(DateTime when, EmptyCallback cb)
    {
        AddEvent(new FutureVxEvent(cb, when));
    }

    public static void AddEvent(TimeSpan delta, EmptyCallback cb)
    {
        DateTime when = DateTime.Now + delta;

        AddEvent(new FutureVxEvent(cb, when));
    }

    // XXX: Should there be a RemoveEvent? Kind of awkward to do.

    public static void RegisterRead(VxSocket c)
    {
        log.print("RegisterRead\n");
        AddAction(new VxEvent(
                    delegate() {
                        // XXX: This should maybe verify that c is not already
                        // in readlist
                        readlist.AddLast(c);
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
        log.print("RegisterWrite\n");
        AddAction(new VxEvent(
                    delegate() {
                        // XXX: This should maybe verify that c is not already
                        // in writelist
                        writelist.AddLast(c);
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
    static Queue<IVxEvent> actions = new Queue<IVxEvent>();

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
    static bool eventloop_running = false;
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
            socks[0].SetSocketOption(SocketOptionLevel.Socket,
                    SocketOptionName.KeepAlive, true);
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
                socks[1].SetSocketOption(SocketOptionLevel.Socket,
                        SocketOptionName.KeepAlive, true);

                // Wait for the connection to be detected by both sides
                if (!socks[0].Poll(POLLTIME, SelectMode.SelectWrite)) {
                    throw new Exception("Problem connecting notification "
                            +"socket connection");
                }

                // Check for race conditions
#if false // 2007/08/23: avery's version of mono has RemoteEndPoint==null!?
                if (!socks[0].LocalEndPoint.Equals(socks[1].RemoteEndPoint)
                        || !socks[0].RemoteEndPoint.Equals(
                            socks[1].LocalEndPoint)) {
                    throw new Exception("Notification socket connected to "
                            +"incorrect endpoint");
                }
#endif

                // Make the connection simplex
                // FIXME: Doing this makes the connection close itself after
                // a couple minutes of inactivity on win32.  Perhaps it's
                // some kind of weird garbage collector bug in MS .NET.
                // In any case, leaving both directions open is pretty 
                // harmless.
                //socks[0].Shutdown(SocketShutdown.Receive);
                //socks[1].Shutdown(SocketShutdown.Send);
            } catch (Exception e) {
                socks[1].Close();
                throw;
            }
        } catch (Exception e) {
            socks[0].Close();
            throw;
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
    }

    private static void SinglePass()
    {
        IList readers = new ArrayList();
        IList writers = new ArrayList();

        // Set up sockets
        readers.Add(notify_socket_receiver);

        foreach (VxSocket r in readlist) {
            readers.Add(r);
        }
        foreach (VxSocket w in writelist) {
            writers.Add(w);
        }

        // Set up events
        int waittime = Int32.MaxValue;
        if (events.Count > 0) {
            DateTime next = NextEventTime;

            TimeSpan sleeptime = next - DateTime.Now;

            if (sleeptime <= TimeSpan.Zero) {
                sleeptime = TimeSpan.Zero;
            }

            if (sleeptime.Ticks/10 > Int32.MaxValue) {
                waittime = Int32.MaxValue;
            } else {
                waittime = (int)(sleeptime.Ticks / 10);
            }
        }

        log.print("Select: {0} readers, {1} writers, delay={2}...\n",
                readers.Count, writers.Count, waittime);

        // Do Select()
        Socket.Select(readers, writers, null, waittime);

        log.print("    --> {0} readers, {1} writers, remain={2}\n",
                readers.Count, writers.Count, waittime);
	
        // Process socket activity
        // FIXME: This can probably be done better

        // - Writing first
        foreach (Socket s in writers) {
            LinkedListNode<VxSocket> node = writelist.First;

            /* This is O(n^2), which makes me sad, but is efficient for
             * removing from the writelist. Also, the writelist should
             * tend to be small with frequent removals, so maybe it's
             * better to do it this way for now?
             *
             * On second thought, probably not. But this works and I don't
             * want to needlessly risk breaking things now. */
            while (node != null) {
                VxSocket w = node.Value;

                if (w == s) {
                    try {
                        log.print("Writability handler\n");
                        if (!w.OnWritable()) {
                            writelist.Remove(node);
                        }
                    } catch (Exception e) {
                        log.print(WvLog.L.Warning,
				  "Executing write handler for socket: {0}",
				  e.ToString());
                    }
                    break;
                }

                node = node.Next;
            }
        }

        // - Then reading
        foreach (Socket s in readers) {
            if (s.Available == 0) {
                log.print("Liar!\n");
            }

            if (s == notify_socket_receiver) {
                log.print("It's the notify socket\n");
                HandleActions();
                continue;
            }

            /* This is O(n^2), which makes me sad, but is efficient for
             * removing from the readlist
            LinkedListNode<VxSocket> node = readlist.First;

            while (node != null) {
                VxSocket r = node.Value;

                if (r == s) {
                    try {
                        log.print("Readability handler\n");
                        if (!r.OnReadable()) {
                            readlist.Remove(node);
                        }
                    } catch (Exception e) {
                        log.print("Executing read handler for socket: {0}\n"
                                e.ToString());
                    }
                    continue;
                }

                node = node.Next;
            }
            */

            /*
             * XXX this is better but then removing from the list is slow
             * Should probably see if sockets can happily go into a hashtable
             * and use that instead. The LinkedList idea was good before the
             * derive-from-Socket idea came through
             */
            VxSocket vs = (VxSocket)s;

            try {
                log.print("Readability handler\n");
                if (!vs.OnReadable()) {
                    UnregisterRead(vs);
                }
            } catch (Exception e) {
                log.print(WvLog.L.Warning,
			  "Executing read handler for socket: {0}\n", e);
            }
        }

        // Process events
        DateTime now = DateTime.Now;

        while (events.Count > 0 && NextEventTime <= now) {
            IVxEvent nextevent = (IVxEvent)events.Dequeue();

            try {
                log.print("Running scheduled event\n");
                nextevent.Run();
            } catch (Exception e) {
                log.print(WvLog.L.Warning,
			  "Executing scheduled event: {0}\n", e);

                // This should probably be fatal
                throw;
            }
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
        log.print("SendNotification\n");
        try {
            byte[] notify_octet = new byte[1];
            notify_octet[0] = 0;
            notify_socket_sender.Send(notify_octet);
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
                rcvd = notify_socket_receiver.Receive(buffer);
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
        // 2. Action events can't add to the action queue for the current pass,
        //    so the sockets can't be starved easily
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
                try {
                    log.print("Running event from main thread\n");
                    e.Run();
                } catch (Exception ex) {
                    log.print(WvLog.L.Warning,
			      "Executing from action queue: {0}\n", ex);

                    // This should probably be fatal
                    throw;
                }
                break;
            case EventContext.ThreadPool:
                log.print("Queueing into thread pool\n");
                ThreadPool.QueueUserWorkItem(
                        delegate(object state) {
                            try {
                                log.print("Running event from thread pool\n");
                                e.Run();
                            } catch (Exception ex) {
                                log.print(WvLog.L.Warning,
				  "Executing in thread pool: {0}\n", ex);

                                // XXX: Not fatal? Look at this later.
                            }
                        });
                break;
            }
        }
    }
}
