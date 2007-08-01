namespace versabanq.Versaplex.Server {

public enum EventContext {
    MainThread,
    ThreadPool
}

public interface IVxEvent {
    void Run();

    EventContext Context {
        get;
    }
}

public interface IFutureVxEvent : IVxEvent {
    DateTime When {
        get;
    }
}

public delegate void EmptyCallback();
public delegate void SingleArgCallback(object arg);

public class VxEvent : IVxEvent {
    private readonly EmptyCallback cb;

    public VxEvent(EmptyCallback cb)
    {
        this.cb = cb;
    }

    public void Run()
    {
        cb();
    }

    public EventContext Context {
        get { return EventContext.MainThread; }
    }
}

public class ArgVxEvent : IVxEvent {
    private readonly SingleArgCallback cb;
    private readonly object arg;

    public ArgVxEvent(SingleArgCallback cb, object arg)
    {
        this.cb = cb;
        this.arg = arg;
    }

    public void Run()
    {
        cb(arg);
    }

    public EventContext Context {
        get { return EventContext.MainThread; }
    }
}

public class FutureVxEvent : VxEvent, IFutureVxEvent {
    private readonly DateTime when;

    public FutureVxEvent(EmptyCallback cb, DateTime when) : base(cb)
    {
        this.when = when;
    }

    public FutureVxEvent(EmptyCallback cb, TimeSpan delta) : base(cb)
    {
        this.when = DateTime.Now + delta;
    }

    public DateTime When {
        get { return when; }
    }
}

public class ArgFutureVxEvent : ArgVxEvent, IFutureVxEvent {
    private readonly DateTime when;

    public ArgFutureVxEvent(SingleArgCallback cb, object arg, DateTime when)
        : base(cb, arg)
    {
        this.when = when;
    }

    public ArgFutureVxEvent(SingleArgCallback cb, object arg, TimeSpan delta)
        : base(cb, arg)
    {
        this.when = DateTime.Now + delta;
    }

    public DateTime When {
        get { return when; }
    }
}

}
