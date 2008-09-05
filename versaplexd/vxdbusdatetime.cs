using System;

struct VxDbusDateTime {
    public long seconds;
    public int microseconds;

    public DateTime DateTime {
        get {
            return new DateTime(seconds*10*1000*1000 + microseconds*10);
        }
    }

    public VxDbusDateTime(DateTime dt)
    {
	long ticks = dt.Ticks + EpochOffset.Ticks;
        seconds = ticks / 10 / 1000 / 1000;
        microseconds = (int)((ticks / 10) % (1000*1000));
    }

    private static readonly DateTime Epoch = new DateTime(1970, 1, 1);
    private static readonly TimeSpan EpochOffset = DateTime.MinValue - Epoch;
}

