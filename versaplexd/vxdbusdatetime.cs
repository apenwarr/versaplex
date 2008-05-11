using System;

struct VxDbusDateTime {
    private long seconds;
    private int microseconds;

    public long Seconds {
        get { return seconds; }
        set { seconds = value; }
    }

    public int Microseconds {
        get { return microseconds; }
        set { microseconds = value; }
    }

    public DateTime DateTime {
        get {
            return new DateTime(seconds*10000000 + microseconds*10);
        }
    }

    public VxDbusDateTime(DateTime dt)
    {
        seconds = (dt.Ticks + EpochOffset.Ticks) / 10000000;
        microseconds = (int)(((dt.Ticks + EpochOffset.Ticks) / 10) % 1000000);
    }

    private static readonly DateTime Epoch = new DateTime(1970, 1, 1);
    private static readonly TimeSpan EpochOffset = DateTime.MinValue - Epoch;
}

