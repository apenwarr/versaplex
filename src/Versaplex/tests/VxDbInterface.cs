using System;
using System.Runtime.Serialization;

// FIXME: This should go in an assembly rather than being pasted from
// Versaplex/server/VxDbus.cs
public struct VxDbusDateTime {
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

struct VxColumnInfo {
    private int size;
    private string colname;
    private string coltype;
    private short precision;
    private short scale;
    private byte nullable;

    public string ColumnName {
        get { return colname; }
        set { colname = value; }
    }

    // XXX: Eww. But keeping this as a string makes the dbus-sharp magic do the
    // right thing when this struct is sent through the MessageWriter
    public VxColumnType VxColumnType {
        get { return (VxColumnType)Enum.Parse(
                typeof(VxColumnType), coltype, true); }
        set { coltype = value.ToString(); }
    }

    public string ColumnType {
        get { return coltype; }
    }

    public bool Nullable {
        get { return (nullable != 0); }
        set { nullable = value ? (byte)1 : (byte)0; }
    }

    public int Size {
        get { return size; }
        set {
            if (value < 0)
                throw new ArgumentOutOfRangeException(
                        "Size must be nonnegative");

            size = value;
        }
    }

    public short Precision {
        get { return precision; }
        set {
            if (value < 0)
                throw new ArgumentOutOfRangeException(
                        "Precision must be nonnegative");

            precision = value;
        }
    }

    public short Scale {
        get { return scale; }
        set {
            if (value < 0)
                throw new ArgumentOutOfRangeException(
                        "Scale must be nonnegative");

            scale = value;
        }
    }

    public VxColumnInfo(string colname, VxColumnType vxcoltype, bool nullable,
            int size, short precision, short scale)
    {
        ColumnName = colname;
        VxColumnType = vxcoltype;
        Nullable = nullable;
        Size = size;
        Precision = precision;
        Scale = scale;
    }
}

enum VxColumnType {
    Int64,
    Int32,
    Int16,
    UInt8,
    Bool,
    Double,
    Uuid,
    Binary,
    String,
    DateTime,
    Decimal
}

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
