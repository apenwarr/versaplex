using System;
using Wv;

public struct VxColumnInfo {
    internal int size;
    internal string colname;
    internal VxColumnType coltype;
    internal short precision;
    internal short scale;
    internal byte nullable;

    public string ColumnName {
        get { return colname; }
        set { colname = value; }
    }

    public VxColumnType VxColumnType {
	get { return coltype; }
	set { coltype = value; }
    }

    public string ColumnType {
        get { return coltype.ToString(); }
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
    
    public void Write(MessageWriter w)
    {
	// (issnny)
	w.WritePad(8); //offset for structs, right?
	w.Write(typeof(Int32),  size);
	w.Write(typeof(string), colname);
	w.Write(typeof(string), coltype.ToString());
	w.Write(typeof(Int16),  precision);
	w.Write(typeof(Int16),  scale);
	w.Write(typeof(Byte),   nullable);
    }
}

public enum VxColumnType {
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

