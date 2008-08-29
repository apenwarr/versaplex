using System;
using System.Data;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Wv
{
    /**
     * A wrapper that will make any object implicitly castable into various
     * basic data types.
     * 
     * This is useful because IDataRecord is pretty terrible at giving you
     * objects in the form you actually want.  If I ask for an integer, I
     * want you to *try really hard* to give me an integer, for example by
     * converting a string to an int.  But IDataRecord simply throws an
     * exception if the object wasn't already an integer.
     * 
     * When converting to bool, we assume any non-zero int is true, just
     * like C/C++ would do. 
     */
    public struct WvAutoCast
    {
	object v;
	public static readonly WvAutoCast _null = new WvAutoCast(null);

	public WvAutoCast(object v)
	{
	    this.v = v;
	}
	
	public bool IsNull { get { return v == null || DBNull.Value.Equals(v); } }
	
	public static implicit operator string(WvAutoCast o)
	{
	    return o.ToString();
	}
	
	public override string ToString()
	{
	    if (v == null)
		return "(nil)"; // shouldn't return null since this != null
	    else
		return v.ToString();
	}
	
	public static implicit operator DateTime(WvAutoCast o)
	{
	    if (o.v == null)
		return DateTime.MinValue;
	    else if (o.v is DateTime)
		return (DateTime)o.v;
	    else
		return wv.date(o.v);
	}

        public static implicit operator byte[](WvAutoCast o)
        {
            return (byte[])o.v;
        }

	Int64 intify()
	{
	    if (v == null)
		return 0;
	    else if (v is Int64)
		return (Int64)v;
	    else if (v is Int32)
		return (Int32)v;
	    else if (v is Int16)
		return (Int16)v;
	    else if (v is Byte)
		return (Byte)v;
	    else
		return wv.atoi(v);
	}

	public static implicit operator Int64(WvAutoCast o)
	{
	    return o.intify();
	}

	public static implicit operator Int32(WvAutoCast o)
	{
	    return (Int32)o.intify();
	}

	public static implicit operator Int16(WvAutoCast o)
	{
	    return (Int16)o.intify();
	}

	public static implicit operator Byte(WvAutoCast o)
	{
	    return (Byte)o.intify();
	}

	public static implicit operator bool(WvAutoCast o)
	{
	    return o.intify() != 0;
	}

	public static implicit operator double(WvAutoCast o)
	{
	    if (o.v == null)
		return 0;
	    else if (o.v is double)
		return (double)o.v;
	    else if (o.v is Int64)
		return (Int64)o.v;
	    else if (o.v is Int32)
		return (Int32)o.v;
	    else if (o.v is Int16)
		return (Int16)o.v;
	    else if (o.v is Byte)
		return (Byte)o.v;
	    else
		return wv.atod(o.v);
	}

	public static implicit operator char(WvAutoCast o)
	{
	    if (o.v == null)
		return Char.MinValue;
	    else if (o.v is char)
		return (char)o.v;
	    else
		return Char.MinValue;
	}

	public static implicit operator Decimal(WvAutoCast o)
	{
	    // FIXME:  double/int to decimal conversions?
	    if (o.v is Decimal)
		return (Decimal)o.v;
	    else
		return Decimal.MinValue;
	}
    }
    

    /**
     * A wrapper for object lists, particularly IDataRecord, that makes
     * each of the objects in the list accesible as a WvAutoCast.
     */
    public struct WvAutoRecord
    {
	object[] a;

	public WvAutoRecord(IDataRecord r)
	{
	    a = new object[r.FieldCount];
	    r.GetValues(a);
	}

	public WvAutoRecord(object[] a)
	{
	    this.a = a;
	}

	public WvAutoRecord(IEnumerable<object> i)
	{
	    a = i.ToArray();
	}

	public WvAutoCast this [int i] {
	    get { return new WvAutoCast(a[i]); }
	}

	public IEnumerable<WvAutoCast> GetEnumerator()
	{
	    foreach (object o in a)
		yield return new WvAutoCast(o);
	}
	
	public int Length { get { return a.Length; } }
    }

    public class WvSqlRow : IEnumerable<WvAutoCast>
    {
	private object[] columns;
	
	public DataTable schema;
	
	public WvSqlRow(object[] _columns, DataTable _schema)
	{
	    columns = _columns;
	    schema = _schema;
	}

	public WvAutoCast this[int i]
	    { get { return new WvAutoCast(columns[i]); } }

	public int Length
	    { get { return columns.Length; } }

	public IEnumerator<WvAutoCast> GetEnumerator()
	{
	    foreach (object colval in columns)
	    {
		yield return new WvAutoCast(colval);
	    }
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
	    // I really hope nobody ever needs to use this
	    foreach (object colval in columns)
	    {
		yield return colval;
	    }
	}
    }
}
