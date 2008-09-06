using System;
using System.Data;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Wv.Extensions;

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
	
	public object inner 
	    { get { return v; } }
	
	public override string ToString()
	{
	    if (v == null)
		return "(nil)"; // shouldn't return null since this != null
            else if (v is Boolean)
                return intify().ToString();
	    else
		return v.ToString();
	}
	
	public static implicit operator DateTime(WvAutoCast o)
	{
	    if (o.v == null)
		return DateTime.MinValue;
	    else if (o.v is DateTime)
		return (DateTime)o.v;
	    else if (o.v is SqlDateTime)
		return ((SqlDateTime)o.v).Value;
	    else
		return wv.date(o.v);
	}

	public static implicit operator SqlDateTime(WvAutoCast o)
	{
	    if (o.v == null)
		return SqlDateTime.MinValue;
	    else if (o.v is SqlDateTime)
		return (SqlDateTime)o.v;
	    else if (o.v is DateTime)
		return (DateTime)o.v;
	    else
		return wv.date(o.v);
	}

        public static implicit operator byte[](WvAutoCast o)
        {
            return (byte[])o.v;
        }

        public static implicit operator SqlBinary(WvAutoCast o)
        {
	    if (o.v == null)
		return null;
	    else if (o.v is SqlBinary)
		return (SqlBinary)o.v;
	    else
		return new SqlBinary((byte[])o.v);
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
            else if (v is Boolean)
                return (Boolean)v ? 1 : 0;
	    else
		return wv.atol(v);
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
            else if (o.v is Boolean)
                return (Boolean)o.v ? 1.0 : 0.0;
	    else
		return wv.atod(o.v);
	}

	public static implicit operator float(WvAutoCast o)
	{
	    if (o.v == null)
		return 0;
	    else if (o.v is float)
		return (float)o.v;
	    else
		return (float)(double)o;
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
	
	public static implicit operator SqlDecimal(WvAutoCast o)
	{
	    // FIXME:  double/int to decimal conversions?
	    if (o.v is SqlDecimal)
		return (SqlDecimal)o.v;
	    else
		return SqlDecimal.MinValue;
	}
	
	public static implicit operator Guid(WvAutoCast o)
	{
	    if (o.v is Guid)
		return (Guid)o.v;
	    else if (o.v is SqlGuid)
		return ((SqlGuid)o.v).Value;
	    else
		return Guid.Empty;
	}
	
	public static implicit operator SqlGuid(WvAutoCast o)
	{
	    if (o.v is SqlGuid)
		return (SqlGuid)o.v;
	    else if (o.v is Guid)
		return (Guid)o.v;
	    else
		return SqlGuid.Null;
	}
    }
    
    
    public struct WvColInfo
    {
	public string name;
	public Type type;
	public bool nullable;
	public int size;
	public short precision;
	public short scale;
	
	public static IEnumerable<WvColInfo> FromDataTable(DataTable schema)
	{
	    foreach (DataRow col in schema.Rows)
		yield return new WvColInfo(col);
	}
	
	WvColInfo(DataRow data)
	{
	    name      = (string)data["ColumnName"];
	    type      = (Type)  data["DataType"];
	    nullable  = (bool)  data["AllowDBNull"];
	    size      = (int)   data["ColumnSize"].atoi();
	    precision = (short) data["NumericPrecision"].atoi();
	    scale     = (short) data["NumericScale"].atoi();
	}
    }
    
    
    public class WvSqlRow : IEnumerable<WvAutoCast>
    {
	object[] data;
	IEnumerable<WvColInfo> schema;
	
	public WvSqlRow(object[] data, IEnumerable<WvColInfo> schema)
	{
	    this.data = data;
	    this.schema = schema;
	}

	public WvAutoCast this[int i]
	    { get { return new WvAutoCast(data[i]); } }

	public int Length
	    { get { return data.Length; } }

	public IEnumerator<WvAutoCast> GetEnumerator()
	{
	    foreach (object colval in data)
		yield return new WvAutoCast(colval);
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
	    foreach (object colval in data)
		yield return colval;
	}
	
	public IEnumerable<WvColInfo> columns
	    { get { return schema; } }
    }
    
    
    public abstract class WvSqlRows : IDisposable, IEnumerable<WvSqlRow>
    {
	public abstract IEnumerable<WvColInfo> columns { get; }
	
	public virtual void Dispose()
	{
	    // nothing to do here
	}
	
	public abstract IEnumerator<WvSqlRow> GetEnumerator();
	
	IEnumerator IEnumerable.GetEnumerator()
	{
	    IEnumerator<WvSqlRow> e = GetEnumerator();
	    return e;
	}
    }
    
    class WvSqlRows_IDataReader : WvSqlRows, IEnumerable<WvSqlRow>
    {
	IDataReader reader;
	WvColInfo[] schema;
	
	public WvSqlRows_IDataReader(IDataReader reader)
	{
	    wv.assert(reader != null);
	    this.reader = reader;
	    var st = reader.GetSchemaTable();
	    if (st != null)
		this.schema = WvColInfo.FromDataTable(st).ToArray();
	    else
		this.schema = new WvColInfo[0];
	}
	
	public override void Dispose()
	{
	    if (reader != null)
		reader.Dispose();
	    reader = null;
	    
	    base.Dispose();
	}
	
	public override IEnumerable<WvColInfo> columns
	    { get { return schema; } }

	public override IEnumerator<WvSqlRow> GetEnumerator()
	{
	    int max = reader.FieldCount;
	    
	    using(this) // handle being called inside a foreach()
	    while (reader.Read())
	    {
		object[] oa = new object[max];
		try
		{
		    reader.GetValues(oa);
		}
		catch (OverflowException)
		{
		    // This garbage is here because mono gets an
		    // OverflowException when trying to use GetDecimal() on
		    // a very large decimal(38,38) field.  But GetSqlDecimal
		    // works.  Sadly, GetValues() seems to do some kind of
		    // GetDecimal-like thing internally, so we have to do this
		    // hack if there's ever a decode error.
		    // (Tested with mono 1.9.1.0; failing unit test is
		    //  versaplex: verifydata.t.cs/VerifyDecimal)
		    for (int i = 0; i < max; i++)
		    {
			if (!reader.IsDBNull(i) 
			      && reader.GetFieldType(i) == typeof(Decimal))
			{
			    if (reader is SqlDataReader)
				oa[i] = ((SqlDataReader)reader)
				    .GetSqlDecimal(i).ToString();
			    else
				oa[i] = reader.GetDecimal(i).ToString();
			}
			else
			    oa[i] = reader.GetValue(i);
		    }
		}
		
		yield return new WvSqlRow(oa, schema);
	    }
	}
    }
}
