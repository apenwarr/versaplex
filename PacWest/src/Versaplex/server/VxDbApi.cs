using System.Data;
using System.Data.SqlClient;
using versabanq.Versaplex.Server;
using versabanq.Versaplex.Dbus;
using NDesk.Dbus;

namespace versabanq.Versaplex.Dbus.Db {

[Interface("com.versabanq.versaplex.db")]
public class VxDb : MarshalByRefObject {
    public void ExecNoResult(string query)
    {
        SqlConnection conn = null;
        try {
            conn = VxSqlPool.TakeConnection();

            using (SqlCommand cmd = conn.CreateCommand(query)) {
                cmd.ExecuteNoResult();
            }
        } finally {
            if (conn != null)
                VxSqlPool.ReleaseConnection(conn);
        }
    }

    public void ExecScalar(string query, out object result)
    {
        SqlConnection conn = null;
        try {
            conn = VxSqlPool.TakeConnection();

            using (SqlCommand cmd = conn.CreateCommand(query)) {
                result = cmd.ExecuteScalar();
            }
        } finally {
            if (conn != null)
                VxSqlPool.ReleaseConnection(conn);
        }
    }

    public void ExecRecordset(string query,
            out string[] colnames, out string[] coltypes_str,
            out VxDbusDbResult[][] data)
    {
        SqlConnection conn = null;
        try {
            conn = VxSqlPool.TakeConnection();

            using (SqlCommand cmd = conn.CreateCommand(query))
            using (SqlDataReader reader = cmd.ExecuteReader()) {
                if (reader.FieldCount <= 0) {
                    colnames = null;
                    coltypes = null;
                    data = null;
                    return;
                }

                VxColumnType[] coltypes;
                ProcessSchema(reader, out colnames, out coltypes);

                coltypes_str = new string[coltypes.Length];
                for (int i=0; i < coltypes.Length; i++) {
                    coltypes_str[i] = coltypes[i].ToString();
                }

                List<VxDbusDbResult[]> rows = new List<VxDbusDbResult[]>();

                while (reader.Read()) {
                    VxDbusDbResult row[]
                        = new VxDbusDbResult[reader.FieldCount];

                    for (int = 0; i < reader.FieldCount; i++) {
                        if (reader.IsDBNull(i)) {
                            row[i].Nullity = true;
                            row[i].Data = null;
                            continue;
                        }

                        row[i].Nullity = false;

                        switch (coltypes[i]) {
                            case VxColumnType.Int64:
                                row[i].Data = reader.GetInt64(i);
                                break;
                            case VxColumnType.Int32:
                                row[i].Data = reader.GetInt32(i);
                                break;
                            case VxColumnType.Int16:
                                row[i].Data = reader.GetInt16(i);
                                break;
                            case VxColumnType.UInt8:
                                row[i].Data = reader.GetByte(i);
                                break;
                            case VxColumnType.Bool:
                                row[i].Data = reader.GetBoolean(i);
                                break;
                            case VxColumnType.Double:
                                row[i].Data = reader.GetDouble(i);
                                break;
                            case VxColumnType.Uuid:
                                row[i].Data = reader.GetGuid(i).ToString();
                                break;
                            case VxColumnType.Binary:
                                row[i].Data = reader.GetBinary(i);
                                break;
                            case VxColumnType.String:
                                row[i].Data = reader.GetString(i);
                                break;
                            case VxColumnType.DateTime:
                                row[i].Data = new VxDbusDateTime(
                                        reader.GetDateTime(i));
                                break;
                            case VxColumnType.Decimal:
                                row[i].Data = reader.GetDecimal().ToString();
                                break;
                        }
                    }
                }
            }
        } finally {
            if (conn != null)
                VxSqlPool.ReleaseConnection(conn);
        }
    }

    private void ProcessSchema(SqlReader reader, out string[] colnames,
            out VxColumnType[] coltypes)
    {
        colnames = new string[reader.FieldCount];
        coltypes = new string[reader.FieldCount];

        int i = 0;

        using (DataTable schema = reader.GetSchemaTable()) {
            foreach (DataRowView col in schema.DefaultView) {
                // This trick is the same one SqlSucker uses
                string dbtStr = (string)col["DataTypeName"];

                if (dbtStr.ToLower() == "sql_variant") {
                    // The parse-the-string-as-enum-value trick works except
                    // that sql_variant becomes Variant. Close. However,
                    // sql_variant types seem to be difficult to handle
                    // properly and thus are not supported.
                    throw new VxBadSchema("Columns of type sql_variant "
                        + "are not supported by Versaplex at this time");
                }

                SqlDbType dbt = (SqlDbType)System.Enum.Parse(
                        typeof(SqlDbType), dbtStr, true);

                string coltype;

                switch (dbt) {
                    case SqlDbType.BigInt:
                        coltype = VxColumnType.Int64;
                        break;
                    case SqlDbType.Int:
                        coltype = VxColumnType.Int32;
                        break;
                    case SqlDbType.SmallInt:
                        coltype = VxColumnType.Int16;
                        break;
                    case SqlDbType.TinyInt:
                        coltype = VxColumnType.UInt8;
                        break;
                    case SqlDbType.Bit:
                        coltype = VxColumnType.Bool;
                        break;
                    case SqlDbType.Float:
                    case SqlDbType.Real:
                        coltype = VxColumnType.Double;
                        break;
                    case SqlDbType.UniqueIdentifier:
                        coltype = VxColumnType.Uuid;
                        break;
                    case SqlDbType.Binary:
                    case SqlDbType.VarBinary:
                    case SqlDbType.Image:
                    case SqlDbType.Timestamp:
                        coltype = VxColumnType.Binary;
                        break;
                    case SqlDbType.Char:
                    case SqlDbType.VarChar:
                    case SqlDbType.Text:
                    case SqlDbType.NChar:
                    case SqlDbType.NVarChar:
                    case SqlDbType.NText:
                    case SqlDbType.Xml:
                        coltype = VxColumnType.String;
                        break;
                    case SqlDbType.DateTime:
                    case SqlDbType.SmallDateTime:
                        coltype = VxColumnType.Datetime;
                        break;
                    case SqlDbType.Decimal:
                    case SqlDbType.Money:
                    case SqlDbType.SmallMoney:
                        coltype = VxColumnType.Decimal;
                        break;
                    default:
                        throw new VxBadSchema("Columns of type "
                                + dbt.ToString() + "are not supported by "
                                + "Versaplex at this time");
                }

                colnames[i] = col["ColumnName"].ToString();
                coltypes[i] = coltype;

                i++;
            }
        }

        return colDefs.ToString();
    }
}

private enum VxColumnType {
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

public class VxSqlError : Exception {
    public VxSqlError()
        : base()
    {
    }
    
    public VxSqlError(string msg)
        : base(msg)
    {
    }

    public VxSqlError(SerializationInfo si, StreamingContext sc)
        : base(si, sc)
    {
    }

    public VxSqlError(string msg, Exception inner)
        : base(msg, inner)
    {
    }
}

public class VxTooMuchData : Exception {
    public VxTooMuchData()
        : base()
    {
    }
    
    public VxTooMuchData(string msg)
        : base(msg)
    {
    }

    public VxTooMuchData(SerializationInfo si, StreamingContext sc)
        : base(si, sc)
    {
    }

    public VxTooMuchData(string msg, Exception inner)
        : base(msg, inner)
    {
    }
}

public class VxBadSchema : Exception {
    public VxBadSchema()
        : base()
    {
    }
    
    public VxBadSchema(string msg)
        : base(msg)
    {
    }

    public VxBadSchema(SerializationInfo si, StreamingContext sc)
        : base(si, sc)
    {
    }

    public VxBadSchema(string msg, Exception inner)
        : base(msg, inner)
    {
    }
}

}
