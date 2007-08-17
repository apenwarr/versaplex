using System;
using System.Data;
using System.Data.SqlClient;
using System.Runtime.Serialization;
using System.Collections.Generic;
using NDesk.DBus;
using versabanq.Versaplex.Server;
using versabanq.Versaplex.Dbus;

namespace versabanq.Versaplex.Dbus.Db {

public static class VxDb {
    public static void ExecNoResult(string query)
    {
        Console.WriteLine("ExecNoResult " + query);

        SqlConnection conn = null;
        try {
            conn = VxSqlPool.TakeConnection();

            using (SqlCommand cmd = conn.CreateCommand()) {
                cmd.CommandText = query;
                cmd.ExecuteNonQuery();
            }
        } catch (SqlException e) {
            throw new VxSqlError("Error in query", e);
        } finally {
            if (conn != null)
                VxSqlPool.ReleaseConnection(conn);
        }
    }

    public static void ExecScalar(string query, out object result)
    {
        Console.WriteLine("ExecScalar " + query);

        SqlConnection conn = null;
        try {
            conn = VxSqlPool.TakeConnection();

            using (SqlCommand cmd = conn.CreateCommand()) {
                cmd.CommandText = query;
                result = cmd.ExecuteScalar();
            }
        } catch (SqlException e) {
            throw new VxSqlError("Error in query", e);
        } finally {
            if (conn != null)
                VxSqlPool.ReleaseConnection(conn);
        }
    }

    public static void ExecRecordset(string query,
            out string[] colnames, out string[] coltypes_str,
            out VxDbusDbResult[][] data)
    {
        Console.WriteLine("ExecRecordset " + query);

        SqlConnection conn = null;
        try {
            conn = VxSqlPool.TakeConnection();

            using (SqlCommand cmd = new SqlCommand(query, conn))
            using (SqlDataReader reader = cmd.ExecuteReader()) {
                if (reader.FieldCount <= 0) {
                    colnames = null;
                    coltypes_str = null;
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
                    VxDbusDbResult[] row
                        = new VxDbusDbResult[reader.FieldCount];

                    for (int i = 0; i < reader.FieldCount; i++) {
                        if (reader.IsDBNull(i)) {
                            row[i].Nullity = true;
                            row[i].Data = (byte)0;
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
                                // Might return a Single or Double
                                row[i].Data = (double)reader.GetValue(i);
                                break;
                            case VxColumnType.Uuid:
                                row[i].Data = reader.GetGuid(i).ToString();
                                break;
                            case VxColumnType.Binary:
                            {
                                byte[] cell = new byte[reader.GetBytes(i, 0,
                                        null, 0, 0)];
                                reader.GetBytes(i, 0, cell, 0, cell.Length);

                                row[i].Data = cell;
                                break;
                            }
                            case VxColumnType.String:
                                row[i].Data = reader.GetString(i);
                                break;
                            case VxColumnType.DateTime:
                                row[i].Data = new VxDbusDateTime(
                                        reader.GetDateTime(i));
                                break;
                            case VxColumnType.Decimal:
                                row[i].Data = reader.GetDecimal(i).ToString();
                                break;
                        }
                    }

                    rows.Add(row);
                }

                data = rows.ToArray();
            }
        } catch (SqlException e) {
            throw new VxSqlError("Error in query", e);
        } finally {
            if (conn != null)
                VxSqlPool.ReleaseConnection(conn);
        }
    }

    private static void ProcessSchema(SqlDataReader reader,
            out string[] colnames, out VxColumnType[] coltypes)
    {
        colnames = new string[reader.FieldCount];
        coltypes = new VxColumnType[reader.FieldCount];

        int i = 0;

        using (DataTable schema = reader.GetSchemaTable()) {
            foreach (DataRowView col in schema.DefaultView) {
                Console.WriteLine("---");
                foreach (DataColumn c in schema.Columns) {
                    Console.WriteLine("{0}:\t{1}", c.ColumnName,
                            col[c.ColumnName]);
                }

                System.Type type = (System.Type)col["DataType"];

                if (type == typeof(object)) {
                    // We're not even going to try to handle this yet
                    throw new VxBadSchema("Columns of type sql_variant "
                        + "are not supported by Versaplex at this time");
                }

                VxColumnType coltype;

                // FIXME: There must be *some* way to turn this into a
                // switch...
                if (type == typeof(Int64)) {
                    coltype = VxColumnType.Int64;
                } else if (type == typeof(Int32)) {
                    coltype = VxColumnType.Int32;
                } else if (type == typeof(Int16)) {
                    coltype = VxColumnType.Int16;
                } else if (type == typeof(Byte)) {
                    coltype = VxColumnType.UInt8;
                } else if (type == typeof(Boolean)) {
                    coltype = VxColumnType.Bool;
                } else if (type == typeof(Single) || type == typeof(Double)) {
                    coltype = VxColumnType.Double;
                } else if (type == typeof(Guid)) {
                    coltype = VxColumnType.Uuid;
                } else if (type == typeof(Byte[])) {
                    coltype = VxColumnType.Binary;
                } else if (type == typeof(string)) {
                    // || type == typeof(Xml)
                    // FIXME: Maybe? But the above doesn't work... and just
                    // testing for string does work. Heh.
                    coltype = VxColumnType.String;
                } else if (type == typeof(DateTime)) {
                    coltype = VxColumnType.DateTime;
                } else if (type == typeof(Decimal)) {
                    coltype = VxColumnType.Decimal;
                } else {
                    throw new VxBadSchema("Columns of type "
                            + type.ToString() + " are not supported by "
                            + "Versaplex at this time " +
                            "(column " + col["ColumnName"].ToString() + ")");
                }

                colnames[i] = col["ColumnName"].ToString();
                coltypes[i] = coltype;

                i++;
            }
        }
    }
}

public class VxDbInterfaceRouter : VxInterfaceRouter
{
    static readonly VxDbInterfaceRouter instance;
    public static VxDbInterfaceRouter Instance {
        get { return instance; }
    }

    static VxDbInterfaceRouter() {
        instance = new VxDbInterfaceRouter();
    }

    private VxDbInterfaceRouter() : base("com.versabanq.versaplex.db")
    {
        methods.Add("ExecNoResult", CallExecNoResult);
        methods.Add("ExecScalar", CallExecScalar);
        methods.Add("ExecRecordset", CallExecRecordset);
    }

    protected override void ExecuteCall(MethodCallProcessor processor,
            Message call, out Message reply)
    {
        try {
            processor(call, out reply);
        } catch (VxSqlError e) {
            reply = VxDbus.CreateError(
                    "com.versabanq.versaplex.sqlerror", e.ToString(), call);
        } catch (VxTooMuchData e) {
            reply = VxDbus.CreateError(
                    "com.versabanq.versaplex.toomuchdata", e.ToString(),
                    call);
        } catch (VxBadSchema e) {
            reply = VxDbus.CreateError(
                    "com.versabanq.versaplex.badschema", e.ToString(),
                    call);
        } catch (Exception e) {
            reply = VxDbus.CreateError(
                    "com.versabanq.versaplex.exception", e.ToString(),
                    call);
        }
    }

    private static void CallExecNoResult(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "s") {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    String.Format(
                        "No overload of ExecNoResult has signature '{0}'",
                        call.Signature), call);
            return;
        }

        if (call.Body == null) {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.InvalidSignature",
                    "Signature provided but no body received", call);
            return;
        }

        MessageReader reader = new MessageReader(call);

        object query;
        reader.GetValue(typeof(string), out query);

        VxDb.ExecNoResult((string)query);

        reply = VxDbus.CreateReply(call);
    }

    private static void CallExecScalar(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "s") {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    String.Format(
                        "No overload of ExecNoResult has signature '{0}'",
                        call.Signature), call);
            return;
        }

        if (call.Body == null) {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.InvalidSignature",
                    "Signature provided but no body received", call);
            return;
        }

        MessageReader reader = new MessageReader(call);

        object query;
        reader.GetValue(typeof(string), out query);

        object result;
        VxDb.ExecScalar((string)query, out result);

        MessageWriter writer =
                new MessageWriter(Connection.NativeEndianness);
        writer.Write(result);

        reply = VxDbus.CreateReply(call, "v", writer);
    }

    private static void CallExecRecordset(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "s") {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    String.Format(
                        "No overload of ExecNoResult has signature '{0}'",
                        call.Signature), call);
            return;
        }

        if (call.Body == null) {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.InvalidSignature",
                    "Signature provided but no body received", call);
            return;
        }

        MessageReader reader = new MessageReader(call);

        object query;
        reader.GetValue(typeof(string), out query);

        string[] colnames, coltypes_str;
        VxDbusDbResult[][] data;
        VxDb.ExecRecordset((string)query, out colnames, out coltypes_str,
                out data);

        MessageWriter writer =
                new MessageWriter(Connection.NativeEndianness);
        writer.Write(typeof(string[]), colnames);
        writer.Write(typeof(string[]), coltypes_str);
        writer.Write(typeof(VxDbusDbResult[][]), data);

        reply = VxDbus.CreateReply(call, "asasaa(bv)", writer);
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
