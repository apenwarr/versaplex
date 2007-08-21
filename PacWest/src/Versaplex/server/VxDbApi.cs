using System;
using System.Text;
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
            out string[] colnames, out VxColumnType[] coltypes,
            out object[][] data, out byte[][] nullity)
    {
        Console.WriteLine("ExecRecordset " + query);

        SqlConnection conn = null;
        try {
            conn = VxSqlPool.TakeConnection();

            using (SqlCommand cmd = new SqlCommand(query, conn))
            using (SqlDataReader reader = cmd.ExecuteReader()) {
                if (reader.FieldCount <= 0) {
                    throw new VxBadSchema("No columns in record set");
                }

                ProcessSchema(reader, out colnames, out coltypes);

                List<object[]> rows = new List<object[]>();
                List<byte[]> rownulls = new List<byte[]>();

                while (reader.Read()) {
                    object[] row = new object[reader.FieldCount];
                    byte[] rownull = new byte[reader.FieldCount];

                    for (int i = 0; i < reader.FieldCount; i++) {
                        bool isnull = reader.IsDBNull(i);

                        row[i] = null;

                        rownull[i] = isnull ? (byte)1 : (byte)0;

                        switch (coltypes[i]) {
                            case VxColumnType.Int64:
                                row[i] = !isnull ?
                                    reader.GetInt64(i) : new Int64();
                                break;
                            case VxColumnType.Int32:
                                row[i] = !isnull ?
                                    reader.GetInt32(i) : new Int32();
                                break;
                            case VxColumnType.Int16:
                                row[i] = !isnull ?
                                    reader.GetInt16(i) : new Int16();
                                break;
                            case VxColumnType.UInt8:
                                row[i] = !isnull ?
                                    reader.GetByte(i) : new Byte();
                                break;
                            case VxColumnType.Bool:
                                row[i] = !isnull ?
                                    reader.GetBoolean(i) : new Boolean();
                                break;
                            case VxColumnType.Double:
                                // Might return a Single or Double
                                // FIXME: Check if getting a single causes this
                                // to croak
                                row[i] = !isnull ?
                                    (double)reader.GetDouble(i) : (double)0.0;
                                break;
                            case VxColumnType.Uuid:
                                row[i] = !isnull ?
                                    reader.GetGuid(i).ToString() : "";
                                break;
                            case VxColumnType.Binary:
                            {
                                if (isnull) {
                                    row[i] = new byte[0];
                                    break;
                                }

                                byte[] cell = new byte[reader.GetBytes(i, 0,
                                        null, 0, 0)];
                                reader.GetBytes(i, 0, cell, 0, cell.Length);

                                row[i] = cell;
                                break;
                            }
                            case VxColumnType.String:
                                row[i] = !isnull ? reader.GetString(i) : "";
                                break;
                            case VxColumnType.DateTime:
                                row[i] = !isnull ?
                                    new VxDbusDateTime(reader.GetDateTime(i)) :
                                    new VxDbusDateTime();
                                break;
                            case VxColumnType.Decimal:
                                row[i] = !isnull ?
                                    reader.GetDecimal(i).ToString() : "";
                                break;
                        }
                    }

                    rows.Add(row);
                    rownulls.Add(rownull);
                }

                data = rows.ToArray();
                nullity = rownulls.ToArray();
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

        string[] colnames;
        VxColumnType[] coltypes;
        object[][] data;
        byte[][] nullity;
        VxDb.ExecRecordset((string)query, out colnames, out coltypes,
                out data, out nullity);

        string[] coltypes_str = new string[coltypes.Length];
        for (int i=0; i < coltypes.Length; i++) {
            coltypes_str[i] = coltypes[i].ToString();
        }

        // FIXME: Add com.versabanq.versaplex.toomuchdata error
        MessageWriter writer =
                new MessageWriter(Connection.NativeEndianness);
        writer.Write(typeof(string[]), colnames);
        writer.Write(typeof(string[]), coltypes_str);
        writer.Write(typeof(Signature), VxColumnTypeToArraySignature(coltypes));
        writer.WriteDelegatePrependSize(delegate(MessageWriter w) {
                    WriteStructArray(w, VxColumnTypeToType(coltypes), data);
                }, 8);
        writer.Write(typeof(byte[][]), nullity);

        reply = VxDbus.CreateReply(call, "asasvaay", writer);

        // For debugging
        reply.WriteHeader();
        VxDbus.MessageDump(reply);
    }

    private static Signature VxColumnTypeToArraySignature(VxColumnType[] vxct)
    {
        StringBuilder sig = new StringBuilder("a(");

        foreach (VxColumnType ct in vxct) {
            switch (ct) {
            case VxColumnType.Int64:
                sig.Append("x");
                break;
            case VxColumnType.Int32:
                sig.Append("i");
                break;
            case VxColumnType.Int16:
                sig.Append("n");
                break;
            case VxColumnType.UInt8:
                sig.Append("y");
                break;
            case VxColumnType.Bool:
                sig.Append("b");
                break;
            case VxColumnType.Double:
                sig.Append("d");
                break;
            case VxColumnType.Uuid:
                sig.Append("s");
                break;
            case VxColumnType.Binary:
                sig.Append("ay");
                break;
            case VxColumnType.String:
                sig.Append("s");
                break;
            case VxColumnType.DateTime:
                sig.Append("(xi)");
                break;
            case VxColumnType.Decimal:
                sig.Append("s");
                break;
            default:
                throw new ArgumentException("Unknown VxColumnType");
            }
        }

        sig.Append(")");

        return new Signature(sig.ToString());
    }

    private static Type[] VxColumnTypeToType(VxColumnType[] vxct)
    {
        Type[] ret = new Type[vxct.Length];

        for (int i=0; i < vxct.Length; i++) {
            switch (vxct[i]) {
            case VxColumnType.Int64:
                ret[i] = typeof(Int64);
                break;
            case VxColumnType.Int32:
                ret[i] = typeof(Int32);
                break;
            case VxColumnType.Int16:
                ret[i] = typeof(Int16);
                break;
            case VxColumnType.UInt8:
                ret[i] = typeof(Byte);
                break;
            case VxColumnType.Bool:
                ret[i] = typeof(Boolean);
                break;
            case VxColumnType.Double:
                ret[i] = typeof(Double);
                break;
            case VxColumnType.Uuid:
                ret[i] = typeof(string);
                break;
            case VxColumnType.Binary:
                ret[i] = typeof(byte[]);
                break;
            case VxColumnType.String:
                ret[i] = typeof(string);
                break;
            case VxColumnType.DateTime:
                ret[i] = typeof(VxDbusDateTime);
                break;
            case VxColumnType.Decimal:
                ret[i] = typeof(string);
                break;
            default:
                throw new ArgumentException("Unknown VxColumnType");
            }
        }

        return ret;
    }

    private static void WriteStructArray(MessageWriter writer,
            Type[] types, object[][] data)
    {
        foreach (object[] row in data) {
            writer.WritePad(8);

            for (int i=0; i < row.Length; i++) {
                if (!types[i].IsInstanceOfType(row[i]))
                    throw new ArgumentException("Data does not match type for "
                            +"column " + i);

                writer.Write(types[i], row[i]);
            }
        }
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
