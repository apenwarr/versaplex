#include "wvtest.cs.h"

using Mono.Unix;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Wv.Test;
using Wv.Utils;
using NDesk.DBus;

// Several mono bugs worked around in this test fixture are filed as mono bug
// #81940

namespace Versaplex.Test
{

// Versaplex now needs DBus clients to provide their Unix UID to the bus.
// FIXME: This is mostly duplicated from Main.cs, as it's a bit tricky to share
// classes between Versaplex and the tests.
class DodgyTransport : NDesk.DBus.Transports.Transport
{
    public override string AuthString()
    {
        long uid = UnixUserInfo.GetRealUserId();
        return uid.ToString();
    }

    public override void WriteCred()
    {
        Stream.WriteByte(0);
    }

    public override void Open(AddressEntry entry)
    {
        Socket sock;
	if (entry.Method == "unix")
	{
	    string path;
	    bool abstr;

	    if (entry.Properties.TryGetValue("path", out path))
		abstr = false;
	    else if (entry.Properties.TryGetValue("abstract", out path))
		abstr = true;
	    else
		throw new Exception("No path specified for UNIX transport");

	    if (abstr)
		sock = OpenAbstractUnix(path);
	    else
		sock = OpenPathUnix(path);
	}
	else if (entry.Method == "tcp")
	{
	    string host = "127.0.0.1";
	    string port = "5555";
	    entry.Properties.TryGetValue("host", out host);
	    entry.Properties.TryGetValue("port", out port);
	    sock = OpenTcp(host, Int32.Parse(port));
	}
	else
	    throw new Exception(String.Format("Unknown connection method {0}",
					      entry.Method));
	
        sock.Blocking = true;
        SocketHandle = (long)sock.Handle;
        Stream = new NetworkStream(sock);
    }

    protected Socket OpenAbstractUnix(string path)
    {
        AbstractUnixEndPoint ep = new AbstractUnixEndPoint(path);
        Socket client = new Socket(AddressFamily.Unix, SocketType.Stream, 0);
        client.Connect(ep);
        return client;
    }

    public Socket OpenPathUnix(string path) 
    {
        UnixEndPoint ep = new UnixEndPoint(path);
        Socket client = new Socket(AddressFamily.Unix, SocketType.Stream, 0);
        client.Connect(ep);
        return client;
    }
    
    public Socket OpenTcp(string host, int port)
    {
	IPHostEntry hent = Dns.GetHostEntry(host);
	IPAddress ip = hent.AddressList[0];
        IPEndPoint ep = new IPEndPoint(ip, port);
        Socket client = new Socket(AddressFamily.InterNetwork,
						   SocketType.Stream, 0);
        client.Connect(ep);
        return client;
    }
}

[TestFixture]
public class VersaplexTest
{
    private const string Server = "amsdev";
    private const string User = "asta";
    private const string Password = "m!ddle-tear";
    private const string Database = "adrian_test";

    private const string DbusConnName = "com.versabanq.versaplex";
    private const string DbusInterface = "com.versabanq.versaplex.db";
    private static readonly ObjectPath DbusObjPath;
    
    static VersaplexTest() {
        DbusObjPath = new ObjectPath("/com/versabanq/versaplex/db");
    }

    // A file full of "lorem ipsum dolor" text
    private const string lipsum_file = "lipsum.txt";
    // A UTF-8 test file
    private const string unicode_file = "UTF-8-demo.txt";
    // A random file of binary goop
    private const string goop_file = "random.bin";
    // THTBACS image
    private const string image_file = "thtbacs.tiff";

    SqlConnection con;
    SqlCommand cmd;
    Bus bus;

    bool Connect(SqlConnection connection)
    {
	connection.Open();

	return true;
    }

    bool Exec(string query)
    {
	Console.WriteLine(" + Exec SQL Query: {0}", query);

	using (SqlCommand execCmd = new SqlCommand(query, con)) {
	    execCmd.ExecuteNonQuery();
	}

	return true;
    }

    bool Scalar(string query, out object result)
    {
	Console.WriteLine(" + Scalar SQL Query: {0}", query);

	using (SqlCommand execCmd = new SqlCommand(query, con)) {
	    result = execCmd.ExecuteScalar();
	}

	return true;
    }

    bool Reader(string query, out SqlDataReader result)
    {
	Console.WriteLine(" + Reader SQL Query: {0}", query);

	using (SqlCommand execCmd = new SqlCommand(query, con)) {
	    result = execCmd.ExecuteReader();
	}

	return true;
    }

    Message CreateMethodCall(string destination, ObjectPath path,
            string iface, string member, string signature)
    {
        Message msg = new Message();
        msg.Connection = bus;
        msg.Header.MessageType = MessageType.MethodCall;
        msg.Header.Flags = HeaderFlag.None;
        msg.Header.Fields[FieldCode.Path] = path;
        msg.Header.Fields[FieldCode.Member] = member;

        if (destination != null && destination != "")
            msg.Header.Fields[FieldCode.Destination] = destination;
        
        if (iface != null && iface != "")
            msg.Header.Fields[FieldCode.Interface] = iface;

        if (signature != null && signature != "")
            msg.Header.Fields[FieldCode.Signature] = new Signature(signature);

        return msg;
    }

    bool VxExec(string query)
    {
	Console.WriteLine(" + VxExec SQL Query: {0}", query);

        Message call = CreateMethodCall(DbusConnName, DbusObjPath,
                DbusInterface, "ExecNoResult", "s");

        MessageWriter mw = new MessageWriter(Connection.NativeEndianness);
        mw.Write(typeof(string), query);

        call.Body = mw.ToArray();

        Message reply = bus.SendWithReplyAndBlock(call);

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
            return true;
        case MessageType.Error:
        {
            object errname;
            if (!reply.Header.Fields.TryGetValue(FieldCode.ErrorName,
                        out errname))
                throw new Exception("D-Bus error received but no error name "
                        +"given");

            object errsig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out errsig) || errsig.ToString() != "s")
                throw new DbusError(errname.ToString());

            MessageReader mr = new MessageReader(reply);

            object errmsg;
            mr.GetValue(typeof(string), out errmsg);

            throw new DbusError(errname.ToString() + ": " + errmsg.ToString());
        }
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    bool VxScalar(string query, out object result)
    {
	Console.WriteLine(" + VxScalar SQL Query: {0}", query);

        Message call = CreateMethodCall(DbusConnName, DbusObjPath,
                DbusInterface, "ExecScalar", "s");

        MessageWriter mw = new MessageWriter(Connection.NativeEndianness);
        mw.Write(typeof(string), query);

        call.Body = mw.ToArray();

        Message reply = bus.SendWithReplyAndBlock(call);

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had no signature");

            if (replysig == null || replysig.ToString() != "v")
                throw new Exception("D-Bus reply had invalid signature");

            MessageReader reader = new MessageReader(reply);
            reader.GetValue(out result); // This overload processes a variant

            return true;
        }
        case MessageType.Error:
        {
            object errname;
            if (!reply.Header.Fields.TryGetValue(FieldCode.ErrorName,
                        out errname))
                throw new Exception("D-Bus error received but no error name "
                        +"given");

            object errsig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out errsig) || errsig.ToString() != "s")
                throw new DbusError(errname.ToString());

            MessageReader mr = new MessageReader(reply);

            object errmsg;
            mr.GetValue(typeof(string), out errmsg);

            throw new DbusError(errname.ToString() + ": " + errmsg.ToString());
        }
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    bool VxRecordset(string query, out VxColumnInfo[] colinfo,
            out object[][] data, out bool[][] nullity)
    {
	Console.WriteLine(" + VxReader SQL Query: {0}", query);

        Message call = CreateMethodCall(DbusConnName, DbusObjPath,
                DbusInterface, "ExecRecordset", "s");

        MessageWriter mw = new MessageWriter(Connection.NativeEndianness);
        mw.Write(typeof(string), query);

        call.Body = mw.ToArray();

        Message reply = bus.SendWithReplyAndBlock(call);

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had no signature");

            if (replysig == null || replysig.ToString() != "a(issnny)vaay")
                throw new Exception("D-Bus reply had invalid signature");

            MessageReader reader = new MessageReader(reply);
            Array ci;
            reader.GetValue(typeof(VxColumnInfo[]), out ci);
            colinfo = (VxColumnInfo[])ci;

            Signature sig;
            reader.GetValue(out sig);

            // TODO: Check that sig matches colinfo
            // Sig should be of the form a(...)

            int arraysz;
            reader.GetValue(out arraysz);

            int endpos = reader.Position + arraysz;;

            List<object[]> results = new List<object[]>();
            while (reader.Position < endpos) {
                object[] row = new object[colinfo.Length];

                // Each structure element is 8-byte aligned
                reader.ReadPad(8);

                for (int i=0; i < row.Length; i++) {
                    switch (colinfo[i].VxColumnType) {
                    case VxColumnType.Int64:
                    {
                        long cell;
                        reader.GetValue(out cell);
                        row[i] = cell;
                        break;
                    }
                    case VxColumnType.Int32:
                    {
                        int cell;
                        reader.GetValue(out cell);
                        row[i] = cell;
                        break;
                    }
                    case VxColumnType.Int16:
                    {
                        short cell;
                        reader.GetValue(out cell);
                        row[i] = cell;
                        break;
                    }
                    case VxColumnType.UInt8:
                    {
                        byte cell;
                        reader.GetValue(out cell);
                        row[i] = cell;
                        break;
                    }
                    case VxColumnType.Bool:
                    {
                        bool cell;
                        reader.GetValue(out cell);
                        row[i] = cell;
                        break;
                    }
                    case VxColumnType.Double:
                    {
                        double cell;
                        reader.GetValue(out cell);
                        row[i] = cell;
                        break;
                    }
                    case VxColumnType.Uuid:
                    {
                        string cell;
                        reader.GetValue(out cell);

                        if (cell == "") {
                            row[i] = new Guid();
                        } else {
                            row[i] = new Guid(cell);
                        }
                        break;
                    }
                    case VxColumnType.Binary:
                    {
                        object cell;
                        reader.GetValue(typeof(byte[]), out cell);
                        row[i] = cell;
                        break;
                    }
                    case VxColumnType.String:
                    {
                        Console.WriteLine("Reading string from pos {0}",
                                reader.Position);
                        string cell;
                        reader.GetValue(out cell);
                        row[i] = cell;
                        break;
                    }
                    case VxColumnType.DateTime:
                    {
                        long seconds;
                        int microseconds;
                        
                        reader.ReadPad(8);
                        reader.GetValue(out seconds);
                        reader.GetValue(out microseconds);

                        VxDbusDateTime dt = new VxDbusDateTime();
                        dt.Seconds = seconds;
                        dt.Microseconds = microseconds;

                        row[i] = dt;
                        break;
                    }
                    case VxColumnType.Decimal:
                    {
                        string cell;
                        reader.GetValue(out cell);

                        if (cell == "") {
                            row[i] = new Decimal();
                        } else {
                            row[i] = Decimal.Parse(cell);
                        }
                        break;
                    }
                    default:
                        throw new Exception("Invalid column type received");
                    }
                }

                results.Add(row);
            }

            if (reader.Position != endpos)
                throw new Exception("Position mismatch after reading data");
 
            data = results.ToArray();

            object rawnulls;
            reader.GetValue(typeof(byte[][]), out rawnulls);

            byte[][] rawnulls_typed = (byte[][])rawnulls;

            nullity = new bool[rawnulls_typed.Length][];

            for (int i=0; i < rawnulls_typed.Length; i++) {
                nullity[i] = new bool[rawnulls_typed[i].Length];

                for (int j=0; j < rawnulls_typed[i].Length; j++) {
                    nullity[i][j] = (rawnulls_typed[i][j] == 0) ? false : true;
                }
            }

            return true;
        }
        case MessageType.Error:
        {
            object errname;
            if (!reply.Header.Fields.TryGetValue(FieldCode.ErrorName,
                        out errname))
                throw new Exception("D-Bus error received but no error name "
                        +"given");

            object errsig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out errsig) || errsig.ToString() != "s")
                throw new DbusError(errname.ToString());

            MessageReader mr = new MessageReader(reply);

            object errmsg;
            mr.GetValue(typeof(string), out errmsg);

            throw new DbusError(errname.ToString() + ": " + errmsg.ToString());
        }
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    bool Insert(string table, params object [] param)
    {
        Console.WriteLine(" + Insert to {0} ({1})", table, String.Join(", ",
                    wv.stringify(param)));

        System.Text.StringBuilder query = new System.Text.StringBuilder();
        query.AppendFormat("INSERT INTO [{0}] VALUES (",
                table.Replace("]","]]"));

        using (SqlCommand insCmd = con.CreateCommand()) {
            for (int i=0; i < param.Length; i++) {
                if (i > 0)
                    query.Append(", ");

                if (param[i] is DBNull) {
                    query.Append("NULL");
                } else {
                    string paramName = string.Format("@col{0}", i);

                    query.Append(paramName);
                    insCmd.Parameters.Add(new SqlParameter(paramName,
                                param[i]));
                }
            }

            query.Append(")");
            insCmd.CommandText = query.ToString();

            insCmd.ExecuteNonQuery();
        }

        return true;
    }

    string read_lipsum()
    {
        WVASSERT(File.Exists(lipsum_file));

        using (StreamReader sr = new StreamReader(lipsum_file)) {
            return sr.ReadToEnd();
        }
    }

    string read_unicode()
    {
        WVASSERT(File.Exists(unicode_file));

        using (StreamReader sr = new StreamReader(unicode_file)) {
            return sr.ReadToEnd();
        }
    }

    Byte [] read_goop()
    {
        WVASSERT(File.Exists(goop_file));

        using (FileStream f = new FileStream(goop_file, FileMode.Open,
                    FileAccess.Read))
        using (BinaryReader sr = new BinaryReader(f)) {
            return sr.ReadBytes((int)Math.Min(f.Length, Int32.MaxValue));
        }
    }

    Byte [] read_image()
    {
        WVASSERT(File.Exists(image_file));

        using (FileStream f = new FileStream(image_file, FileMode.Open,
                    FileAccess.Read))
        using (BinaryReader sr = new BinaryReader(f)) {
            return sr.ReadBytes((int)Math.Min(f.Length, Int32.MaxValue));
        }
    }

    long GetInt64(SqlDataReader reader, int colnum) {
        // For some reason, it won't just up-convert int32 to int64
        if (reader.GetFieldType(colnum) == typeof(System.Int32)) {
            return reader.GetInt32(colnum);
        } else if (reader.GetFieldType(colnum) == typeof(System.Int64)) {
            return reader.GetInt64(colnum);
        } else if (reader.GetFieldType(colnum) == typeof(System.Decimal)) {
            return (long)reader.GetDecimal(colnum);
        } else {
            // Unknown type
            bool unknown_type_in_result = true;
            WVFAIL(unknown_type_in_result);

            return -1;
        }
    }

    [SetUp]
    public void init()
    {
	con = new SqlConnection(string.Format(
		    "Server={0};UID={1};Password={2};Database={3}",
		    Server, User, Password, Database));

	WVASSERT(Connect(con));

	cmd = con.CreateCommand();

        if (Address.Session == null)
            throw new Exception ("DBUS_SESSION_BUS_ADDRESS not set");
        AddressEntry aent = AddressEntry.Parse(Address.Session);
        DodgyTransport trans = new DodgyTransport();
        trans.Open(aent);
        bus = new Bus(trans);
    }

    [TearDown]
    public void cleanup()
    {
        bus = null;

	if (cmd != null)
	    cmd.Dispose();
	cmd = null;

	if (con != null)
	    con.Dispose();
	con = null;
    }

    [Test, Category("Data"), Category("Sanity")]
    public void EmptyTable()
    {
	// Check that an empty table is read ok
        try { VxExec("DROP TABLE test1"); } catch {}

        try {
            WVASSERT(VxExec("CREATE TABLE test1 (testcol int not null)"));

            object result;
            WVASSERT(VxScalar("SELECT COUNT(*) FROM test1", out result));
            WVPASSEQ((int)result, 0);

            VxColumnInfo[] colinfo;
            object[][] data;
            bool[][] nullity;
            WVASSERT(VxRecordset("SELECT * FROM test1", out colinfo, out data,
                        out nullity));

            WVPASSEQ(colinfo.Length, 1);
            WVPASSEQ(colinfo[0].ColumnName, "testcol");
            WVPASSEQ(colinfo[0].ColumnType.ToLowerInvariant(), "int32");

            WVPASSEQ(data.Length, 0);
            WVPASSEQ(nullity.Length, 0);
        } finally {
            try { VxExec("DROP TABLE test1"); } catch {}
        }
    }

    [Test, Category("Running"), Category("Errors")]
    public void NonexistantTable()
    {
	// Check that a nonexistant table throws an error
	try {
            VxColumnInfo[] colinfo;
            object[][] data;
            bool[][] nullity;
	    WVEXCEPT(VxRecordset("SELECT * FROM #nonexistant", out colinfo,
                        out data, out nullity));
	} catch (Wv.Test.WvAssertionFailure e) {
	    throw e;
	} catch (System.Exception e) {
            // FIXME: This should check for a com.versabanq.versaplex.sqlerror
            // rather than any dbus error
	    WVPASS(e is DbusError);
	}
	
	// The only way to get here is for the test to pass (otherwise an
	// exception has been generated somewhere), as WVEXCEPT() always throws
	// something.
    }

    [Test, Category("Schema"), Category("Sanity")]
    public void ColumnTypes()
    {
	// Check that column types are copied correctly to the output table
        try { VxExec("DROP TABLE test1"); } catch {}

	string[] colTypes = {
	    // Pulled from the SQL Server management gui app's dropdown list in
	    // the table design screen
	    "bigint", "binary(50)", "bit", "char(10)", "datetime",
	    "decimal(18, 0)", "float", "image", "int", "money", "nchar(10)",
	    "ntext", "numeric(18, 0)", "nvarchar(50)", "nvarchar(MAX)", "real",
	    "smalldatetime", "smallint", "smallmoney", "text",
            "timestamp", "tinyint", "uniqueidentifier", "varbinary(50)",
	    "varbinary(MAX)", "varchar(50)", "varchar(MAX)", "xml",
            // "sql_variant", // this is problematic, so it is unsupported

	    // Plus a few more to mix up the parameters a bit, and providing
	    // edge cases
	    "numeric(1, 0)", "numeric(38, 38)", "numeric(1, 1)",
	    "numeric(38, 0)", "nvarchar(4000)", "nvarchar(1)",
	    "varchar(8000)", "varchar(1)", "char(1)", "char(8000)",
	    "nchar(1)", "nchar(4000)", "decimal(1, 0)", "decimal(38, 38)",
	    "decimal(1, 1)", "decimal(38, 0)", "binary(1)", "binary(8000)"
	};

	foreach (String colType in colTypes) {
	    WVASSERT(VxExec(string.Format("CREATE TABLE test1 (testcol {0})",
			    colType)));
	    // This makes sure it runs the prepare statement
	    WVASSERT(Insert("test1", DBNull.Value));

	    SqlDataReader reader;
	    DataTable schema;

	    WVASSERT(Reader("SELECT * FROM test1", out reader));
	    using (reader)
		schema = reader.GetSchemaTable();

            VxColumnInfo[] vxcolinfo;
            object[][] data;
            bool[][] nullity;
	    WVASSERT(VxRecordset("SELECT * FROM test1", out vxcolinfo, out data,
                        out nullity));

	    WVPASSEQ(schema.Rows.Count, vxcolinfo.Length);

            try {
	    for (int colNum = 0; colNum < schema.Rows.Count; colNum++) {
		DataRow colInfo = schema.Rows[colNum];

		WVPASSEQ((string)colInfo["ColumnName"],
                        vxcolinfo[colNum].ColumnName);
		WVPASSEQ((int)colInfo["ColumnOrdinal"], colNum);
                // FIXME: There must be *some* way to turn this into a
                // switch...
                Type type = (Type)colInfo["DataType"];
                string vxtype = vxcolinfo[colNum].ColumnType.ToLowerInvariant();
                if (type == typeof(Int64)) {
                    WVPASSEQ(vxtype, "int64");
                } else if (type == typeof(Int32)) {
                    WVPASSEQ(vxtype, "int32");
                } else if (type == typeof(Int16)) {
                    WVPASSEQ(vxtype, "int16");
                } else if (type == typeof(Byte)) {
                    WVPASSEQ(vxtype, "uint8");
                } else if (type == typeof(Boolean)) {
                    WVPASSEQ(vxtype, "bool");
                } else if (type == typeof(Single) || type == typeof(Double)) {
                    WVPASSEQ(vxtype, "double");
                } else if (type == typeof(Guid)) {
                    WVPASSEQ(vxtype, "uuid");
                } else if (type == typeof(Byte[])) {
                    WVPASSEQ(vxtype, "binary");
                } else if (type == typeof(string)) {
                    WVPASSEQ(vxtype, "string");
                } else if (type == typeof(DateTime)) {
                    WVPASSEQ(vxtype, "datetime");
                } else if (type == typeof(Decimal)) {
                    WVPASSEQ(vxtype, "decimal");
                } else {
                    bool return_column_type_is_known = false;
                    WVASSERT(return_column_type_is_known);
                }

                WVPASSEQ((int)colInfo["ColumnSize"],
                        vxcolinfo[colNum].Size);
                // These next two may have problems with mono vs microsoft
                // differences
                WVPASSEQ((short)colInfo["NumericPrecision"],
                        vxcolinfo[colNum].Precision);
                WVPASSEQ((short)colInfo["NumericScale"],
                        vxcolinfo[colNum].Scale);
            }
            } finally {
                try { VxExec("DROP TABLE test1"); } catch {}
            }
        }
    }

    [Test, Category("Schema"), Category("Errors")]
    public void EmptyColumnName()
    {
	// Check that a query with a missing column name is ok
	SqlDataReader reader;
	WVASSERT(Reader("SELECT 1", out reader));
	using (reader)
	using (DataTable schema = reader.GetSchemaTable()) {
	    WVPASSEQ(reader.FieldCount, 1);
	    WVPASSEQ(schema.Rows.Count, 1);

	    DataRow schemaRow = schema.Rows[0];
	    WVPASSEQ((string)schemaRow["ColumnName"], "");

            WVPASS(reader.Read());
            WVPASSEQ((int)reader[0], 1);

            WVFAIL(reader.Read());
            WVFAIL(reader.NextResult());
	}
    }

    [Test, Category("Data")]
    public void RowOrdering()
    {
        // Make sure that data comes out in the right order when ordering is
        // requested from Versaplex

        // If these are all prime then the permutation is guaranteed to work
        // without any duplicates (I think it actually works as long as numElems
        // is coprime with the other two, but making them all prime is safe)
        const int numElems = 101;
        const int prime1 = 47;
        const int prime2 = 53;

	WVASSERT(Exec("CREATE TABLE #test1 (seq int NOT NULL, "
                    + "num int NOT NULL)"));

        // j will be a permutation of 0..numElems without resorting to random
        // numbers, while making sure that we're not inserting in sorted order.
        for (int i=0, j=0; i < numElems; i++, j = (i*prime1) % numElems) {
            // This inserts 0..numElems into seq (in a permuted order), with
            // 0..numElems in num, but permuted in a different order.
            Insert("#test1", j, (j*prime2) % numElems);
        }

        SqlDataReader reader;
        WVASSERT(Reader("SELECT num FROM #test1 ORDER BY seq",
                    out reader));

        using (reader) {
            for (int i=0; i < numElems; i++) {
                WVASSERT(reader.Read());
                WVPASSEQ((int)reader["num"], (i*prime2) % numElems);
            }

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Schema")]
    public void ColumnOrdering()
    {
        // Make a bunch of columns and check that they come back in the right
        // order

        // For an explanation about the permutation stuff here, see the
        // RowOrdering test, above
        const int numCols = 101;
        const int numSelected = 83;
        const int prime1 = 47;
        const int prime2 = 53;

        System.Text.StringBuilder query = new System.Text.StringBuilder(
                "CREATE TABLE #test1 (");

        for (int i=0, j=0; i < numCols; i++, j = (i*prime1) % numCols) {
            if (i > 0)
                query.Append(", ");

            query.AppendFormat("col{0} int", j);
        }

        query.Append(")");

        WVASSERT(Exec(query.ToString()));

        query = new System.Text.StringBuilder("SELECT ");

        // Don't select all of them, in case that makes a difference. But still
        // select from the entire range (as opposed to the first few), so still
        // mod by numCols instead of numSelected.
        for (int i=0, j=0; i < numSelected; i++, j = (i*prime2) % numCols) {
            if (i > 0)
                query.Append(", ");

            query.AppendFormat("col{0}", j);
        }
        query.Append(" FROM #test1");

        SqlDataReader reader;
        WVASSERT(Reader(query.ToString(), out reader));

        using (reader) {
            WVPASSEQ(reader.FieldCount, numSelected);

            for (int i=0; i < numSelected; i++) {
                WVPASSEQ((string)reader.GetName(i),
                        string.Format("col{0}", (i*prime2) % numCols));
            }

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Data")]
    public void VerifyIntegers()
    {
        // bigint, int, smallint, tinyint
        // Insert 6 rows: max, 10, 0, -10, min, nulls (except tinyint is
        // unsigned so it has 0 again instead of -10)
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)

        WVASSERT(Exec("CREATE TABLE #test1 (bi bigint, i int, si smallint, "
                    + "ti tinyint, roworder int not null)"));

        WVASSERT(Insert("#test1", Int64.MaxValue, Int32.MaxValue,
                    Int16.MaxValue, Byte.MaxValue, 1));
        WVASSERT(Insert("#test1", 10, 10, 10, 10, 2));
        WVASSERT(Insert("#test1", 0, 0, 0, 0, 3));
        WVASSERT(Insert("#test1", -10, -10, -10, 0, 4));
        WVASSERT(Insert("#test1", Int64.MinValue, Int32.MinValue,
                    Int16.MinValue, Byte.MinValue, 5));
        WVASSERT(Insert("#test1", DBNull.Value, DBNull.Value, DBNull.Value,
                    DBNull.Value, 6));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT bi,i,si,ti FROM #test1 ORDER BY roworder",
                    out reader));

        using (reader) {
            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetInt64(0), Int64.MaxValue);
            WVPASSEQ(reader.GetInt32(1), Int32.MaxValue);
            WVPASSEQ(reader.GetInt16(2), Int16.MaxValue);
            WVPASSEQ(reader.GetByte(3), Byte.MaxValue);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetInt64(0), 10);
            WVPASSEQ(reader.GetInt32(1), 10);
            WVPASSEQ(reader.GetInt16(2), 10);
            WVPASSEQ(reader.GetByte(3), 10);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetInt64(0), 0);
            WVPASSEQ(reader.GetInt32(1), 0);
            WVPASSEQ(reader.GetInt16(2), 0);
            WVPASSEQ(reader.GetByte(3), 0);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetInt64(0), -10);
            WVPASSEQ(reader.GetInt32(1), -10);
            WVPASSEQ(reader.GetInt16(2), -10);
            WVPASSEQ(reader.GetByte(3), 0);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetInt64(0), Int64.MinValue);
            WVPASSEQ(reader.GetInt32(1), Int32.MinValue);
            WVPASSEQ(reader.GetInt16(2), Int16.MinValue);
            WVPASSEQ(reader.GetByte(3), Byte.MinValue);

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));
            WVPASS(reader.IsDBNull(1));
            WVPASS(reader.IsDBNull(2));
            WVPASS(reader.IsDBNull(3));

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Data")]
    public void VerifyBinary()
    {
        // binary, varbinary (not max)
        
        // This must be sorted
        int [] sizes = { 1, 10, 50, 255, 4000, 8000 };

        string [] types = { "binary", "varbinary" };
        int [] typemax = { 8000, 8000 };
        int [] charsize = { 1, 1 };
        bool [] varsize = { false, true };

        Byte [] binary_goop = read_goop();

        WVASSERT(binary_goop.Length >= sizes[sizes.Length-1]);

        for (int i=0; i < types.Length; i++) {
            for (int j=0; j < sizes.Length && sizes[j] <= typemax[i]; j++) {
                WVASSERT(Exec(string.Format("CREATE TABLE #test1 "
                                + "(data {0}({1}), roworder int not null)",
                                types[i], sizes[j])));

                for (int k=0; k <= j; k++) {
                    Byte [] data = new byte[sizes[k]];
                    Array.Copy(binary_goop, data, sizes[k]);

                    WVASSERT(Insert("#test1", new SqlBinary(data), k));
                }

                WVASSERT(Insert("#test1", DBNull.Value, j+1));

                SqlDataReader reader;
                WVASSERT(Reader("SELECT LEN(data), DATALENGTH(data), data FROM "
                            + "#test1 ORDER BY roworder",
                            out reader));

                using (reader) {
                    for (int k=0; k <= j; k++) {
                        Byte [] data = new byte[sizes[k]];
                        Array.Copy(binary_goop, data, sizes[k]);

                        WVASSERT(reader.Read());

                        int len = sizes[varsize[i] ? k : j];
                        WVPASSEQ(GetInt64(reader, 0), len);

                        int datalen = sizes[varsize[i] ? k : j]*charsize[i];
                        WVPASSEQ(GetInt64(reader, 1), datalen);

                        WVPASSEQ(reader.GetSqlBinary(2), new SqlBinary(data));
                    }

                    WVASSERT(reader.Read());
                    WVPASS(reader.IsDBNull(2));

                    WVFAIL(reader.Read());
                }

                WVASSERT(Exec("DROP TABLE #test1"));
            }
        }
    }

    [Test, Category("Data")]
    public void VerifyBit()
    {
        // bit
        // Insert 3 rows: true, false, null
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)

        WVASSERT(Exec("CREATE TABLE #test1 (b bit, roworder int not null)"));

        WVASSERT(Insert("#test1", true, 1));
        WVASSERT(Insert("#test1", false, 2));
        WVASSERT(Insert("#test1", DBNull.Value, 3));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT b FROM #test1 ORDER BY roworder",
                    out reader));

        using (reader) {
            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetBoolean(0), true);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetBoolean(0), false);

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Data")]
    public void VerifyChar()
    {
        try { VxExec("DROP TABLE test1"); } catch {}
        // char, nchar, varchar (in-row or max), nvarchar (in-row or max),
        // text, ntext
        // This doesn't try to use any non-ascii characters. There is a separate
        // test for that.
        
        // This must be sorted
        int [] sizes = { 1, 10, 50, 255, 4000, 8000, 8040, 8192, 16080, 16384,
            24120, 32160, 32767, 50157 };

        string [] types = { "char", "varchar", "nchar", "nvarchar", "text",
            "ntext", "varchar(max)", "nvarchar(max)" };
        int [] typemax = { 8000, 8000, 4000, 4000, Int32.MaxValue,
            Int32.MaxValue/2, Int32.MaxValue, Int32.MaxValue/2 };
        int [] charsize = { 1, 1, 2, 2, 1, 2, 1, 2 };
        bool [] varsize = { false, true, false, true, true, true, true, true };
        bool [] sizeparam = { true, true, true, true, false, false, false,
            false };
        bool [] lenok = { true, true, true, true, false, false, true, true };

        string lipsum_text = read_lipsum();

        WVASSERT(lipsum_text.Length >= sizes[sizes.Length-1]);

        // FIXME: For any values past the first 4 in each of these arrays,
        // dbus-sharp chokes with a "Read length mismatch" exception.  It's
        // probably related to the packets being longer than usual.  See
        // GoogleCode bug #1.
        for (int i=0; i < 4 /*types.Length*/; i++) {
            for (int j=0; j < 4 /*sizes.Length*/ && sizes[j] <= typemax[i]; j++) {
                if (sizeparam[i]) {
                    WVASSERT(VxExec(string.Format("CREATE TABLE test1 "
                                    + "(data {0}({1}), roworder int not null)",
                                    types[i], sizes[j])));
                } else {
                    WVASSERT(VxExec(string.Format("CREATE TABLE test1 "
                                    + "(data {0}, roworder int not null)",
                                    types[i])));
                    j = sizes.Length-1;
                }

                for (int k=0; k <= j; k++) {
                    WVASSERT(VxExec(string.Format(
                                    "INSERT INTO test1 VALUES ('{0}', {1})",
                                    lipsum_text.Substring(0,
                                        sizes[k]).Replace("'", "''"), k)));
                    /* This doesn't work because it truncates to 4000 chars
                     * regardless of if it's a nchar/nvarchar or plain
                     * char/varchar.
                    WVASSERT(Insert("test1",
                                new SqlString(
                                    lipsum_text.Substring(0, sizes[k])), k));
                                    */
                }

                WVASSERT(Insert("test1", DBNull.Value, j+1));

                VxColumnInfo[] colinfo;
                object[][] data;
                bool[][] nullity;

                if (lenok[i]) {
                    WVASSERT(VxRecordset("SELECT LEN(data), DATALENGTH(data), "
                                +" data FROM test1 ORDER BY roworder",
                                out colinfo, out data, out nullity));
                } else {
                    WVASSERT(VxRecordset("SELECT -1, "
                                + "DATALENGTH(data), data FROM test1 "
                                + "ORDER BY roworder",
                                out colinfo, out data, out nullity));
                }

                WVPASSEQ(data.Length, j+2);

                for (int k=0; k <= j; k++) {
                    if (lenok[i])
                        WVPASSEQ((int)data[k][0], sizes[k]);

                    WVPASSEQ((int)data[k][1],
                            sizes[varsize[i] ? k : j]*charsize[i]);
                    WVPASSEQ(((string)data[k][2]).Substring(0, sizes[k]),
                            lipsum_text.Substring(0, sizes[k]));
                }

                WVPASS(nullity[j+1][2]);

                WVASSERT(Exec("DROP TABLE test1"));
            }
        }
    }

    [Test, Category("Data")]
    public void VerifyDateTime()
    {
        // datetime, smalldatetime
        // Insert 7 rows: max, a date in the future, now, a date in the past,
        // datetime epoch, min, null
        //
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)
        //
        // The actual dates don't really matter, but:
        // - The date in the past is adewhurst's birthday (approximately, PST)
        // - The date in the future is 1 second past the signed 32-bit overflow
        //   of seconds since the unix epoch (UTC)
        // - The datetime epoch is January 1 1900 at midnight
        //
        // Other notes:
        // - The min/max values of SqlDateTime are supposed to correspond to the
        //   min/max values of the SQL Server datetime type, except Mono doesn't
        //   quite have the semantics right, so the min/max values are
        //   hard-coded in instead. Bug filed with Mono.
        // - All smalldatetime values are rounded down to the nearest minute,
        //   since it only has per-minute granularity
        
        SqlDateTime epoch = new SqlDateTime(0, 0);
        SqlDateTime smallMin = epoch;
        SqlDateTime smallMax = new SqlDateTime(2079, 6, 6, 23, 59, 0, 0);

        SqlDateTime dtMin = new SqlDateTime(1753, 1, 1, 0, 0, 0, 0);
        // This is wrong, but mono seems to have trouble with the fractional
        // parts.
        SqlDateTime dtMax = new SqlDateTime(9999, 12, 31, 23, 59, 59, 0);

        SqlDateTime pastDate = new SqlDateTime(1984, 12, 2, 3, 0, 0, 0);
        SqlDateTime pastDateSmall = new SqlDateTime(1984, 12, 2, 3, 0, 0, 0);
        SqlDateTime futureDate = new SqlDateTime(2038, 6, 19, 3, 14, 8, 0);
        SqlDateTime futureDateSmall = new SqlDateTime(2038, 6, 19, 3, 14, 0, 0);

        // Mono has difficulties converting DateTime to SqlDateTime directly, so
        // take it down to per-second precision, which works reliably
        // Bug filed with Mono.
        DateTime now = DateTime.Now;
        SqlDateTime sqlNow = new SqlDateTime(now.Year, now.Month, now.Day,
                now.Hour, now.Minute, now.Second);
        SqlDateTime sqlNowSmall = new SqlDateTime(now.Year, now.Month, now.Day,
                now.Hour, now.Minute, 0);

        WVASSERT(Exec("CREATE TABLE #test1 (dt datetime, sdt smalldatetime, "
                    + "roworder int not null)"));

        WVASSERT(Insert("#test1", dtMin, smallMin, 1));
        WVASSERT(Insert("#test1", epoch, epoch, 2));
        WVASSERT(Insert("#test1", pastDate, pastDateSmall, 3));
        WVASSERT(Insert("#test1", sqlNow, sqlNowSmall, 4));
        WVASSERT(Insert("#test1", futureDate, futureDateSmall, 5));
        WVASSERT(Insert("#test1", dtMax, smallMax, 6));
        WVASSERT(Insert("#test1", DBNull.Value, DBNull.Value, 7));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT dt, sdt FROM #test1 ORDER BY roworder",
                    out reader));

        using (reader) {
            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), dtMin);
            WVPASSEQ(reader.GetSqlDateTime(1), smallMin);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), epoch);
            WVPASSEQ(reader.GetSqlDateTime(1), epoch);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), pastDate);
            WVPASSEQ(reader.GetSqlDateTime(1), pastDateSmall);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), sqlNow);
            WVPASSEQ(reader.GetSqlDateTime(1), sqlNowSmall);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), futureDate);
            WVPASSEQ(reader.GetSqlDateTime(1), futureDateSmall);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), dtMax);
            WVPASSEQ(reader.GetSqlDateTime(1), smallMax);

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));
            WVPASS(reader.IsDBNull(1));

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Data")]
    public void VerifyDecimal()
    {
        // decimal(38,0), decimal(38,38), decimal(18,0), decimal(1,0),
        // decimal(1,1), numeric as same types
        // Insert 6 rows: max, something positive, 0, something negative, min,
        // nulls
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)

        Byte [,] sizes = {
            // {precision, scale}
            {38, 0},
            {38, 38},
            {18, 0},
            {1, 0},
            {1, 1}
        };

        // Construct all of the things we will insert
        // These are all strings because attempting to use the SqlDecimal class
        // just leads to no end of problems. Even Microsoft's .NET
        // implementation seems to have issues with the max/min value ones.
        object [,] values = {
            {
                "99999999999999999999999999999999999999",
                "0.99999999999999999999999999999999999999",
                "999999999999999999",
                "9",
                "0.9"
            }, {
                "123456",
                "0.12345600000000000000000000000000000000",
                "123456",
                "1",
                "0.1"
            }, {
                /*
                 * The "zero" data set actually makes Mono's TDS library croak.
                 * But that's not a Versaplex bug. The other data sets should
                 * give reasonable confidence in Versaplex anyway.
                 * Bug filed with Mono.
                "0",
                "0.00000000000000000000000000000000000000",
                "0",
                "0",
                "0.0"
            }, {
                */
                "-654321",
                "-0.65432100000000000000000000000000000000",
                "-654321",
                "-1",
                "-0.1"
            }, {
                "-99999999999999999999999999999999999999",
                "-0.99999999999999999999999999999999999999",
                "-999999999999999999",
                "-9",
                "-0.9"
            }, {
                DBNull.Value,
                DBNull.Value,
                DBNull.Value,
                DBNull.Value,
                DBNull.Value
            }
        };

        // Make sure that the data is specified correctly here
        WVPASSEQ(sizes.GetLength(0), values.GetLength(1));

        // Make the table we're going to create
        System.Text.StringBuilder schema = new System.Text.StringBuilder(
                "CREATE TABLE #test1 (");

        // Make one of each decimal and numeric column. These are in fact
        // identical, but since either may show up in real-world tables, testing
        // both is a good plan
        for (int i=0; i < sizes.GetLength(0); i++) {
            schema.AppendFormat("d{0}_{1} decimal({0},{1}), "
                    + "n{0}_{1} numeric({0},{1}), ", sizes[i,0], sizes[i,1]);
        }

        schema.Append("roworder int not null)");

        WVASSERT(Exec(schema.ToString()));

        // Now insert them
        object [] insertParams = new object[2*values.GetLength(1)+1];

        for (int i=0; i < values.GetLength(0); i++) {
            insertParams[insertParams.Length-1] = i;
            for (int j=0; j < insertParams.Length-1; j++) {
                insertParams[j] = values[i,j/2];
            }
            WVASSERT(Insert("#test1", insertParams));
        }

        SqlDataReader reader;
        WVASSERT(Reader("SELECT * FROM #test1 ORDER BY roworder",
                    out reader));

        using (reader) {
            for (int i=0; i < values.GetLength(0); i++) {
                WVASSERT(reader.Read());

                for (int j=0; j < insertParams.Length-1; j++) {
                    if (values[i,j/2] is DBNull) {
                        WVPASS(reader.IsDBNull(j));
                    } else {
                        // The preprocessor doesn't like the comma in the array
                        // subscripts
                        string val = (string)values[i,j/2];
                        string fromdb = reader.GetSqlDecimal(j).ToString();

                        // Mono produces ".1" and "-.1"
                        // Microsoft .NET produces "0.1" and "-0.1"
                        // Deal with that here.
                        // Bug filed with Mono.
                        if (val[0] == '0' && fromdb[0] == '.') {
                            WVPASSEQ(fromdb, val.Substring(1));
                        } else if (val[0] == '-' && val[1] == '0'
                                && fromdb[0] == '-' && fromdb[1] == '.') {
                            WVPASSEQ(fromdb, "-" + val.Substring(2));
                        } else {
                            WVPASSEQ(fromdb, val);
                        }
                    }
                }
            }

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Data")]
    public void VerifyFloat()
    {
        // float(53), float(24), real
        // Insert 8 rows: max, something positive, smallest positive, 0,
        // smallest negative, something negative, min, nulls
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)
        //
        // Specifically, infinity, -infinity and NaN are not tested here because
        // SQL Server appears to reject them as values for float columns

        // Construct all of the things we will insert
        object [,] values = {
            {
                /* Can't use SqlDouble.MaxValue et al. because there are
                 * rounding issues in Mono somewhere that make it reject the
                 * exact maximum value. These numbers come from the SQL Server
                 * 2005 reference for the float data type
                 * Bug filed with Mono.
                SqlDouble.MaxValue,
                SqlSingle.MaxValue,
                SqlSingle.MaxValue */
                1.79E+308d,
                3.40E+38f,
                3.40E+38f
            }, {
                /* Mono has problems with sending Math.E in a way that is
                 * roundtrip-able
                 * Bug filed with Mono.
                (double)Math.E,
                (float)Math.E,
                (float)Math.E */
                2.71828182845905d,
                2.718282f,
                2.718282f
            }, {
                /* Can't use Double.Epsilon or Single.Epsilon because SQL server
                 * complains, even on the Microsoft .NET implementation
                 * These numbers come from the SQL Server 2005 reference for the
                 * float data type
                Double.Epsilon,
                Single.Epsilon,
                Single.Epsilon */
                2.23E-308d,
                1.18E-38f,
                1.18E-38f
            }, {
                0.0d,
                0.0f,
                0.0f
            }, {
                /*
                -Double.Epsilon,
                -Single.Epsilon,
                -Single.Epsilon */
                -2.23E-308d,
                -1.18E-38f,
                -1.18E-38f
            }, {
                -127.001d,
                -1270.01f,
                -12700.1f
            }, {
                /*
                SqlDouble.MinValue,
                SqlSingle.MinValue,
                SqlSingle.MinValue */
                -1.79E+308d,
                -3.40E+38f,
                -3.40E+38f
            }, {
                DBNull.Value,
                DBNull.Value,
                DBNull.Value
            }
        };

        WVASSERT(Exec("CREATE TABLE #test1 (f53 float(53), f24 float(24), "
                    + "r real, roworder int not null)"));

        // Now insert them
        object [] insertParams = new object[values.GetLength(1)+1];

        for (int i=0; i < values.GetLength(0); i++) {
            insertParams[insertParams.Length-1] = i;
            for (int j=0; j < insertParams.Length-1; j++) {
                insertParams[j] = values[i,j];
            }
            WVASSERT(Insert("#test1", insertParams));
        }

        SqlDataReader reader;
        WVASSERT(Reader("SELECT * FROM #test1 ORDER BY roworder",
                    out reader));

        using (reader) {
            for (int i=0; i < values.GetLength(0); i++) {
                WVASSERT(reader.Read());

                for (int j=0; j < insertParams.Length-1; j++) {
                    // The preprocessor doesn't like the comma in the array
                    // subscripts
                    object val = values[i,j];

                    if (val is DBNull) {
                        WVPASS(reader.IsDBNull(j));
                    } else if (val is double) {
                        WVPASSEQ(reader.GetDouble(j), (double)val);
                    } else if (val is float) {
                        WVPASSEQ(reader.GetFloat(j), (float)val);
                    } else {
                        // If we get here, a data type was used in the values
                        // array that's not handled by one of the above cases
                        bool test_is_broken = true;
                        WVFAIL(test_is_broken);
                    }
                }
            }

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Data")]
    public void VerifyMoney()
    {
        // money, smallmoney
        // Insert 6 rows: max, a positive amount, 0, a negative amount, min,
        // null
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)

        WVASSERT(Exec("CREATE TABLE #test1 (m money, sm smallmoney, "
                    + "roworder int not null)"));

        WVASSERT(Insert("#test1", SqlMoney.MaxValue, 214748.3647m, 1));
        WVASSERT(Insert("#test1", 1337.42m, 1337.42m, 2));
        WVASSERT(Insert("#test1", 0.0m, 0.0m, 3));
        WVASSERT(Insert("#test1", -3.141m, -3.141m, 5));
        WVASSERT(Insert("#test1", SqlMoney.MinValue, -214748.3648m, 6));
        WVASSERT(Insert("#test1", DBNull.Value, DBNull.Value, 7));

        SqlDataReader reader;
        // Cast the return type because Mono doesn't properly handle negative
        // money amounts
        // Bug filed with Mono.
        WVASSERT(Reader("SELECT CAST(m as decimal(20,4)),"
                    + "CAST(sm as decimal(20,4)) "
                    + "FROM #test1 ORDER BY roworder", out reader));

        using (reader) {
            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetDecimal(0), SqlMoney.MaxValue.ToDecimal());
            WVPASSEQ(reader.GetDecimal(1), 214748.3647m);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetDecimal(0), 1337.42m);
            WVPASSEQ(reader.GetDecimal(1), 1337.42m);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetDecimal(0), 0m);
            WVPASSEQ(reader.GetDecimal(1), 0m);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetDecimal(0), -3.141m);
            WVPASSEQ(reader.GetDecimal(1), -3.141m);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetDecimal(0), SqlMoney.MinValue.ToDecimal());
            WVPASSEQ(reader.GetDecimal(1), -214748.3648m);

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));
            WVPASS(reader.IsDBNull(1));

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Data")]
    public void VerifyTimestamp()
    {
        // Create a table with a timestamp column, create a bunch of rows in a
        // particular order, then check that they match up after copying

        // This permutation strategy is discussed in the RowOrdering test
        const int numElems = 101;
        const int prime1 = 47;

        WVASSERT(Exec("CREATE TABLE #test1 (ts timestamp, "
                    + "roworder int not null)"));

        for (int i=0, j=0; i < numElems; i++, j = (i*prime1) % numElems) {
            Insert("#test1", DBNull.Value, j);
        }

        SqlDataReader reader;
        WVASSERT(Reader("SELECT ts,roworder FROM #test1 ORDER BY roworder",
                    out reader));
        
        SqlBinary [] tsdata = new SqlBinary[numElems];

        using (reader) {
            for (int i=0; i < numElems; i++) {
                WVASSERT(reader.Read());
                WVPASSEQ(reader.GetInt32(1), i);
                tsdata[i] = reader.GetSqlBinary(0);
            }

            WVFAIL(reader.Read());
        }

        WVASSERT(Reader("SELECT ts,roworder FROM #test1 ORDER BY ts",
                    out reader));

        using (reader) {
            for (int i=0, j=0; i < numElems; i++, j = (i*prime1) % numElems) {
                WVASSERT(reader.Read());
                WVPASSEQ(reader.GetInt32(1), j);
                WVPASSEQ(reader.GetSqlBinary(0), tsdata[j]);
            }

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Data")]
    public void VerifyUniqueIdentifier()
    {
        // uniqueidentifier
        // Insert 2 rows: a valid number, null
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)

        SqlGuid guid = new SqlGuid("6F9619FF-8B86-D011-B42D-00C04FC964FF");

        WVASSERT(Exec("CREATE TABLE #test1 (u uniqueidentifier, "
                    + "roworder int not null)"));

        WVASSERT(Insert("#test1", guid, 1));
        WVASSERT(Insert("#test1", DBNull.Value, 2));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT u FROM #test1 ORDER BY roworder",
                    out reader));

        using (reader) {
            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlGuid(0), guid);

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Data")]
    public void VerifyVarBinaryMax()
    {
        // varbinary(max), image

        // This must be sorted
        long [] sizes = { 1, 10, 50, 255, 4000, 8000, 8040, 8192, 16080, 16384,
            24120, 32160, 32768, 40200, 65536, 131072, 262144, 524288, 1048576,
            2097152, 3076506 };

        string [] types = { "varbinary(max)", "image" };

        Byte [] image_data = read_image();

        WVASSERT(image_data.Length >= sizes[sizes.Length-1]);

        foreach (string type in types) {
            WVASSERT(Exec(string.Format("CREATE TABLE #test1 "
                            + "(data {0}, roworder int not null)", type)));

            for (int k=0; k < sizes.Length; k++) {
                Byte [] data = new byte[sizes[k]];
                Array.Copy(image_data, data, sizes[k]);

                WVASSERT(Insert("#test1", new SqlBinary(data), k));
            }

            WVASSERT(Insert("#test1", DBNull.Value, sizes.Length));

            SqlDataReader reader;
            WVASSERT(Reader("SELECT DATALENGTH(data), data FROM "
                        + "#test1 ORDER BY roworder",
                        out reader));

            using (reader) {
                for (int k=0; k < sizes.Length; k++) {
                    Byte [] data = new byte[sizes[k]];
                    Array.Copy(image_data, data, sizes[k]);

                    WVASSERT(reader.Read());

                    WVPASSEQ(GetInt64(reader, 0), sizes[k]);
                    WVPASSEQ(reader.GetSqlBinary(1), new SqlBinary(data));
                }

                WVASSERT(reader.Read());
                WVPASS(reader.IsDBNull(1));

                WVFAIL(reader.Read());
            }

            WVASSERT(Exec("DROP TABLE #test1"));
        }
    }

    [Test, Category("Data")]
    public void VerifyXML()
    {
        // xml
        // Insert 2 rows: some sample XML, null
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)
        // This isn't very exhaustive, so improvements are welcome.
        // This was going to use SqlXml instead of using a string, but Mono
        // doesn't support that very well.

        // This MUST not have any extra whitespace, as it will be stripped by
        // some SQL parser and won't be reproduced when it comes back out.
        // This is the style that Microsoft's .NET returns
        string xml =
            "<outside><!--hi--><element1 />Text<element2 type=\"pretty\" />"
            + "</outside>";
        // This is the style that Mono returns
        string altxml =
            "<outside><!--hi--><element1/>Text<element2 type=\"pretty\"/>"
            + "</outside>";

        WVASSERT(Exec("CREATE TABLE #test1 (x xml, "
                    + "roworder int not null)"));

        WVASSERT(Insert("#test1", xml, 1));
        WVASSERT(Insert("#test1", DBNull.Value, 2));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT x FROM #test1 ORDER BY roworder",
                    out reader));

        using (reader) {
            WVASSERT(reader.Read());
            // Sigh. 
            if (reader.GetString(0) == altxml) {
                WVPASSEQ(reader.GetString(0), altxml);
            } else {
                WVPASSEQ(reader.GetString(0), xml);
            }

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Data")]
    public void Unicode()
    {
        // nchar, nvarchar (in-row or max), ntext
        // Using lots of non-ascii characters
        
        string unicode_text = read_unicode();

        int [] sizes = { 4000, unicode_text.Length };
        WVASSERT(unicode_text.Length >= sizes[0]);

        string [] types = { "nchar", "nvarchar", "ntext", "nvarchar(max)" };
        int [] typemax = { 4000, 4000, Int32.MaxValue/2, Int32.MaxValue/2 };
        int [] charsize = { 2, 2, 2, 2 };
        bool [] varsize = { false, true, true, true };
        bool [] sizeparam = { true, true, false, false };
        bool [] lenok = { true, true, false, true };

        for (int i=0; i < types.Length; i++) {
            for (int j=0; j < sizes.Length && sizes[j] <= typemax[i]; j++) {
                if (sizeparam[i]) {
                    WVASSERT(Exec(string.Format("CREATE TABLE #test1 "
                                    + "(data {0}({1}), roworder int not null)",
                                    types[i], sizes[j])));
                } else {
                    WVASSERT(Exec(string.Format("CREATE TABLE #test1 "
                                    + "(data {0}, roworder int not null)",
                                    types[i])));
                    j = sizes.Length-1;
                }

                for (int k=0; k <= j; k++) {
                    WVASSERT(Exec(string.Format(
                                    "INSERT INTO #test1 VALUES (N'{0}', {1})",
                                    unicode_text.Substring(0,
                                        sizes[k]).Replace("'", "''"), k)));
                }

                SqlDataReader reader;

                if (lenok[i]) {
                    WVASSERT(Reader("SELECT LEN(data), DATALENGTH(data), data "
                                + "FROM #test1 ORDER BY roworder",
                                out reader));
                } else {
                    WVASSERT(Reader("SELECT -1, "
                                + "DATALENGTH(data), data FROM #test1 "
                                + "ORDER BY roworder",
                                out reader));
                }

                using (reader) {
                    for (int k=0; k <= j; k++) {
                        WVASSERT(reader.Read());

                        if (lenok[i])
                            WVPASSEQ(GetInt64(reader, 0), sizes[k]);

                        WVPASSEQ(GetInt64(reader, 1),
                                sizes[varsize[i] ? k : j]*charsize[i]);
                        WVPASSEQ(reader.GetString(2).Substring(0, sizes[k]),
                                unicode_text.Substring(0, sizes[k]));
                    }

                    WVFAIL(reader.Read());
                }

                WVASSERT(Exec("DROP TABLE #test1"));
            }
        }
    }

    public static void Main()
    {
        VersaplexTest tests = new VersaplexTest();
        WvTest tester = new WvTest();

        tester.RegisterTest("EmptyTable", tests.EmptyTable);
        tester.RegisterTest("NonexistantTable", tests.NonexistantTable);
        tester.RegisterTest("ColumnTypes", tests.ColumnTypes);
        tester.RegisterTest("EmptyColumnName", tests.EmptyColumnName);
        tester.RegisterTest("RowOrdering", tests.RowOrdering);
        tester.RegisterTest("ColumnOrdering", tests.ColumnOrdering);
        tester.RegisterTest("VerifyIntegers", tests.VerifyIntegers);
        tester.RegisterTest("VerifyBinary", tests.VerifyBinary);
        tester.RegisterTest("VerifyBit", tests.VerifyBit);
        tester.RegisterTest("VerifyChar", tests.VerifyChar);
        tester.RegisterTest("VerifyDateTime", tests.VerifyDateTime);
        tester.RegisterTest("VerifyDecimal", tests.VerifyDecimal);
        tester.RegisterTest("VerifyFloat", tests.VerifyFloat);
        tester.RegisterTest("VerifyMoney", tests.VerifyMoney);
        tester.RegisterTest("VerifyTimestamp", tests.VerifyTimestamp);
        tester.RegisterTest("VerifyUniqueIdentifier",
                tests.VerifyUniqueIdentifier);
        tester.RegisterTest("VerifyVarBinaryMax", tests.VerifyVarBinaryMax);
        tester.RegisterTest("VerifyXML", tests.VerifyXML);
        tester.RegisterTest("Unicode", tests.Unicode);

        tester.RegisterInit(tests.init);
        tester.RegisterCleanup(tests.cleanup);

        tester.Run();

        Environment.Exit(tester.Failures > 0 ? 1 : 0);
    }
}

}

