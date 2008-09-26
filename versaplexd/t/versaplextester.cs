#include "wvtest.cs.h"

using System;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Wv;
using Wv.Test;

// Several mono bugs worked around in this test fixture are filed as mono bug
// #81940

public class VersaplexTester: IDisposable
{
    // A file full of "lorem ipsum dolor" text
    private const string lipsum_file = "lipsum.txt";
    // A UTF-8 test file
    private const string unicode_file = "UTF-8-demo.txt";
    // A random file of binary goop
    private const string goop_file = "random.bin";
    // THTBACS image
    private const string image_file = "thtbacs.tiff";

    public WvDbi dbi;
    protected Bus bus;

    public VersaplexTester()
    {
        // Places to look for the config file.
        string [] searchfiles = 
        { 
            "versaplexd.ini", 
            Path.Combine("..", "versaplexd.ini") 
        };

        string cfgfile = null;
        foreach (string searchfile in searchfiles)
            if (File.Exists(searchfile))
                cfgfile = searchfile;

        if (cfgfile == null)
            throw new Exception("Cannot locate versaplexd.ini.");

        WvIni cfg = new WvIni(cfgfile);
            
        string uname = Mono.Unix.UnixUserInfo.GetRealUser().UserName;
        string dbname = cfg.get("User Map", uname);
	if (dbname == null)
	    dbname = cfg.get("User Map", "*");
        if (dbname == null)
            throw new Exception(String.Format(
                "User '{0}' (and '*') missing from config.", uname));

        string cfgval = cfg.get("Connections", dbname);
        if (cfgval == null)
            throw new Exception(String.Format(
                "Connection string for '{0}' missing from config.", dbname));

	dbi = WvDbi.create(cfgval);

        if (Address.Session == null)
            throw new Exception ("DBUS_SESSION_BUS_ADDRESS not set");
        AddressEntry aent = AddressEntry.Parse(Address.Session);
        DodgyTransport trans = new DodgyTransport();
        trans.Open(aent);
        bus = new Bus(trans);
    }

    public void Dispose()
    {
        bus = null;
	dbi.Dispose();
    }

    public static string GetTempDir()
    {
        WvLog log = new WvLog("GetTempDir");
        string t = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        log.print("Using temporary directory " + t + "\n");

        return t;
    }

    internal bool Exec(string query)
    {
	Console.WriteLine(" + Exec SQL Query: {0}", query);
	dbi.exec(query);
	return true;
    }

    internal bool Scalar(string query, out object result)
    {
	Console.WriteLine(" + Scalar SQL Query: {0}", query);
	result = dbi.select_one(query).inner;
	return true;
    }

    internal WvSqlRows Reader(string query)
    {
	Console.WriteLine(" + Reader SQL Query: {0}", query);
	return dbi.select(query);
    }

    internal bool VxExec(string query)
    {
	Console.WriteLine(" + VxExec SQL Query: {0}", query);

        Message call = VxDbusUtils.CreateMethodCall(bus, "ExecRecordset", "s");

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

            string errmsg = mr.ReadString();

            throw new DbusError(errname.ToString() + ": " + errmsg.ToString());
        }
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    internal bool VxScalar(string query, out object result)
    {
	Console.WriteLine(" + VxScalar SQL Query: {0}", query);

        Message call = VxDbusUtils.CreateMethodCall(bus, "ExecScalar", "s");

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
            result = reader.ReadVariant();

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

            object errmsg = mr.ReadString();

            throw new DbusError(errname.ToString() + ": " + errmsg.ToString());
        }
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    // Read the standard issnny signature for column information.  We can't
    // just read a VxColumnInfo[] straight from the reader any more, as the
    // format of VxColumnInfo differs from the format on the wire.
    internal VxColumnInfo[] ReadColInfo(MessageReader reader)
    {
        List<VxColumnInfo> colinfolist = new List<VxColumnInfo>();

	reader.ReadArrayFunc(8, (r) => {
	    int size = reader.ReadInt32();
	    string colname = reader.ReadString();
	    string coltype_str = reader.ReadString();
	    short precision = reader.ReadInt16();
	    short scale = reader.ReadInt16();
	    byte nullable = reader.ReadByte();

            VxColumnType coltype = (VxColumnType)Enum.Parse(
                typeof(VxColumnType), coltype_str, true);

            Console.WriteLine("Read colname={0}, coltype={1}, nullable={2}, " + 
                "size={3}, precision={4}, scale={5}", 
                colname, coltype.ToString(), nullable, size, precision, scale);

            colinfolist.Add(new VxColumnInfo(colname, coltype, nullable > 0,
                size, precision, scale));
	});

        return colinfolist.ToArray();
    }

    internal bool VxChunkRecordset(string query, out VxColumnInfo[] colinfo,
				    out object[][]data, out bool[][] nullity)
    {
	Console.WriteLine(" + VxChunkRecordset SQL Query: {0}", query);

	Message call = VxDbusUtils.CreateMethodCall(bus, "ExecChunkRecordset", "s");
	//call.Header.Flags = HeaderFlag.NoReplyExpected | HeaderFlag.NoAutoStart;

	MessageWriter mw = new MessageWriter(Connection.NativeEndianness);
	mw.Write(typeof(string), query);

	call.Body = mw.ToArray();

	bus.Send(call);

	colinfo = null;
	List<object[]> rowlist = new List<object[]>();
	List<bool[]> rownulllist = new List<bool[]>();
	while (true)
	{
	    object[][] tdata;
	    bool[][] tnullity;
	    Message tmp = bus.GetNext();
	    if (tmp.Header.MessageType == MessageType.Signal)
	    {
		RecordsetWorker(tmp, out colinfo, out tdata, out tnullity);
		rowlist.AddRange(tdata);
		rownulllist.AddRange(tnullity);
	    }
	    else if (tmp.Header.MessageType == MessageType.Error)
	    {
		object errname;
		if (!tmp.Header.Fields.TryGetValue(FieldCode.ErrorName,
                        out errname))
		    throw new Exception("D-Bus error received but no error "
			+ "name given");

		object errsig;
		if (!tmp.Header.Fields.TryGetValue(FieldCode.Signature,
                        out errsig) || errsig.ToString() != "s")
		    throw new DbusError(errname.ToString());

		MessageReader mr = new MessageReader(tmp);

		string errmsg = mr.ReadString();
		throw new DbusError(errname.ToString() + ": " + errmsg);
	    }
	    else
	    {
	    	//Method return
		object retsig;
		if (!tmp.Header.Fields.TryGetValue(FieldCode.Signature,
		    out retsig) || retsig.ToString() != "s")
		    throw new DbusError("Garbled response for ExecChunkRecordSet");
		//otherwise, we presume it's our method return response
		data = rowlist.ToArray();
		nullity = rownulllist.ToArray();
		break;
	    }
	}

	return true;
    }

    internal bool RecordsetWorker(Message reply, out VxColumnInfo[] _colinfo,
				    out object[][] data, out bool[][] nullity)
    {
        object replysig;
        if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                    out replysig))
            throw new Exception("D-Bus reply had no signature");

        if (replysig == null || !replysig.ToString().StartsWith("a(issnny)vaay"))
            throw new Exception("D-Bus reply had invalid signature");

        MessageReader reader = new MessageReader(reply);

        // Read the column information
        VxColumnInfo[] colinfo = _colinfo = ReadColInfo(reader);

	reader.ReadSignature();

        // TODO: Check that sig matches colinfo
        // Sig should be of the form a(...)

        List<object[]> results = new List<object[]>();
	reader.ReadArrayFunc(8, (r) => {
            object[] row = new object[colinfo.Length];

            for (int i=0; i < row.Length; i++) {
                switch (colinfo[i].VxColumnType) {
                case VxColumnType.Int64:
                {
		    long cell = r.ReadInt64();
                    row[i] = cell;
                    break;
                }
                case VxColumnType.Int32:
                {
		    int cell = r.ReadInt32();
                    row[i] = cell;
                    break;
                }
                case VxColumnType.Int16:
                {
		    short cell = r.ReadInt16();
                    row[i] = cell;
                    break;
                }
                case VxColumnType.UInt8:
                {
		    byte cell = r.ReadByte();
                    row[i] = cell;
                    break;
                }
                case VxColumnType.Bool:
                {
		    bool cell = r.ReadBoolean();
                    row[i] = cell;
                    break;
                }
                case VxColumnType.Double:
                {
		    double cell = r.ReadDouble();
                    row[i] = cell;
                    break;
                }
                case VxColumnType.Uuid:
                {
		    string cell = r.ReadString();

                    if (cell == "") {
                        row[i] = new Guid();
                    } else {
			row[i] = new Guid(cell);
                    }
                    break;
                }
                case VxColumnType.Binary:
                {
		    object cell = r.ReadArray<byte>();
                    row[i] = cell;
                    break;
                }
                case VxColumnType.String:
                {
		    string cell = r.ReadString();
                    row[i] = cell;
                    break;
                }
                case VxColumnType.DateTime:
                {
		    long seconds = r.ReadInt64();
		    int microseconds = r.ReadInt32();

                    VxDbusDateTime dt = new VxDbusDateTime();
                    dt.seconds = seconds;
                    dt.microseconds = microseconds;

                    row[i] = dt;
		    break;
                }
                case VxColumnType.Decimal:
                {
		    string cell = r.ReadString();

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
	});

        data = results.ToArray();

	object rawnulls = reader.ReadArray<byte[]>();

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

    internal bool VxRecordset(string query, out VxColumnInfo[] colinfo,
            out object[][] data, out bool[][] nullity)
    {
	Console.WriteLine(" + VxReader SQL Query: {0}", query);

        Message call = VxDbusUtils.CreateMethodCall(bus, "ExecRecordset", "s");

        MessageWriter mw = new MessageWriter(Connection.NativeEndianness);
        mw.Write(typeof(string), query);

        call.Body = mw.ToArray();

        Message reply = bus.SendWithReplyAndBlock(call);

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
	    return RecordsetWorker(reply, out colinfo, out data, out nullity);
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

            string errmsg = mr.ReadString();

            throw new DbusError(errname.ToString() + ": " + errmsg.ToString());
        }
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    internal bool Insert(string table, params object [] param)
    {
        Console.WriteLine(" + Insert to {0} ({1})", table, String.Join(", ",
                    wv.stringify(param)));

        var query = new StringBuilder();
        query.AppendFormat("INSERT INTO [{0}] VALUES (",
                table.Replace("]","]]"));
	
	for (int i=0; i < param.Length; i++)
	{
	    if (i > 0)
		query.Append(", ");
	    
	    if (param[i] is DBNull)
		query.Append("NULL");
	    else
		query.AppendFormat("@col{0}", i);
	}
	
	query.Append(")");
	
	Console.WriteLine(" ++ ({0})", query.ToString());
	
	dbi.exec(query.ToString(), param);
        return true;
    }

    internal string read_lipsum()
    {
        WVASSERT(File.Exists(lipsum_file));

        using (StreamReader sr = new StreamReader(lipsum_file)) {
            return sr.ReadToEnd();
        }
    }

    internal string read_unicode()
    {
        WVASSERT(File.Exists(unicode_file));

        using (StreamReader sr = new StreamReader(unicode_file)) {
            return sr.ReadToEnd();
        }
    }

    internal Byte [] read_goop()
    {
        WVASSERT(File.Exists(goop_file));

        using (FileStream f = new FileStream(goop_file, FileMode.Open,
                    FileAccess.Read))
        using (BinaryReader sr = new BinaryReader(f)) {
            return sr.ReadBytes((int)Math.Min(f.Length, Int32.MaxValue));
        }
    }

    internal Byte [] read_image()
    {
        WVASSERT(File.Exists(image_file));

        using (FileStream f = new FileStream(image_file, FileMode.Open,
                    FileAccess.Read))
        using (BinaryReader sr = new BinaryReader(f)) {
            return sr.ReadBytes((int)Math.Min(f.Length, Int32.MaxValue));
        }
    }

    internal long GetInt64(SqlDataReader reader, int colnum) {
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
}

