#include "wvtest.cs.h"

using System;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Wv;
using Wv.Test;
using Wv.Extensions;

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
    protected Connection bus;

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
        bus = Connection.session_bus;
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

    internal WvAutoCast Scalar(string query)
    {
	Console.WriteLine(" + Scalar SQL Query: {0}", query);
        return dbi.select_one(query);
    }

    internal WvSqlRows Reader(string query)
    {
	Console.WriteLine(" + Reader SQL Query: {0}", query);
	return dbi.select(query);
    }
    
    Message methodcall(string method, string signature)
    {
        return new MethodCall("vx.versaplexd", "/db", 
			      "vx.db", method, signature);
    }

    internal bool VxExec(string query)
    {
	Console.WriteLine(" + VxExec SQL Query: {0}", query);

        Message call = methodcall("ExecRecordset", "s");

        MessageWriter mw = new MessageWriter();
        mw.Write(query);

        call.Body = mw.ToArray();

        Message reply = bus.send_and_wait(call);
	reply.check("a(issnny)vaay");
	return true;
    }

    internal bool VxScalar(string query, out object result)
    {
	Console.WriteLine(" + VxScalar SQL Query: {0}", query);

        Message call = methodcall("ExecScalar", "s");

        MessageWriter mw = new MessageWriter();
        mw.Write(query);

        call.Body = mw.ToArray();

        Message reply = bus.send_and_wait(call);
	reply.check("v");
	result = reply.iter().pop().inner;
	return true;
    }

    // Read the standard issnny signature for column information.  We can't
    // just read a VxColumnInfo[] straight from the reader any more, as the
    // format of VxColumnInfo differs from the format on the wire.
    internal VxColumnInfo[] ReadColInfo(IEnumerable<WvAutoCast> rows)
    {
        var colinfolist = new List<VxColumnInfo>();
	
	foreach (var _r in rows)
	{
	    var r = _r.GetEnumerator();
	    int size           = r.pop();
	    string colname     = r.pop();
	    string coltype_str = r.pop();
	    short precision    = r.pop();
	    short scale        = r.pop();
	    byte nullable      = r.pop();

            VxColumnType coltype = (VxColumnType)Enum.Parse(
                typeof(VxColumnType), coltype_str, true);

            Console.WriteLine("Read colname={0}, coltype={1}, nullable={2}, " + 
                "size={3}, precision={4}, scale={5}", 
                colname, coltype.ToString(), nullable, size, precision, scale);

            colinfolist.Add(new VxColumnInfo(colname, coltype, nullable > 0,
                size, precision, scale));
	}

        return colinfolist.ToArray();
    }

    internal bool VxChunkRecordset(string query, out VxColumnInfo[] colinfo,
				    out object[][]data, out bool[][] nullity)
    {
	Console.WriteLine(" + VxChunkRecordset SQL Query: {0}", query);

	Message call = methodcall("ExecChunkRecordset", "s");

	MessageWriter mw = new MessageWriter();
	mw.Write(query);

	call.Body = mw.ToArray();

	bus.send(call);

	colinfo = null;
	List<object[]> rowlist = new List<object[]>();
	List<bool[]> rownulllist = new List<bool[]>();
	while (true)
	{
	    object[][] tdata;
	    bool[][] tnullity;
	    Message tmp = bus.readmessage(-1);
	    if (tmp.type == MessageType.Signal)
	    {
		tmp.check("a(issnny)vaayu");
		RecordsetWorker(tmp, out colinfo, out tdata, out tnullity);
		rowlist.AddRange(tdata);
		rownulllist.AddRange(tnullity);
	    }
	    else
	    {
	    	//Method return
		tmp.check("a(issnny)vaay");
		RecordsetWorker(tmp, out colinfo, out tdata, out tnullity);
		rowlist.AddRange(tdata);
		rownulllist.AddRange(tnullity);
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
        if (reply.signature.e())
	    throw new Exception("D-Bus reply had no signature");

        if (!reply.signature.StartsWith("a(issnny)vaay"))
            throw new Exception("D-Bus reply had invalid signature");

	var it = reply.iter();

        // Read the column information
        VxColumnInfo[] colinfo = _colinfo = ReadColInfo(it.pop());

        // TODO: Check that variant sig matches colinfo
        // Sig should be of the form a(...)

        var results = new List<object[]>();
	foreach (var _r in it.pop())
	{
	    var r = _r.GetEnumerator();
            object[] row = new object[colinfo.Length];

            for (int i=0; i < row.Length; i++)
	    {
                switch (colinfo[i].VxColumnType)
		{
                case VxColumnType.Uuid:
		    string cell = r.pop();
                    if (cell == "")
                        row[i] = new Guid();
		    else
			row[i] = new Guid(cell);
                    break;
                case VxColumnType.DateTime:
		    var xit = r.pop().GetEnumerator();
		    long seconds     = xit.pop();
		    int microseconds = xit.pop();

                    VxDbusDateTime dt = new VxDbusDateTime();
                    dt.seconds = seconds;
                    dt.microseconds = microseconds;

                    row[i] = dt;
		    break;
		case VxColumnType.Decimal:
		    string dcell = r.pop();
                    if (dcell == "")
                        row[i] = new Decimal();
		    else
                        row[i] = Decimal.Parse(dcell);
                    break;
                default:
		    row[i] = r.pop().inner;
		    break;
		}
	    }
	    results.Add(row);
	}

        data = results.ToArray();
	nullity =
	    (from rr in it.pop() 
	     select (from c in (IEnumerable<WvAutoCast>)rr
		     select (bool)(c!=0)).ToArray()).ToArray();
	return true;
    }

    internal bool VxRecordset(string query, out VxColumnInfo[] colinfo,
            out object[][] data, out bool[][] nullity)
    {
	Console.WriteLine(" + VxReader SQL Query: {0}", query);

        Message call = methodcall("ExecRecordset", "s");

        MessageWriter mw = new MessageWriter();
        mw.Write(query);

        call.Body = mw.ToArray();

        Message reply = bus.send_and_wait(call);
	reply.check("a(issnny)vaay");
	return RecordsetWorker(reply, out colinfo, out data, out nullity);
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

