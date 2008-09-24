using System;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Collections.Generic;
using NDesk.DBus;
using Wv;
using Wv.Extensions;

internal static class VxDb {
    static WvLog log = new WvLog("VxDb", WvLog.L.Debug2);

    internal static void ExecScalar(string connid, string query, 
        out object result)
    {
        log.print(WvLog.L.Debug3, "ExecScalar {0}\n", query);

	try
	{
	    using (var dbi = VxSqlPool.create(connid))
		result = dbi.select_one(query).inner;
        }
	catch (DbException e)
	{
            throw new VxSqlException(e.Message, e);
        }
    }


    internal static void SendChunkRecordSignal(Message call, object sender,
					    VxColumnInfo[] colinfo,
					    object[][] data, byte[][] nulls)
    {
	MessageWriter writer =
	    VxDbInterfaceRouter.PrepareRecordsetWriter(colinfo, data, nulls);
	writer.Write(typeof(uint), call.Header.Serial);

	Message signal = VxDbus.CreateSignal(sender, "ChunkRecordsetSig",
				   	"a(issnny)vaayu",
					writer);
		    
	// For debugging
	VxDbus.MessageDump(" S>> ", signal);

	call.Connection.Send(signal);
    }


    internal static void ExecChunkRecordset(Message call, out Message reply)
    {
	string connid = VxDbInterfaceRouter.GetClientId(call);
	
        if (connid == null)
        {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.Failed",
                    "Could not identify the client", call);
            return;
        }
        
	MessageReader reader = new MessageReader(call);

        string query = reader.ReadString();
	string iquery = query.ToLower().Trim();
	reply = null;
        // XXX this is fishy, really... whitespace fucks it up.

        if (iquery.StartsWith("list tables"))
            query = "exec sp_tables";
        else if (iquery.StartsWith("list columns "))
            query = String.Format("exec sp_columns @table_name='{0}'",
                    query.Substring(13));
        else if (iquery.StartsWith("list all table") &&
                iquery.StartsWith("list all tablefunction") == false)
            query = "select distinct cast(Name as varchar(max)) Name"
                + " from sysobjects "
                + " where objectproperty(id,'IsTable')=1 "
                + " and xtype='U' "
                + " order by Name ";
        else if (iquery.StartsWith("list all"))
            // Format: list all {view|trigger|procedure|scalarfunction|tablefunction}
            // Returns: a list of all of whatever
            query = String.Format(
                    "select distinct "
                    + " cast (object_name(id) as varchar(256)) Name "
                    + " from syscomments "
                    + " where objectproperty(id,'Is{0}') = 1 "
                    + " order by Name ",
                    query.Split(' ')[2].Trim());
        else if (iquery.StartsWith("get object"))
            // Format: 
            // get object {view|trigger|procedure|scalarfunction|tablefunction} name
            // Returns: the "source code" to the object
            query = String.Format(
                    "select cast(text as varchar(max)) text "
                    + "from syscomments "
                    + "where objectproperty(id, 'Is{0}') = 1 "
                    + "and object_name(id) = '{1}' "
                    + "order by number, colid ",
                    query.Split(' ')[2].Trim(), 
                    query.Split(' ')[3].Trim());
	else if (iquery.StartsWith("drop") || iquery.StartsWith("create") ||
		 iquery.StartsWith("insert") || iquery.StartsWith("update"))
	{
	    //FIXME:  Are the above the only ways to modify a DB, aka the only
	    //        cases where we have to call the old ExecRecordSet?
	    //FIXME:  This is an ugly way to handle these cases, but it works!
	    VxDbInterfaceRouter.CallExecRecordset(call, out reply);
	    return;
	}

        log.print(WvLog.L.Debug3, "ExecChunkRecordset {0}\n", query);

	object sender;
        if (!call.Header.Fields.TryGetValue(FieldCode.Sender, out sender))
	    sender = null;

        try
	{
	    // FIXME:  Sadly, this is stupidly similar to ExecRecordset.
	    // Anything we can do here to identify commonalities?
            using (var dbi = VxSqlPool.create(connid))
            using (WvSqlRows resultset = dbi.select(query))
	    {
		List<object[]> rows = new List<object[]>();
		List<byte[]> rownulls = new List<byte[]>();
		
		// Our size here is just an approximation.  Start it at 1
		// to ensure that at least one packet always gets sent.
		int cursize = 1;  
		
		var columns = resultset.columns.ToArray();
		VxColumnInfo[] colinfo  = ProcessSchema(columns);
		int ncols = columns.Count();
		
		foreach (WvSqlRow cur_row in resultset)
		{
		    object[] row = new object[ncols];
		    byte[] rownull = new byte[ncols];
		    cursize += rownull.Length;
		    
		    for (int i = 0; i < ncols; i++) 
		    {
			WvAutoCast cval = cur_row[i];
			bool isnull = cval.IsNull;
			
			row[i] = null;
			
			rownull[i] = isnull ? (byte)1 : (byte)0;
			
			switch (colinfo[i].VxColumnType) 
			{
                            case VxColumnType.Int64:
                                row[i] = !isnull ?
                                    (Int64)cval : new Int64();
				cursize += sizeof(Int64);
                                break;
                            case VxColumnType.Int32:
                                row[i] = !isnull ?
                                    (Int32)cval : new Int32();
				cursize += sizeof(Int32);
                                break;
                            case VxColumnType.Int16:
                                row[i] = !isnull ?
                                    (Int16)cval : new Int16();
				cursize += sizeof(Int16);
                                break;
                            case VxColumnType.UInt8:
                                row[i] = !isnull ?
                                    (Byte)cval : new Byte();
				cursize += sizeof(Byte);
                                break;
                            case VxColumnType.Bool:
                                row[i] = !isnull ?
                                    (bool)cval : new Boolean();
				cursize += sizeof(Boolean);
                                break;
                            case VxColumnType.Double:
                                // Might return a Single or Double
                                // FIXME: Check if getting a single causes this
                                // to croak
                                row[i] = !isnull ?
                                    (double)cval : (double)0.0;
				cursize += sizeof(double);
                                break;
                            case VxColumnType.Uuid:
				//FIXME:  Do I work?
                                row[i] = !isnull ?
                                    (string)cval : "";
				cursize += !isnull ?
				    ((string)cval).Length * sizeof(Char) : 0;
                                break;
                            case VxColumnType.Binary:
                                {
                                    if (isnull) 
                                    {
                                        row[i] = new byte[0];
                                        break;
                                    }

				    row[i] = (byte[])cval;
				    cursize += ((byte[])cval).Length;
                                    break;
                                }
                            case VxColumnType.String:
                                row[i] = !isnull ? (string)cval : "";
				cursize += !isnull ?
				    ((string)cval).Length * sizeof(Char) : 0;
                                break;
                            case VxColumnType.DateTime:
                                row[i] = !isnull ?
                                    new VxDbusDateTime((DateTime)cval) :
                                    new VxDbusDateTime();
				cursize += System.Runtime.InteropServices.Marshal.SizeOf((VxDbusDateTime)row[i]);
                                break;
                            case VxColumnType.Decimal:
                                row[i] = !isnull ?
                                    ((Decimal)cval).ToString() : "";
				cursize += ((string)(row[i])).Length *
					    sizeof(Char);
                                break;
			}
		    } // column iterator
		    
		    rows.Add(row);
		    rownulls.Add(rownull);
		    
		    if (cursize >= 1024*1024) //approx 1 MB
		    {
			log.print(WvLog.L.Debug4,
				  "(1 MB reached; {0} rows)\n",
				  rows.Count);
			
			SendChunkRecordSignal(call, sender, colinfo,
					      rows.ToArray(),
					      rownulls.ToArray());
			
			rows = new List<object[]>();
			rownulls = new List<byte[]>();
			cursize = 0;
		    }
		} // row iterator
		
		if (cursize > 0)
		{
		    log.print(WvLog.L.Debug4, "(Remaining data; {0} rows)\n",
			      rows.Count);
		    
		    SendChunkRecordSignal(call, sender, colinfo,
					  rows.ToArray(),
					  rownulls.ToArray());
		}
	    } // using
	    MessageWriter replywriter =
	    	new MessageWriter(Connection.NativeEndianness);
	    replywriter.Write(typeof(string), "ChunkRecordset sent you all your data!");
	    reply = VxDbus.CreateReply(call, "s", replywriter);
        } catch (DbException e) {
            throw new VxSqlException(e.Message, e);
        }
    }

    internal static void ExecRecordset(string connid, string query,
            out VxColumnInfo[] colinfo, out object[][] data,
            out byte[][] nullity)
    {
        // XXX this is fishy

        if (query.ToLower().StartsWith("list tables"))
            query = "exec sp_tables";
        else if (query.ToLower().StartsWith("list columns "))
            query = String.Format("exec sp_columns @table_name='{0}'",
                    query.Substring(13));
        else if (query.ToLower().StartsWith("list all table") &&
                query.ToLower().StartsWith("list all tablefunction") == false)
            query = "select distinct cast(Name as varchar(max)) Name"
                + " from sysobjects "
                + " where objectproperty(id,'IsTable')=1 "
                + " and xtype='U' "
                + " order by Name ";
        else if (query.ToLower().StartsWith("list all"))
            // Format: list all {view|trigger|procedure|scalarfunction|tablefunction}
            // Returns: a list of all of whatever
            query = String.Format(
                    "select distinct "
                    + " cast (object_name(id) as varchar(256)) Name "
                    + " from syscomments "
                    + " where objectproperty(id,'Is{0}') = 1 "
                    + " order by Name ",
                    query.Split(' ')[2].Trim());
        else if (query.ToLower().StartsWith("get object"))
            // Format: 
            // get object {view|trigger|procedure|scalarfunction|tablefunction} name
            // Returns: the "source code" to the object
            query = String.Format(
                    "select cast(text as varchar(max)) text "
                    + "from syscomments "
                    + "where objectproperty(id, 'Is{0}') = 1 "
                    + "and object_name(id) = '{1}' "
                    + "order by number, colid ",
                    query.Split(' ')[2].Trim(), 
                    query.Split(' ')[3].Trim());

        log.print(WvLog.L.Debug3, "ExecRecordset {0}\n", query);

        try {
            List<object[]> rows = new List<object[]>();
            List<byte[]> rownulls = new List<byte[]>();

	    using (var dbi = VxSqlPool.create(connid))
            using (var result = dbi.select(query))
            {
		var columns = result.columns.ToArray();
		
                if (columns.Length <= 0) 
                    log.print("No columns in resulting data set.");

                colinfo = ProcessSchema(result.columns);

                foreach (var r in result)
                {
                    object[] row = new object[columns.Length];
                    byte[] rownull = new byte[columns.Length];

                    for (int i = 0; i < columns.Length; i++) 
                    {
                        bool isnull = r[i].IsNull;
                        row[i] = null;
                        rownull[i] = isnull ? (byte)1 : (byte)0;

                        switch (colinfo[i].VxColumnType) 
                        {
                            case VxColumnType.Int64:
                                row[i] = !isnull ? (Int64)r[i] : new Int64();
                                break;
                            case VxColumnType.Int32:
			        row[i] = !isnull ? (Int32)r[i] : new Int32();
                                break;
                            case VxColumnType.Int16:
                                row[i] = !isnull ? (Int16)r[i] : new Int16();
                                break;
                            case VxColumnType.UInt8:
                                row[i] = !isnull ? (Byte)r[i] : new Byte();
                                break;
                            case VxColumnType.Bool:
                                row[i] = !isnull ? (bool)r[i] : new Boolean();
                                break;
                            case VxColumnType.Double:
                                // Might return a Single or Double
                                // FIXME: Check if getting a single causes this
                                // to croak
                                row[i] = !isnull ? (double)r[i] : (double)0.0;
                                break;
                            case VxColumnType.Uuid:
                                row[i] = !isnull ? ((Guid)r[i]).ToString() : "";
                                break;
                            case VxColumnType.Binary:
			        row[i] = !isnull ? (byte[])r[i] : new byte[0];
			        break;
                            case VxColumnType.String:
                                row[i] = !isnull ? (string)r[i] : "";
                                break;
                            case VxColumnType.DateTime:
                                row[i] = !isnull ?
                                    new VxDbusDateTime(r[i]) :
                                    new VxDbusDateTime();
                                break;
                            case VxColumnType.Decimal:
                                row[i] = !isnull 
			             ? ((decimal)r[i]).ToString() : "";
                                break;
                        }
                    }

                    rows.Add(row);
                    rownulls.Add(rownull);
                }

                data = rows.ToArray();
                nullity = rownulls.ToArray();
                log.print(WvLog.L.Debug4, "({0} rows)\n", data.Length);
                wv.assert(nullity.Length == data.Length);
            }
        } catch (DbException e) {
            throw new VxSqlException(e.Message, e);
        }
    }

    private static VxColumnInfo[] ProcessSchema(IEnumerable<WvColInfo> columns)
    {
	// FIXME:  This is stupidly similar to ProcessSchema
	int ncols = columns.Count();
        VxColumnInfo[] colinfo = new VxColumnInfo[ncols];

        if (ncols <= 0) 
            return colinfo;

        int i = 0;

        foreach (WvColInfo col in columns)
	{
            System.Type type = col.type;

            if (type == typeof(object))
	    {
                // We're not even going to try to handle this yet
                throw new VxBadSchemaException("Columns of type sql_variant "
                    + "are not supported by Versaplex at this time");
            }

            VxColumnType coltype;

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
                coltype = VxColumnType.String;
            } else if (type == typeof(DateTime)) {
                coltype = VxColumnType.DateTime;
            } else if (type == typeof(Decimal)) {
                coltype = VxColumnType.Decimal;
            } else {
                throw new VxBadSchemaException("Columns of type "
                        + type.ToString() + " are not supported by "
                        + "Versaplex at this time " +
                        "(column " + col.name + ")");
            }

            colinfo[i] = new VxColumnInfo(col.name, coltype,
			  col.nullable, col.size, col.precision, col.scale);

            i++;
        }

	return colinfo;
    }
}

public class VxDbInterfaceRouter : VxInterfaceRouter 
{

    static WvLog log = new WvLog("VxDbInterfaceRouter");
    static readonly VxDbInterfaceRouter instance;
    public static VxDbInterfaceRouter Instance {
        get { return instance; }
    }

    static VxDbInterfaceRouter() {
        instance = new VxDbInterfaceRouter();
    }

    private VxDbInterfaceRouter() : base("vx.db")
    {
        methods.Add("Test", CallTest);
        methods.Add("Quit", CallQuit);
        methods.Add("ExecScalar", CallExecScalar);
        methods.Add("ExecRecordset", CallExecRecordset);
        methods.Add("ExecChunkRecordset", CallExecChunkRecordset);
        methods.Add("GetSchemaChecksums", CallGetSchemaChecksums);
        methods.Add("GetSchema", CallGetSchema);
        methods.Add("PutSchema", CallPutSchema);
        methods.Add("DropSchema", CallDropSchema);
        methods.Add("GetSchemaData", CallGetSchemaData);
        methods.Add("PutSchemaData", CallPutSchemaData);
    }

    protected override void ExecuteCall(MethodCallProcessor processor,
            Message call, out Message reply)
    {
        try {
            processor(call, out reply);
        } catch (VxRequestException e) {
            reply = VxDbus.CreateError(e.DBusErrorType, e.Message, call);
            log.print("SQL result: {0}\n", e.Short());
        } catch (Exception e) {
            reply = VxDbus.CreateError(
                    "vx.db.exception", 
                    "An internal error occurred.", call);
            log.print("{0}\n", e.ToString());
        }
    }

    static Dictionary<string,string> usernames = new Dictionary<string, string>();

    public static string GetClientId(Message call)
    {
        object sender_obj;
        if (!call.Header.Fields.TryGetValue(FieldCode.Sender, out sender_obj))
            return null;
        string sender = (string)sender_obj;

        // For now, the client ID is just the username of the Unix UID that
        // DBus has associated with the connection.
        string username;
        if (!usernames.TryGetValue(sender, out username))
        {
	    try
	    {
		// FIXME:  We should be using VersaMain.conn here,
		//   not the session bus!!
		// FIXME:  This will likely change as we find a more
		//   universal way to do SSL authentication via D-Bus.
		username = VxSqlPool.GetUsernameForCert(
				Bus.Session.GetCertFingerprint(sender));
	    }
	    catch
	    {
		try
		{
		    // FIXME: This system call isn't actually standard
		    // FIXME: we should be using VersaMain.conn here,
		    //   not the session bus!!
		    username = Bus.Session.GetUnixUserName(sender);
		}
		catch
		{
		    try
		    {
			// FIXME: This system call is standard, but not useful
			//   on Windows.
			// FIXME: we should be using VersaMain.conn here,
			//   not the session bus!!
			username = Bus.Session.GetUnixUser(sender).ToString();
		    }
		    catch
		    {
			username = "*"; // use default connection, if any
		    }
		}
	    }


	    
            // Remember the result, so we don't have to ask DBus all the time
            usernames[sender] = username;
	    
	    log.print(WvLog.L.Info,
		      "New connection '{0}' is user '{1}'\n",
		      sender, username);
        }

        return username;
    }

    private static Message CreateUnknownMethodReply(Message call, 
        string methodname)
    {
        return VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    String.Format(
                        "No overload of {0} has signature '{1}'",
                        methodname, call.Signature), call);
    }

    private static void CallTest(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "") {
            reply = CreateUnknownMethodReply(call, "Test");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.Failed",
                    "Could not identify the client", call);
            return;
        }

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        VxDb.ExecRecordset(clientid, "select 'Works! :D'", 
            out colinfo, out data, out nullity);

        // FIXME: Add vx.db.toomuchdata error
	MessageWriter writer = PrepareRecordsetWriter(colinfo, data, nullity);
	
        reply = VxDbus.CreateReply(call, "a(issnny)vaay", writer);
	
        // For debugging
        VxDbus.MessageDump(" >> ", reply);
    }

    private static void CallQuit(Message call, out Message reply)
    {
	// FIXME: Check permissions here
        MessageWriter writer =
                new MessageWriter(Connection.NativeEndianness);
	writer.Write(typeof(string), "Quit");
        reply = VxDbus.CreateReply(call, "s", writer);
	VersaMain.want_to_die = true;
	
        // For debugging
        VxDbus.MessageDump(" >> ", reply);
    }

    private static void CallExecScalar(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "s") {
            reply = CreateUnknownMethodReply(call, "ExecScalar");
            return;
        }

        if (call.Body == null) {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.InvalidSignature",
                    "Signature provided but no body received", call);
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.Failed",
                    "Could not identify the client", call);
            return;
        }

        MessageReader reader = new MessageReader(call);

        string query = reader.ReadString();

        object result;
        VxDb.ExecScalar(clientid, (string)query, out result);

        MessageWriter writer =
                new MessageWriter(Connection.NativeEndianness);
        writer.Write(result);

        reply = VxDbus.CreateReply(call, "v", writer);
	
        // For debugging
        VxDbus.MessageDump(" >> ", reply);
    }
    
    private static void _WriteColInfo(MessageWriter w,
				      VxColumnInfo[] colinfo)
    {
	// a(issnny)
	foreach (VxColumnInfo c in colinfo)
	    c.Write(w);
    }
    
    private static void WriteColInfo(MessageWriter writer, 
				     VxColumnInfo[] colinfo)
    {
	writer.WriteDelegatePrependSize(delegate(MessageWriter w) 
	    {
		_WriteColInfo(w, colinfo);
	    }, 8);
    }

    public static void CallExecRecordset(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "s") {
            reply = CreateUnknownMethodReply(call, "ExecRecordset");
            return;
        }

        if (call.Body == null) {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.InvalidSignature",
                    "Signature provided but no body received", call);
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.Failed",
                    "Could not identify the client", call);
            return;
        }

        MessageReader reader = new MessageReader(call);

        string query = reader.ReadString();

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        VxDb.ExecRecordset(clientid, (string)query, 
            out colinfo, out data, out nullity);

        // FIXME: Add vx.db.toomuchdata error
	MessageWriter writer = PrepareRecordsetWriter(colinfo, data, nullity);
	
        reply = VxDbus.CreateReply(call, "a(issnny)vaay", writer);

        // For debugging
        VxDbus.MessageDump(" >> ", reply);
    }

    private static void CallExecChunkRecordset(Message call, out Message reply)
    {
	// XXX: Stuff in this comment block shamelessly stolen from
	// "CallExecRecordset".
        if (call.Signature.ToString() != "s") {
            reply = CreateUnknownMethodReply(call, "ExecChunkRecordset");
            return;
        }

        if (call.Body == null) {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.InvalidSignature",
                    "Signature provided but no body received", call);
            return;
        }
	/// XXX

        VxDb.ExecChunkRecordset(call, out reply);
	
        // For debugging
        VxDbus.MessageDump(" >> ", reply);
    }

    private static Signature VxColumnInfoToArraySignature(VxColumnInfo[] vxci)
    {
        StringBuilder sig = new StringBuilder("a(");

        foreach (VxColumnInfo ci in vxci) {
            switch (ci.VxColumnType) {
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

    private static Type[] VxColumnInfoToType(VxColumnInfo[] vxci)
    {
        Type[] ret = new Type[vxci.Length];

        for (int i=0; i < vxci.Length; i++) {
            switch (vxci[i].VxColumnType) {
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

    private static void CallGetSchemaChecksums(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "") {
            reply = CreateUnknownMethodReply(call, "GetSchemaChecksums");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.Failed",
                    "Could not identify the client", call);
            return;
        }

        // FIXME: Add vx.db.toomuchdata error
        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            VxSchemaChecksums sums = backend.GetChecksums();
            sums.WriteChecksums(writer);
        }

        reply = VxDbus.CreateReply(call, 
            VxSchemaChecksums.GetDbusSignature(), writer);

        // For debugging
        VxDbus.MessageDump(" >> ", reply);
    }

    private static void CallGetSchema(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "as") {
            reply = CreateUnknownMethodReply(call, "GetSchema");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.Failed",
                    "Could not identify the client", call);
            return;
        }

        Array names_untyped;

        MessageReader mr = new MessageReader(call);
        names_untyped = mr.ReadArray(typeof(string));

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            VxSchema schema = backend.Get(names_untyped.Cast<string>());
            schema.WriteSchema(writer);
        }

        reply = VxDbus.CreateReply(call, VxSchema.GetDbusSignature(), writer);

        // For debugging
        VxDbus.MessageDump(" >> ", reply);
    }

    private static void CallDropSchema(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "as") {
            reply = CreateUnknownMethodReply(call, "DropSchema");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.Failed",
                    "Could not identify the client", call);
            return;
        }

        MessageReader mr = new MessageReader(call);
	string[] keys = (string[])mr.ReadArray(typeof(string));

        VxSchemaErrors errs;
        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            errs = backend.DropSchema(keys);
        }

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);
        VxSchemaErrors.WriteErrors(writer, errs);

        reply = VxDbus.CreateReply(call, VxSchemaErrors.GetDbusSignature(), 
            writer);
        if (errs != null && errs.Count > 0)
        {
            reply.Header.MessageType = MessageType.Error;
            reply.Header.Fields[FieldCode.ErrorName] = 
                "org.freedesktop.DBus.Error.Failed";
        }
    }

    private static void CallPutSchema(Message call, out Message reply)
    {
        if (call.Signature.ToString() != String.Format("{0}i", 
                VxSchema.GetDbusSignature())) {
            reply = CreateUnknownMethodReply(call, "PutSchema");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.Failed",
                    "Could not identify the client", call);
            return;
        }

        MessageReader mr = new MessageReader(call);
        VxSchema schema = new VxSchema(mr);
        int opts = mr.ReadInt32();

        VxSchemaErrors errs;
        
        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            errs = backend.Put(schema, null, (VxPutOpts)opts);
        }

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);
        VxSchemaErrors.WriteErrors(writer, errs);

        reply = VxDbus.CreateReply(call, VxSchemaErrors.GetDbusSignature(), 
            writer);
        if (errs != null && errs.Count > 0)
        {
            reply.Header.MessageType = MessageType.Error;
            reply.Header.Fields[FieldCode.ErrorName] = 
                "org.freedesktop.DBus.Error.Failed";
        }
    }

    private static void CallGetSchemaData(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "ss") {
            reply = CreateUnknownMethodReply(call, "GetSchemaData");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.Failed",
                    "Could not identify the client", call);
            return;
        }

        MessageReader mr = new MessageReader(call);
        string tablename = mr.ReadString();
	string where = mr.ReadString();

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            string schemadata = backend.GetSchemaData(tablename, 0, where);
            writer.Write(schemadata);
        }

        reply = VxDbus.CreateReply(call, "s", writer);
    }

    private static void CallPutSchemaData(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "ss") {
            reply = CreateUnknownMethodReply(call, "PutSchemaData");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.Failed",
                    "Could not identify the client", call);
            return;
        }

        MessageReader mr = new MessageReader(call);
        string tablename = mr.ReadString();
        string text = mr.ReadString();

        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            backend.PutSchemaData(tablename, text, 0);
        }

        reply = VxDbus.CreateReply(call);
    }

    public static MessageWriter PrepareRecordsetWriter(VxColumnInfo[] colinfo,
						object[][] data,
						byte[][] nulldata)
    {
	MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

	WriteColInfo(writer, colinfo);
	if (colinfo.Length <= 0)
	{
	    // Some clients can't parse a() (empty struct) properly, so
	    // we'll have an empty array of (i) instead.
	    writer.Write(typeof(Signature), new Signature("a(i)"));
	}
	else
	    writer.Write(typeof(Signature), VxColumnInfoToArraySignature(colinfo));
	writer.WriteDelegatePrependSize(delegate(MessageWriter w)
		{
		    WriteStructArray(w, VxColumnInfoToType(colinfo), data);
		}, 8);
	writer.Write(typeof(byte[][]), nulldata);

	return writer;
    }
}

