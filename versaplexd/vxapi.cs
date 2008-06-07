using System;
using System.Text;
using System.Text.RegularExpressions;
using System.Data;
using System.Data.SqlClient;
using System.Runtime.Serialization;
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

        SqlConnection conn = null;
        try {
            conn = VxSqlPool.TakeConnection(connid);

            using (SqlCommand cmd = conn.CreateCommand()) {
                cmd.CommandText = query;
                result = cmd.ExecuteScalar();
            }
        } catch (SqlException e) {
            throw new VxSqlException(e.Message, e);
        } finally {
            if (conn != null)
                VxSqlPool.ReleaseConnection(conn);
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
	
        SqlConnection conn = null;
        try {
            conn = VxSqlPool.TakeConnection(connid);
			List<object[]> rows = new List<object[]>();
			List<byte[]> rownulls = new List<byte[]>();

            using (SqlCommand cmd = new SqlCommand(query, conn))
            using (SqlDataReader reader = cmd.ExecuteReader()) {
                if (reader.FieldCount <= 0) {
            		log.print("No columns in resulting data set.");
                }
               	ProcessSchema(reader, out colinfo);

                while (reader.Read()) {
                    object[] row = new object[reader.FieldCount];
                    byte[] rownull = new byte[reader.FieldCount];

                    for (int i = 0; i < reader.FieldCount; i++) {
                        bool isnull = reader.IsDBNull(i);

                        row[i] = null;

                        rownull[i] = isnull ? (byte)1 : (byte)0;

                        switch (colinfo[i].VxColumnType) {
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
                log.print(WvLog.L.Debug4, "({0} rows)\n", data.Length);
                wv.assert(nullity.Length == data.Length);
            }
        } catch (SqlException e) {
            throw new VxSqlException(e.Message, e);
        } finally {
            if (conn != null)
                VxSqlPool.ReleaseConnection(conn);
        }
    }

    private static void ProcessSchema(SqlDataReader reader,
            out VxColumnInfo[] colinfo)
    {
        colinfo = new VxColumnInfo[reader.FieldCount];

		if (reader.FieldCount <= 0) {
			return;
		}

        int i = 0;

        using (DataTable schema = reader.GetSchemaTable()) {
            foreach (DataRowView col in schema.DefaultView) {
                foreach (DataColumn c in schema.Columns) {
                    log.print(WvLog.L.Debug4,
			      "{0}:'{1}'  ", c.ColumnName,
			      col[c.ColumnName]);
                }
				log.print(WvLog.L.Debug4, "\n\n");

                System.Type type = (System.Type)col["DataType"];

                if (type == typeof(object)) {
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
                            "(column " + col["ColumnName"].ToString() + ")");
                }

                bool isnull = (bool)col["AllowDBNull"];
                int size = (int)col["ColumnSize"];
                short precision = (short)col["NumericPrecision"];
                short scale = (short)col["NumericScale"];

                colinfo[i] = new VxColumnInfo(col["ColumnName"].ToString(),
                        coltype, isnull, size, precision, scale);

                i++;
            }
        }
    }
}

public class VxDbInterfaceRouter : VxInterfaceRouter {

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
        methods.Add("GetSchema", CallGetSchema);
        methods.Add("GetSchemaChecksums", CallGetSchemaChecksums);
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
	    
            // Remember the result, so we don't have to ask DBus all the time
            usernames[sender] = username;
	    
	    log.print(WvLog.L.Info,
		      "New connection '{0}' is user '{1}'\n",
		      sender, username);
        }

        return username;
    }

    private static void CallTest(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "") {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    String.Format(
                        "No overload of Test has signature '{0}'",
                        call.Signature), call);
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
        MessageWriter writer =
                new MessageWriter(Connection.NativeEndianness);

	WriteColInfo(writer, colinfo);
	writer.Write(typeof(Signature), VxColumnInfoToArraySignature(colinfo));
	writer.WriteDelegatePrependSize(delegate(MessageWriter w) 
	    {
		WriteStructArray(w, VxColumnInfoToType(colinfo), data);
	    }, 8);
	writer.Write(typeof(byte[][]), nullity);
	
        reply = VxDbus.CreateReply(call, "a(issnny)vaay", writer);
    }

    private static void CallQuit(Message call, out Message reply)
    {
	// FIXME: Check permissions here
        MessageWriter writer =
                new MessageWriter(Connection.NativeEndianness);
	writer.Write(typeof(string), "Quit");
        reply = VxDbus.CreateReply(call, "s", writer);
	VersaMain.want_to_die = true;
    }

    private static void CallExecScalar(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "s") {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    String.Format(
                        "No overload of ExecScalar has signature '{0}'",
                        call.Signature), call);
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

        object query;
        reader.GetValue(typeof(string), out query);

        object result;
        VxDb.ExecScalar(clientid, (string)query, out result);

        MessageWriter writer =
                new MessageWriter(Connection.NativeEndianness);
        writer.Write(result);

        reply = VxDbus.CreateReply(call, "v", writer);
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

    private static void CallExecRecordset(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "s") {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    String.Format(
                        "No overload of ExecRecordset has signature '{0}'",
                        call.Signature), call);
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

        object query;
        reader.GetValue(typeof(string), out query);

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        VxDb.ExecRecordset(clientid, (string)query, 
            out colinfo, out data, out nullity);

        // FIXME: Add vx.db.toomuchdata error
        MessageWriter writer =
                new MessageWriter(Connection.NativeEndianness);

	WriteColInfo(writer, colinfo);
	if (colinfo.Length <= 0) {
	    // Some clients can't parse a() (empty struct) properly, so
	    // we'll have an empty array of (i) instead.
	    writer.Write(typeof(Signature), new Signature("a(i)"));
	} else {
	    writer.Write(typeof(Signature), 
			 VxColumnInfoToArraySignature(colinfo));
	}
	writer.WriteDelegatePrependSize(delegate(MessageWriter w)
	    {
		WriteStructArray(w, VxColumnInfoToType(colinfo), data);
	    }, 8);
	writer.Write(typeof(byte[][]), nullity);
	
        reply = VxDbus.CreateReply(call, "a(issnny)vaay", writer);

        // For debugging
        reply.WriteHeader();
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

    private static void GetProcChecksums(VxSchemaChecksums sums, 
            string clientid, string type, int encrypted)
    {
        string encrypt_str = encrypted > 0 ? "-Encrypted" : "";

        log.print("Indexing: {0}{1}\n", type, encrypt_str);

        string query = @"
            select convert(varchar(128), object_name(id)) name,
                     convert(int, colid) colid,
                     convert(varchar(3900), text) text
                into #checksum_calc
                from syscomments
                where objectproperty(id, 'Is" + type + @"') = 1
                    and encrypted = " + encrypted + @"
                    and object_name(id) like '%'
            select name, convert(varbinary(8), getchecksum(text))
                from #checksum_calc
                order by name, colid
            drop table #checksum_calc";

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        
        VxDb.ExecRecordset(clientid, query, out colinfo, out data, out nullity);

        foreach (object[] row in data)
        {
            string name = (string)row[0];
            ulong checksum = 0;
            foreach (byte b in (byte[])row[1])
            {
                checksum <<= 8;
                checksum |= b;
            }

            // Ignore dt_* functions and sys* views
            if (name.StartsWith("dt_") || name.StartsWith("sys"))
                continue;

            // Fix characters not allowed in filenames
            name.Replace('/', '!');
            name.Replace('\n', '!');
            string key = String.Format("{0}{1}/{2}", type, encrypt_str, name);

            log.print("name={0}, checksum={1}, key={2}\n", name, checksum, key);
            sums.Add(key, checksum);
        }
    }

    private static void GetTableChecksums(VxSchemaChecksums sums, 
            string clientid)
    {
        log.print("Indexing: Tables\n");

        // The weird "replace" in defval is because different versions of
        // mssql (SQL7 vs. SQL2005, at least) add different numbers of parens
        // around the default values.  Weird, but it messes up the checksums,
        // so we just remove all the parens altogether.
        string query = @"
            select convert(varchar(128), t.name) tabname,
               convert(varchar(128), c.name) colname,
               convert(varchar(64), typ.name) typename,
               convert(int, c.length) len,
               convert(int, c.xprec) xprec,
               convert(int, c.xscale) xscale,
               convert(varchar(128),
                   replace(replace(def.text, '(', ''), ')', ''))
                   defval,
               convert(int, c.isnullable) nullable,
               convert(int, columnproperty(t.id, c.name, 'IsIdentity')) isident,
               convert(int, ident_seed(t.name)) ident_seed,
               convert(int, ident_incr(t.name)) ident_incr
              into #checksum_calc
              from sysobjects t
              join syscolumns c on t.id = c.id 
              join systypes typ on c.xtype = typ.xtype
              left join syscomments def on def.id = c.cdefault
              where t.xtype = 'U'
                and typ.name <> 'sysname'
              order by tabname, c.colorder, colname, typ.status
           select tabname, convert(varbinary(8), getchecksum(tabname))
               from #checksum_calc
           drop table #checksum_calc";

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        
        VxDb.ExecRecordset(clientid, query, out colinfo, out data, out nullity);

        foreach (object[] row in data)
        {
            string name = (string)row[0];
            ulong checksum = 0;
            foreach (byte b in (byte[])row[1])
            {
                checksum <<= 8;
                checksum |= b;
            }

            // Tasks_#* should be ignored
            if (name.StartsWith("Tasks_#")) 
                continue;

            string key = String.Format("Table/{0}", name);

            log.print("name={0}, checksum={1}, key={2}\n", name, checksum, key);
            sums.Add(key, checksum);
        }
    }

    private static void GetIndexChecksums(VxSchemaChecksums sums, 
            string clientid)
    {
        string query = @"
            select 
               convert(varchar(128), object_name(i.object_id)) tabname,
               convert(varchar(128), i.name) idxname,
               convert(int, i.type) idxtype,
               convert(int, i.is_unique) idxunique,
               convert(int, i.is_primary_key) idxprimary,
               convert(varchar(128), c.name) colname,
               convert(int, ic.index_column_id) colid,
               convert(int, ic.is_descending_key) coldesc
              into #checksum_calc
              from sys.indexes i
              join sys.index_columns ic
                 on ic.object_id = i.object_id
                 and ic.index_id = i.index_id
              join sys.columns c
                 on c.object_id = i.object_id
                 and c.column_id = ic.column_id
              where object_name(i.object_id) not like 'sys%' 
                and object_name(i.object_id) not like 'queue_%'
              order by i.name, i.object_id, ic.index_column_id
              
            select
               tabname, idxname, colid, 
               convert(varbinary(8), getchecksum(idxname))
              from #checksum_calc
            drop table #checksum_calc";

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        
        VxDb.ExecRecordset(clientid, query, out colinfo, out data, out nullity);

        foreach (object[] row in data)
        {
            string tablename = (string)row[0];
            string indexname = (string)row[1];
            ulong checksum = 0;
            foreach (byte b in (byte[])row[3])
            {
                checksum <<= 8;
                checksum |= b;
            }

            string key = String.Format("Index/{0}/{1}", tablename, indexname);

            log.print("tablename={0}, indexname={1}, checksum={2}, key={3}, colid={4}\n", 
                tablename, indexname, checksum, key, (int)row[2]);
            sums.Add(key, checksum);
        }
    }

    private static void GetXmlSchemaChecksums(VxSchemaChecksums sums, 
            string clientid)
    {
        string query = @"
            select sch.name owner,
               xsc.name sch,
               cast(XML_Schema_Namespace(sch.name,xsc.name) 
                    as varchar(max)) contents
              into #checksum_calc
              from sys.xml_schema_collections xsc 
              join sys.schemas sch on xsc.schema_id = sch.schema_id
              where sch.name <> 'sys'
              order by sch.name, xsc.name

            select sch, convert(varbinary(8), checksum(contents))
                from #checksum_calc
            drop table #checksum_calc";

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        
        VxDb.ExecRecordset(clientid, query, out colinfo, out data, out nullity);

        foreach (object[] row in data)
        {
            string schemaname = (string)row[0];
            ulong checksum = 0;
            foreach (byte b in (byte[])row[1])
            {
                checksum <<= 8;
                checksum |= b;
            }

            string key = String.Format("XMLSchema/{0}", schemaname);

            log.print("schemaname={0}, checksum={1}, key={2}\n", 
                schemaname, checksum, key);
            sums.Add(key, checksum);
        }
    }

    private static string[] ProcedureTypes = new string[] { 
//            "CheckCnst", 
//            "Constraint",
//            "Default",
//            "DefaultCnst",
//            "Executed",
            "ScalarFunction",
            "TableFunction",
//            "InlineFunction",
//            "ExtendedProc",
//            "ForeignKey",
//            "MSShipped",
//            "PrimaryKey",
            "Procedure",
            "ReplProc",
//            "Rule",
//            "SystemTable",
//            "Table",
            "Trigger",
//            "UniqueCnst",
            "View",
//            "OwnerId"
        };

    private static void CallGetSchemaChecksums(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "") {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    String.Format(
                        "No overload of GetSchemaChecksums has signature '{0}'",
                        call.Signature), call);
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

        VxSchemaChecksums sums = new VxSchemaChecksums();

        foreach (string type in ProcedureTypes)
        {
            if (type == "Procedure")
            {
                // Set up self test
                object result;
                VxDb.ExecScalar(clientid, "create procedure " + 
                    "schemamatic_checksum_test as print 'hello' ", out result);
            }

            GetProcChecksums(sums, clientid, type, 0);

            if (type == "Procedure")
            {
                object result;
                VxDb.ExecScalar(clientid, 
                    "drop procedure schemamatic_checksum_test", out result);

                // Self-test the checksum feature.  If mssql's checksum
                // algorithm changes, we don't want to pretend our checksum
                // list makes any sense!
                string test_csum_label = "Procedure/schemamatic_checksum_test";
                ulong got_csum = sums[test_csum_label].checksums[0];
                ulong want_csum = 0x173d6ee8;
                if (want_csum != got_csum)
                {
                    reply = VxDbus.CreateError(
                        "org.freedesktop.DBus.Error.Failed",
                        String.Format("checksum_test mismatch! {0} != {1}", 
                            got_csum, want_csum), call);
                    return;
                }
                sums.Remove(test_csum_label);
            }

            GetProcChecksums(sums, clientid, type, 1);
        }

        // Do tables separately
        GetTableChecksums(sums, clientid);

        // Do indexes separately
        GetIndexChecksums(sums, clientid);

        // Do XML schema collections separately (FIXME: only if SQL2005)
        GetXmlSchemaChecksums(sums, clientid);

        // FIXME: Add vx.db.toomuchdata error
        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        sums.WriteChecksums(writer);

        reply = VxDbus.CreateReply(call, 
            VxSchemaChecksums.GetSignature(), writer);

        // For debugging
        reply.WriteHeader();
        VxDbus.MessageDump(" >> ", reply);
    }

    private static string RetrieveProcSchemasQuery(string type, int encrypted, 
        bool countonly, List<string> names)
    {
        string name_q = names.Count > 0 
            ? " and object_name(id) in ('" + 
                String.Join("','", names.ToArray()) + "')"
            : "";

        string textcol = encrypted > 0 ? "ctext" : "text";
        string cols = countonly 
            ? "count(*)"
            : "object_name(id), colid, " + textcol + " ";

        return "select " + cols + " from syscomments " + 
            "where objectproperty(id, 'Is" + type + "') = 1 " + 
                "and encrypted = " + encrypted + name_q;
    }

    private static void RetrieveProcSchemas(VxSchema schema, List<string> names, 
        string clientid, string type, int encrypted)
    {
        string query = RetrieveProcSchemasQuery(type, encrypted, false, names);

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        
        VxDb.ExecRecordset(clientid, query, out colinfo, out data, out nullity);

        int num = 0;
        int total = data.Length;
        foreach (object[] row in data)
        {
            num++;
            string name = (string)row[0];
            short colid = (short)row[1];
            string text;
            if (encrypted > 0)
            {
                byte[] bytes = (byte[])row[2];
                // BitConverter.ToString formats the bytes as "01-23-cd-ef", 
                // but we want to have them as just straight "0123cdef".
                text = System.BitConverter.ToString(bytes);
                text = text.Replace("-", "");
                log.print("bytes.Length = {0}, text={1}\n", bytes.Length, text);
            }
            else
                text = (string)row[2];


            // Skip dt_* functions and sys_* views
            if (name.StartsWith("dt_") || name.StartsWith("sys_"))
                continue;

            log.print("{0}/{1} {2}{3}/{4} #{5}\n", num, total, type, 
                encrypted > 0 ? "-Encrypted" : "", name, colid);
            // Fix characters not allowed in filenames
            name.Replace('/', '!');
            name.Replace('\n', '!');

            schema.Add(name, type, text, encrypted > 0);
        }
        log.print("{0}/{1} {2}{3} done\n", num, total, type, 
            encrypted > 0 ? "-Encrypted" : "");
    }

    private static void RetrieveIndexSchemas(VxSchema schema, List<string> names, 
        string clientid)
    {
        string idxnames = (names.Count > 0) ? 
            "and ((object_name(i.object_id)+'/'+i.name) in ('" + 
                String.Join("','", names.ToArray()) + "'))"
            : "";

        string query = @"
          select 
           convert(varchar(128), object_name(i.object_id)) tabname,
           convert(varchar(128), i.name) idxname,
           convert(int, i.type) idxtype,
           convert(int, i.is_unique) idxunique,
           convert(int, i.is_primary_key) idxprimary,
           convert(varchar(128), c.name) colname,
           convert(int, ic.index_column_id) colid,
           convert(int, ic.is_descending_key) coldesc
          from sys.indexes i
          join sys.index_columns ic
             on ic.object_id = i.object_id
             and ic.index_id = i.index_id
          join sys.columns c
             on c.object_id = i.object_id
             and c.column_id = ic.column_id
          where object_name(i.object_id) not like 'sys%' 
            and object_name(i.object_id) not like 'queue_%' " + 
            idxnames + 
          @" order by i.name, i.object_id, ic.index_column_id";

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        
        VxDb.ExecRecordset(clientid, query, out colinfo, out data, out nullity);

        int old_colid = 0;
        List<string> cols = new List<string>();
        for (int ii = 0; ii < data.Length; ii++)
        {
            object[] row = data[ii];

            string tabname = (string)row[0];
            string idxname = (string)row[1];
            int idxtype = (int)row[2];
            int idxunique = (int)row[3];
            int idxprimary = (int)row[4];
            string colname = (string)row[5];
            int colid = (int)row[6];
            int coldesc = (int)row[7];

            // Check that we're getting the rows in order.
            wv.assert(colid == old_colid + 1 || colid == 1);
            old_colid = colid;

            cols.Add(coldesc == 0 ? colname : colname + " DESC");

            object[] nextrow = ((ii+1) < data.Length) ? data[ii+1] : null;
            string next_tabname = (nextrow != null) ? (string)nextrow[0] : null;
            string next_idxname = (nextrow != null) ? (string)nextrow[1] : null;
            
            // If we've finished reading the columns for this index, add the
            // index to the schema.  Note: depends on the statement's ORDER BY.
            if (tabname != next_tabname || idxname != next_idxname)
            {
                string colstr = String.Join(",", cols.ToArray());
                string indexstr;
                if (idxprimary != 0)
                {
                    indexstr = String.Format(
                        "ALTER TABLE [{0}] ADD CONSTRAINT [{1}] PRIMARY KEY{2}\n" + 
                        "\t({3});\n\n", 
                        tabname,
                        idxname,
                        (idxtype == 1 ? " CLUSTERED" : " NONCLUSTERED"),
                        colstr);
                }
                else
                {
                    indexstr = String.Format(
                        "CREATE {0}{1}INDEX [{2}] ON [{3}] \n\t({4});\n\n",
                        (idxunique != 0 ? "UNIQUE " : ""),
                        (idxtype == 1 ? "CLUSTERED " : ""),
                        idxname,
                        tabname,
                        colstr);
                }
                schema.Add(tabname + "/" + idxname, "Index", indexstr, false);
                cols.Clear();
            }
        }
    }

    private static string XmlSchemasQuery(int count, List<string> names)
    {
        int start = count * 4000;

        string namestr = (names.Count > 0) ? 
            "and xsc.name in ('" + 
                String.Join("','", names.ToArray()) + "')"
            : "";

        string query = @"select sch.name owner,
           xsc.name sch, 
           cast(substring(
                 cast(XML_Schema_Namespace(sch.name,xsc.name) as varchar(max)), 
                 " + start + @", 4000) 
            as varchar(4000)) contents
          from sys.xml_schema_collections xsc 
          join sys.schemas sch on xsc.schema_id = sch.schema_id
          where sch.name <> 'sys'" + 
            namestr + 
          @" order by sch.name, xsc.name";

        return query;
    }

    private static void RetrieveXmlSchemas(VxSchema schema, List<string> names, 
        string clientid)
    {
        bool do_again = true;
        for (int count = 0; do_again; count++)
        {
            do_again = false;
            string query = XmlSchemasQuery(count, names);

            VxColumnInfo[] colinfo;
            object[][] data;
            byte[][] nullity;
            
            VxDb.ExecRecordset(clientid, query, out colinfo, out data, 
                out nullity);

            foreach (object[] row in data)
            {
                string owner = (string)row[0];
                string name = (string)row[1];
                string contents = (string)row[2];

                if (contents == "")
                    continue;

                do_again = true;

                if (count == 0)
                    schema.Add(name, "XMLSchema", String.Format(
                        "CREATE XML SCHEMA COLLECTION [{0}].[{1}] AS '", 
                        owner, name), false);

                schema.Add(name, "XMLSchema", contents, false);
            }
        }

        // Close the quotes on all the XMLSchemas
        foreach (KeyValuePair<string, VxSchemaElement> p in schema)
        {
            if (p.Value.type == "XMLSchema")
                p.Value.text += "'\n";
        }
    }

    private static void RetrieveTableColumns(VxSchema schema, 
        List<string> names, string clientid)
    {
        string tablenames = (names.Count > 0 
            ? "and t.name in ('" + String.Join("','", names.ToArray()) + "')"
            : "");

        string query = @"select t.name tabname,
	   c.name colname,
	   typ.name typename,
	   c.length len,
	   c.xprec xprec,
	   c.xscale xscale,
	   def.text defval,
	   c.isnullable nullable,
	   columnproperty(t.id, c.name, 'IsIdentity') isident,
	   ident_seed(t.name) ident_seed, ident_incr(t.name) ident_incr
	  from sysobjects t
	  join syscolumns c on t.id = c.id 
	  join systypes typ on c.xtype = typ.xtype
	  left join syscomments def on def.id = c.cdefault
	  where t.xtype = 'U'
	    and typ.name <> 'sysname' " + 
	    tablenames + @"
	  order by tabname, c.colorder, typ.status";

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        
        VxDb.ExecRecordset(clientid, query, out colinfo, out data, out nullity);

        List<string> cols = new List<string>();
        for (int ii = 0; ii < data.Length; ii++)
        {
            object[] row = data[ii];

            string tabname = (string)row[0];
            string colname = (string)row[1];
            string typename = (string)row[2];
            short len = (short)row[3];
            byte xprec = (byte)row[4];
            byte xscale = (byte)row[5];
            string defval = (string)row[6];
            int isnullable = (int)row[7];
            int isident = (int)row[8];
            string ident_seed = (string)row[9];
            string ident_incr = (string)row[10];

            if (isident == 0)
                ident_seed = ident_incr = null;

            string lenstr = "";
            if (typename.EndsWith("nvarchar") || typename.EndsWith("nchar"))
            {
                if (len == -1)
                    lenstr = "(max)";
                else
                {
                    len /= 2;
                    lenstr = String.Format("({0})", len);
                }
            }
            else if (typename.EndsWith("char") || typename.EndsWith("binary"))
            {
                lenstr = (len == -1 ? "(max)" : String.Format("({0})", len));
            }
            else if (typename.EndsWith("decimal") || 
                typename.EndsWith("numeric") || typename.EndsWith("real"))
            {
                lenstr = String.Format("({0},{1})", xprec,xscale);
            }

            if (defval != null && defval != "")
            {
                // MSSQL returns default values wrapped in ()s
                if (defval[0] == '(' && defval[defval.Length - 1] == ')')
                    defval = defval.Substring(1, defval.Length - 2);
            }

            cols.Add(String.Format("[{0}] [{1}]{2}{3}{4}{5}",
                colname, typename, 
                ((lenstr != "") ? " " + lenstr : ""),
                ((defval != "") ? " DEFAULT " + defval : ""),
                ((isnullable != 0) ? " NULL" : " NOT NULL"),
                ((isident != 0) ?  String.Format(
                    " IDENTITY({0},{1})", ident_seed, ident_incr) :
                    "")));

            string next_tabname = ((ii+1) < data.Length ? 
                (string)data[ii+1][0] : null);
            if (tabname != next_tabname)
            {
                string tablestr = String.Format(
                    "CREATE TABLE [{0}] (\n\t{1});\n\n",
                    tabname, String.Join(",\n\t", cols.ToArray()));
                schema.Add(tabname, "Table", tablestr, false);

                cols.Clear();
            }
        }
    }

    // Escape the schema element names supplied, to make sure they don't have
    // evil characters.
    private static string EscapeSchemaElementName(string name)
    {
        // Replace any nasty non-ASCII characters with an !
        string escaped = Regex.Replace(name, "[^\\p{IsBasicLatin}]", "!");

        // Escape quote marks
        return escaped.Replace("'", "''");
    }

    private static void CallGetSchema(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "as") {
            reply = VxDbus.CreateError(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    String.Format(
                        "No overload of GetSchemaChecksums has signature '{0}'",
                        call.Signature), call);
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
        List<string> all_names = new List<string>();
        List<string> proc_names = new List<string>();
        List<string> xml_names = new List<string>();
        List<string> tab_names = new List<string>();
        List<string> idx_names = new List<string>();

        MessageReader mr = new MessageReader(call);
        mr.GetValue(typeof(string[]), out names_untyped);
        foreach (object nameobj in names_untyped)
        {
            string fullname = EscapeSchemaElementName((string)nameobj);
            Console.WriteLine("CallGetSchema: Read name " + fullname);
            all_names.Add(fullname);

            string[] parts = fullname.Split(new char[] {'/'}, 2);
            if (parts.Length == 2)
            {
                string type = parts[0];
                string name = parts[1];
                if (type == "Table")
                    tab_names.Add(name);
                else if (type == "Index")
                    idx_names.Add(name);
                else if (type == "XMLSchema")
                    xml_names.Add(name);
                else
                    proc_names.Add(name);
            }
            else
            {
                // No type given, just try them all
                proc_names.Add(fullname);
                xml_names.Add(fullname);
                tab_names.Add(fullname);
                idx_names.Add(fullname);
            }
        }

        VxSchema schema = new VxSchema();

        if (proc_names.Count > 0 || all_names.Count == 0)
        {
            foreach (string type in ProcedureTypes)
            {
                RetrieveProcSchemas(schema, proc_names, clientid, type, 0);
                RetrieveProcSchemas(schema, proc_names, clientid, type, 1);
            }
        }

        if (idx_names.Count > 0 || all_names.Count == 0)
            RetrieveIndexSchemas(schema, idx_names, clientid);

        if (xml_names.Count > 0 || all_names.Count == 0)
            RetrieveXmlSchemas(schema, xml_names, clientid);

        if (tab_names.Count > 0 || all_names.Count == 0)
            RetrieveTableColumns(schema, tab_names, clientid);

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        schema.WriteSchema(writer);

        reply = VxDbus.CreateReply(call, VxSchema.GetSignature(), writer);

        // For debugging
        reply.WriteHeader();
        VxDbus.MessageDump(" >> ", reply);
    }

}

class VxRequestException : Exception {
    public string DBusErrorType;

    public VxRequestException(string errortype)
        : base()
    {
        DBusErrorType = errortype;
    }
    
    public VxRequestException(string errortype, string msg)
        : base(msg)
    {
        DBusErrorType = errortype;
    }

    public VxRequestException(string errortype, SerializationInfo si, 
            StreamingContext sc)
        : base(si, sc)
    {
        DBusErrorType = errortype;
    }

    public VxRequestException(string errortype, string msg, Exception inner)
        : base(msg, inner)
    {
        DBusErrorType = errortype;
    }
}

class VxSqlException : VxRequestException {
    public VxSqlException()
        : base("vx.db.sqlerror")
    {
    }
    
    public VxSqlException(string msg)
        : base("vx.db.sqlerror", msg)
    {
    }

    public VxSqlException(SerializationInfo si, StreamingContext sc)
        : base("vx.db.sqlerror", si, sc)
    {
    }

    public VxSqlException(string msg, Exception inner)
        : base("vx.db.sqlerror", msg, inner)
    {
    }
}

class VxTooMuchDataException : VxRequestException {
    public VxTooMuchDataException()
        : base("vx.db.toomuchdata")
    {
    }
    
    public VxTooMuchDataException(string msg)
        : base("vx.db.toomuchdata", msg)
    {
    }

    public VxTooMuchDataException(SerializationInfo si, StreamingContext sc)
        : base("vx.db.toomuchdata", si, sc)
    {
    }

    public VxTooMuchDataException(string msg, Exception inner)
        : base("vx.db.toomuchdata", msg, inner)
    {
    }
}

class VxBadSchemaException : VxRequestException {
    public VxBadSchemaException()
        : base("vx.db.badschema")
    {
    }
    
    public VxBadSchemaException(string msg)
        : base("vx.db.badschema", msg)
    {
    }

    public VxBadSchemaException(SerializationInfo si, StreamingContext sc)
        : base("vx.db.badschema", si, sc)
    {
    }

    public VxBadSchemaException(string msg, Exception inner)
        : base("vx.db.badschema", msg, inner)
    {
    }
}

class VxConfigException : VxRequestException {
    public VxConfigException()
        : base("vx.db.configerror")
    {
    }
    
    public VxConfigException(string msg)
        : base("vx.db.configerror", msg)
    {
    }

    public VxConfigException(SerializationInfo si, StreamingContext sc)
        : base("vx.db.configerror", si, sc)
    {
    }

    public VxConfigException(string msg, Exception inner)
        : base("vx.db.configerror", msg, inner)
    {
    }
}
