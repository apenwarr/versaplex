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
        methods.Add("GetSchemaChecksums", CallGetSchemaChecksums);
        methods.Add("GetSchema", CallGetSchema);
        methods.Add("PutSchema", CallPutSchema);
        methods.Add("DropSchema", CallDropSchema);
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

        foreach (string type in Schemamatic.ProcedureTypes)
        {
            if (type == "Procedure")
            {
                // Set up self test
                object result;
                VxDb.ExecScalar(clientid, "create procedure " + 
                    "schemamatic_checksum_test as print 'hello' ", out result);
            }

            Schemamatic.GetProcChecksums(sums, clientid, type, 0);

            if (type == "Procedure")
            {
                object result;
                VxDb.ExecScalar(clientid, 
                    "drop procedure schemamatic_checksum_test", out result);

                // Self-test the checksum feature.  If mssql's checksum
                // algorithm changes, we don't want to pretend our checksum
                // list makes any sense!
                string test_csum_label = "Procedure/schemamatic_checksum_test";
                ulong got_csum = 0;
                if (sums.ContainsKey(test_csum_label))
                    got_csum = sums[test_csum_label].checksums[0];
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

            Schemamatic.GetProcChecksums(sums, clientid, type, 1);
        }

        // Do tables separately
        Schemamatic.GetTableChecksums(sums, clientid);

        // Do indexes separately
        Schemamatic.GetIndexChecksums(sums, clientid);

        // Do XML schema collections separately (FIXME: only if SQL2005)
        Schemamatic.GetXmlSchemaChecksums(sums, clientid);

        // FIXME: Add vx.db.toomuchdata error
        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        sums.WriteChecksums(writer);

        reply = VxDbus.CreateReply(call, 
            VxSchemaChecksums.GetSignature(), writer);

        // For debugging
        reply.WriteHeader();
        VxDbus.MessageDump(" >> ", reply);
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
            foreach (string type in Schemamatic.ProcedureTypes)
            {
                Schemamatic.RetrieveProcSchemas(schema, proc_names, 
                    clientid, type, 0);
                Schemamatic.RetrieveProcSchemas(schema, proc_names, 
                    clientid, type, 1);
            }
        }

        if (idx_names.Count > 0 || all_names.Count == 0)
            Schemamatic.RetrieveIndexSchemas(schema, idx_names, clientid);

        if (xml_names.Count > 0 || all_names.Count == 0)
            Schemamatic.RetrieveXmlSchemas(schema, xml_names, clientid);

        if (tab_names.Count > 0 || all_names.Count == 0)
            Schemamatic.RetrieveTableColumns(schema, tab_names, clientid);

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        schema.WriteSchema(writer);

        reply = VxDbus.CreateReply(call, VxSchema.GetSignature(), writer);

        // For debugging
        reply.WriteHeader();
        VxDbus.MessageDump(" >> ", reply);
    }

    private static void CallDropSchema(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "ss") {
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

        string type, name;

        MessageReader mr = new MessageReader(call);
        mr.GetValue(out type);
        mr.GetValue(out name);

        Schemamatic.DropSchema(clientid, type, name);

        reply = VxDbus.CreateReply(call);
    }

    private static void CallPutSchema(Message call, out Message reply)
    {
        if (call.Signature.ToString() != "sssy") {
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

        string type, name, text;
        byte destructive;

        MessageReader mr = new MessageReader(call);
        mr.GetValue(out type);
        mr.GetValue(out name);
        mr.GetValue(out text);
        mr.GetValue(out destructive);

        Schemamatic.PutSchema(clientid, type, name, text, destructive);

        reply = VxDbus.CreateReply(call);
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

    public bool ContainsSqlError(int errno)
    {
        if (!(InnerException is SqlException))
            return false;

        SqlException sqle = (SqlException)InnerException;
        foreach (SqlError err in sqle.Errors)
        {
            if (err.Number == errno)
                return true;
        }
        return false;
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
