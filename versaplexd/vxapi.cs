/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
using System;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Collections.Generic;
using Wv;
using Wv.Extensions;

internal static class VxDb {
    static WvLog log = new WvLog("VxDb", WvLog.L.Debug2);

    internal static void ExecScalar(string connid, string query, 
			    out VxColumnType coltype, out object result)
    {
        log.print(WvLog.L.Debug3, "ExecScalar {0}\n", query);

	try
	{
	    using (var dbi = VxSqlPool.create(connid))
	    using (var qr = dbi.select(query))
	    {
		var ci = ProcessSchema(qr.columns);
		if (ci.Length == 0)
		{
		    coltype = VxColumnType.String;
		    result = "";
		    return;
		}
		
		coltype = ci[0].VxColumnType;
		var en = qr.GetEnumerator();
		if (en.MoveNext())
		    result = en.Current[0].inner;
		else
		{
		    coltype = VxColumnType.String;
		    result = "";
		}
	    }
        }
	catch (DbException e)
	{
            throw new VxSqlException(e.Message, e);
        }
    }


    internal static void SendChunkRecordSignal(WvDbus conn,
					       WvDbusMsg call, string sender,
					    VxColumnInfo[] colinfo,
					    object[][] data, byte[][] nulls)
    {
	WvDbusWriter writer =
	    VxDbusRouter.PrepareRecordsetWriter(colinfo, data, nulls);
	writer.Write(call.serial);

	new WvDbusSignal(call.sender, call.path, "vx.db", "ChunkRecordsetSig",
		   "a(issnny)vaayu")
	    .write(writer)
	    .send(conn);
    }

    static string[] simple_commands = {
		"alter", "checkpoint", "close", "commit",
		"create", "dbcc", "deallocate",
		"declare", "delete", "drop",
		"dump", "exec", "execute",
		"fetch", "grant", "insert", "kill",
		"load", "open", "print",
		"raiserror", "readtext", "reconfigure",
		"revoke", "rollback", "save",
		"select", "set", "setuser",
		"shutdown", "truncate", "update",
		"use", "waitfor", "while",
		"writetext",
	};

    private static bool IsSimpleCommand(VxSqlToken t)
    {
	if (!t.IsKeyword())
	    return false;

	foreach (string s in simple_commands)
	    if (t.name.ToLower() == s)
		return true;

	return false;
    }


    internal static string query_parser(string query, int access_restrictions)
    {
	VxSqlTokenizer tokenizer = new VxSqlTokenizer(query);
	VxSqlToken[] tokens = tokenizer.gettokens().ToArray();
	List<VxSqlToken> result = new List<VxSqlToken>();

	for (int i = 0; i < tokens.Length; ++i)
	{
	    VxSqlToken tok = tokens[i];
	    VxSqlTokenizer ret = new VxSqlTokenizer();

	    if (tok.NotQuotedAndLowercaseEq("get") &&
		i < tokens.Length - 3 &&
		tokens[i + 1].NotQuotedAndLowercaseEq("object"))
	    {
		// Format: 
		// get object {view|trigger|procedure|scalarfunction|tablefunction} name
		// Returns: the "source code" to the object
		ret.tokenize(String.Format(
		    "select cast(text as varchar(max)) text "
		    + "from syscomments "
		    + "where objectproperty(id, 'Is{0}') = 1 "
		    + "and object_name(id) = '{1}' "
		    + "order by number, colid;",
		    tokens[i + 2].name, tokens[i + 3].name));
		i += 3;
	    }
	    else if (i < tokens.Length - 1)
	    {
		VxSqlToken next_tok = tokens[i + 1];
		if (tok.IsKeywordEq("use") && next_tok.IsIdentifier())
		{
		    // Drop "use x" statements completely
		    ret.tokenize(";");
		    ++i;
		}
		else if (tok.NotQuotedAndLowercaseEq("list"))
		{
		    if (next_tok.NotQuotedAndLowercaseEq("tables"))
		    {
			ret.tokenize("exec sp_tables;");
			++i;
		    }
		    else if (next_tok.NotQuotedAndLowercaseEq("columns")
				&& i < tokens.Length - 2)
		    {
			string tabname = tokens[i + 2].name;
			var sb = new StringBuilder(tabname.Length*2);
			for (int b = 0; b < tabname.Length; b++)
			{
			    if (tabname[b] == '\\' && b < tabname.Length-1)
				sb.Append(new char[] 
				      { '[', tabname[++b], ']' });
			    else
				sb.Append(tabname[b]);
			}
			ret.tokenize("exec sp_columns @table_name='{0}';",
				     sb);
			i += 2;
		    }
		    else if (next_tok.NotQuotedAndLowercaseEq("all")
				&& i < tokens.Length - 2)
		    {
			VxSqlToken nextnext_tok = tokens[i + 2];
			i += 2;
			if (nextnext_tok.NotQuotedAndLowercaseEq("table"))
			{
			    ret.tokenize(
			    "select distinct cast(Name as varchar(max)) Name"
			    + " from sysobjects "
			    + " where objectproperty(id,'IsTable')=1 "
			    + " and xtype='U' "
			    + " order by Name;");
			}
			else
			{
			    // Format: list all {view|trigger|procedure|scalarfunction|tablefunction}
			    // Returns: a list of all of whatever
			    // Note:  the parameter we pass in can be case insensitive
			    ret.tokenize(String.Format(
			    "select distinct "
			    + " cast (object_name(id) as varchar(256)) Name "
			    + " from syscomments "
			    + " where objectproperty(id,'Is{0}') = 1 "
			    + " order by Name;", nextnext_tok.name));
			}
		    }
		}
	    }

	    if (ret.gettokens() != null)
		foreach (VxSqlToken t in ret.gettokens())
		    result.Add(t);
	    else
		result.Add(tok);
	}

	if (access_restrictions == 0) //no restrictions
	    goto end;

	// see below for what these all do
	bool have_command = false;
	bool have_begin = false;
	bool have_insert = false;
	bool have_update = false;
	bool insert_valid_for_exec = true;
	bool have_alter = false;
	bool have_alterdb = false;
	bool have_altertable = false;
	bool altertable_valid_for_set = true;

	string exception_txt = "Insufficient security permissions for this type of query.";

	foreach (VxSqlToken t in result)
	{
	    // skip comments
	    if (t.IsComment())
		continue;

	    if (have_begin)
	    {
		// Need to identify if a 'begin' is a 'begin...end' block
		// (which we'll basically skip over), or an actual command
		// ('begin transaction' or 'begin distributed transaction').
		have_begin = false;
		if (t.IsKeywordEq("transaction") ||
		    t.IsKeywordEq("distributed"))
		{
		    //access_restrictions == 2 => no non-select
		    if (!have_command && access_restrictions < 2)
			have_command = true;
		    else
			throw new VxSecurityException(exception_txt);
		}
	    }

	    if (t.IsKeywordEq("begin"))
	    {
		have_begin = true;
		continue;
	    }

	    //FIXME:  Does this heuristic suck?
	    //Have to take care of "INSERT INTO x.y (a, "b", [c]) EXEC foo"
	    //If we notice anything other than valid column identifiers and
	    //table names, or the word 'into', commas, periods, or parentheses,
	    //we ban a following 'EXEC', otherwise we allow it
	    if (have_insert && !(t.IsKeywordEq("into") ||
		    t.IsValidIdentifier() ||
		    t.type == VxSqlToken.TokenType.Comma ||
		    t.type == VxSqlToken.TokenType.Period ||
		    t.type == VxSqlToken.TokenType.LParen ||
		    t.type == VxSqlToken.TokenType.RParen ||
		    t.IsKeywordEq("exec")))
		insert_valid_for_exec = false;

	    if (have_alter)
	    {
		have_alter = false;
		if (t.IsKeywordEq("database"))
		{
		    have_alterdb = true;
		    continue;
		}
		else if (t.IsKeywordEq("table"))
		{
		    have_altertable = true;
		    continue;
		}
	    }

	    //If we have "ALTER TABLE x.y", it's OK to have the word 'set' right
	    //afterwards.  otherwise, it's not OK.
	    if (have_altertable && !(t.IsValidIdentifier() ||
				    t.IsKeywordEq("set")))
		altertable_valid_for_set = false;

	    if (IsSimpleCommand(t))
	    {
		//access_restrictions == 2 => no non-select
		if (!have_command && (access_restrictions < 2 ||
					t.IsKeywordEq("select")))
		{
		    // No command yet?  Now we have one. Flag a few
		    have_command = true;
		    if (t.IsKeywordEq("insert"))
			have_insert = true;
		    else if (t.IsKeywordEq("update"))
			have_update = true;
		    else if (t.IsKeywordEq("alter"))
			have_alter = true;
		}
		else if (have_update && t.IsKeywordEq("set"))
		    //'SET' is OK as part of "UPDATE x SET y"
		    have_update = false;
		else if (have_alterdb && t.IsKeywordEq("set"))
		    //'SET' is OK as part of "ALTER DATABASE x SET y"
		    have_alterdb = false;
		else if (have_insert && insert_valid_for_exec &&
			(t.IsKeywordEq("exec") || t.IsKeywordEq("execute")))
		    have_insert = false;  //Insert's exec just finished here
		else if (have_altertable && altertable_valid_for_set &&
			t.IsKeywordEq("set"))
		    have_altertable = false;
		else if (!t.IsKeywordEq("select"))
		    throw new VxSecurityException(exception_txt);
	    }
	}
end:
	return result.join("");
    }


    internal static void ExecChunkRecordset(WvDbus conn, 
					    WvDbusMsg call, out WvDbusMsg reply)
    {
	string connid = VxDbusRouter.GetClientId(call);
	
        if (connid == null)
        {
            reply = call.err_reply(
                    "org.freedesktop.DBus.Error.Failed",
                    "Could not identify the client");
            return;
        }
        
	var it = call.iter();

	string query = query_parser(it.pop(),
			    VxSqlPool.access_restrictions(connid));

        log.print(WvLog.L.Debug3, "ExecChunkRecordset {0}\n", query);

	//Times we tried going through this loop to completion
	int numtries = 0;

	while (true)
	{
	    try
	    {
		List<object[]> rows = new List<object[]>();
		List<byte[]> rownulls = new List<byte[]>();
		VxColumnInfo[] colinfo;
		
		// Our size here is just an approximation.
		int cursize = 0;
		
		// FIXME:  Sadly, this is stupidly similar to ExecRecordset.
		// Anything we can do here to identify commonalities?
		using (var dbi = VxSqlPool.create(connid))
		using (WvSqlRows resultset = dbi.select(query))
		{
		    var columns = resultset.columns.ToArray();
		    colinfo = ProcessSchema(columns);
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
				    // FIXME: Check if getting a single causes
				    // this to croak
				    row[i] = !isnull ?
					(double)cval : (double)0.0;
				    cursize += sizeof(double);
				    break;
				case VxColumnType.Uuid:
				    //FIXME:  Do I work?
				    row[i] = !isnull ?
					(string)cval : "";
				    cursize += !isnull ?
					((string)cval).Length * sizeof(Char):0;
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
					((string)cval).Length * sizeof(Char):0;
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


			    SendChunkRecordSignal(conn, call, call.sender,
						  colinfo, rows.ToArray(),
						  rownulls.ToArray());
			
			    rows = new List<object[]>();
			    rownulls = new List<byte[]>();
			    cursize = 0;
			}
		    } // row iterator
		} // using

		// OK, we're down to either one more packet or no more data
		// Package that up in the 'reply' to this message, saving some
		// data being sent.
		if (cursize > 0)
		    log.print(WvLog.L.Debug4, "(Remaining data; {0} rows)\n",
			    rows.Count);

		// Create reply, either with or with no data
		WvDbusWriter replywriter =
		    VxDbusRouter.PrepareRecordsetWriter(colinfo,
							rows.ToArray(),
							rownulls.ToArray());
		reply = call.reply("a(issnny)vaay").write(replywriter);
		break;
	    } catch (DbException e) {
		throw new VxSqlException(e.Message, e);
	    } catch (VxConfigException e)
	    {
		if (e.Message.StartsWith("Connect: A network-related or instance-specific error")
		    && ++numtries <= 1)  //only one retry
		{
		    log.print(WvLog.L.Debug4,
			"Can't connect to DB, fishy..., attempting to reconnect... (attempt #{0})\n",
			numtries);
		}
		else
		    throw e;  //rethrow
	    }
	} //while
    }

    internal static void ExecRecordset(string connid, string query,
            out VxColumnInfo[] colinfo, out object[][] data,
            out byte[][] nullity)
    {
	query = query_parser(query,
			    VxSqlPool.access_restrictions(connid));

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
            }

            data = rows.ToArray();
            nullity = rownulls.ToArray();
            log.print(WvLog.L.Debug4, "({0} rows)\n", data.Length);
            wv.assert(nullity.Length == data.Length);
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

public class VxDbusRouter
{
    static WvLog log = new WvLog("VxDbusRouter");
    protected delegate
        void MethodCallProcessor(WvDbus conn, WvDbusMsg call,
				 out WvDbusMsg reply);
    
    public VxDbusRouter()
    {
    }
    
    public bool route(WvDbus conn, WvDbusMsg msg, out WvDbusMsg reply)
    {
	MethodCallProcessor p;
	
	if (msg.ifc != "vx.db" || msg.path != "/db")
	{
	    reply = null;
	    return false;
	}
	
	if (msg.method == "Test")
	    p = CallTest;
	else if (msg.method == "Quit")
	    p = CallQuit;
	else if (msg.method == "ExecScalar")
	    p = CallExecScalar;
	else if (msg.method == "ExecRecordset")
	    p = CallExecRecordset;
	else if (msg.method == "ExecChunkRecordset")
	    p = CallExecChunkRecordset;
	else if (msg.method == "GetSchemaChecksums")
	    p = CallGetSchemaChecksums;
	else if (msg.method == "GetSchema")
	    p = CallGetSchema;
	else if (msg.method == "PutSchema")
	    p = CallPutSchema;
	else if (msg.method == "DropSchema")
	    p = CallDropSchema;
	else if (msg.method == "GetSchemaData")
	    p = CallGetSchemaData;
	else if (msg.method == "PutSchemaData")
	    p = CallPutSchemaData;
	else
	{
	    // FIXME: this should be done at a higher level somewhere
            reply = msg.err_reply(
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    "Method name {0} not found on interface {1}",
                    msg.method, msg.ifc);
	    return true;
	}
	ExecuteCall(p, conn, msg, out reply);
	return true;
    }

    void ExecuteCall(MethodCallProcessor processor,
		     WvDbus conn,
		     WvDbusMsg call, out WvDbusMsg reply)
    {
        try {
            processor(conn, call, out reply);
        } catch (VxRequestException e) {
            reply = call.err_reply(e.DBusErrorType, e.Message);
            log.print("SQL result: {0}\n", e.Short());
        } catch (Exception e) {
            reply = call.err_reply("vx.db.exception", 
                    "An internal error occurred.");
            log.print("{0}\n", e.ToString());
        }
    }

    static Dictionary<string,string> usernames = new Dictionary<string, string>();
    
    public static string GetClientId(WvDbusMsg call)
    {
        string sender = call.sender;

        // For now, the client ID is just the username of the Unix UID that
        // DBus has associated with the connection.
        string username;
	var conn = Versaplexd.conn;
        if (!usernames.TryGetValue(sender, out username))
        {
	    try
	    {
		// FIXME:  This will likely change as we find a more
		//   universal way to do SSL authentication via D-Bus.
		username = VxSqlPool.GetUsernameForCert(
				conn.GetCertFingerprint(sender));
	    }
	    catch
	    {
		try
		{
		    // FIXME: This system call isn't actually standard
		    username = conn.GetUnixUserName(sender);
		}
		catch
		{
		    try
		    {
			// FIXME: This system call is standard, but not useful
			//   on Windows.
			username = conn.GetUnixUser(sender).ToString();
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

    static WvDbusMsg CreateUnknownMethodReply(WvDbusMsg call, 
        string methodname)
    {
        return call.err_reply("org.freedesktop.DBus.Error.UnknownMethod",
			      "No overload of {0} has signature '{1}'",
			      methodname, call.signature);
    }

    static void CallTest(WvDbus conn,
				 WvDbusMsg call, out WvDbusMsg reply)
    {
        if (call.signature.ne()) {
            reply = CreateUnknownMethodReply(call, "Test");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = call.err_reply("org.freedesktop.DBus.Error.Failed",
				   "Could not identify the client");
            return;
        }

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        VxDb.ExecRecordset(clientid, "select 'Works! :D'", 
            out colinfo, out data, out nullity);

        // FIXME: Add vx.db.toomuchdata error
	WvDbusWriter writer = PrepareRecordsetWriter(colinfo, data, nullity);
        reply = call.reply("a(issnny)vaay").write(writer);
    }

    static void CallQuit(WvDbus conn,
				 WvDbusMsg call, out WvDbusMsg reply)
    {
	// FIXME: Check permissions here
        WvDbusWriter writer = new WvDbusWriter();
	writer.Write("Quit");
        reply = call.reply("s").write(writer);
	Versaplexd.want_to_die = true;
    }

    static void CallExecScalar(WvDbus conn,
				       WvDbusMsg call, out WvDbusMsg reply)
    {
        if (call.signature != "s") {
            reply = CreateUnknownMethodReply(call, "ExecScalar");
            return;
        }

        if (call.Body == null) {
            reply = call.err_reply
		("org.freedesktop.DBus.Error.InvalidSignature",
		 "Signature provided but no body received");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = call.err_reply("org.freedesktop.DBus.Error.Failed",
				   "Could not identify the client");
            return;
        }

	var it = call.iter();
        string query = it.pop();

        object result;
	VxColumnType coltype;
        VxDb.ExecScalar(clientid, (string)query,
			out coltype, out result);

        WvDbusWriter writer = new WvDbusWriter();
	writer.WriteSig(VxColumnTypeToSignature(coltype));
	WriteV(writer, coltype, result);

        reply = call.reply("v").write(writer);
    }
    
    static void WriteColInfo(WvDbusWriter writer, VxColumnInfo[] colinfo)
    {
	// a(issnny)
	writer.WriteArray(8, colinfo, (w2, i) => {
	    w2.Write(i.size);
	    w2.Write(i.colname);
	    w2.Write(i.coltype.ToString());
	    w2.Write(i.precision);
	    w2.Write(i.scale);
	    w2.Write(i.nullable);
	});
    }

    public static void CallExecRecordset(WvDbus conn,
					 WvDbusMsg call, out WvDbusMsg reply)
    {
        if (call.signature != "s") {
            reply = CreateUnknownMethodReply(call, "ExecRecordset");
            return;
        }

        if (call.Body == null) {
            reply = call.err_reply
		("org.freedesktop.DBus.Error.InvalidSignature",
		 "Signature provided but no body received");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = call.err_reply("org.freedesktop.DBus.Error.Failed",
				   "Could not identify the client");
            return;
        }

	var it = call.iter();
        string query = it.pop();

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        VxDb.ExecRecordset(clientid, (string)query, 
            out colinfo, out data, out nullity);

        // FIXME: Add vx.db.toomuchdata error
	WvDbusWriter writer = PrepareRecordsetWriter(colinfo, data, nullity);
	
        reply = call.reply("a(issnny)vaay").write(writer);
    }

    static void CallExecChunkRecordset(WvDbus conn,
					       WvDbusMsg call, out WvDbusMsg reply)
    {
	// XXX: Stuff in this comment block shamelessly stolen from
	// "CallExecRecordset".
        if (call.signature != "s") {
            reply = CreateUnknownMethodReply(call, "ExecChunkRecordset");
            return;
        }

        if (call.Body == null) {
            reply = call.err_reply
		("org.freedesktop.DBus.Error.InvalidSignature",
		 "Signature provided but no body received");
            return;
        }
	/// XXX

        VxDb.ExecChunkRecordset(conn, call, out reply);
    }
    
    static string VxColumnTypeToSignature(VxColumnType t)
    {
	switch (t)
	{
	case VxColumnType.Int64:
	    return "x";
	case VxColumnType.Int32:
	    return "i";
	case VxColumnType.Int16:
	    return "n";
	case VxColumnType.UInt8:
	    return "y";
	case VxColumnType.Bool:
	    return "b";
	case VxColumnType.Double:
	    return "d";
	case VxColumnType.Uuid:
	    return "s";
	case VxColumnType.Binary:
	    return "ay";
	case VxColumnType.String:
	    return "s";
	case VxColumnType.DateTime:
	    return "(xi)";
	case VxColumnType.Decimal:
	    return "s";
	default:
	    throw new ArgumentException("Unknown VxColumnType");
	}
    }

    static string VxColumnInfoToArraySignature(VxColumnInfo[] vxci)
    {
        StringBuilder sig = new StringBuilder("a(");
        foreach (VxColumnInfo ci in vxci)
	    sig.Append(VxColumnTypeToSignature(ci.VxColumnType));
        sig.Append(")");

        return sig.ToString();
    }

    static void CallGetSchemaChecksums(WvDbus conn,
					       WvDbusMsg call, out WvDbusMsg reply)
    {
        if (call.signature.ne()) {
            reply = CreateUnknownMethodReply(call, "GetSchemaChecksums");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = call.err_reply("org.freedesktop.DBus.Error.Failed",
				   "Could not identify the client");
            return;
        }

        // FIXME: Add vx.db.toomuchdata error
        WvDbusWriter writer = new WvDbusWriter();

	//FIXME:  No exception catching?
        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            VxSchemaChecksums sums = backend.GetChecksums();
            sums.WriteChecksums(writer);
        }

        reply = call.reply(VxSchemaChecksums.GetDbusSignature()).write(writer);
    }

    static void CallGetSchema(WvDbus conn,
				      WvDbusMsg call, out WvDbusMsg reply)
    {
        if (call.signature != "as") {
            reply = CreateUnknownMethodReply(call, "GetSchema");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = call.err_reply("org.freedesktop.DBus.Error.Failed",
				   "Could not identify the client");
            return;
        }

	var it = call.iter();
        string[] names = it.pop().Cast<string>().ToArray();

        WvDbusWriter writer = new WvDbusWriter();

        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            VxSchema schema = backend.Get(names);
            schema.WriteSchema(writer);
        }

        reply = call.reply(VxSchema.GetDbusSignature()).write(writer);
    }

    static void CallDropSchema(WvDbus conn,
				       WvDbusMsg call, out WvDbusMsg reply)
    {
        if (call.signature != "as") {
            reply = CreateUnknownMethodReply(call, "DropSchema");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = call.err_reply("org.freedesktop.DBus.Error.Failed",
				   "Could not identify the client");
            return;
        }

	var it = call.iter();
	string[] keys = it.pop().Cast<string>().ToArray();

        VxSchemaErrors errs;
        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            errs = backend.DropSchema(keys);
        }

        WvDbusWriter writer = new WvDbusWriter();
        VxSchemaErrors.WriteErrors(writer, errs);

        reply = call.reply(VxSchemaErrors.GetDbusSignature()).write(writer);
        if (errs != null && errs.Count > 0)
        {
            reply.type = Wv.Dbus.MType.Error;
            reply.err = "org.freedesktop.DBus.Error.Failed";
        }
    }

    static void CallPutSchema(WvDbus conn,
				      WvDbusMsg call, out WvDbusMsg reply)
    {
        if (call.signature != String.Format("{0}i", 
                VxSchema.GetDbusSignature())) {
            reply = CreateUnknownMethodReply(call, "PutSchema");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = call.err_reply("org.freedesktop.DBus.Error.Failed",
				   "Could not identify the client");
            return;
        }

	var it = call.iter();
        VxSchema schema = new VxSchema(it.pop());
        int opts = it.pop();

        VxSchemaErrors errs;
        
        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            errs = backend.Put(schema, null, (VxPutOpts)opts);
        }

        WvDbusWriter writer = new WvDbusWriter();
        VxSchemaErrors.WriteErrors(writer, errs);

        reply = call.reply(VxSchemaErrors.GetDbusSignature()).write(writer);
        if (errs != null && errs.Count > 0)
        {
            reply.type = Wv.Dbus.MType.Error;
            reply.err = "org.freedesktop.DBus.Error.Failed";
        }
    }

    static void CallGetSchemaData(WvDbus conn,
					  WvDbusMsg call, out WvDbusMsg reply)
    {
        if (call.signature != "ss") {
            reply = CreateUnknownMethodReply(call, "GetSchemaData");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = call.err_reply("org.freedesktop.DBus.Error.Failed",
				   "Could not identify the client");
            return;
        }

	var it = call.iter();
        string tablename = it.pop();
	string where = it.pop();

        WvDbusWriter writer = new WvDbusWriter();

	// FIXME: No exception catching?
	// FIXME: Should receive the replace/skip parameters via dbus
        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            string schemadata = backend.GetSchemaData(tablename, 0, where,
						      null, null);
            writer.Write(schemadata);
        }

        reply = call.reply("s").write(writer);
    }

    static void CallPutSchemaData(WvDbus conn,
					  WvDbusMsg call, out WvDbusMsg reply)
    {
        if (call.signature != "ss") {
            reply = CreateUnknownMethodReply(call, "PutSchemaData");
            return;
        }

        string clientid = GetClientId(call);
        if (clientid == null)
        {
            reply = call.err_reply("org.freedesktop.DBus.Error.Failed",
				   "Could not identify the client");
            return;
        }

	var it = call.iter();
        string tablename = it.pop();
        string text = it.pop();

        using (var dbi = VxSqlPool.create(clientid))
        {
            VxDbSchema backend = new VxDbSchema(dbi);
            backend.PutSchemaData(tablename, text, 0);
        }

        reply = call.reply();
    }
    
    static void WriteV(WvDbusWriter w, VxColumnType t, object v)
    {
	switch (t)
	{
	case VxColumnType.Int64:
	    w.Write((Int64)v);
	    break;
	case VxColumnType.Int32:
	    w.Write((Int32)v);
	    break;
	case VxColumnType.Int16:
	    w.Write((Int16)v);
	    break;
	case VxColumnType.UInt8:
	    w.Write((byte)v);
	    break;
	case VxColumnType.Bool:
	    w.Write((bool)v);
	    break;
	case VxColumnType.Double:
	    w.Write((double)v);
	    break;
	case VxColumnType.Binary:
	    w.Write((byte[])v);
	    break;
	case VxColumnType.String:
	case VxColumnType.Decimal:
	case VxColumnType.Uuid:
	    w.Write((string)v);
	    break;
	case VxColumnType.DateTime:
	    {
		var dt = (VxDbusDateTime)v;
		w.Write(dt.seconds);
		w.Write(dt.microseconds);
		break;
	    }
	default:
	    throw new ArgumentException("Unknown VxColumnType");
	}
    }

    // a(issnny)vaay
    public static WvDbusWriter PrepareRecordsetWriter(VxColumnInfo[] colinfo,
						object[][] data,
						byte[][] nulldata)
    {
	WvDbusWriter writer = new WvDbusWriter();
	
	// a(issnny)
	WriteColInfo(writer, colinfo);
	
	// v
	if (colinfo.Length <= 0)
	{
	    // Some clients can't parse a() (empty struct) properly, so
	    // we'll have an empty array of (i) instead.
	    writer.WriteSig("a(i)");
	}
	else
	    writer.WriteSig(VxColumnInfoToArraySignature(colinfo));

	// a(whatever)
	writer.WriteArray(8, data, (w2, r) => {
	    for (int i = 0; i < colinfo.Length; i++)
		WriteV(w2, colinfo[i].VxColumnType, r[i]);
	});
	
	// aay
	writer.WriteArray(4, nulldata, (w2, r) => {
	    w2.Write(r);
	});

	return writer;
    }
}

