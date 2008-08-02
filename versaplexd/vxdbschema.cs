using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using Wv;

// An ISchemaBackend that uses a direct database connection as a backing
// store.
internal class VxDbSchema : ISchemaBackend
{
    internal static string[] ProcedureTypes = new string[] { 
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

    string connstr;
    WvLog log;
    public VxDbSchema(string _connstr)
    {
        connstr = _connstr;
        log = new WvLog("VxDbSchema", WvLog.L.Debug2);
    }

    //
    // The ISchema interface
    //

    public VxSchemaErrors Put(VxSchema schema, VxSchemaChecksums sums, 
        VxPutOpts opts)
    {
        bool no_retry = (opts & VxPutOpts.NoRetry) != 0;
        int old_err_count = -1;
        IEnumerable<string> keys = schema.Keys;
        VxSchemaErrors errs = new VxSchemaErrors();

        // Sometimes we'll get schema elements in the wrong order, so retry
        // until the number of errors stops decreasing.
        while (errs.Count != old_err_count)
        {
            log.print("Calling Put on {0} entries\n", 
                old_err_count == -1 ? schema.Count : errs.Count);
            old_err_count = errs.Count;
            errs.Clear();
            foreach (string key in keys)
            {
                try {
                    log.print("Calling PutSchema on {0}\n", key);
                    PutSchemaElement(schema[key], opts);
                } catch (VxSqlException e) {
                    log.print("Got error from {0}\n", key);
                    errs.Add(key, new VxSchemaError(key, e.Message, 
                        e.GetFirstSqlErrno()));
                }
            }
            // If we only had one schema element, retrying it isn't going to
            // fix anything.  We retry to fix ordering problems.
            if (no_retry || errs.Count == 0 || schema.Count == 1)
                break;

            log.print("Got {0} errors, old_errs={1}, retrying\n", 
                errs.Count, old_err_count);

            keys = errs.Keys.ToList();
        }
        return errs.Count > 0 ? errs : null;
    }

    // FIXME: NYI
    public VxSchema Get(IEnumerable<string> keys)
    {
        return new VxSchema();
    }

    public VxSchemaChecksums GetChecksums()
    {
        VxSchemaChecksums sums = new VxSchemaChecksums();

        foreach (string type in ProcedureTypes)
        {
            if (type == "Procedure")
            {
                // Set up self test
                SqlExecScalar("create procedure schemamatic_checksum_test " + 
                    "as print 'hello' ");
            }

            GetProcChecksums(sums, type, 0);

            if (type == "Procedure")
            {
                SqlExecScalar("drop procedure schemamatic_checksum_test");

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
                    // FIXME
                    /*
                    reply = VxDbus.CreateError(
                        "org.freedesktop.DBus.Error.Failed",
                        String.Format("checksum_test mismatch! {0} != {1}", 
                            got_csum, want_csum), call);
                    */
                    throw new Exception(String.Format("checksum_test_mismatch!"
                        + " {0} != {1}", got_csum, want_csum));
                }
                sums.Remove(test_csum_label);
            }

            GetProcChecksums(sums, type, 1);
        }

        // Do tables separately
        GetTableChecksums(sums);

        // Do indexes separately
        GetIndexChecksums(sums);

        // Do XML schema collections separately (FIXME: only if SQL2005)
        GetXmlSchemaChecksums(sums);

        return sums;
    }

    // 
    // Non-ISchemaBackend methods
    //

    private SqlConnection GetConnection()
    {
        SqlConnection con = new SqlConnection(connstr);
        con.Open();
        return con;
    }

    private void ReleaseConnection(SqlConnection conn)
    {
        if (conn != null)
            conn.Close();
    }

    // FIXME: An alarming duplicate of VxDb.ExecScalar.
    private object SqlExecScalar(string query)
    {
        SqlConnection conn = null;
        try
        {
            conn = GetConnection();
            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = query;
                return cmd.ExecuteScalar();
            }
        }
        catch (SqlException e)
        {
            throw new VxSqlException(e.Message, e);
        }
        finally
        {
            ReleaseConnection(conn);
        }
    }

    // Like VxDb.ExecRecordset, except we don't care about colinfo or nullity
    private object[][] SqlExecRecordset(string query)
    {
        List<object[]> result = new List<object[]>();
        SqlConnection conn = null;
        try
        {
            conn = GetConnection();
            using (SqlCommand cmd = new SqlCommand(query, conn))
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    object[] row = new object[reader.FieldCount];
                    reader.GetValues(row);
                    result.Add(row);
                }
            }
        }
        catch (SqlException e)
        {
            throw new VxSqlException(e.Message, e);
        }
        finally
        {
            ReleaseConnection(conn);
        }

        return result.ToArray();
    }

    private static string GetDropCommand(string type, string name)
    {
        if (type.StartsWith("Function"))
            type = "Function";
        else if (type == "XMLSchema")
            type = "XML Schema Collection";
        else if (type == "Index")
        {
            string[] parts = name.Split(new char[] {'/'}, 2);
            if (parts.Length == 2)
            {
                string tabname = parts[0];
                string idxname = parts[1];
                return String.Format(
                    @"declare @x int;
                      select @x = is_primary_key 
                          from sys.indexes 
                          where object_name(object_id) = '{0}' 
                            and name = '{1}';
                      if @x = 1 
                          ALTER TABLE [{0}] DROP CONSTRAINT [{1}]; 
                      else 
                          DROP {2} [{0}].[{1}]", 
                    tabname, idxname, type);
            } 
            else 
                throw new ArgumentException(String.Format(
                    "Invalid index name '{0}'!", name));
        }

        return String.Format("DROP {0} [{1}]", type, name);
    }

    // Deletes the named object in the database.
    public void DropSchema(string type, string name)
    {
        string query = GetDropCommand(type, name);

        SqlExecScalar(query);
    }

    // Replaces the named object in the database.  elem.text is a verbatim
    // hunk of text returned earlier by GetSchema.  'destructive' says whether
    // or not to perform potentially destructive operations while making the
    // change, e.g. dropping a table so we can re-add it with the right
    // columns.
    private void PutSchemaElement(VxSchemaElement elem, VxPutOpts opts)
    {
        bool destructive = (opts & VxPutOpts.Destructive) != 0;
        if (destructive || !elem.type.StartsWith("Table"))
        {
            try { 
                DropSchema(elem.type, elem.name); 
            } catch (VxSqlException e) {
                // Check if it's a "didn't exist" error, rethrow if not.
                // SQL Error 3701 means "can't drop sensible item because
                // it doesn't exist or you don't have permission."
                // SQL Error 15151 means "can't drop XML Schema collection 
                // because it doesn't exist or you don't have permission."
                if (!e.ContainsSqlError(3701) && !e.ContainsSqlError(15151))
                    throw e;
            }
        }

        if (elem.text != null && elem.text != "")
            SqlExecScalar(elem.text);
    }

    internal void GetProcChecksums(VxSchemaChecksums sums, 
            string type, int encrypted)
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

        object[][] data = SqlExecRecordset(query);

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

    internal void GetTableChecksums(VxSchemaChecksums sums)
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

        object[][] data = SqlExecRecordset(query);

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

    internal void GetIndexChecksums(VxSchemaChecksums sums)
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

        object[][] data = SqlExecRecordset(query);

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

    internal void GetXmlSchemaChecksums(VxSchemaChecksums sums)
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

        object[][] data = SqlExecRecordset(query);

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

}

