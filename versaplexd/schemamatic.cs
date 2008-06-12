using System;
using System.Collections.Generic;
using System.Text;
using NDesk.DBus;
using Wv;
using Wv.Extensions;

internal static class Schemamatic
{
    static WvLog log = new WvLog("Schemamatic", WvLog.L.Debug2);

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

    internal static void GetProcChecksums(VxSchemaChecksums sums, 
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

    internal static void GetTableChecksums(VxSchemaChecksums sums, 
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

    internal static void GetIndexChecksums(VxSchemaChecksums sums, 
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

    internal static void GetXmlSchemaChecksums(VxSchemaChecksums sums, 
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

    internal static string RetrieveProcSchemasQuery(string type, int encrypted, 
        bool countonly, List<string> names)
    {
        string name_q = names.Count > 0 
            ? " and object_name(id) in ('" + names.Join("','") + "')"
            : "";

        string textcol = encrypted > 0 ? "ctext" : "text";
        string cols = countonly 
            ? "count(*)"
            : "object_name(id), colid, " + textcol + " ";

        return "select " + cols + " from syscomments " + 
            "where objectproperty(id, 'Is" + type + "') = 1 " + 
                "and encrypted = " + encrypted + name_q;
    }

    internal static void RetrieveProcSchemas(VxSchema schema, List<string> names, 
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

    internal static void RetrieveIndexSchemas(VxSchema schema, List<string> names, 
        string clientid)
    {
        string idxnames = (names.Count > 0) ? 
            "and ((object_name(i.object_id)+'/'+i.name) in ('" + 
                names.Join("','") + "'))"
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
                string colstr = cols.Join(",");
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

    internal static string XmlSchemasQuery(int count, List<string> names)
    {
        int start = count * 4000;

        string namestr = (names.Count > 0) ? 
            "and xsc.name in ('" + names.Join("','") + "')"
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

    internal static void RetrieveXmlSchemas(VxSchema schema, List<string> names, 
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

    internal static void RetrieveTableColumns(VxSchema schema, 
        List<string> names, string clientid)
    {
        string tablenames = (names.Count > 0 
            ? "and t.name in ('" + names.Join("','") + "')"
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
                    tabname, cols.Join(",\n\t"));
                schema.Add(tabname, "Table", tablestr, false);

                cols.Clear();
            }
        }
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

    internal static void DropSchema(string clientid, 
        string type, string name)
    {
        string query = GetDropCommand(type, name);

        object result;
        VxDb.ExecScalar(clientid, query, out result);
    }

    internal static void PutSchema(string clientid, string type, string name, 
        string text, byte destructive)
    {
        if (destructive > 0 || !type.StartsWith("Table"))
        {
            try { 
                DropSchema(clientid, type, name); 
            } catch (VxSqlException e) {
                // Check if it's a "didn't exist" error, rethrow otherwise.
                // SQL Error 3701 means "can't drop sensible item because
                // it doesn't exist or you don't have permission."
                // SQL Error 15151 means "can't drop XML Schema collection 
                // because it doesn't exist or you don't have permission."
                if (!e.ContainsSqlError(3701) && !e.ContainsSqlError(15151))
                    throw e;
            }
        }

        object result;
        VxDb.ExecScalar(clientid, text, out result);
    }

    internal static string GetSchemaData(string clientid, string tablename)
    {
        string query = "SELECT * FROM " + tablename;

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        
        VxDb.ExecRecordset(clientid, query, out colinfo, out data, out nullity);

        List<string> cols = new List<string>();
        foreach (VxColumnInfo ci in colinfo)
            cols.Add("[" + ci.ColumnName + "]");

        string prefix = String.Format("INSERT INTO {0} ({1}) VALUES (", 
            tablename, cols.Join(","));

        StringBuilder result = new StringBuilder();
        List<string> values = new List<string>();
        for(int ii = 0; ii < data.Length; ii++)
        {
            object[] row = data[ii];
            values.Clear();
            for (int jj = 0; jj < row.Length; jj++)
            {
                object elem = row[jj];
                VxColumnInfo ci = colinfo[jj];
                log.print("Col {0}, name={1}, type={2}\n", jj, 
                    ci.ColumnName, ci.VxColumnType.ToString());
                if (elem == null)
                    values.Add("NULL");
                else if (ci.VxColumnType == VxColumnType.String ||
                    ci.VxColumnType == VxColumnType.DateTime)
                {
                    // Double-quote chars for SQL safety
                    string esc = elem.ToString().Replace("'", "''");
                    values.Add("'" + esc + "'");
                }
                else
                    values.Add(elem.ToString());
            }
            result.Append(prefix + values.Join(",") + ");\n");

            if (ii > 0 && (ii % 10) == 0)
                result.Append("GO\n");
        }

        return result.ToString();
    }
}
