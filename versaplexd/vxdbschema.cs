using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Wv;
using Wv.Extensions;

// An ISchemaBackend that uses a direct database connection as a backing
// store.
[WvMoniker]
internal class VxDbSchema : ISchemaBackend
{
    static WvLog log = new WvLog("VxDbSchema", WvLog.L.Debug2);

    public static void wvmoniker_register()
    {
	WvMoniker<ISchemaBackend>.register("dbi",
		  (string m, object o) => new VxDbSchema(WvDbi.create(m)));
    }
	
    static string[] ProcedureTypes = new string[] { 
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

    WvDbi dbi;

    public VxDbSchema(WvDbi _dbi)
    {
        dbi = _dbi;
        dbi.execute("set quoted_identifier off");
        dbi.execute("set ansi_nulls on");
    }

    //
    // The ISchema interface
    //

    public VxSchemaErrors Put(VxSchema schema, VxSchemaChecksums sums, 
        VxPutOpts opts)
    {
        log.print("Put\n");
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

            List<string> tables = new List<string>();
            List<string> nontables = new List<string>();
            foreach (string key in keys)
            {
                if (schema[key].type == "Table")
                    tables.Add(key);
                else
                    nontables.Add(key);
            }

            errs.Add(PutSchemaTables(tables, schema, sums, opts));
            foreach (string key in nontables)
            {
                log.print("Calling PutSchema on {0}\n", key);
                VxSchemaError e = PutSchemaElement(schema[key], opts);
                if (e != null)
                    errs.Add(key, e);
            }
            // If we only had one schema element, retrying it isn't going to
            // fix anything.  We retry to fix ordering problems.
            if (no_retry || errs.Count == 0 || schema.Count == 1)
                break;

            log.print("Got {0} errors, old_errs={1}, retrying\n", 
                errs.Count, old_err_count);

            keys = errs.Keys.ToList();
        }
        return errs;
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

    public VxSchema Get(IEnumerable<string> keys)
    {
        log.print("Get\n");
        List<string> all_names = new List<string>();
        List<string> proc_names = new List<string>();
        List<string> xml_names = new List<string>();
        List<string> tab_names = new List<string>();
        // FIXME: This variable is unused.  Get rid of it, and perhaps throw
        // an error if we see an index show up.
        List<string> idx_names = new List<string>();

        foreach (string key in keys)
        {
            string fullname = EscapeSchemaElementName(key);
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
                RetrieveProcSchemas(schema, proc_names, type, 0);
                RetrieveProcSchemas(schema, proc_names, type, 1);
            }
        }

        if (xml_names.Count > 0 || all_names.Count == 0)
            RetrieveXmlSchemas(schema, xml_names);

        if (tab_names.Count > 0 || all_names.Count == 0)
            RetrieveTableSchema(schema, tab_names);

        return schema;
    }

    public VxSchemaChecksums GetChecksums()
    {
        log.print("GetChecksums\n");
        VxSchemaChecksums sums = new VxSchemaChecksums();

        foreach (string type in ProcedureTypes)
        {
            try
            {
                if (type == "Procedure")
                {
                    // Set up self test
                    DbiExec("create procedure schemamatic_checksum_test " + 
                        "as print 'hello' ");
                }

                GetProcChecksums(sums, type, 0);

                if (type == "Procedure")
                {
                    // Self-test the checksum feature.  If mssql's checksum
                    // algorithm changes, we don't want to pretend our checksum
                    // list makes any sense!
                    string test_csum = "Procedure/schemamatic_checksum_test";
                    ulong got_csum = 0;
                    if (sums.ContainsKey(test_csum))
                        got_csum = sums[test_csum].checksums.First();
                    ulong want_csum = 0x173d6ee8;
                    if (want_csum != got_csum)
                    {
                        throw new Exception(String.Format(
                            "checksum_test_mismatch! {0} != {1}", 
                            got_csum, want_csum));
                    }
                    sums.Remove(test_csum);
                }
            }
            finally
            {
                if (type == "Procedure")
                {
                    DbiExec("drop procedure schemamatic_checksum_test");
                }
            }

            GetProcChecksums(sums, type, 1);
        }

        // Do tables separately
        GetTableChecksums(sums);

        // Do indexes separately
        AddIndexChecksumsToTables(sums);

        // Do XML schema collections separately (FIXME: only if SQL2005)
        GetXmlSchemaChecksums(sums);

        return sums;
    }

    // Deletes the named objects in the database.
    public VxSchemaErrors DropSchema(IEnumerable<string> keys)
    {
        return DropSchema(keys.ToArray());
    }

    // Deletes the named objects in the database.
    public VxSchemaErrors DropSchema(params string[] keys)
    {
        VxSchemaErrors errs = new VxSchemaErrors();
        foreach (string key in keys)
        {
            VxSchemaError e = DropSchemaElement(key);
            if (e != null)
                errs.Add(key, e);
        }

        return errs;
    }

    // 
    // Non-ISchemaBackend methods
    //
    
    public VxSchemaError DropSchemaElement(string key)
    {
        log.print("DropSchemaElement({0})\n", key);
        if (key == null)
            return null;

        string type, name;
        VxSchema.ParseKey(key, out type, out name);
        if (type == null || name == null)
            return new VxSchemaError(key, "Malformed key: " + key, -1);

        string query = GetDropCommand(type, name);

        try {
            DbiExec(query);
        } catch (VxSqlException e) {
            log.print("Got error dropping {0}: {1} ({2})\n", key, 
                e.Message, e.Number);
            return new VxSchemaError(key, e);
        }

        return null;
    }

    // Translate SqlExceptions from dbi.execute into VxSqlExceptions
    private int DbiExec(string query, params string[] args)
    {
        try
        {
            return dbi.execute(query, args);
        }
        catch (DbException e)
        {
            throw new VxSqlException(e.Message, e);
        }
    }

    // Roll back the given transaction, but eat SQL error 3903, where the
    // rollback fails because it's already been rolled back.
    // Returns true if the transaction was actually rolled back.
    // Returns false (or throws some other exception) if the rollback failed.
    private bool DbiExecRollback(string trans_name)
    {
        try 
        { 
            dbi.execute("ROLLBACK TRANSACTION " + trans_name); 
            return true;
        } 
        catch (SqlException e)
        { 
            log.print("Caught rollback exception: {0} ({1})\n", 
                e.Message, e.Number);
            // Eat any "The Rollback Transaction request has no
            // corresponding Begin Transaction." errors - some errors 
            // will automatically roll us back, some won't.
            if (e.Number != 3903)
                throw;
        }
        return false;
    }

    // Translate SqlExceptions from dbi.select into VxSqlExceptions.
    private IEnumerable<WvSqlRow> DbiSelect(string query, 
        params object[] bound_vars)
    {
        log.print("In DbiSelect\n");
        try
        {
            return dbi.select(query, bound_vars).ToArray();
        }
        catch (DbException e)
        {
            throw new VxSqlException(e.Message, e);
        }
    }

    private static string GetDropCommand(string type, string name)
    {
        if (type.EndsWith("Function"))
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

    private VxSchemaErrors PutSchemaTables(List<string> tables, 
        VxSchema newschema, VxSchemaChecksums newsums, VxPutOpts opts)
    {
        VxSchema curschema = Get(tables);
        VxSchemaErrors errs = new VxSchemaErrors();

        foreach (string key in tables)
        {
            log.print("Putting table {0}\n", key);
            string curtype = curschema.ContainsKey(key) ? 
                curschema[key].type : "Table";
            string newtype = newschema.ContainsKey(key) ? 
                newschema[key].type : "Table";

            if (newtype != "Table" || curtype != "Table")
                throw new ArgumentException("PutSchemaTables called on " + 
                    "non-table element '" + key + "'.");

            // Check for the easy cases, an all-new table or table deletion
            if (!curschema.ContainsKey(key))
            {
                // New table, let PutSchemaElement handle it like before.
                VxSchemaError e = PutSchemaElement(newschema[key], opts);
                if (e != null)
                    errs.Add(key, e);
                continue;
            }
            if (!newschema.ContainsKey(key))
            {
                // Deleted table, let DropSchemaElement deal with it.
                VxSchemaError e = DropSchemaElement(key);
                if (e != null)
                    errs.Add(key, e);
                continue;
            }

            // An existing table has been modified.

            VxSchemaTable newtable;
            VxSchemaTable curtable;
            if (newschema[key] is VxSchemaTable)
                newtable = (VxSchemaTable)newschema[key];
            else
                newtable = new VxSchemaTable(newschema[key]);

            if (curschema[key] is VxSchemaTable)
                curtable = (VxSchemaTable)curschema[key];
            else
                curtable = new VxSchemaTable(curschema[key]);

            VxSchemaErrors put_table_errs = null;
            put_table_errs = PutSchemaTable(curtable, newtable, opts);

            // If anything goes wrong updating a table in destructive mode, 
            // drop and re-add it.  We want to be sure the schema is updated
            // exactly.
            bool destructive = (opts & VxPutOpts.Destructive) != 0;
            if (destructive && put_table_errs.Count > 0)
            {
                put_table_errs = null;

                log.print("Couldn't cleanly modify table '{0}'.  Dropping " + 
                    "and re-adding it.\n", newtable.name);
                VxSchemaError e = PutSchemaElement(newschema[key], opts);

                if (e != null)
                    errs.Add(key, e);
            }

            if (put_table_errs != null && put_table_errs.Count > 0)
                errs.Add(put_table_errs);
        }

        return errs;
    }

    private VxSchemaError PutSchemaTableIndex(string key, VxSchemaTable table, 
        VxSchemaTableElement elem)
    {
        string query = "";
        if (elem.elemtype == "primary-key")
            query = table.PrimaryKeyToSql(elem);
        else if (elem.elemtype == "index")
            query = table.IndexToSql(elem);
        else
            return new VxSchemaError(key, wv.fmt(
                "Unknown table element '{0}'.", elem.elemtype), -1);

        try 
        {
            if (query != "")
                dbi.execute(query);
        }
        catch (SqlException e)
        {
            return new VxSchemaError(key, e);
        }

        return null;
    }

    // Create a new table element that allows nulls
    private VxSchemaTableElement GetNullableColumn(VxSchemaTableElement elem)
    {
        var nullable = new VxSchemaTableElement(elem.elemtype);
        foreach (var kvp in elem.parameters)
            if (kvp.Key == "null")
                nullable.AddParam("null", "1");
            else
                nullable.AddParam(kvp.Key, kvp.Value);

        return nullable;
    }

    private void DropTableColumn(VxSchemaTable table, VxSchemaTableElement col)
    {
        string colname = col.GetParam("name");
        if (col.HasDefault())
        {
            string defquery = wv.fmt("ALTER TABLE [{0}] " + 
                "DROP CONSTRAINT {1}", 
                table.name, table.GetDefaultDefaultName(colname));

            dbi.execute(defquery);
        }

        string query = wv.fmt("ALTER TABLE [{0}] DROP COLUMN [{1}]",
            table.name, colname);

        dbi.execute(query);
    }

    private VxSchemaErrors ApplyChangedColumn(VxSchemaTable table, 
        VxSchemaTableElement oldelem, VxSchemaTableElement newelem, 
        VxSchemaError expected_err, VxPutOpts opts)
    {
        VxSchemaErrors errs = new VxSchemaErrors();
        log.print("Altering {0}\n", newelem.ToString());

        bool destructive = (opts & VxPutOpts.Destructive) != 0;
        string colname = newelem.GetParam("name");

        // Remove any old default constraint; even if it doesn't change, it 
        // can get in the way of modifying the column.  We'll add it again
        // later if needed.
        if (oldelem.HasDefault())
        {
            string defquery = wv.fmt("ALTER TABLE [{0}] DROP CONSTRAINT {1}", 
                table.name, table.GetDefaultDefaultName(colname));

            log.print("Executing {0}\n", defquery);

            dbi.execute(defquery);
        }

        bool did_default_constraint = false;

        // Don't try to alter the table if we know it won't work.
        if (expected_err == null)
        {
            string query = wv.fmt("ALTER TABLE [{0}] ALTER COLUMN {1}",
                table.name, table.ColumnToSql(newelem, false));

            log.print("Executing {0}\n", query);
            
            dbi.execute(query);
        }
        else
        {
            // Some table attributes can't be changed by ALTER TABLE, 
            // such as changing identity values, or data type changes that
            // would truncate data.  If the client has set the Destructive
            // flag though, we can try to drop and re-add the column.
            if (destructive)
            {
                log.print("Alter column would fail, dropping and adding.\n");
                log.print("Expected error message: {0} ({1})\n", 
                    expected_err.msg, expected_err.errnum);
                string delquery = wv.fmt("ALTER TABLE [{0}] " + 
                    "DROP COLUMN [{1}]",
                    table.name, colname);
                // We need to include the default value here (the second
                // parameter to ColumnToSql), otherwise adding a column to a
                // table with data in it might not work.
                string addquery = wv.fmt("ALTER TABLE [{0}] ADD {1}", 
                    table.name, table.ColumnToSql(newelem, true));

                log.print("Executing {0}\n", delquery);
                dbi.execute(delquery);
                log.print("Executing {0}\n", addquery);
                dbi.execute(addquery);
                did_default_constraint = true;
            }
            else
            {
                // Error 515: Can't modify a column because it contains nulls 
                // and the column requires non-nulls.
                if (expected_err.errnum == 515)
                {
                    log.print("Couldn't modify column due to null " + 
                        "restriction.  Making column nullable.\n");
                    var nullable = GetNullableColumn(newelem);

                    string query = wv.fmt("ALTER TABLE [{0}] ALTER COLUMN {1}",
                        table.name, table.ColumnToSql(nullable, false));

                    log.print("Executing {0}\n", query);
                    
                    dbi.execute(query);
                }
                else
                {
                    log.print("Can't alter table and destructive flag " + 
                        "not set.  Giving up.\n");
                    string key = table.key;
                    string errmsg = wv.fmt("Refusing to drop and re-add " +
                            "column [{0}] when the destructive option " +
                            "is not set.  Error when altering was: '{1}'",
                            colname, expected_err.msg);
                    errs.Add(key, new VxSchemaError(key, errmsg, -1));
                }
            }
        }

        // No errors so far, let's try to add the new default values if we
        // didn't do it already.
        // FIXME: Check for actual errors, don't care about warnings.
        if (errs.Count == 0 && newelem.HasDefault() && !did_default_constraint)
        {
            string defquery = wv.fmt("ALTER TABLE [{0}] ADD CONSTRAINT {1} " + 
                "DEFAULT {2} FOR {3}", 
                table.name, table.GetDefaultDefaultName(colname), 
                newelem.GetParam("default"), colname);

            log.print("Executing {0}\n", defquery);

            dbi.execute(defquery);
        }

        if (errs.Count != 0)
            log.print("Altering column had errors: " + errs.ToString());

        return errs;
    }

    private VxSchemaErrors PutSchemaTable(VxSchemaTable curtable, 
        VxSchemaTable newtable, VxPutOpts opts)
    {
        bool destructive = (opts & VxPutOpts.Destructive) != 0;

        string tabname = newtable.name;
        string key = newtable.key;

        var diff = VxSchemaTable.GetDiff(curtable, newtable);

        var coladd = new List<VxSchemaTableElement>();
        var coldel = new List<VxSchemaTableElement>();
        var colchanged = new List<VxSchemaTableElement>();
        var otheradd = new List<VxSchemaTableElement>();
        var otherdel = new List<VxSchemaTableElement>();
        foreach (var kvp in diff)
        {
            VxSchemaTableElement elem = kvp.Key;
            VxDiffType difftype = kvp.Value;
            if (elem.elemtype == "primary-key" || elem.elemtype == "index")
            {
                if (difftype == VxDiffType.Add)
                    otheradd.Add(elem);
                else if (difftype == VxDiffType.Remove)
                    otherdel.Add(elem);
                else if (difftype == VxDiffType.Change)
                {
                    // We don't want to bother trying to change indexes or
                    // primary keys; it's easier to just delete and re-add
                    // them.
                    otherdel.Add(curtable[elem.GetElemKey()]);
                    otheradd.Add(elem);
                }
            }
            else
            {
                if (difftype == VxDiffType.Add)
                    coladd.Add(elem);
                else if (difftype == VxDiffType.Remove)
                    coldel.Add(elem);
                else if (difftype == VxDiffType.Change)
                    colchanged.Add(elem);
            }
        }

        var errs = new VxSchemaErrors();

        // Might as well check this sooner rather than later.
        if (!destructive && coldel.Count > 0)
        {
            List<string> colstrs = new List<string>();
            foreach (var elem in coldel)
                colstrs.Add(elem.GetParam("name"));
            // Sorting this is mostly unnecessary, except it makes life a lot
            // nicer in the unit tests.
            colstrs.Sort();

            string errmsg = wv.fmt("Refusing to drop columns ([{0}]) " + 
                    "when the destructive option is not set.", 
                    colstrs.Join("], ["));
            errs.Add(key, new VxSchemaError(key, errmsg, -1));
            goto done;
        }

        // Perform any needed column changes.
        // Note: we call dbi.execute directly, instead of DbiExec, as we're
        // running SQL we generated ourselves so we shouldn't blame any 
        // errors on the client's SQL.  We'll catch the DbExceptions and 
        // turn them into VxSchemaErrors.

        var deleted_indexes = new List<VxSchemaTableElement>();
        var added_columns = new List<VxSchemaTableElement>(); 

        bool transaction_started = false;
        bool transaction_resolved = false;
        try
        {
            // Delete any to-delete indexes first, to get them out of the way.
            // Indexes are easy to deal with, they don't cause data loss.
            // Note: we can't do this inside the transaction, MSSQL doesn't
            // let you change columns that used to be covered by the dropped
            // indexes.  Instead we'll drop the indexes outside the
            // transaction, and restore them by hand if there's an error.
            foreach (var elem in otherdel)
            {
                log.print("Dropping {0}\n", elem.ToString());
                string idxname = elem.GetParam("name");

                // Use the default primary key name if none was specified.
                if (elem.elemtype == "primary-key" && idxname.e())
                    idxname = curtable.GetDefaultPKName();

                var err = DropSchemaElement("Index/" + tabname + "/" + idxname);
                if (err != null)
                {
                    errs.Add(key, err);
                    goto done;
                }

                deleted_indexes.Add(elem);
            }

            // If an ALTER TABLE query fails inside a transaction, the 
            // transaction is automatically rolled back, even if you start
            // an inner transaction first.  This makes error handling
            // annoying.  So before we start the real transaction, try to make
            // the column changes in a test transaction that we'll always roll
            // back to see if they'd fail.
            var ErrWhenAltering = new Dictionary<string, VxSchemaError>();
            foreach (var elem in colchanged)
            {
                VxSchemaError err = null;
                log.print("Doing a trial run of modifying {0}\n", 
                    elem.GetElemKey());
                dbi.execute("BEGIN TRANSACTION coltest");
                try
                {
                    // Try to change the column the easy way, without dropping
                    // or adding anything and without any expected errors.
                    var change_errs = ApplyChangedColumn(newtable, 
                        curtable[elem.GetElemKey()], elem, null, VxPutOpts.None);
                    if (change_errs.Count > 0)
                        err = change_errs[newtable.key][0];
                }
                catch (SqlException e)
                {
                    // OK, the easy way doesn't work.  Remember the error for
                    // when we do it for real.
                    log.print("Caught exception in trial run: {0} ({1})\n", 
                        e.Message, e.Number);
                    err = new VxSchemaError(key, e);
                }

                log.print("Rolling back, err='{0}'\n", 
                    err == null ? "" : err.ToString());

                DbiExecRollback("coltest");

                ErrWhenAltering.Add(elem.GetElemKey(), err);
            }
            log.print("About to begin real transaction\n");

            // Add new columns before deleting old ones; MSSQL won't let a
            // table have no data columns in it, even temporarily.
            // Do this outside the transaction since failures here will
            // automatically cause a rollback, even if we handle them.
            // It's easy enough for us to roll back by hand if needed.
            foreach (var elem in coladd)
            {
                log.print("Adding {0}\n", elem.ToString());
                string add_format = "ALTER TABLE [{0}] ADD {1}\n"; 
                string query = wv.fmt(add_format, 
                    tabname, newtable.ColumnToSql(elem, true));

                try
                {
                    dbi.execute(query);
                }
                catch (SqlException e)
                {
                    // Error 4901: adding a column on a non-empty table failed
                    // due to neither having a default nor being nullable.
                    // Don't try anything special in destructive mode, just
                    // fail and nuke the table.
                    if (!destructive && e.Number == 4901)
                    {
                        log.print("Couldn't add a new non-nullable column " +
                            "without a default.  Making column nullable.\n");
                        var nullable = GetNullableColumn(elem);

                        string nullquery = wv.fmt(add_format, 
                            tabname, newtable.ColumnToSql(nullable, true));

                        log.print("Executing {0}", nullquery);
                        dbi.execute(nullquery);
                    }
                    else
                        throw;
                }
                added_columns.Add(elem);
            }

            transaction_started = true;
            dbi.execute("BEGIN TRANSACTION TableUpdate");

            foreach (var elem in coldel)
            {
                log.print("Dropping {0}\n", elem.ToString());
                DropTableColumn(newtable, elem);
            }

            foreach (var elem in colchanged)
            {
                var expected_err = ErrWhenAltering[elem.GetElemKey()];
                var change_errs = ApplyChangedColumn(newtable, 
                    curtable[elem.GetElemKey()], elem, expected_err, opts);

                if (change_errs != null && change_errs.Count > 0)
                {
                    errs.Add(change_errs);
                    goto done;
                }
            }

            // Now that all the columns are finalized, add in any new indices.
            foreach (var elem in otheradd)
            {
                log.print("Adding {0}\n", elem.ToString());
                VxSchemaError err = PutSchemaTableIndex(key, curtable, elem);
                if (err != null)
                {
                    errs.Add(key, err);
                    goto done;
                }
            }

            log.print("All changes made, committing transaction.\n");

            dbi.execute("COMMIT TRANSACTION TableUpdate");
            transaction_resolved = true;
        }
        catch (SqlException e)
        {
            var err = new VxSchemaError(key, e);
            log.print("Caught exception: {0}\n", err.ToString());
            errs.Add(key, err);
        }
        finally
        {
            if (transaction_started && !transaction_resolved)
            {
                log.print("Transaction failed, rolling back.\n");
                if (transaction_started)
                    DbiExecRollback("TableUpdate");

                foreach (var elem in added_columns)
                {
                    log.print("Restoring {0}\n", elem.ToString());
                    try
                    {
                        DropTableColumn(newtable, elem);
                    }
                    catch (SqlException e)
                    { 
                        log.print("Caught error clearing column: {0}\n",
                            e.Message);
                    }
                }

                foreach (var elem in deleted_indexes)
                {
                    log.print("Restoring index {0}\n", elem.ToString());
                    var err = PutSchemaTableIndex(key, curtable, elem);
                    if (err != null)
                        errs.Add(key, err);
                }
            }
        }

        // Check for null entries in columns that are supposed to be non-null
        if (errs.Count == 0)
        {
            foreach (var elem in newtable)
            {
                string nullity = elem.GetParam("null");
                if (elem.elemtype == "column" && nullity.ne() && nullity != "1")
                {
                    string colname = elem.GetParam("name");
                    string query = wv.fmt("SELECT count(*) FROM [{0}] " + 
                        "WHERE [{1}] IS NULL",
                        tabname, colname);

                    int num_nulls = -1;
                    try
                    {
                        num_nulls = dbi.select_one(query);
                    }
                    catch (SqlException e)
                    {
                        string errmsg = wv.fmt(
                            "Couldn't figure out if '{0}' has nulls: {1}",
                            colname, e.Message);
                        log.print(errmsg + "\n");
                        errs.Add(key, new VxSchemaError(
                            key, errmsg, -1, WvLog.L.Warning));
                    }

                    if (num_nulls > 0)
                    {
                        string errmsg = wv.fmt("Column '{0}' was requested " + 
                                "to be non-null but has {1} null elements.", 
                                colname, num_nulls);
                        log.print(errmsg + "\n");
                        errs.Add(key, new VxSchemaError(
                            key, errmsg, -1, WvLog.L.Warning));
                    }
                }
            }
        }

    done:
        return errs;
    }

    // Replaces the named object in the database.  elem.text is a verbatim
    // hunk of text returned earlier by GetSchema.  'destructive' says whether
    // or not to perform potentially destructive operations while making the
    // change, e.g. dropping a table so we can re-add it with the right
    // columns.
    private VxSchemaError PutSchemaElement(VxSchemaElement elem, VxPutOpts opts)
    {
        try 
        {
            bool destructive = (opts & VxPutOpts.Destructive) != 0;
            if (destructive || elem.type != "Table")
            {
                try { 
                    DropSchema(elem.key);
                } catch (VxSqlException e) {
                    // Check if it's a "didn't exist" error, rethrow if not.
                    // SQL Error 3701 means "can't drop sensible item because
                    // it doesn't exist or you don't have permission."
                    // SQL Error 15151 means "can't drop XML Schema collection 
                    // because it doesn't exist or you don't have permission."
                    if (!e.ContainsSqlError(3701) && !e.ContainsSqlError(15151))
                        throw;
                }
            }

            if (elem.text.ne())
            {
                log.print("Putting element: {0}\n", elem.ToSql());
                DbiExec(elem.ToSql());
            }
        } 
        catch (VxRequestException e)
        {
            log.print("Got error from {0}: {1}\n", elem.key, e.Message);
            return new VxSchemaError(elem.key, e);
        }

        return null;
    }

    // Functions used for GetSchemaChecksums

    void GetProcChecksums(VxSchemaChecksums sums, 
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
                    and encrypted = @col0
                    and object_name(id) like '%'
            select name, convert(varbinary(8), getchecksum(text))
                from #checksum_calc
                order by name, colid
            drop table #checksum_calc";


        foreach (WvSqlRow row in DbiSelect(query, encrypted))
        {
            string name = row[0];

            // Ignore dt_* functions and sys* views
            if (name.StartsWith("dt_") || name.StartsWith("sys"))
                continue;

            ulong checksum = 0;
            foreach (byte b in (byte[])row[1])
            {
                checksum <<= 8;
                checksum |= b;
            }

            // Fix characters not allowed in filenames
            name.Replace('/', '!');
            name.Replace('\n', '!');
            string key = String.Format("{0}{1}/{2}", type, encrypt_str, name);

            log.print("name={0}, checksum={1}, key={2}\n", name, checksum, key);
            sums.AddSum(key, checksum);
        }
    }

    void GetTableChecksums(VxSchemaChecksums sums)
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

        foreach (WvSqlRow row in DbiSelect(query))
        {
            string name = row[0];
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
            sums.AddSum(key, checksum);
        }
    }

    void AddIndexChecksumsToTables(VxSchemaChecksums sums)
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

        foreach (WvSqlRow row in DbiSelect(query))
        {
            string tablename = row[0];
            string indexname = row[1];
            ulong checksum = 0;
            foreach (byte b in (byte[])row[3])
            {
                checksum <<= 8;
                checksum |= b;
            }

            string key = String.Format("Table/{0}", tablename);

            log.print("tablename={0}, indexname={1}, checksum={2}, colid={3}\n", 
                tablename, indexname, checksum, (int)row[2]);
            sums.AddSum(key, checksum);
        }
    }

    void GetXmlSchemaChecksums(VxSchemaChecksums sums)
    {
        string query = @"
            select sch.name owner,
               xsc.name sch,
               cast(XML_Schema_Namespace(sch.name,xsc.name) 
                    as nvarchar(max)) contents
              into #checksum_calc
              from sys.xml_schema_collections xsc 
              join sys.schemas sch on xsc.schema_id = sch.schema_id
              where sch.name <> 'sys'
              order by sch.name, xsc.name

            select sch, convert(varbinary(8), checksum(contents))
                from #checksum_calc
            drop table #checksum_calc";

        foreach (WvSqlRow row in DbiSelect(query))
        {
            string schemaname = row[0];
            ulong checksum = 0;
            foreach (byte b in (byte[])row[1])
            {
                checksum <<= 8;
                checksum |= b;
            }

            string key = String.Format("XMLSchema/{0}", schemaname);

            log.print("schemaname={0}, checksum={1}, key={2}\n", 
                schemaname, checksum, key);
            sums.AddSum(key, checksum);
        }
    }

    // Functions used for GetSchema

    static string RetrieveProcSchemasQuery(string type, int encrypted, 
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

    void RetrieveProcSchemas(VxSchema schema, List<string> names, 
        string type, int encrypted)
    {
        string query = RetrieveProcSchemasQuery(type, encrypted, false, names);
        log.print("Query={0}\n", query);

        foreach (WvSqlRow row in DbiSelect(query))
        {
            string name = row[0];
            //short colid = row[1];
            string text;
            // FIXME: Retrieving encrypted data is kind of broken anyway.
            if (encrypted > 0)
                text = row[2];//.ToHex();
            else
                text = row[2];


            // Skip dt_* functions and sys_* views
            if (name.StartsWith("dt_") || name.StartsWith("sys_"))
                continue;

            // Fix characters not allowed in filenames
            name.Replace('/', '!');
            name.Replace('\n', '!');

            schema.Add(type, name, text, encrypted > 0);
        }
    }

    // Adds the indexes for each table in "names" to the table elements.
    void AddIndexesToTables(VxSchema schema, List<string> names)
    {
        string tabnames = (names.Count > 0) ? 
            "and (object_name(i.object_id) in ('" + 
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
            tabnames + 
          @" order by i.name, i.object_id, ic.index_column_id";

        log.print("Adding index information for {0}\n", 
            names.Count > 0 ? names.Join(",") : "all tables");

        WvSqlRow[] data = DbiSelect(query).ToArray();

        int old_colid = 0;
        List<string> cols = new List<string>();
        // FIXME: use foreach
        for (int ii = 0; ii < data.Length; ii++)
        {
            WvSqlRow row = data[ii];

            string tabname = row[0];
            string idxname = row[1];
            int idxtype = row[2];
            int idxunique = row[3];
            int idxprimary = row[4];
            string colname = row[5];
            int colid = row[6];
            int coldesc = row[7];

            // Check that we're getting the rows in order.
            wv.assert(colid == old_colid + 1 || colid == 1);
            old_colid = colid;

            cols.Add(coldesc == 0 ? colname : colname + " DESC");

            WvSqlRow nextrow = ((ii+1) < data.Length) ? data[ii+1] : null;
            string next_tabname = (nextrow != null) ? (string)nextrow[0] : null;
            string next_idxname = (nextrow != null) ? (string)nextrow[1] : null;
            
            // If we've finished reading the columns for this index, add the
            // index to the schema.  Note: depends on the statement's ORDER BY.
            if (tabname != next_tabname || idxname != next_idxname)
            {
                VxSchemaTable table;
                string tabkey = "Table/" + tabname;
                if (schema.ContainsKey(tabkey))
                {
                    table = (VxSchemaTable)schema[tabkey];
                    log.print("Found table, idxtype={0}, cols={1}\n",
                        idxtype, cols.Join(","));

                    if (idxprimary != 0)
                        table.AddPrimaryKey(idxname, idxtype, cols.ToArray());
                    else
                        table.AddIndex(idxname, idxunique, idxtype, 
                            cols.ToArray());
                }
                else
                    throw new ArgumentException(
                        "Schema is missing table '" + tabkey + "'!");

                cols.Clear();
            }
        }

        return;
    }

    static string XmlSchemasQuery(int count, List<string> names)
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

    void RetrieveXmlSchemas(VxSchema schema, List<string> names)
    {
        bool do_again = true;
        for (int count = 0; do_again; count++)
        {
            do_again = false;
            string query = XmlSchemasQuery(count, names);

            foreach (WvSqlRow row in DbiSelect(query))
            {
                string owner = row[0];
                string name = row[1];
                string contents = row[2];

                if (contents.e())
                    continue;

                do_again = true;

                if (count == 0)
                    schema.Add("XMLSchema", name, String.Format(
                        "\nCREATE XML SCHEMA COLLECTION [{0}].[{1}] AS '", 
                        owner, name), false);

                schema.Add("XMLSchema", name, contents, false);
            }
        }

        // Close the quotes on all the XMLSchemas
        foreach (KeyValuePair<string, VxSchemaElement> p in schema)
        {
            if (p.Value.type == "XMLSchema")
                p.Value.text += "'\n";
        }
    }

    // Removes any matching enclosing parens from around a string.
    // E.g. "foo" => "foo", "(foo)" => "foo", "((foo))" => "foo", 
    // "((2)-(1))" => "(2)-(1)"
    public static string StripMatchingParens(string s)
    {
        WvLog log = new WvLog("StripMatchingParens");
        int len = s.Length;

        // Count the initial and trailing number of parens
        int init_parens = 0;
        while (init_parens < len && s[init_parens] == '(')
            init_parens++;

        int trailing_parens = 0;
        while (trailing_parens < len && s[len - trailing_parens - 1] == ')')
            trailing_parens++;

        // No leading or trailing parens means there can't possibly be any
        // matching parens.
        if (init_parens == 0 || trailing_parens == 0)
            return s;

        // Count all the parens in between the leading and trailing ones.
        bool is_escaped = false;
        int paren_count = init_parens;
        int min_parens = init_parens;
        for (int i = init_parens; i < s.Length - trailing_parens; i++)
        {
            if (s[i] == '(' && !is_escaped)
                paren_count++;
            else if (s[i] == ')' && !is_escaped)
                paren_count--;
            else if (s[i] == '\'')
                is_escaped = !is_escaped;

            if (paren_count < min_parens)
                min_parens = paren_count;
        }

        // The minimum number of outstanding parens found while iterating over
        // the string is the number of parens to strip.  Unless there aren't
        // enough trailing parens to match the leading ones, of course.
        min_parens = Math.Min(min_parens, trailing_parens);
        log.print("Trimming {0} parens\n", min_parens);
        return s.Substring(min_parens, len - 2*min_parens);
    }

    void RetrieveTableSchema(VxSchema schema, List<string> names)
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

        VxSchemaTable table = null;
        foreach (WvSqlRow row in DbiSelect(query))
        {
            string tabname = row[0];
            string colname = row[1];
            string typename = row[2];
            short len = row[3];
            byte xprec = row[4];
            byte xscale = row[5];
            string defval = row[6].IsNull ? (string)null : row[6];
            int isnullable = row[7];
            int isident = row[8];
            string ident_seed = row[9];
            string ident_incr = row[10];

            if (table != null && tabname != table.name)
            {
                schema.Add(table.key, table);
                table = null;
            }

            if (isident == 0)
                ident_seed = ident_incr = null;

            string lenstr = "";
            string precstr = null;
            string scalestr = null;
            if (typename.EndsWith("nvarchar") || typename.EndsWith("nchar"))
            {
                if (len == -1)
                    lenstr = "max";
                else
                {
                    len /= 2;
                    lenstr = len.ToString();
                }
            }
            else if (typename.EndsWith("char") || typename.EndsWith("binary"))
            {
                lenstr = (len == -1 ? "max" : len.ToString());
            }
            else if (typename.EndsWith("decimal") || 
                typename.EndsWith("numeric") || typename.EndsWith("real"))
            {
                precstr = xprec.ToString();
                scalestr = xscale.ToString();
            }

            if (defval.ne())
            {
                // MSSQL returns default values wrapped in an irritatingly
                // variable number of ()s
                defval = StripMatchingParens(defval);
            }

            if (table == null)
                table = new VxSchemaTable(tabname);

            table.AddColumn(colname, typename, isnullable, lenstr, 
                defval, precstr, scalestr, isident, ident_seed, ident_incr);
        }

        if (table != null)
        {
            log.print("Adding table {0}\n", table.key);
            schema.Add(table.key, table);
        }

        AddIndexesToTables(schema, names);
    }

    // Returns a blob of text that can be used with PutSchemaData to fill 
    // the given table.
    public string GetSchemaData(string tablename, int seqnum, string where)
    {
        log.print("GetSchemaData({0},{1},{2})\n", tablename, seqnum, where);

        string ident_query = @"select 
            columnproperty(t.id, c.name, 'IsIdentity') isident
	  from sysobjects t
	  join syscolumns c on t.id = c.id 
	  where t.xtype = 'U'
            and t.name = '" + tablename + "'";

        bool has_ident = false;
        foreach (WvSqlRow row in dbi.select(ident_query))
        {
            int isident = row[0];
            if (isident > 0)
            {
                has_ident = true;
                break;
            }
        }

        string query = "SELECT * FROM " + tablename;

        if (where != null && where.Length > 0)
            query += " WHERE " + where;

        bool did_preamble = false;
        string prefix = "";
        System.Type[] types = null;

        StringBuilder result = new StringBuilder();
        List<string> values = new List<string>();

        foreach (WvSqlRow row in DbiSelect(query))
        {
            if (!did_preamble)
            {
                // Read the column name and type information for the query.
                // We only need to do this once.
                List<string> cols = new List<string>();
                types = new System.Type[row.Length];

		WvColInfo[] columns = row.columns.ToArray();
                for (int ii = 0; ii < columns.Length; ii++)
                {
                    WvColInfo col = columns[ii];
                    cols.Add("[" + col.name + "]");
                    types[ii] = col.type;
                }

                prefix = String.Format("INSERT INTO {0} ({1}) VALUES (", 
                    tablename, cols.Join(","));

                did_preamble = true;
            }

            values.Clear();
            int colnum = 0;
            foreach (WvAutoCast elem in row)
            {
                if (elem.IsNull)
                    values.Add("NULL");
                else if (types[colnum] == typeof(System.String) || 
                    types[colnum] == typeof(System.DateTime))
                {
                    string str;
                    // The default formatting is locale-dependent, and stupid.
                    if (types[colnum] == typeof(System.DateTime))
                    {
                        str = ((DateTime)elem).ToString(
                            "yyyy-MM-dd HH:mm:ss.fff");
                    }
                    else
                        str = (string)elem;

                    // Double-quote chars for SQL safety
                    string esc = str.Replace("'", "''");
                    values.Add("'" + esc + "'");
                }
                else
                    values.Add(elem);

                colnum++;
            }
            result.Append(prefix + values.Join(",") + ");\n");
        }

        if (has_ident)
        {
            return "SET IDENTITY_INSERT [" + tablename + "] ON;\n" + 
                result.ToString() + 
                "SET IDENTITY_INSERT [" + tablename + "] OFF;\n";
        }
        return result.ToString();
    }

    // Delete all rows from the given table and replace them with the given
    // data.  text is an opaque hunk of text returned from GetSchemaData.
    public void PutSchemaData(string tablename, string text, int seqnum)
    {
        log.print("Calling PutSchemaData on {0}\n", tablename);
        // There may be extra static files in the DATA/ directory that 
        // Schemamatic didn't create and don't have an official table name, 
        // but that we still want to run.  So if the tablename is empty, 
        // don't do anything fancy but still run the query.
        if (tablename.ne())
            DbiExec(String.Format("DELETE FROM [{0}]", tablename));
	
	log.print("text size: {0}\n", text.Length);
	if (text.Length > 50000)
	{
	    string[] parts = text.Split("\nINSERT ");
	    log.print("Split into {0} parts.\n", parts.Length);
	    
	    log.print("Part 1...\n");
	    DbiExec(parts[0]);
	    
	    int count = 1;
	    var sb = new StringBuilder();
	    for (int i = 1; i < parts.Length; i++)
	    {
		sb.Append("\nINSERT ");
		sb.Append(parts[i]);
		if (sb.Length > 50000)
		{
		    log.print("Part {0}...\n", ++count);
		    DbiExec(sb.ToString());
		    sb = new StringBuilder();
		}
	    }
	    if (sb.Length > 0)
	    {
		log.print("Part {0}...\n", ++count);
		DbiExec(sb.ToString());
	    }
	}
	else
	    DbiExec(text);
    }
}

