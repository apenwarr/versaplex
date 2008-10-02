#include "wvtest.cs.h"
// Test the Schemamatic functions that deal with Put-ing schema elements

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using System.Security.Cryptography;
using Wv;
using Wv.Extensions;
using Wv.Test;
using NDesk.DBus;

[TestFixture]
class PutSchemaTests : SchemamaticTester
{
    VxDbusSchema dbus;
    WvLog log;

    public PutSchemaTests()
    {
        dbus = new VxDbusSchema(bus);
        log = new WvLog("Schemamatic Tests");
    }

    public void CheckTable(VxSchema schema, string tabname, string schemastr)
    {
        string key = "Table/" + tabname;
        WVPASSEQ(schema[key].type, "Table");
        WVPASSEQ(schema[key].name, tabname);
        WVPASSEQ(schema[key].text, schemastr);
        WVPASSEQ(schema[key].encrypted, false);
    }

    public void TestTableUpdate(string tabname, string tabschema)
    {
        TestTableUpdate(tabname, tabschema, VxPutOpts.None, null);
    }

    public void TestTableUpdate(string tabname, string tabschema, 
        VxPutOpts opts)
    {
        TestTableUpdate(tabname, tabschema, opts, null);
    }

    public void TestTableUpdate(string tabname, string tabschema, 
        VxPutOpts opts, string expected_warning)
    {
        VxSchema schema = new VxSchema();
        schema.Add("Table", tabname, tabschema, false);

        VxSchemaErrors errs = dbus.Put(schema, null, opts);

        log.print("Received errors: {0}\n", errs.ToString());
        int errcount = 0;
        int warncount = 0;
        foreach (var errlist in errs)
            foreach (var err in errlist.Value)
            {
                log.print("Got error, err.level={0}\n", err.level);
                if (err.level == WvLog.L.Error)
                    errcount++;
                else if (err.level == WvLog.L.Warning)
                    warncount++;
            }
        WVPASSEQ(errcount, 0);
        WVPASSEQ(warncount, expected_warning.ne() ? 1 : 0);
        WVPASSEQ(errs.Count, errcount + warncount);

        if (expected_warning.ne())
        {
            string key = "Table/" + tabname;
            WVPASSEQ(errs[key][0].key, key);
            WVPASSEQ(errs[key][0].msg, expected_warning);
            WVPASSEQ(errs[key][0].errnum, -1);
            WVPASSEQ((int)errs[key][0].level, (int)WvLog.L.Warning);
            WVPASSEQ(errs[key].Count, 1);
        }

        schema = dbus.Get("Table/" + tabname);
        WVPASSEQ(schema.Count, 1);
        CheckTable(schema, tabname, tabschema);
    }

    public void TestTableUpdateError(string tabname, string tabschema, 
        string errmsg, string oldval)
    {
        TestTableUpdateError(tabname, tabschema, errmsg, oldval, 
            VxPutOpts.None, -1, WvLog.L.Error);
    }

    public void TestTableUpdateError(string tabname, string tabschema, 
        string errmsg, string oldval, VxPutOpts opts)
    {
        TestTableUpdateError(tabname, tabschema, errmsg, oldval, opts, -1, 
            WvLog.L.Error);
    }

    public void TestTableUpdateError(string tabname, string tabschema, 
        string errmsg, string oldval, VxPutOpts opts, int errno)
    {
        TestTableUpdateError(tabname, tabschema, errmsg, oldval, opts, errno,
            WvLog.L.Error);
    }

    public void TestTableUpdateError(string tabname, string tabschema, 
        string errmsg, string oldval, VxPutOpts opts, int errno, WvLog.L level)
    {
        string key = "Table/" + tabname;
        VxSchema schema = new VxSchema();
        schema.Add("Table", tabname, tabschema, false);

        VxSchemaErrors errs = dbus.Put(schema, null, opts);
        log.print("Received errors: {0}\n", errs.ToString());

        WVPASSEQ(errs.Count, 1);
        WVPASSEQ(errs[key][0].key, key);
        WVPASS(errs[key][0].msg.StartsWith(errmsg));
        WVPASSEQ(errs[key][0].errnum, errno);
        WVPASSEQ((int)errs[key][0].level, (int)level);
        WVPASSEQ(errs[key].Count, 1);

        // Ensure that we didn't break what was already there.
        schema = dbus.Get(key);
        if (oldval.ne())
        {
            WVPASSEQ(schema.Count, 1);
            CheckTable(schema, tabname, oldval);
        }
        else
            WVPASSEQ(schema.Count, 0);
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestChangingIndexes()
    {
        try { VxExec("drop table TestTable"); } catch { }

        WVPASS(1);
        string testschema = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", testschema);

        // Change the index: line slightly.
        WVPASS(2);
        string testschema2 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", testschema2);

        // No index at all
        WVPASS(3);
        string testschema3 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", testschema3);

        // Add the index back, and another for good measure
        WVPASS(4);
        string testschema4 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "index: column=f2,column=f3,name=Idx2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        VxSchema schema = new VxSchema();
        schema.Add("Table", "TestTable", testschema4, false);

        VxSchemaErrors errs = dbus.Put(schema, null, VxPutOpts.None);
        log.print("Received errors: {0}\n", errs.ToString());
        WVPASSEQ(errs.Count, 0);

        // Check that we get the default unique=0 and clustered=2 parameters.
        testschema4 = testschema4.Replace("Idx2", "Idx2,unique=0,clustered=2");

        schema = dbus.Get("Table/TestTable");
        WVPASSEQ(schema.Count, 1);
        CheckTable(schema, "TestTable", testschema4);

        // Check that duplicate index names give errors.
        // Note: VxSchemaTable now checks for this, which makes it hard to
        // actually test that the server rejects these.  It's pretty safe to
        // assume that the server would have just as much trouble creating one
        // as we would though, and that the exception would make its way back.
        WVPASS(5);
        string testschema5 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "index: column=f2,column=f3,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        string errmsg = "Duplicate table entry 'index: Idx1' found.";
        schema = new VxSchema();
        try 
        {
            WVEXCEPT(schema.Add("Table", "TestTable", testschema5, false));
        } 
        catch (VxBadSchemaException e) 
        {
            WVPASSEQ(e.Message, errmsg);
            log.print(e.ToString() + "\n");
        }

        // Try renaming an index.
        // Note that indexes are returned alphabetically, and the default name
        // for a primary key is PK_TableName, so the renamed index will show 
        // up after the primary key.
        WVPASS(6);
        string testschema6 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "primary-key: column=f1,column=f2,clustered=1\n" + 
                "index: column=f1,column=f3 DESC,name=RenamedIndex," + 
                    "unique=1,clustered=2\n";
        TestTableUpdate("TestTable", testschema6);

        try { VxExec("drop table TestTable"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestChangingPrimaryKeys()
    {
        try { VxExec("drop table TestTable"); } catch { }

        WVPASS(1);
        string testschema = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", testschema);

        // Change the primary-key: line: try specifying a name, changing the
        // columns, and omitting the optional "clustered" parameter.
        WVPASS(2);
        string testschema2 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: name=TestPK,column=f1\n";
        VxSchema schema = new VxSchema();
        schema.Add("Table", "TestTable", testschema2, false);
        VxSchemaErrors errs = dbus.Put(schema, null, VxPutOpts.None);

        // We'll get the default clustered value added back
        testschema2 = testschema2.Replace("primary-key: name=TestPK,column=f1", 
            "primary-key: name=TestPK,column=f1,clustered=2");

        log.print("Received errors: {0}\n", errs.ToString());
        WVPASSEQ(errs.Count, 0);

        schema = dbus.Get("Table/TestTable");
        WVPASSEQ(schema.Count, 1);
        CheckTable(schema, "TestTable", testschema2);

        // Remove the primary-key: line.
        WVPASS(3);
        string testschema3 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n";
        TestTableUpdate("TestTable", testschema3);

        // Try to add two primary keys
        // Note: VxSchemaTable now checks for this, which makes it hard to
        // actually test that the server rejects these.  It's pretty safe to
        // assume that the server would have just as much trouble creating one
        // as we would though, and that the exception would make its way back.
        WVPASS(4);
        string testschema4 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" + 
                "primary-key: column=f1,clustered=1\n" + 
                "primary-key: column=f2,clustered=1\n";
        string errmsg = "Duplicate table entry 'primary-key' found.";
        schema = new VxSchema();
        try 
        {
            WVEXCEPT(schema.Add("Table", "TestTable", testschema4, false));
        } 
        catch (VxBadSchemaException e) 
        {
            WVPASSEQ(e.Message, errmsg);
            log.print(e.ToString() + "\n");
        }

        try { VxExec("drop table TestTable"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestChangingEmptyColumns()
    {
        try { VxExec("drop table TestTable"); } catch { }

        WVPASS(1);
        string schema1 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", schema1);

        // Add a new column
        WVPASS(2);
        string schema2 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "column: name=f4,type=tinyint,null=0\n" + 
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", schema2);

        // Check that adding an identity attribute doesn't work without
        // specifying the destructive option.
        WVPASS(3);
        string schema3 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "column: name=f4,type=tinyint,null=0,identity_seed=4,identity_incr=5\n" + 
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        string errmsg = "Refusing to drop and re-add column [f4] " + 
            "when the destructive option is not set.  Error when altering " + 
            "was: 'Incorrect syntax near the keyword 'IDENTITY'.'";
        TestTableUpdateError("TestTable", schema3, errmsg, schema2);

        // Just try lightly changing a column, change the nullity on f4
        WVPASS(5);
        string schema5 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "column: name=f4,type=tinyint,null=1\n" + 
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", schema5, VxPutOpts.Destructive);

        // Try changing the column type and some attributes of f4
        WVPASS(6);
        string schema6 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "column: name=f4,type=bigint,null=0\n" + 
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", schema6);

        // Try renaming f4 without specifying Destructive.
        WVPASS(7);
        string schema7 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "column: name=f4renamed,type=bigint,null=0\n" + 
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        errmsg = "Refusing to drop columns ([f4]) " + 
            "when the destructive option is not set.";
        TestTableUpdateError("TestTable", schema7, errmsg, schema6);

        // Try to drop f4 without specifying Destructive.
        WVPASS(8);
        string schema8 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        errmsg = "Refusing to drop columns ([f4]) " + 
            "when the destructive option is not set.";
        TestTableUpdateError("TestTable", schema8, errmsg, schema6);

        // Actually rename f4.
        WVPASS(9);
        string schema9 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "column: name=f4renamed,type=bigint,null=0\n" + 
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", schema9, VxPutOpts.Destructive);

        // Actually drop f4 (f4renamed at this point).
        WVPASS(10);
        string schema10 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", schema10, VxPutOpts.Destructive);

        // Get rid of all the columns, except for one that gets renamed.
        WVPASS(11);
        string schema11 = "column: name=f1renamed,type=int,null=0\n" + 
                "primary-key: column=f1renamed,clustered=1\n";
        TestTableUpdate("TestTable", schema11, VxPutOpts.Destructive);

        // Try to get rid of all the columns.
        WVPASS(12);
        string schema12 = "primary-key: column=f1renamed,clustered=1\n";
        errmsg = "No columns in schema.";
        TestTableUpdateError("TestTable", schema12, errmsg, null,
            VxPutOpts.Destructive);

        // Try to get rid of all the columns, and rename the remaining index.
        WVPASS(13);
        string schema13 = "primary-key: name=NewPK,column=f1renamed,clustered=1\n";
        errmsg = "No columns in schema.";
        TestTableUpdateError("TestTable", schema13, errmsg, null,
            VxPutOpts.Destructive);

        try { VxExec("drop table TestTable"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestChangingColumnsWithData()
    {
        try { VxExec("drop table TestTable"); } catch { }

        WVPASS(1);
        string schema1 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", schema1);

        // Insert a number big enough to overflow a tinyint, and a string long
        // enough to overflow a varchar(10).
        string test_varchar = 
            "I am a varchar(80).  See? 12345678901234567890";
        WVASSERT(VxExec(wv.fmt("INSERT INTO [TestTable] VALUES (" + 
            1024*1024*1024 + ",1234.56,'{0}')", test_varchar)));

        WVPASSEQ(Scalar("select count(*) from TestTable"), 1);
        WVPASSEQ((int)Scalar("select f1 from TestTable"), 1024*1024*1024);
        WVPASSEQ(Scalar("select f2 from TestTable"), 1234.56);
        WVPASSEQ(Scalar("select f3 from TestTable"), test_varchar);

        // First check that you can't change a column out from under a 
        // primary key constraint, without the Destructive option.
        WVPASS(2);
        string schema2 = "column: name=f1,type=tinyint,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        string errmsg = "Refusing to drop and re-add column [f1] when " + 
            "the destructive option is not set.  Error when altering was: " + 
            "'The object 'PK_TestTable' is dependent on column 'f1'.'";
        TestTableUpdateError("TestTable", schema2, errmsg, schema1);

        WVPASSEQ(Scalar("select count(*) from TestTable"), 1);
        WVPASSEQ((int)Scalar("select f1 from TestTable"), 1024*1024*1024);
        WVPASSEQ(Scalar("select f2 from TestTable"), 1234.56);
        WVPASSEQ(Scalar("select f3 from TestTable"), test_varchar);

        // The Destructive operation destroys such puny limitations...
        WVPASS(3);
        string schema3 = "column: name=f1,type=tinyint,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("TestTable", schema3, VxPutOpts.Destructive);

        // ... but also destroys the data.
        WVPASSEQ(Scalar("select count(*) from TestTable"), 0);

        // Restore the original schema and data
        WVPASS(3.5);
        TestTableUpdate("TestTable", schema1, VxPutOpts.Destructive);
        WVASSERT(VxExec(wv.fmt("INSERT INTO [TestTable] VALUES (" + 
            1024*1024*1024 + ",1234.56,'{0}')", test_varchar)));

        WVPASSEQ(Scalar("select count(*) from TestTable"), 1);
        WVPASSEQ((int)Scalar("select f1 from TestTable"), 1024*1024*1024);
        WVPASSEQ(Scalar("select f2 from TestTable"), 1234.56);
        WVPASSEQ(Scalar("select f3 from TestTable"), test_varchar);

        // Now try truncating columns with no indexes in the way.
        WVPASS(4);
        string schema4 = "column: name=f1,type=tinyint,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n";
        errmsg = "Refusing to drop and re-add column [f1] when the " + 
            "destructive option is not set.  Error when altering was: " + 
            "'Arithmetic overflow error for data type tinyint, " + 
            "value = 1073741824.'";
        TestTableUpdateError("TestTable", schema4, errmsg, schema1);

        WVPASS(5);
        string schema5 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=10\n";
        errmsg = "Refusing to drop and re-add column [f3] when the " + 
            "destructive option is not set.  Error when altering was: " + 
            "'String or binary data would be truncated.'";
        TestTableUpdateError("TestTable", schema5, errmsg, schema1);

        WVPASSEQ(Scalar("select count(*) from TestTable"), 1);
        WVPASSEQ((int)Scalar("select f1 from TestTable"), 1024*1024*1024);
        WVPASSEQ(Scalar("select f2 from TestTable"), 1234.56);
        WVPASSEQ(Scalar("select f3 from TestTable"), test_varchar);

        // Oops, that pesky destructive flag wasn't set.  Set it now, and 
        // change both columns at once.
        WVPASS(6);
        string schema6 = "column: name=f2,type=money,null=0\n" + 
                "column: name=f1,type=tinyint,null=0,default=123\n" +
                "column: name=f3,type=varchar,null=1,length=10\n";
        TestTableUpdate("TestTable", schema6, VxPutOpts.Destructive);

        WVPASSEQ(Scalar("select count(*) from TestTable"), 1);
        WVPASSEQ((int)Scalar("select f1 from TestTable"), 123);
        WVPASSEQ(Scalar("select f2 from TestTable"), 1234.56);
        WVPASS(Scalar("select f3 from TestTable").IsNull);

        // Try to drop the nullity on a column that contains a null value.
        // Doesn't work without the Destructive option
        WVPASS(7);
        string schema7 = "column: name=f2,type=money,null=0\n" + 
                "column: name=f1,type=tinyint,null=0,default=0\n" +
                "column: name=f3,type=varchar,null=0,length=10\n";
        errmsg = "Refusing to drop and re-add column [f3] when the " + 
            "destructive option is not set.  Error when altering was: " + 
            "'Cannot insert the value NULL into column 'f3'";
        TestTableUpdateError("TestTable", schema7, errmsg, schema6, 
            VxPutOpts.None, -1, WvLog.L.Error);

        WVPASSEQ(Scalar("select count(*) from TestTable"), 1);
        WVPASSEQ((int)Scalar("select f1 from TestTable"), 123);
        WVPASSEQ(Scalar("select f2 from TestTable"), 1234.56);
        WVPASS(Scalar("select f3 from TestTable").IsNull);

        // The destructive options permits the update, but clobbers the data.
        WVPASS(7.5);
        TestTableUpdate("TestTable", schema7, VxPutOpts.Destructive);
        WVPASSEQ(Scalar("select count(*) from TestTable"), 0);

        // Restore the data
        TestTableUpdate("TestTable", schema6, VxPutOpts.None);
        WVASSERT(VxExec("INSERT INTO [TestTable] ([f1],[f2],[f3]) " + 
            "VALUES (123,1234.56,NULL)"));

        WVPASSEQ(Scalar("select count(*) from TestTable"), 1);
        WVPASSEQ((int)Scalar("select f1 from TestTable"), 123);
        WVPASSEQ(Scalar("select f2 from TestTable"), 1234.56);
        WVPASS(Scalar("select f3 from TestTable").IsNull);

        // Try to add a new column that doesn't allow null.  Should give a
        // warning, but otherwise work.  
        // Note: the added column shows up before the changed column.
        WVPASS(8);
        string schema8 = "column: name=f2,type=money,null=0\n" + 
                "column: name=f1,type=tinyint,null=0,default=0\n" +
                "column: name=f3,type=varchar,null=1,length=10\n" +
                "column: name=f4,type=int,null=0\n";
        string schema8_returned = "column: name=f2,type=money,null=0\n" + 
                "column: name=f1,type=tinyint,null=0,default=0\n" +
                "column: name=f3,type=varchar,null=1,length=10\n" +
                "column: name=f4,type=int,null=1\n"; 
        errmsg = "Column 'f4' was requested to be non-null but has " + 
            "1 null elements.";
        TestTableUpdateError("TestTable", schema8, errmsg, 
            schema8_returned, VxPutOpts.None, -1, WvLog.L.Warning);

        WVPASSEQ(Scalar("select count(*) from TestTable"), 1);
        WVPASSEQ((int)Scalar("select f1 from TestTable"), 123);
        WVPASSEQ(Scalar("select f2 from TestTable"), 1234.56);
        WVPASS(Scalar("select f3 from TestTable").IsNull);
        WVPASS(Scalar("select f4 from TestTable").IsNull);

        /*
        // Try to add a new column that doesn't explicitly mention
        // nullability.  We'll still get whining about f4.
        WVPASS(9);
        string schema9 = "column: name=f2,type=money,null=0\n" + 
                "column: name=f1,type=tinyint,null=0,default=0\n" +
                "column: name=f3,type=varchar,null=1,length=10\n" +
                "column: name=f4,type=int,null=0\n" + 
                "column: name=f5,type=int\n";
        string schema9_returned = "column: name=f2,type=money,null=0\n" + 
                "column: name=f1,type=tinyint,null=0,default=0\n" +
                "column: name=f3,type=varchar,null=1,length=10\n" +
                "column: name=f5,type=int,null=1\n" + 
                "column: name=f4,type=int,null=1\n";
        errmsg = "Column 'f4' was requested to be non-null but has " + 
            "1 null elements.";
        TestTableUpdateError("TestTable", schema9, errmsg, schema9_returned, 
            VxPutOpts.None, -1, WvLog.L.Warning);

        WVPASSEQ(Scalar("select count(*) from TestTable"), 1);
        WVPASSEQ((int)Scalar("select f1 from TestTable"), 123);
        WVPASSEQ(Scalar("select f2 from TestTable"), 1234.56);
        WVPASS(Scalar("select f3 from TestTable").IsNull);
        WVPASS(Scalar("select f4 from TestTable").IsNull);
        WVPASS(Scalar("select f5 from TestTable").IsNull);
        */

        try { VxExec("drop table TestTable"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestColumnInsertOrder()
    {
        string tabname = "TestTable";
        try { VxExec("drop table " + tabname); } catch { }

        // Make sure that table columns get added in the order we give them,
        // not alphabetical or something.
        WVPASS(1);
        string schema1 = "column: name=c,type=int,null=0\n" + 
                "column: name=d,type=int,null=0\n" +
                "column: name=b,type=int,null=0\n";
        TestTableUpdate(tabname, schema1);

        // Modified columns go to the end if we had to drop and add them.
        WVPASS(2);
        string schema2_sent = "column: name=c,type=tinyint,null=0,identity_seed=1,identity_incr=1\n" + 
                "column: name=d,type=int,null=0\n" +
                "column: name=b,type=int,null=0\n";
        string schema2_returned = "column: name=d,type=int,null=0\n" +
                "column: name=b,type=int,null=0\n" + 
                "column: name=c,type=tinyint,null=0,identity_seed=1,identity_incr=1\n";

        VxSchema schema = new VxSchema();
        schema.Add("Table", tabname, schema2_sent, false);

        VxSchemaErrors errs = dbus.Put(schema, null, VxPutOpts.Destructive);

        log.print("Received errors: {0}\n", errs.ToString());
        WVPASSEQ(errs.Count, 0);

        schema = dbus.Get("Table/" + tabname);
        WVPASSEQ(schema.Count, 1);
        CheckTable(schema, tabname, schema2_returned);

        // New columns go to the end too, but stay in order.
        WVPASS(3);
        string schema3_sent = "column: name=e,type=int,null=0\n" +
                "column: name=a,type=int,null=0\n" +
                "column: name=c,type=tinyint,null=0,identity_seed=1,identity_incr=1\n" +
                "column: name=d,type=int,null=0\n" +
                "column: name=b,type=int,null=0\n";
        string schema3_returned = "column: name=d,type=int,null=0\n" +
                "column: name=b,type=int,null=0\n" +
                "column: name=c,type=tinyint,null=0,identity_seed=1,identity_incr=1\n" + 
                "column: name=e,type=int,null=0\n" +
                "column: name=a,type=int,null=0\n";

        schema = new VxSchema();
        schema.Add("Table", tabname, schema3_sent, false);

        errs = dbus.Put(schema, null, VxPutOpts.Destructive);

        log.print("Received errors: {0}\n", errs.ToString());
        WVPASSEQ(errs.Count, 0);

        schema = dbus.Get("Table/" + tabname);
        WVPASSEQ(schema.Count, 1);
        CheckTable(schema, tabname, schema3_returned);

        // Changing the columns completely inserts the new ones in order
        WVPASS(4);
        string schema4 = "column: name=cc,type=int,null=0\n" + 
                "column: name=bb,type=int,null=0\n" +
                "column: name=dd,type=int,null=0\n";
        TestTableUpdate(tabname, schema4, VxPutOpts.Destructive);

        try { VxExec("drop table " + tabname); } catch { }
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestDefaultConstraints()
    {
        string tabname = "TestTable";
        try { VxExec("drop table " + tabname); } catch { }

        // Check that we can add columns with default values
        WVPASS(1);
        string schema1 = "column: name=f1,type=int,null=1,default=1\n";
        TestTableUpdate(tabname, schema1);

        WVASSERT(VxExec("INSERT INTO [TestTable] VALUES (2)"));

        // Check that we can drop default values
        WVPASS(2);
        string schema2 = "column: name=f1,type=int,null=0\n";
        TestTableUpdate(tabname, schema2);

        // Check that we can add new default values
        WVPASS(3);
        string schema3 = "column: name=f1,type=int,null=0,default=3\n";
        TestTableUpdate(tabname, schema3);

        WVASSERT(VxExec("INSERT INTO [TestTable] VALUES (DEFAULT)"));
        object count;
        WVASSERT(Scalar("SELECT COUNT(*) FROM [TestTable] WHERE [f1] = 3", 
            out count));
        WVPASSEQ((int)count, 1);

        // Check that we can drop columns with default values
        WVPASS(4);
        string schema4 = "column: name=f2,type=int,null=0,default=4\n";
        TestTableUpdate(tabname, schema4, VxPutOpts.Destructive);

        // When we added the new column, it gave its default value to both
        // existing rows.
        WVASSERT(Scalar("SELECT COUNT(*) FROM [TestTable] WHERE [f2] = 4", 
            out count));
        WVPASSEQ((int)count, 2);

        try { VxExec("drop table " + tabname); } catch { }
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestAddingNonNullColumns()
    {
        try { VxExec("drop table TestTable"); } catch { }

        string tabname = "TestTable";

        WVPASS(1);
        string schema1 = "column: name=f1,type=int,null=0\n";
        TestTableUpdate(tabname, schema1);

        WVASSERT(VxExec(wv.fmt("INSERT INTO [{0}] VALUES (123)", tabname)));

        // Check that nullability gets added without the Destructive option.
        WVPASS(2);
        string schema2 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=int,null=0\n";
        string schema2_returned = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=int,null=1\n";
        string errmsg = "Column 'f2' was requested " + 
            "to be non-null but has 1 null elements.";
        TestTableUpdateError(tabname, schema2, errmsg, schema2_returned, 
            VxPutOpts.None, -1, WvLog.L.Warning);

        WVPASSEQ(Scalar("select count(*) from TestTable"), 1);
        WVPASSEQ((int)Scalar("select f1 from TestTable"), 123);
        WVPASS(Scalar("select f2 from TestTable").IsNull);

        // Check that the Destructive option forces an exact table update.
        TestTableUpdate(tabname, schema2, VxPutOpts.Destructive);

        try { VxExec("drop table TestTable"); } catch { }
    }

    public static void Main()
    {
        WvTest.DoMain();
    }
}
