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
        VxSchema schema = new VxSchema();
        schema.Add("Table", tabname, tabschema, false);

        VxSchemaErrors errs = dbus.Put(schema, null, VxPutOpts.None);

        log.print("Received errors: {0}\n", errs.ToString());
        WVPASSEQ(errs.Count, 0);

        schema = dbus.Get("Table/" + tabname);
        WVPASSEQ(schema.Count, 1);
        CheckTable(schema, tabname, tabschema);
    }

    public void TestTableUpdateError(string tabname, string tabschema, 
        string errmsg, string oldval)
    {
        string key = "Table/" + tabname;
        VxSchema schema = new VxSchema();
        schema.Add("Table", tabname, tabschema, false);

        VxSchemaErrors errs = dbus.Put(schema, null, VxPutOpts.None);
        log.print("Received errors: {0}\n", errs.ToString());

        WVPASSEQ(errs.Count, 1);
        WVPASSEQ(errs[key].key, key);
        WVPASSEQ(errs[key].msg, errmsg);
        WVPASSEQ(errs[key].errnum, -1);

        // Ensure that we didn't break what was already there.
        schema = dbus.Get(key);
        WVPASSEQ(schema.Count, 1);
        CheckTable(schema, tabname, oldval);
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestChangingIndexes()
    {
        try { VxExec("drop table Tab1"); } catch { }

        WVPASS(1);
        string tab1sch = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("Tab1", tab1sch);

        // Change the index: line slightly.
        WVPASS(2);
        string tab1sch2 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("Tab1", tab1sch2);

        // No index at all
        WVPASS(3);
        string tab1sch3 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("Tab1", tab1sch3);

        // Add the index back, and another for good measure
        WVPASS(4);
        string tab1sch4 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "index: column=f2,column=f3,name=Idx2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        VxSchema schema = new VxSchema();
        schema.Add("Table", "Tab1", tab1sch4, false);

        VxSchemaErrors errs = dbus.Put(schema, null, VxPutOpts.None);
        log.print("Received errors: {0}\n", errs.ToString());
        WVPASSEQ(errs.Count, 0);

        // Check that we get the default unique=0 and clustered=2 parameters.
        tab1sch4 = tab1sch4.Replace("Idx2", "Idx2,unique=0,clustered=2");

        schema = dbus.Get("Table/Tab1");
        WVPASSEQ(schema.Count, 1);
        CheckTable(schema, "Tab1", tab1sch4);

        // Check that duplicate index names give errors.
        WVPASS(5);
        string tab1sch5 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "index: column=f2,column=f3,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        string errmsg = "Duplicate table entry 'index: Idx1' found.";
        TestTableUpdateError("Tab1", tab1sch5, errmsg, tab1sch4);

        // Try renaming an index.
        // Note that indexes are returned alphabetically, and the default name
        // for a primary key is PK_TableName, so the renamed index will show 
        // up after the primary key.
        WVPASS(6);
        string tab1sch6 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "primary-key: column=f1,column=f2,clustered=1\n" + 
                "index: column=f1,column=f3 DESC,name=RenamedIndex," + 
                    "unique=1,clustered=2\n";
        TestTableUpdate("Tab1", tab1sch6);

        try { VxExec("drop table Tab1"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestChangingPrimaryKeys()
    {
        try { VxExec("drop table Tab1"); } catch { }

        WVPASS(1);
        string tab1sch = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
        TestTableUpdate("Tab1", tab1sch);

        // Change the primary-key: line: try specifying a name, changing the
        // columns, and omitting the optional "clustered" parameter.
        WVPASS(2);
        string tab1sch2 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: name=TestPK,column=f1\n";
        VxSchema schema = new VxSchema();
        schema.Add("Table", "Tab1", tab1sch2, false);
        VxSchemaErrors errs = dbus.Put(schema, null, VxPutOpts.None);

        // We'll get the default clustered value added back
        tab1sch2 = tab1sch2.Replace("primary-key: name=TestPK,column=f1", 
            "primary-key: name=TestPK,column=f1,clustered=2");

        log.print("Received errors: {0}\n", errs.ToString());
        WVPASSEQ(errs.Count, 0);

        schema = dbus.Get("Table/Tab1");
        WVPASSEQ(schema.Count, 1);
        CheckTable(schema, "Tab1", tab1sch2);

        // Remove the primary-key: line.
        WVPASS(3);
        string tab1sch3 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n";
        TestTableUpdate("Tab1", tab1sch3);

        // Try to add two primary keys
        WVPASS(4);
        string tab1sch4 = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f1,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" + 
                "primary-key: column=f1,clustered=1\n" + 
                "primary-key: column=f2,clustered=1\n";
        string errmsg = "Duplicate table entry 'primary-key' found.";

        TestTableUpdateError("Tab1", tab1sch4, errmsg, tab1sch3);

        try { VxExec("drop table Tab1"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestChangingColumns()
    {
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestAddingColumns()
    {
    }

    public static void Main()
    {
        WvTest.DoMain();
    }
}
