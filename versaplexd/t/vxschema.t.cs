#include "wvtest.cs.h"
// Test VxSchema data structures and relatives.

using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using Wv;
using Wv.Extensions;
using Wv.Test;

[TestFixture]
class VxSchemaTests 
{
    WvLog log;

    public VxSchemaTests()
    {
        log = new WvLog("VxSchema tests");
    }

    [Test, Category("Schemamatic"), Category("VxTableSchema")]
    public void TestToSql()
    {
        string tab1schema = "column: name=f1,type=int,null=0\n" + 
            "column: name=f2,type=money,null=0\n" + 
            "column: name=f3,type=varchar,null=0,length=80\n" + 
            "column: name=f4,type=varchar,null=1,length=max,default='Default Value'\n" + 
            "column: name=f5,type=decimal,null=1,precision=3,scale=2\n" + 
            "column: name=f6,type=bigint,null=0,identity_seed=4,identity_incr=5\n" + 
            "primary-key: column=f1,column=f2,clustered=1\n" + 
            "index: name=idx1,column=f3,column=f4,unique=1,clustered=1\n" + 
            "index: name=idx2,column=f5,unique=0\n";

        VxSchemaTable table = new VxSchemaTable("testtable", tab1schema);

        string sql = table.ToSql();

        string expected = "CREATE TABLE [testtable] (\n" + 
            "\t[f1] [int] NOT NULL,\n" +
            "\t[f2] [money] NOT NULL,\n" + 
            "\t[f3] [varchar] (80) NOT NULL,\n" + 
            "\t[f4] [varchar] (max) DEFAULT 'Default Value' NULL,\n" + 
            "\t[f5] [decimal] (3,2) NULL,\n" +
            "\t[f6] [bigint] NOT NULL IDENTITY (4,5));\n\n" + 
            "ALTER TABLE [testtable] ADD CONSTRAINT [PK_testtable] " + 
            "PRIMARY KEY CLUSTERED\n" + 
            "\t(f1, f2);\n\n" + 
            "CREATE UNIQUE CLUSTERED INDEX [idx1] ON [testtable] \n\t(f3, f4);\n" + 
            "CREATE INDEX [idx2] ON [testtable] \n\t(f5);\n";

        log.print("Returned SQL: " + sql + "\n");
        log.print("Expected: " + expected + "\n");

        WVPASSEQ(sql, expected);

    }

    [Test, Category("Schemamatic"), Category("VxTableSchema")]
    public void TestMultiplePrimaryKeys()
    {
        string sch1 = "column: name=f1,type=int,null=0\n" + 
            "column: name=f2,type=int,null=0\n";

        string pk1 = sch1 + "primary-key: column=f1\n";
        string pk2 = pk1 + "primary-key: column=f2\n";
        string expected_sql1 = "CREATE TABLE [testtable] (" + 
            "\n\t[f1] [int] NOT NULL,\n\t[f2] [int] NOT NULL);\n\n\n";
        string expected_pksql1 = "CREATE TABLE [testtable] (\n" + 
            "\t[f1] [int] NOT NULL,\n\t[f2] [int] NOT NULL);\n\n" + 
            "ALTER TABLE [testtable] ADD CONSTRAINT [PK_testtable] " + 
            "PRIMARY KEY NONCLUSTERED\n" + 
            "\t(f1);\n\n\n";

        VxSchemaTable table = new VxSchemaTable("testtable");
        table.text = sch1;
        log.print("Expected sch1: " + expected_sql1 + "\n");
        log.print("Actual sch1: " + table.ToSql() + "\n");
        WVPASSEQ(table.text, sch1);
        WVPASSEQ(table.ToSql(), expected_sql1);
        table.text = pk1;
        WVPASSEQ(table.text, pk1);
        log.print("Expected PK1: " + expected_pksql1 + "\n");
        log.print("Actual PK1: " + table.ToSql() + "\n");
        WVPASSEQ(table.ToSql(), expected_pksql1);
        try {
            WVEXCEPT(table.text = pk2);
        } catch (VxBadSchemaException e) {
            WVPASSEQ(e.Message, "Duplicate table entry 'primary-key' found.");
            log.print(e.ToString() + "\n");
        }
    }

    // Check that we can name primary keys, and that names are assigned 
    // by default.
    [Test, Category("Schemamatic"), Category("VxTableSchema")]
    public void TestPKNames()
    {
        string sch1 = "column: name=f1,type=int,null=0\n" + 
            "primary-key: column=f1\n";
        string sch2 = "column: name=f1,type=int,null=0\n" + 
            "primary-key: name=mypkname,column=f1\n";

        string expected_sql1 = "CREATE TABLE [testtable] (\n" + 
            "\t[f1] [int] NOT NULL);\n\n" + 
            "ALTER TABLE [testtable] ADD CONSTRAINT [PK_testtable] " + 
            "PRIMARY KEY NONCLUSTERED\n" + 
            "\t(f1);\n\n\n";
        string expected_sql2 = "CREATE TABLE [testtable] (\n" + 
            "\t[f1] [int] NOT NULL);\n\n" + 
            "ALTER TABLE [testtable] ADD CONSTRAINT [mypkname] " + 
            "PRIMARY KEY NONCLUSTERED\n" + 
            "\t(f1);\n\n\n";
        VxSchemaTable table = new VxSchemaTable("testtable");
        table.text = sch1;
        log.print("Expected sql1: " + expected_sql1 + "\n");
        log.print("Actual sql1: " + table.ToSql() + "\n");
        WVPASSEQ(table.text, sch1);
        WVPASSEQ(table.ToSql(), expected_sql1);
        table.text = sch2;
        WVPASSEQ(table.text, sch2);
        log.print("Expected sql2: " + expected_sql2 + "\n");
        log.print("Actual sql2: " + table.ToSql() + "\n");
        WVPASSEQ(table.ToSql(), expected_sql2);
    }

    [Test, Category("Schemamatic"), Category("VxTableSchema")]
    public void TestTextParsing()
    {
        string tab1schema = "column: name=f1,type=int,null=0\n" + 
            "column: name=f2,type=money,null=1\n" + 
            "column: name=f3,type=varchar,null=0,length=80\n" + 
            "column: name=f4,type=varchar,null=1,length=max,default='Default Value'\n" + 
            "column: name=f5,type=decimal,null=1,precision=3,scale=2\n" + 
            "column: name=f6,type=bigint,null=0,identity_seed=4,identity_incr=5\n" + 
            "primary-key: column=f1,column=f2,clustered=1\n" + 
            "index: name=idx1,column=f3,column=f4,unique=1\n";

        VxSchemaTable table = new VxSchemaTable("testtable");
        table.text = tab1schema;

        // Check that it gets parsed as expected.
        IEnumerator<VxSchemaTableElement> iter = table.GetElems();

        iter.Reset();
        WVPASS(iter.MoveNext());
        WVPASSEQ(iter.Current.elemtype, "column");
        WVPASSEQ(iter.Current.parameters[0].Key, "name");
        WVPASSEQ(iter.Current.parameters[0].Value, "f1");
        WVPASSEQ(iter.Current.parameters[1].Key, "type");
        WVPASSEQ(iter.Current.parameters[1].Value, "int");
        WVPASSEQ(iter.Current.parameters[2].Key, "null");
        WVPASSEQ(iter.Current.parameters[2].Value, "0");
        WVPASSEQ(iter.Current.parameters.Count, 3);

        WVPASS(iter.MoveNext());
        WVPASSEQ(iter.Current.elemtype, "column");
        WVPASSEQ(iter.Current.parameters[0].Key, "name");
        WVPASSEQ(iter.Current.parameters[0].Value, "f2");
        WVPASSEQ(iter.Current.parameters[1].Key, "type");
        WVPASSEQ(iter.Current.parameters[1].Value, "money");
        WVPASSEQ(iter.Current.parameters[2].Key, "null");
        WVPASSEQ(iter.Current.parameters[2].Value, "1");
        WVPASSEQ(iter.Current.parameters.Count, 3);

        WVPASS(iter.MoveNext());
        WVPASSEQ(iter.Current.elemtype, "column");
        WVPASSEQ(iter.Current.parameters[0].Key, "name");
        WVPASSEQ(iter.Current.parameters[0].Value, "f3");
        WVPASSEQ(iter.Current.parameters[1].Key, "type");
        WVPASSEQ(iter.Current.parameters[1].Value, "varchar");
        WVPASSEQ(iter.Current.parameters[2].Key, "null");
        WVPASSEQ(iter.Current.parameters[2].Value, "0");
        WVPASSEQ(iter.Current.parameters[3].Key, "length");
        WVPASSEQ(iter.Current.parameters[3].Value, "80");
        WVPASSEQ(iter.Current.parameters.Count, 4);

        WVPASS(iter.MoveNext());
        WVPASSEQ(iter.Current.elemtype, "column");
        WVPASSEQ(iter.Current.parameters[0].Key, "name");
        WVPASSEQ(iter.Current.parameters[0].Value, "f4");
        WVPASSEQ(iter.Current.parameters[1].Key, "type");
        WVPASSEQ(iter.Current.parameters[1].Value, "varchar");
        WVPASSEQ(iter.Current.parameters[2].Key, "null");
        WVPASSEQ(iter.Current.parameters[2].Value, "1");
        WVPASSEQ(iter.Current.parameters[3].Key, "length");
        WVPASSEQ(iter.Current.parameters[3].Value, "max");
        WVPASSEQ(iter.Current.parameters[4].Key, "default");
        WVPASSEQ(iter.Current.parameters[4].Value, "'Default Value'");
        WVPASSEQ(iter.Current.parameters.Count, 5);

        WVPASS(iter.MoveNext());
        WVPASSEQ(iter.Current.elemtype, "column");
        WVPASSEQ(iter.Current.parameters[0].Key, "name");
        WVPASSEQ(iter.Current.parameters[0].Value, "f5");
        WVPASSEQ(iter.Current.parameters[1].Key, "type");
        WVPASSEQ(iter.Current.parameters[1].Value, "decimal");
        WVPASSEQ(iter.Current.parameters[2].Key, "null");
        WVPASSEQ(iter.Current.parameters[2].Value, "1");
        WVPASSEQ(iter.Current.parameters[3].Key, "precision");
        WVPASSEQ(iter.Current.parameters[3].Value, "3");
        WVPASSEQ(iter.Current.parameters[4].Key, "scale");
        WVPASSEQ(iter.Current.parameters[4].Value, "2");
        WVPASSEQ(iter.Current.parameters.Count, 5);
        
        WVPASS(iter.MoveNext());
        WVPASSEQ(iter.Current.elemtype, "column");
        WVPASSEQ(iter.Current.parameters[0].Key, "name");
        WVPASSEQ(iter.Current.parameters[0].Value, "f6");
        WVPASSEQ(iter.Current.parameters[1].Key, "type");
        WVPASSEQ(iter.Current.parameters[1].Value, "bigint");
        WVPASSEQ(iter.Current.parameters[2].Key, "null");
        WVPASSEQ(iter.Current.parameters[2].Value, "0");
        WVPASSEQ(iter.Current.parameters[3].Key, "identity_seed");
        WVPASSEQ(iter.Current.parameters[3].Value, "4");
        WVPASSEQ(iter.Current.parameters[4].Key, "identity_incr");
        WVPASSEQ(iter.Current.parameters[4].Value, "5");
        WVPASSEQ(iter.Current.parameters.Count, 5);

        WVPASS(iter.MoveNext());
        WVPASSEQ(iter.Current.elemtype, "primary-key");
        WVPASSEQ(iter.Current.parameters[0].Key, "column");
        WVPASSEQ(iter.Current.parameters[0].Value, "f1");
        WVPASSEQ(iter.Current.parameters[1].Key, "column");
        WVPASSEQ(iter.Current.parameters[1].Value, "f2");
        WVPASSEQ(iter.Current.parameters[2].Key, "clustered");
        WVPASSEQ(iter.Current.parameters[2].Value, "1");
        WVPASSEQ(iter.Current.parameters.Count, 3);

        WVPASS(iter.MoveNext());
        WVPASSEQ(iter.Current.elemtype, "index");
        WVPASSEQ(iter.Current.parameters[0].Key, "name");
        WVPASSEQ(iter.Current.parameters[0].Value, "idx1");
        WVPASSEQ(iter.Current.parameters[1].Key, "column");
        WVPASSEQ(iter.Current.parameters[1].Value, "f3");
        WVPASSEQ(iter.Current.parameters[2].Key, "column");
        WVPASSEQ(iter.Current.parameters[2].Value, "f4");
        WVPASSEQ(iter.Current.parameters[3].Key, "unique");
        WVPASSEQ(iter.Current.parameters[3].Value, "1");
        WVPASSEQ(iter.Current.parameters.Count, 4);

        // Check that we get back what we put in.
        WVPASSEQ(table.text, tab1schema);
    }

    public static void Main()
    {
        WvTest.DoMain();
    }
}

