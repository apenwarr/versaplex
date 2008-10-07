#include "wvtest.cs.h"
// Test the VxDiskSchema backend

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using System.Security.Cryptography;
using Wv;
using Wv.Extensions;
using Wv.Test;

[TestFixture]
class DiskSchemaTests : SchemamaticTester
{
    VxDbusSchema dbus;
    WvLog log;

    public DiskSchemaTests()
    {
        dbus = new VxDbusSchema(bus);
        log = new WvLog("Schemamatic Tests");
    }

    [Test, Category("Schemamatic"), Category("DropSchema"), Category("DiskBackend")]
    public void TestDropSchemaFromDisk()
    {
        string tmpdir = GetTempDir();
        try
        {
            Directory.CreateDirectory(tmpdir);
            VxDiskSchema backend = new VxDiskSchema(tmpdir);

            VxSchema schema = new VxSchema();
            schema.Add("Table", "Foo", "column: name=foo,type=int\n", false);
            schema.Add("Table", "Bar", "column: name=bar,type=int\n", false);
            schema.Add("Procedure", "Func1", "Func1 contents", false);
            schema.Add("ScalarFunction", "Func2", "Func2 contents", false);

            VxSchemaChecksums sums = new VxSchemaChecksums();
            sums.AddSum("Table/Foo", 1);
            sums.AddSum("Table/Bar", 2);
            sums.AddSum("Procedure/Func1", 3);
            sums.AddSum("ScalarFunction/Func2", 5);

            backend.Put(schema, sums, VxPutOpts.None);

            WVPASS(File.Exists(Path.Combine(tmpdir, "Table/Foo")));
            WVPASS(File.Exists(Path.Combine(tmpdir, "Table/Bar")));
            WVPASS(File.Exists(Path.Combine(tmpdir, "Procedure/Func1")));
            WVPASS(File.Exists(Path.Combine(tmpdir, "ScalarFunction/Func2")));

            VxSchema newschema = backend.Get(null);
            VxSchemaChecksums newsums = backend.GetChecksums();

            WVPASSEQ(newschema.Count, schema.Count);
            WVPASSEQ(newsums.Count, sums.Count);
            WVPASS(newschema.ContainsKey("Table/Foo"));
            WVPASS(newschema.ContainsKey("Table/Bar"));
            WVPASS(newschema.ContainsKey("Procedure/Func1"));
            WVPASS(newschema.ContainsKey("ScalarFunction/Func2"));

            string[] todrop = { "Table/Foo" };
            backend.DropSchema(todrop);

            WVPASS(!File.Exists(Path.Combine(tmpdir, "Table/Foo")))
            WVPASS(File.Exists(Path.Combine(tmpdir, "Table/Bar")))
            WVPASS(File.Exists(Path.Combine(tmpdir, "Procedure/Func1")))
            WVPASS(File.Exists(Path.Combine(tmpdir, "ScalarFunction/Func2")))

            newschema = backend.Get(null);
            newsums = backend.GetChecksums();
            WVPASSEQ(newschema.Count, 3);
            WVPASSEQ(newsums.Count, 3);
            WVPASS(!newschema.ContainsKey("Table/Foo"));
            WVPASS(newschema.ContainsKey("Table/Bar"));
            WVPASS(newschema.ContainsKey("Procedure/Func1"));
            WVPASS(newschema.ContainsKey("ScalarFunction/Func2"));

            todrop = new string[] { "Procedure/Func1", "ScalarFunction/Func2" };
            backend.DropSchema(todrop);

            WVPASS(!File.Exists(Path.Combine(tmpdir, "Table/Foo")))
            WVPASS(File.Exists(Path.Combine(tmpdir, "Table/Bar")))
            WVPASS(!File.Exists(Path.Combine(tmpdir, "Procedure/Func1")))
            WVPASS(!File.Exists(Path.Combine(tmpdir, "ScalarFunction/Func2")))

            newschema = backend.Get(null);
            newsums = backend.GetChecksums();
            WVPASSEQ(newschema.Count, 1);
            WVPASSEQ(newsums.Count, 1);
            WVPASS(!newschema.ContainsKey("Table/Foo"));
            WVPASS(newschema.ContainsKey("Table/Bar"));
            WVPASS(!newschema.ContainsKey("Procedure/Func1"));
            WVPASS(!newschema.ContainsKey("ScalarFunction/Func2"));
        }
        finally
        {
            Directory.Delete(tmpdir, true);
        }
	WVPASS(!Directory.Exists(tmpdir));
    }

    [Test, Category("Schemamatic"), Category("PutSchemaData"), Category("DiskBackend")]
    public void TestDiskPutData()
    {
        string tmpdir = GetTempDir();
        try
        {
            Directory.CreateDirectory(tmpdir);
            VxDiskSchema backend = new VxDiskSchema(tmpdir);

            string filename = "10100-TestTable.sql";
            string datadir = Path.Combine(tmpdir, "DATA");
            string fullpath = Path.Combine(datadir, filename);
            string contents = "Random\nContents\n";

            backend.PutSchemaData("TestTable", contents, 10100);
            WVPASS(Directory.Exists(datadir));
            WVPASS(File.Exists(fullpath));
            WVPASSEQ(File.ReadAllText(fullpath), contents);

            WVPASSEQ(backend.GetSchemaData("TestTable", 10100, ""), contents);
        }
        finally
        {
            Directory.Delete(tmpdir, true);
        }
	WVPASS(!Directory.Exists(tmpdir));
    }

    [Test, Category("Schemamatic"), Category("DiskBackend")]
    public void TestExportEmptySchema()
    {
        string tmpdir = GetTempDir();

        try 
        {
            Directory.CreateDirectory(tmpdir);

            VxSchema schema = new VxSchema();
            VxSchemaChecksums sums = new VxSchemaChecksums();

            // Check that exporting an empty schema doesn't touch anything.
            VxDiskSchema backend = new VxDiskSchema(tmpdir);
            backend.Put(schema, sums, VxPutOpts.None);
            WVPASSEQ(Directory.GetDirectories(tmpdir).Length, 0);
            WVPASSEQ(Directory.GetFiles(tmpdir).Length, 0);
        }
        finally
        {
            Directory.Delete(tmpdir);
        }
	WVASSERT(!Directory.Exists(tmpdir));
    }

    private void CheckExportedFileContents(string filename, string header, 
        string text)
    {
        log.print("Checking contents of {0}\n", filename);
        WVPASS(File.Exists(filename));
        using (StreamReader sr = new StreamReader(filename))
        {
            WVPASSEQ(sr.ReadLine(), header);
            string line;
            StringBuilder sb = new StringBuilder();
            while ((line = sr.ReadLine()) != null)
                sb.Append(line + "\n");
            WVPASSEQ(sb.ToString(), text);
        }
    }

    private Dictionary<string, int> GetFileCounts(string exportdir)
    {
        string procdir = Path.Combine(exportdir, "Procedure");
        string scalardir = Path.Combine(exportdir, "ScalarFunction");
        string tabfuncdir = Path.Combine(exportdir, "TableFunction");
        string tabdir = Path.Combine(exportdir, "Table");
        string triggerdir = Path.Combine(exportdir, "Trigger");
        string viewdir = Path.Combine(exportdir, "View");
        string xmldir = Path.Combine(exportdir, "XMLSchema");

        Dictionary<string, int> retval = new Dictionary<string, int>();
        int num_procs = 0;
        int num_scalars = 0;
        int num_tables = 0;
        int num_tabfuncs = 0;
        int num_triggers = 0;
        int num_views = 0;
        int num_xmls = 0;

        if (Directory.Exists(procdir))
            num_procs = Directory.GetFiles(procdir).Length;
        if (Directory.Exists(scalardir))
            num_scalars = Directory.GetFiles(scalardir).Length;
        if (Directory.Exists(tabdir))
            num_tables = Directory.GetFiles(tabdir).Length;
        if (Directory.Exists(tabfuncdir))
            num_tabfuncs = Directory.GetFiles(tabfuncdir).Length;
        if (Directory.Exists(triggerdir))
            num_triggers = Directory.GetFiles(triggerdir).Length;
        if (Directory.Exists(viewdir))
            num_views = Directory.GetFiles(viewdir).Length;
        if (Directory.Exists(xmldir))
            num_xmls = Directory.GetFiles(xmldir).Length;

        retval.Add("Procedure", num_procs);
        retval.Add("ScalarFunction", num_scalars);
        retval.Add("Table", num_tables);
        retval.Add("TableFunction", num_tabfuncs);
        retval.Add("Trigger", num_triggers);
        retval.Add("View", num_views);
        retval.Add("XMLSchema", num_xmls);

        return retval;
    }

    private void VerifyExportedSchema(string exportdir, VxSchema schema, 
        VxSchemaChecksums sums, SchemaCreator sc, int backupnum, 
        Dictionary<string, int> base_filecounts)
    {
        int filemultiplier = backupnum + 1;
        string suffix = backupnum == 0 ? "" : "-" + backupnum;

        string procdir = Path.Combine(exportdir, "Procedure");
        string scalardir = Path.Combine(exportdir, "ScalarFunction");
        string idxdir = Path.Combine(exportdir, "Index");
        string tabdir = Path.Combine(exportdir, "Table");
        string tabfuncdir = Path.Combine(exportdir, "TableFunction");
        string triggerdir = Path.Combine(exportdir, "Trigger");
        string viewdir = Path.Combine(exportdir, "View");
        string xmldir = Path.Combine(exportdir, "XMLSchema");

        WVPASSEQ(Directory.GetFiles(exportdir).Length, 0);
        WVPASS(Directory.Exists(procdir));
        WVPASS(Directory.Exists(scalardir));
        // We no longer store indexes in a separate directory; make sure 
        // that directory doesn't get created.
        WVPASS(!Directory.Exists(idxdir));
        WVPASS(Directory.Exists(tabdir));
        WVPASS(Directory.Exists(tabfuncdir));
        WVPASS(Directory.Exists(triggerdir));
        WVPASS(Directory.Exists(viewdir));
        WVPASS(Directory.Exists(xmldir));
        WVPASSEQ(Directory.GetDirectories(exportdir).Length, 7);

        Dictionary<string, int> filecounts = GetFileCounts(exportdir);

        // Procedures
        WVPASSEQ(Directory.GetDirectories(procdir).Length, 0);
        WVPASSEQ(filecounts["Procedure"], 
            (1 + base_filecounts["Procedure"]) * filemultiplier);
        string func1file = Path.Combine(procdir, "Func1" + suffix);
        CheckExportedFileContents(func1file, 
            "!!SCHEMAMATIC 2ae46ac0748aede839fb9cd167ea1180 0xd983a305 ",
            sc.func1q);

        // Scalar functions
        WVPASSEQ(Directory.GetDirectories(scalardir).Length, 0);
        WVPASSEQ(filecounts["ScalarFunction"], 
            (1 + base_filecounts["ScalarFunction"]) * filemultiplier);
        string func2file = Path.Combine(scalardir, "Func2" + suffix);
        CheckExportedFileContents(func2file, 
            "!!SCHEMAMATIC c7c257ba4f7817e4e460a3cef0c78985 0xd6fe554f ",
            sc.func2q);

        // Table-valued functions
        WVPASSEQ(Directory.GetDirectories(tabfuncdir).Length, 0);
        WVPASSEQ(filecounts["TableFunction"], 
            (1 + base_filecounts["TableFunction"]) * filemultiplier);
        string tabfunc1file = Path.Combine(tabfuncdir, "TabFunc1" + suffix);
        CheckExportedFileContents(tabfunc1file, 
            "!!SCHEMAMATIC 1d3f1392a80e44876254209feebe7860 0x4b96fbe4 ",
            sc.tabfuncq);

        // Tables
        WVPASSEQ(Directory.GetDirectories(tabdir).Length, 0);
        WVPASSEQ(filecounts["Table"], 
            (2 + base_filecounts["Table"]) * filemultiplier);

        string tab1file = Path.Combine(tabdir, "Tab1" + suffix);
        string tab2file = Path.Combine(tabdir, "Tab2" + suffix);

        WVPASS(File.Exists(tab1file));
        CheckExportedFileContents(tab1file, 
            "!!SCHEMAMATIC 72c64bda7c48a954e63f359ff1fa4e79 " + 
            sums["Table/Tab1"].GetSumString() + " ",
            sc.tab1sch);

        WVPASS(File.Exists(tab2file));
        CheckExportedFileContents(tab2file, 
            "!!SCHEMAMATIC 69b15b6da6961a0f006fa55106cb243b " +
            sums["Table/Tab2"].GetSumString() + " ", sc.tab2sch);

        // Triggers
        WVPASSEQ(Directory.GetDirectories(triggerdir).Length, 0);
        WVPASSEQ(filecounts["Trigger"], 
            (1 + base_filecounts["Trigger"]) * filemultiplier);
        string triggerfile = Path.Combine(triggerdir, "Trig1" + suffix);
        CheckExportedFileContents(triggerfile, 
            "!!SCHEMAMATIC 363b2dc0cba5b41362fba5683b7885d5 0x55e66255 ",
            sc.triggerq);

        // Views
        WVPASSEQ(Directory.GetDirectories(viewdir).Length, 0);
        WVPASSEQ(filecounts["View"], 
            (1 + base_filecounts["View"]) * filemultiplier);
        string viewfile = Path.Combine(viewdir, "View1" + suffix);
        CheckExportedFileContents(viewfile, 
            "!!SCHEMAMATIC b43a8c712d3a274a6842fc2413516665 0xe0af9ccd ",
            sc.viewq);

        // XML Schemas
        WVPASSEQ(Directory.GetDirectories(xmldir).Length, 0);
        WVPASSEQ(filecounts["XMLSchema"], 
            (1 + base_filecounts["XMLSchema"]) * filemultiplier);

        string testschemafile = Path.Combine(xmldir, "TestSchema" + suffix);
        WVPASS(File.Exists(testschemafile));
        CheckExportedFileContents(testschemafile, 
            "!!SCHEMAMATIC f45c4ea54c268c91f41c7054c8f20bc9 0xf4b2c764 ",
            sc.xmlq);

    }

    [Test, Category("Schemamatic"), Category("DiskBackend")]
    public void TestExportSchema()
    {
        SchemaCreator sc = new SchemaCreator(this);

        string tmpdir = GetTempDir();

        DirectoryInfo tmpdirinfo = new DirectoryInfo(tmpdir);
        try
        {
            tmpdirinfo.Create();

            // Establish a baseline for the number of existing elements.
            VxSchema schema = dbus.Get();
            VxSchemaChecksums sums = dbus.GetChecksums();

            VxDiskSchema disk = new VxDiskSchema(tmpdir);
            disk.Put(schema, sums, VxPutOpts.None);

            var base_filecounts = GetFileCounts(tmpdir);

            // Clobber the directory and start fresh.
            tmpdirinfo.Delete(true);
            tmpdirinfo.Create();

            // Now create our test schema and do the real tests.
            sc.Create();

            // Check that having mangled checksums fails
            schema = dbus.Get();
            sums = new VxSchemaChecksums();

            disk = new VxDiskSchema(tmpdir);
            try {
                WVEXCEPT(disk.Put(schema, sums, VxPutOpts.None));
            } catch (Wv.Test.WvAssertionFailure e) {
                throw e;
            } catch (System.Exception e) {
                WVPASS(e is ArgumentException);
                WVPASS(e.Message.StartsWith("Missing checksum for "));
                log.print(e.ToString() + "\n");
            }

            // Check that the normal exporting works.
            schema = dbus.Get();
            sums = dbus.GetChecksums();

            WVPASSEQ(schema.Count, sums.Count);
            disk.Put(schema, sums, VxPutOpts.None);

            int backup_generation = 0;
            VerifyExportedSchema(tmpdir, schema, sums, sc, backup_generation,
                base_filecounts);

            // Check that we read back the same stuff
            VxSchema schemafromdisk = disk.Get(null);
            VxSchemaChecksums sumsfromdisk = disk.GetChecksums();

            WVPASS(1);

            TestSchemaEquality(schema, schemafromdisk);
            TestChecksumEquality(sums, sumsfromdisk);

            WVPASS(2);

            // Doing it twice doesn't change anything.
            disk.Put(schema, sums, VxPutOpts.None);

            VerifyExportedSchema(tmpdir, schema, sums, sc, backup_generation, 
                base_filecounts);

            WVPASS(3);

            // Check backup mode
            disk.Put(schema, sums, VxPutOpts.IsBackup);
            backup_generation++;

            VerifyExportedSchema(tmpdir, schema, sums, sc, backup_generation, 
                base_filecounts);

            WVPASS(4);

            // Check backup mode again
            disk.Put(schema, sums, VxPutOpts.IsBackup);
            backup_generation++;

            VerifyExportedSchema(tmpdir, schema, sums, sc, backup_generation, 
                base_filecounts);

            WVPASS(5);
        }
        finally
        {
            tmpdirinfo.Delete(true);
            sc.Cleanup();
        }
	WVASSERT(!tmpdirinfo.Exists);
    }

    [Test, Category("Schemamatic"), Category("DiskBackend")]
    public void TestReadChecksums()
    {
        SchemaCreator sc = new SchemaCreator(this);
        sc.Create();

        string tmpdir = GetTempDir();

        DirectoryInfo tmpdirinfo = new DirectoryInfo(tmpdir);
        try
        {
            tmpdirinfo.Create();

            VxSchema schema = dbus.Get();
            VxSchemaChecksums sums = dbus.GetChecksums();
            VxDiskSchema backend = new VxDiskSchema(tmpdir);
            backend.Put(schema, sums, VxPutOpts.None);

            VxSchemaChecksums fromdisk = backend.GetChecksums();

            foreach (KeyValuePair<string, VxSchemaChecksum> p in sums)
            {
                WVPASSEQ(p.Value.GetSumString(), fromdisk[p.Key].GetSumString());
            }
            WVPASSEQ(sums.Count, fromdisk.Count);

            // Test that changing a file invalidates its checksums, and that
            // we skip directories named "DATA"
            using (StreamWriter sw = File.AppendText(
                wv.PathCombine(tmpdir, "Table", "Tab1")))
            {
                sw.WriteLine("Ooga Booga");
            }

            Directory.CreateDirectory(Path.Combine(tmpdir, "DATA"));
            File.WriteAllText(wv.PathCombine(tmpdir, "DATA", "Decoy"),
                "Decoy file, shouldn't have checksums");

            VxSchemaChecksums mangled = backend.GetChecksums();

            // Check that the decoy file didn't get read
            WVFAIL(mangled.ContainsKey("DATA/Decoy"));

            // Check that the mangled checksums exist, but are empty.
            WVASSERT(mangled.ContainsKey("Table/Tab1"));
            WVASSERT(mangled["Table/Tab1"].GetSumString() != 
                sums["Table/Tab1"].GetSumString());
            WVPASSEQ(mangled["Table/Tab1"].GetSumString(), "");

            // Check that everything else is still sensible
            foreach (KeyValuePair<string, VxSchemaChecksum> p in sums)
            {
                if (p.Key != "Table/Tab1")
                    WVPASSEQ(p.Value.GetSumString(), 
                        mangled[p.Key].GetSumString());
            }
        }
        finally
        {
            tmpdirinfo.Delete(true);
            sc.Cleanup();
        }
	WVASSERT(!tmpdirinfo.Exists);
    }

    [Test, Category("Schemamatic"), Category("DiskBackend")]
    public void TestSubmoduleSupport()
    {
        VxSchema schema1 = new VxSchema();
        VxSchemaChecksums sums1 = new VxSchemaChecksums();

        VxSchema schema2 = new VxSchema();
        VxSchemaChecksums sums2 = new VxSchemaChecksums();

        schema1.Add("Table", "Tab1", "column: name=random\n", false);
        sums1.AddSum("Table/Tab1", 1);
        schema2.Add("Table", "Tab2", "column: name=ignored\n", false);
        sums2.AddSum("Table/Tab2", 2);

        string tmpdir = GetTempDir();
        try
        {
            Directory.CreateDirectory(tmpdir);
            VxDiskSchema disk = new VxDiskSchema(tmpdir);

            disk.Put(schema2, sums2, VxPutOpts.None);

            VxDiskSchema.AddFromDir(tmpdir, schema1, sums1);

            WVPASSEQ(sums1["Table/Tab1"].GetSumString(), "0x00000001");
            WVPASSEQ(schema1["Table/Tab1"].name, "Tab1");
            WVPASSEQ(schema1["Table/Tab1"].type, "Table");
            WVPASSEQ(schema1["Table/Tab1"].text, "column: name=random\n");
            WVPASSEQ(schema1["Table/Tab1"].encrypted, false);

            WVPASSEQ(sums1["Table/Tab2"].GetSumString(), "0x00000002");
            WVPASSEQ(schema1["Table/Tab2"].name, "Tab2");
            WVPASSEQ(schema1["Table/Tab2"].type, "Table");
            WVPASSEQ(schema1["Table/Tab2"].text, "column: name=ignored\n");
            WVPASSEQ(schema1["Table/Tab2"].encrypted, false);
        }
        finally
        {
            Directory.Delete(tmpdir, true);
        }
	WVPASS(!Directory.Exists(tmpdir));
    }

    [Test, Category("Schemamatic"), Category("DiskBackend")]
    public void TestSubmoduleExceptions()
    {
        VxSchema schema1 = new VxSchema();
        VxSchemaChecksums sums1 = new VxSchemaChecksums();

        VxSchema schema2 = new VxSchema();
        VxSchemaChecksums sums2 = new VxSchemaChecksums();

        schema1.Add("Procedure", "Func1", "Random contents", false);
        sums1.AddSum("Procedure/Func1", 1);
        schema2.Add("Procedure", "Func1", "Random contents 2", false);
        sums2.AddSum("Procedure/Func1", 2);

        string tmpdir = GetTempDir();
        try
        {
            Directory.CreateDirectory(tmpdir);
            VxDiskSchema disk = new VxDiskSchema(tmpdir);

            disk.Put(schema2, sums2, VxPutOpts.None);

            try {
                WVEXCEPT(VxDiskSchema.AddFromDir(tmpdir, schema1, sums1))
            } catch (System.ArgumentException e) {
                WVPASSEQ(e.Message, "Conflicting schema key: Procedure/Func1");
            }
        }
        finally
        {
            Directory.Delete(tmpdir, true);
        }
	WVPASS(!Directory.Exists(tmpdir));
    }

    [Test, Category("Schemamatic"), Category("CopySchema")]
    public void TestCopySchema()
    {
        SchemaCreator sc = new SchemaCreator(this);
        sc.Create();

        string msg2 = "Hello, world, this used to be Func1!";
        string func1q2 = "create procedure Func1 as select '" + msg2 + "'\n";

        VxSchema origschema = dbus.Get();
        VxSchemaChecksums origsums = dbus.GetChecksums();

        string tmpdir = GetTempDir();
        try
        {
            Directory.CreateDirectory(tmpdir);
            VxDiskSchema disk = new VxDiskSchema(tmpdir);

            // Test that the copy function will create new elements
            VxSchema.CopySchema(dbus, disk);

            VxSchema newschema = disk.Get(null);
            VxSchemaChecksums newsums = disk.GetChecksums();

            WVPASS(1);
            TestSchemaEquality(origschema, newschema);
            WVPASS(2);
            TestChecksumEquality(origsums, newsums);
            WVPASS(3);

            // Test that the copy function updates changed elements, and
            // deletes old ones.
            origschema["Procedure/Func1"].text = func1q2;

            dbus.Put(origschema, null, VxPutOpts.None);
            dbus.DropSchema("Table/Tab2");
            origschema.Remove("Table/Tab2");
            origsums = dbus.GetChecksums();

            VxSchema.CopySchema(dbus, disk);
            newschema = disk.Get(null);
            newsums = disk.GetChecksums();

            WVPASS(4);
            TestSchemaEquality(origschema, newschema);
            WVPASS(5);
            TestChecksumEquality(origsums, newsums);
            WVPASS(6);
        }
        finally
        {
            Directory.Delete(tmpdir, true);
        }
	WVPASS(!Directory.Exists(tmpdir));
        sc.Cleanup();
    }

    public static void Main()
    {
        WvTest.DoMain();
    }
}
