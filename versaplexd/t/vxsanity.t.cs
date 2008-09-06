#include "wvtest.cs.h"

using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using Wv;
using Wv.Test;

[TestFixture]
class VxSanityTests : VersaplexTester
{
    [Test, Category("Data"), Category("Sanity")]
    public void EmptyTable()
    {
	// Check that an empty table is read ok
        try { VxExec("DROP TABLE test1"); } catch {}

        try {
            WVASSERT(VxExec("CREATE TABLE test1 (testcol int not null)"));

            object result;
            WVASSERT(VxScalar("SELECT COUNT(*) FROM test1", out result));
            WVPASSEQ((int)result, 0);

            VxColumnInfo[] colinfo;
            object[][] data;
            bool[][] nullity;
            WVASSERT(VxRecordset("SELECT * FROM test1", out colinfo, out data,
                        out nullity));

            WVPASSEQ(colinfo.Length, 1);
            WVPASSEQ(colinfo[0].ColumnName, "testcol");
            WVPASSEQ(colinfo[0].ColumnType.ToLowerInvariant(), "int32");

            WVPASSEQ(data.Length, 0);
            WVPASSEQ(nullity.Length, 0);
        } finally {
            try { VxExec("DROP TABLE test1"); } catch {}
        }
    }

    [Test, Category("Sanity"), Category("Errors")]
    public void NonexistantTable()
    {
	// Check that a nonexistant table throws an error
	try {
            VxColumnInfo[] colinfo;
            object[][] data;
            bool[][] nullity;
	    WVEXCEPT(VxRecordset("SELECT * FROM #nonexistant", out colinfo,
                        out data, out nullity));
	} catch (Wv.Test.WvAssertionFailure e) {
	    throw e;
	} catch (System.Exception e) {
            // FIXME: This should check for a vx.db.sqlerror
            // rather than any dbus error
	    WVPASS(e is DbusError);
	}
	
	// The only way to get here is for the test to pass (otherwise an
	// exception has been generated somewhere), as WVEXCEPT() always throws
	// something.
    }

    [Test, Category("Schema"), Category("Sanity"), Category("Errors")]
    public void EmptyColumnName()
    {
	// Check that a query with a missing column name is ok
	using (var result = Reader("SELECT 1")) {
	    var columns = result.columns.ToArray();
	    WVPASSEQ(columns.Length, 1);

	    WVPASSEQ(columns[0].name, "");

	    WvSqlRow[] rows = result.ToArray();
	    WVPASSEQ(rows.Length, 1);
	    WVPASSEQ(rows[0][0], 1);
	}
    }

    [Test, Category("Schema"), Category("Sanity")]
    public void ColumnTypes()
    {
	// Check that column types are copied correctly to the output table
        try { VxExec("DROP TABLE test1"); } catch {}

	string[] colTypes = {
	    // Pulled from the SQL Server management gui app's dropdown list in
	    // the table design screen
	    "bigint", "binary(50)", "bit", "char(10)", "datetime",
	    "decimal(18, 0)", "float", "image", "int", "money", "nchar(10)",
	    "ntext", "numeric(18, 0)", "nvarchar(50)", "nvarchar(MAX)", "real",
	    "smalldatetime", "smallint", "smallmoney", "text",
            "timestamp", "tinyint", "uniqueidentifier", "varbinary(50)",
	    "varbinary(MAX)", "varchar(50)", "varchar(MAX)", "xml",
            // "sql_variant", // this is problematic, so it is unsupported

	    // Plus a few more to mix up the parameters a bit, and providing
	    // edge cases
	    "numeric(1, 0)", "numeric(38, 38)", "numeric(1, 1)",
	    "numeric(38, 0)", "nvarchar(4000)", "nvarchar(1)",
	    "varchar(8000)", "varchar(1)", "char(1)", "char(8000)",
	    "nchar(1)", "nchar(4000)", "decimal(1, 0)", "decimal(38, 38)",
	    "decimal(1, 1)", "decimal(38, 0)", "binary(1)", "binary(8000)"
	};

	foreach (String colType in colTypes) {
	    WVASSERT(VxExec(string.Format("CREATE TABLE test1 (testcol {0})",
			    colType)));
	    // This makes sure it runs the prepare statement
	    WVASSERT(Insert("test1", DBNull.Value));

	    WvColInfo[] schema;
	    using (var result = Reader("SELECT * FROM test1"))
		schema = result.columns.ToArray();

            VxColumnInfo[] vxcolinfo;
            object[][] data;
            bool[][] nullity;
	    WVASSERT(VxRecordset("SELECT * FROM test1", out vxcolinfo, out data,
                        out nullity));

	    WVPASSEQ(schema.Length, vxcolinfo.Length);

            try {
	    for (int colNum = 0; colNum < schema.Length; colNum++) {
		var c = schema[colNum];

		WVPASSEQ(c.name, vxcolinfo[colNum].ColumnName);
                // FIXME: There must be *some* way to turn this into a
                // switch...
                Type type = c.type;
                string vxtype = vxcolinfo[colNum].ColumnType.ToLowerInvariant();
                if (type == typeof(Int64)) {
                    WVPASSEQ(vxtype, "int64");
                } else if (type == typeof(Int32)) {
                    WVPASSEQ(vxtype, "int32");
                } else if (type == typeof(Int16)) {
                    WVPASSEQ(vxtype, "int16");
                } else if (type == typeof(Byte)) {
                    WVPASSEQ(vxtype, "uint8");
                } else if (type == typeof(Boolean)) {
                    WVPASSEQ(vxtype, "bool");
                } else if (type == typeof(Single) || type == typeof(Double)) {
                    WVPASSEQ(vxtype, "double");
                } else if (type == typeof(Guid)) {
                    WVPASSEQ(vxtype, "uuid");
                } else if (type == typeof(Byte[])) {
                    WVPASSEQ(vxtype, "binary");
                } else if (type == typeof(string)) {
                    WVPASSEQ(vxtype, "string");
                } else if (type == typeof(DateTime)) {
                    WVPASSEQ(vxtype, "datetime");
                } else if (type == typeof(Decimal)) {
                    WVPASSEQ(vxtype, "decimal");
                } else {
                    bool return_column_type_is_known = false;
                    WVASSERT(return_column_type_is_known);
                }

                WVPASSEQ(c.size, vxcolinfo[colNum].Size);
                // These next two may have problems with mono vs microsoft
                // differences
                WVPASSEQ(c.precision, vxcolinfo[colNum].Precision);
                WVPASSEQ(c.scale, vxcolinfo[colNum].Scale);
            }
            } finally {
                try { VxExec("DROP TABLE test1"); } catch {}
            }
        }
    }

    public static void Main()
    {
	WvTest.DoMain();
    }
}
