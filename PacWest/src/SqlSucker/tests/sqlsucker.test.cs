#include "wvtest.cs.h"

using System;
using System.Data;
using System.Data.SqlClient;
using NUnit.Framework;
using Wv.Test;

namespace SqlSucker.Test
{

[TestFixture]
public class SqlSuckerTest
{
    private const string Server = "amsdev";
    private const string User = "asta";
    private const string Password = "m!ddle-tear";
    private const string Database = "adrian_test";

    SqlConnection con;
    SqlCommand cmd;

    bool Connect(SqlConnection connection)
    {
	connection.Open();

	return true;
    }

    bool Exec(string query)
    {
	Console.WriteLine(" + Exec SQL Query: {0}", query);

	using (SqlCommand execCmd = new SqlCommand(query, con)) {
	    execCmd.ExecuteNonQuery();
	}

	return true;
    }

    bool Scalar(string query, out object result)
    {
	Console.WriteLine(" + Scalar SQL Query: {0}", query);

	using (SqlCommand execCmd = new SqlCommand(query, con)) {
	    result = execCmd.ExecuteScalar();
	}

	return true;
    }

    bool Reader(string query, out SqlDataReader result)
    {
	Console.WriteLine(" + Reader SQL Query: {0}", query);

	using (SqlCommand execCmd = new SqlCommand(query, con)) {
	    result = execCmd.ExecuteReader();
	}

	return true;
    }

    bool SetupOutputTable(string name)
    {
	Exec(string.Format(
		    "CREATE TABLE [{0}] (_ int)",
		    name.Replace("]","]]")));
	return true;
    }

    bool RunSucker(string table, string query)
    {
        return RunSucker(table, query, false);
    }

    bool RunSucker(string table, string query, bool ordered)
    {
	Exec(string.Format(
		    "EXEC SqlSucker '{0}', '{1}', {2}",
		    table.Replace("'", "''"),
		    query.Replace("'", "''"),
                    ordered));
	return true;
    }

    [SetUp]
    public void init()
    {
	con = new SqlConnection(string.Format(
		    "Server={0};UID={1};Password={2};Database={3}",
		    Server, User, Password, Database));

	WVASSERT(Connect(con));

	WVASSERT(SetupOutputTable("#suckout"));

	cmd = con.CreateCommand();
    }

    [TearDown]
    public void cleanup()
    {
	if (cmd != null)
	    cmd.Dispose();
	cmd = null;

	if (con != null) {
            try {
                WVASSERT(Exec("DROP TABLE #suckout"));
            } catch { }

	    con.Dispose();
        }
	con = null;
    }

    [Test, Category("Sanity")]
    public void EmptyTable()
    {
	// Check that an empty table stays empty

	WVASSERT(Exec("CREATE TABLE #test1 (testcol int not null)"));

	WVASSERT(RunSucker("#suckout", "SELECT * FROM #test1"));

	object result;
	WVASSERT(Scalar("SELECT COUNT(*) FROM #suckout", out result));
	WVPASSEQ((int)result, 0);

	SqlDataReader reader;
	WVASSERT(Reader("SELECT * FROM #suckout", out reader));
	using (reader)
	using (DataTable schema = reader.GetSchemaTable()) {
	    WVPASSEQ(reader.FieldCount, 1);
	    WVPASSEQ(schema.Rows.Count, 1);

	    DataRow schemaRow = schema.Rows[0];
	    WVPASSEQ((string)schemaRow["ColumnName"], "testcol");
	    WVPASSEQ(schemaRow["DataType"], typeof(System.Int32));

            WVFAIL(reader.Read());
            WVFAIL(reader.NextResult());
	}
    }

    [Test, Category("Errors")]
    public void NonexistantTable()
    {
	// Check that a nonexistant output table throws an error
	
	WVASSERT(Exec("CREATE TABLE #test1 (testcol int not null)"));

	try {
	    WVEXCEPT(RunSucker("#nonexistant", "SELECT * FROM #test1"));
	} catch (NUnit.Framework.AssertionException e) {
	    throw e;
	} catch (System.Exception e) {
	    WVPASS(e is SqlException);
	}
	
	// The only way to get here is for the test to pass (otherwise an
	// exception has been generated somewhere), as WVEXCEPT() always throws
	// something.
    }

    [Test, Category("Errors")]
    public void BadSchemaTable()
    {
	// Check that an output table with bad schema throws an error
	
	WVASSERT(Exec("CREATE TABLE #test1 (testcol int not null)"));

	string[] schemas = {
	    "_ int, a int",
	    "a int",
	    "a int, _ int"
	};

	foreach (string s in schemas) {
	    WVASSERT(Exec(string.Format("CREATE TABLE #badschema ({0})", s)));

	    try {
		WVEXCEPT(RunSucker("#badschema", "SELECT * FROM #test1"));
	    } catch (NUnit.Framework.AssertionException e) {
		throw e;
	    } catch (System.Exception e) {
		WVPASS(e is SqlException);
	    }

	    WVASSERT(Exec("DROP TABLE #badschema"));
	}
    }

    [Test, Category("Pedantic")]
    public void OutputNotEmpty()
    {
	// Check that if an output table is non-empty that its contents are
	// truncated (i.e. there are no extra rows afterwards)
	
	WVASSERT(Exec("CREATE TABLE #test1 (testcol int not null)"));

	WVASSERT(Exec("INSERT INTO #suckout VALUES (1)"));
	WVASSERT(Exec("INSERT INTO #suckout VALUES (2)"));

	object result;

	WVASSERT(Scalar("SELECT COUNT(*) FROM #suckout", out result));
	WVPASSEQ((int)result, 2);

	WVASSERT(RunSucker("#suckout", "SELECT * FROM #test1"));

	WVASSERT(Scalar("SELECT COUNT(*) FROM #suckout", out result));
	WVPASSEQ((int)result, 0);
    }

    [Test, Category("Sanity")]
    public void ColumnTypes()
    {
	// Check that column types are copied correctly to the output table

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
	    "numeric(1, 0)", "numeric(38, 18)", "numeric(1, 1)",
	    "numeric(38, 0)", "nvarchar(4000)", "nvarchar(1)",
	    "varchar(8000)", "varchar(1)", "char(1)", "char(8000)",
	    "nchar(1)", "nchar(4000)", "decimal(1, 0)", "decimal(38, 18)",
	    "decimal(1, 1)", "decimal(38, 0)", "binary(1)", "binary(8000)"
	};

	foreach (String colType in colTypes) {
	    WVASSERT(Exec(string.Format("CREATE TABLE #test1 (testcol {0})",
			    colType)));
	    // This makes sure it runs the prepare statement
	    WVASSERT(Exec("INSERT INTO #test1 VALUES (NULL)"));

	    WVASSERT(SetupOutputTable("#test1out"));

	    WVASSERT(RunSucker("#test1out", "SELECT * FROM #test1"));

	    SqlDataReader reader;
	    DataTable[] schemas = new DataTable[2];

	    WVASSERT(Reader("SELECT * FROM #test1", out reader));
	    using (reader)
		schemas[0] = reader.GetSchemaTable();

	    WVASSERT(Reader("SELECT * FROM #test1out", out reader));
	    using (reader)
		schemas[1] = reader.GetSchemaTable();

	    WVPASSEQ(schemas[0].Rows.Count, schemas[1].Rows.Count);

	    for (int colNum = 0; colNum < schemas[0].Rows.Count; colNum++) {
		DataRow[] colInfo = {
		    schemas[0].Rows[colNum],
		    schemas[1].Rows[colNum]
		};

		WVPASSEQ((IComparable)colInfo[0]["ColumnName"],
			(IComparable)colInfo[1]["ColumnName"]);
		WVPASSEQ((IComparable)colInfo[0]["ColumnOrdinal"],
			(IComparable)colInfo[1]["ColumnOrdinal"]);
		WVPASSEQ((IComparable)colInfo[0]["ColumnSize"],
			(IComparable)colInfo[1]["ColumnSize"]);
		WVPASSEQ((IComparable)colInfo[0]["NumericPrecision"],
			(IComparable)colInfo[1]["NumericPrecision"]);
		WVPASSEQ((IComparable)colInfo[0]["NumericScale"],
			(IComparable)colInfo[1]["NumericScale"]);
		// This one shouldn't be casted to IComparable or it doesn't
		// work
		WVPASSEQ(colInfo[0]["DataType"], colInfo[1]["DataType"]);
                // Timestamp gets converted into a varbinary(8), so there's
                // some discrepancy here. Ignore it (other tests make sure
                // that timestamp is handled properly).
                if (colType != "timestamp") {
                    WVPASSEQ((IComparable)colInfo[0]["ProviderType"],
                            (IComparable)colInfo[1]["ProviderType"]);
                }
		WVPASSEQ((IComparable)colInfo[0]["IsLong"],
			(IComparable)colInfo[1]["IsLong"]);
	    }

	    WVASSERT(Exec("DROP TABLE #test1out"));
	    WVASSERT(Exec("DROP TABLE #test1"));
	}
    }

    [Test, Category("Errors")]
    public void EmptyColumnName()
    {
	// Check that a query with a missing column name throws an error
	
        try {
            WVEXCEPT(RunSucker("#suckout", "SELECT 1"));
        } catch (NUnit.Framework.AssertionException e) {
            throw e;
        } catch (System.Exception e) {
            WVPASS(e is SqlException);
        }

        // The failed run shouldn't have modified #suckout, so this should work
        WVASSERT(RunSucker("#suckout", "SELECT 1 as foo"));

	SqlDataReader reader;
	WVASSERT(Reader("SELECT * FROM #suckout", out reader));
	using (reader)
	using (DataTable schema = reader.GetSchemaTable()) {
	    WVPASSEQ(reader.FieldCount, 1);
	    WVPASSEQ(schema.Rows.Count, 1);

	    DataRow schemaRow = schema.Rows[0];
	    WVPASSEQ((string)schemaRow["ColumnName"], "foo");

            WVPASS(reader.Read());
            WVPASSEQ((int)reader["foo"], 1);

            WVFAIL(reader.Read());
            WVFAIL(reader.NextResult());
	}
    }

    [Test]
    public void RowOrdering()
    {
        // If these are all prime then the permutation is guaranteed to work
        // without any duplicates (I think it actually works as long as numElems
        // is coprime with the other two, but making them all prime is safe)
        const int numElems = 101;
        const int prime1 = 47;
        const int prime2 = 53;

	WVASSERT(Exec("CREATE TABLE #test1 (seq int NOT NULL, "
                    + "num int NOT NULL)"));

        // j will be a permutation of 0..numElems without resorting to random
        // numbers, while making sure that we're not inserting in sorted order.
        for (int i=0, j=0; i < numElems; i++, j = (i*prime1) % numElems) {
            // This inserts 0..numElems into seq (in a permuted order), with
            // 0..numElems in num, but permuted in a different order.
            Exec(string.Format("INSERT INTO #test1 VALUES ({0}, {1})", j, 
                        (j*prime2) % numElems));
        }

        WVASSERT(RunSucker("#suckout", "SELECT num from #test1 ORDER BY seq",
                    true));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT num FROM #suckout ORDER BY _", out reader));

        using (reader) {
            for (int i=0; i < numElems; i++) {
                WVASSERT(reader.Read());
                WVPASSEQ((int)reader["num"], (i*prime2) % numElems);
            }

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }
}

}

