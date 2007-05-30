#include "wvtest.cs.h"

using System;
using System.Data;
using System.Data.SqlClient;
using NUnit.Framework;
using Wv.Test;
using Wv.Utils;

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

    bool Insert(string table, params object [] param)
    {
        Console.WriteLine(" + Insert to {0} ({1})", table, String.Join(", ",
                    wv.stringify(param)));

        System.Text.StringBuilder query = new System.Text.StringBuilder();
        query.AppendFormat("INSERT INTO [{0}] VALUES (",
                table.Replace("]","]]"));

        using (SqlCommand insCmd = con.CreateCommand()) {
            for (int i=0; i < param.Length; i++) {
                if (i > 0)
                    query.Append(", ");

                if (param[i] is DBNull) {
                    query.Append("NULL");
                } else {
                    string paramName = string.Format("@col{0}", i);

                    query.Append(paramName);
                    insCmd.Parameters.Add(new SqlParameter(paramName,
                                param[i]));
                }
            }

            query.Append(")");
            insCmd.CommandText = query.ToString();

            insCmd.ExecuteNonQuery();
        }

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

	WVASSERT(Insert("#suckout", 1));
	WVASSERT(Insert("#suckout", 2));

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
	    WVASSERT(Insert("#test1", DBNull.Value));

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

    [Test, Category("Data")]
    public void RowOrdering()
    {
        // Make sure that data comes out in the right order when ordering is
        // requested from SqlSucker

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
            Insert("#test1", j, (j*prime2) % numElems);
        }

        WVASSERT(RunSucker("#suckout",
                    "SELECT num from #test1 ORDER BY seq DESC", true));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT num FROM #suckout ORDER BY _ DESC",
                    out reader));

        using (reader) {
            for (int i=0; i < numElems; i++) {
                WVASSERT(reader.Read());
                WVPASSEQ((int)reader["num"], (i*prime2) % numElems);
            }

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Schema")]
    public void ColumnOrdering()
    {
        // Make a bunch of columns and check that they come back in the right
        // order

        // For an explanation about the permutation stuff here, see the
        // RowOrdering test, above
        const int numCols = 101;
        const int numSelected = 83;
        const int prime1 = 47;
        const int prime2 = 53;

        System.Text.StringBuilder query = new System.Text.StringBuilder(
                "CREATE TABLE #test1 (");

        for (int i=0, j=0; i < numCols; i++, j = (i*prime1) % numCols) {
            if (i > 0)
                query.Append(", ");

            query.AppendFormat("col{0} int", j);
        }

        query.Append(")");

        WVASSERT(Exec(query.ToString()));

        query = new System.Text.StringBuilder("SELECT ");

        // Don't select all of them, in case that makes a difference. But still
        // select from the entire range (as opposed to the first few), so still
        // mod by numCols instead of numSelected.
        for (int i=0, j=0; i < numSelected; i++, j = (i*prime2) % numCols) {
            if (i > 0)
                query.Append(", ");

            query.AppendFormat("col{0}", j);
        }
        query.Append(" FROM #test1");

        WVASSERT(RunSucker("#suckout", query.ToString()));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT * FROM #suckout", out reader));

        using (reader) {
            WVPASSEQ(reader.FieldCount, numSelected);

            for (int i=0; i < numSelected; i++) {
                WVPASSEQ((string)reader.GetName(i),
                        string.Format("col{0}", (i*prime2) % numCols));
            }

            WVFAIL(reader.Read());
        }

        WVASSERT(Exec("DROP TABLE #test1"));
    }

    [Test, Category("Data")]
    public void VerifyIntegers()
    {
        // bigint, int, smallint, tinyint
        // Insert 6 rows: max, 10, 0, -10, min, nulls (except tinyint is
        // unsigned so it has 0 again instead of -10)
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)

        WVASSERT(Exec("CREATE TABLE #test1 (bi bigint, i int, si smallint, "
                    + "ti tinyint, roworder int not null)"));

        WVASSERT(Insert("#test1", Int64.MaxValue, Int32.MaxValue,
                    Int16.MaxValue, Byte.MaxValue, 1));
        WVASSERT(Insert("#test1", 10, 10, 10, 10, 2));
        WVASSERT(Insert("#test1", 0, 0, 0, 0, 3));
        WVASSERT(Insert("#test1", -10, -10, -10, 0, 4));
        WVASSERT(Insert("#test1", Int64.MinValue, Int32.MinValue,
                    Int16.MinValue, Byte.MinValue, 5));
        WVASSERT(Insert("#test1", DBNull.Value, DBNull.Value, DBNull.Value,
                    DBNull.Value, 6));

        WVASSERT(RunSucker("#suckout", "SELECT * FROM #test1"));

        WVASSERT(Exec("DROP TABLE #test1"));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT bi,i,si,ti FROM #suckout ORDER BY roworder",
                    out reader));

        using (reader) {
            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetInt64(0), Int64.MaxValue);
            WVPASSEQ(reader.GetInt32(1), Int32.MaxValue);
            WVPASSEQ(reader.GetInt16(2), Int16.MaxValue);
            WVPASSEQ(reader.GetByte(3), Byte.MaxValue);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetInt64(0), 10);
            WVPASSEQ(reader.GetInt32(1), 10);
            WVPASSEQ(reader.GetInt16(2), 10);
            WVPASSEQ(reader.GetByte(3), 10);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetInt64(0), 0);
            WVPASSEQ(reader.GetInt32(1), 0);
            WVPASSEQ(reader.GetInt16(2), 0);
            WVPASSEQ(reader.GetByte(3), 0);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetInt64(0), -10);
            WVPASSEQ(reader.GetInt32(1), -10);
            WVPASSEQ(reader.GetInt16(2), -10);
            WVPASSEQ(reader.GetByte(3), 0);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetInt64(0), Int64.MinValue);
            WVPASSEQ(reader.GetInt32(1), Int32.MinValue);
            WVPASSEQ(reader.GetInt16(2), Int16.MinValue);
            WVPASSEQ(reader.GetByte(3), Byte.MinValue);

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));
            WVPASS(reader.IsDBNull(1));
            WVPASS(reader.IsDBNull(2));
            WVPASS(reader.IsDBNull(3));

            WVFAIL(reader.Read());
        }
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyBinary()
    {
    }

    [Test, Category("Data")]
    public void VerifyBit()
    {
        // bit
        // Insert 3 rows: true, false, null
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)

        WVASSERT(Exec("CREATE TABLE #test1 (b bit, roworder int not null)"));

        WVASSERT(Insert("#test1", true, 1));
        WVASSERT(Insert("#test1", false, 2));
        WVASSERT(Insert("#test1", DBNull.Value, 3));

        WVASSERT(RunSucker("#suckout", "SELECT * FROM #test1"));

        WVASSERT(Exec("DROP TABLE #test1"));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT b FROM #suckout ORDER BY roworder",
                    out reader));

        using (reader) {
            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetBoolean(0), true);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetBoolean(0), false);

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));

            WVFAIL(reader.Read());
        }
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyChar()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyDateTime()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyDecimal()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyFloat()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyImage()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyMoney()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyNChar()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyNText()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyNumeric()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyNVarCharMax()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyNVarChar()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyReal()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifySmallDateTime()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifySmallMoney()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyText()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyTimestamp()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyUniqueIdentifier()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyVarBinary()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyVarBinaryMax()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyVarChar()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyVarCharMax()
    {
    }

    [Test, Category("Data")]
    [Ignore("Not done")]
    public void VerifyXML()
    {
    }
}

}

