#include "wvtest.cs.h"

using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using NUnit.Framework;
using Wv.Test;
using Wv.Utils;

// Several mono bugs worked around in this test fixture are filed as mono bug
// #81940

namespace SqlSucker.Test
{

[TestFixture]
public class SqlSuckerTest
{
    private const string Server = "amsdev";
    private const string User = "asta";
    private const string Password = "m!ddle-tear";
    private const string Database = "adrian_test";

    // A file full of "lorem ipsum dolor" text
    private const string lipsum_file = "lipsum.txt";
    // A UTF-8 test file
    private const string unicode_file = "UTF-8-demo.txt";
    // A random file of binary goop
    private const string goop_file = "random.bin";
    // THTBACS image
    private const string image_file = "thtbacs.tiff";

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

    string read_lipsum()
    {
        WVASSERT(File.Exists(lipsum_file));

        using (StreamReader sr = new StreamReader(lipsum_file)) {
            return sr.ReadToEnd();
        }
    }

    string read_unicode()
    {
        WVASSERT(File.Exists(unicode_file));

        using (StreamReader sr = new StreamReader(unicode_file)) {
            return sr.ReadToEnd();
        }
    }

    Byte [] read_goop()
    {
        WVASSERT(File.Exists(goop_file));

        using (FileStream f = new FileStream(goop_file, FileMode.Open,
                    FileAccess.Read))
        using (BinaryReader sr = new BinaryReader(f)) {
            return sr.ReadBytes((int)Math.Min(f.Length, Int32.MaxValue));
        }
    }

    Byte [] read_image()
    {
        WVASSERT(File.Exists(image_file));

        using (FileStream f = new FileStream(image_file, FileMode.Open,
                    FileAccess.Read))
        using (BinaryReader sr = new BinaryReader(f)) {
            return sr.ReadBytes((int)Math.Min(f.Length, Int32.MaxValue));
        }
    }

    long GetInt64(SqlDataReader reader, int colnum) {
        // For some reason, it won't just up-convert int32 to int64
        if (reader.GetFieldType(colnum) == typeof(System.Int32)) {
            return reader.GetInt32(colnum);
        } else if (reader.GetFieldType(colnum) == typeof(System.Int64)) {
            return reader.GetInt64(colnum);
        } else if (reader.GetFieldType(colnum) == typeof(System.Decimal)) {
            return (long)reader.GetDecimal(colnum);
        } else {
            // Unknown type
            bool unknown_type_in_result = true;
            WVFAIL(unknown_type_in_result);

            return -1;
        }
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

    [Test, Category("Data"), Category("Sanity")]
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

    [Test, Category("Running"), Category("Errors")]
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

    [Test, Category("Running"), Category("Errors")]
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

    [Test, Category("Running"), Category("Pedantic")]
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

    [Test, Category("Schema"), Category("Sanity")]
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
	    "numeric(1, 0)", "numeric(38, 38)", "numeric(1, 1)",
	    "numeric(38, 0)", "nvarchar(4000)", "nvarchar(1)",
	    "varchar(8000)", "varchar(1)", "char(1)", "char(8000)",
	    "nchar(1)", "nchar(4000)", "decimal(1, 0)", "decimal(38, 38)",
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

    [Test, Category("Schema"), Category("Errors")]
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
    public void VerifyBinary()
    {
        // binary, varbinary (not max)
        
        // This must be sorted
        int [] sizes = { 1, 10, 50, 255, 4000, 8000 };

        string [] types = { "binary", "varbinary" };
        int [] typemax = { 8000, 8000 };
        int [] charsize = { 1, 1 };
        bool [] varsize = { false, true };

        Byte [] binary_goop = read_goop();

        WVASSERT(binary_goop.Length >= sizes[sizes.Length-1]);

        for (int i=0; i < types.Length; i++) {
            for (int j=0; j < sizes.Length && sizes[j] <= typemax[i]; j++) {
                WVASSERT(Exec(string.Format("CREATE TABLE #test1 "
                                + "(data {0}({1}), roworder int not null)",
                                types[i], sizes[j])));

                for (int k=0; k <= j; k++) {
                    Byte [] data = new byte[sizes[k]];
                    Array.Copy(binary_goop, data, sizes[k]);

                    WVASSERT(Insert("#test1", new SqlBinary(data), k));
                }

                WVASSERT(Insert("#test1", DBNull.Value, j+1));

                WVASSERT(SetupOutputTable("#test1out"));

                WVASSERT(RunSucker("#test1out",
                            "SELECT * FROM #test1 ORDER BY roworder", true));

                WVASSERT(Exec("DROP TABLE #test1"));

                SqlDataReader reader;
                WVASSERT(Reader("SELECT LEN(data), DATALENGTH(data), data FROM "
                            + "#test1out ORDER BY _",
                            out reader));

                using (reader) {
                    for (int k=0; k <= j; k++) {
                        Byte [] data = new byte[sizes[k]];
                        Array.Copy(binary_goop, data, sizes[k]);

                        WVASSERT(reader.Read());

                        int len = sizes[varsize[i] ? k : j];
                        WVPASSEQ(GetInt64(reader, 0), len);

                        int datalen = sizes[varsize[i] ? k : j]*charsize[i];
                        WVPASSEQ(GetInt64(reader, 1), datalen);

                        WVPASSEQ(reader.GetSqlBinary(2), new SqlBinary(data));
                    }

                    WVASSERT(reader.Read());
                    WVPASS(reader.IsDBNull(2));

                    WVFAIL(reader.Read());
                }

                WVASSERT(Exec("DROP TABLE #test1out"));
            }
        }
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
    public void VerifyChar()
    {
        // char, nchar, varchar (in-row or max), nvarchar (in-row or max),
        // text, ntext
        // This doesn't try to use any non-ascii characters. There is a separate
        // test for that.
        
        // This must be sorted
        int [] sizes = { 1, 10, 50, 255, 4000, 8000, 8040, 8192, 16080, 16384,
            24120, 32160, 32767, 50157 };

        string [] types = { "char", "varchar", "nchar", "nvarchar", "text",
            "ntext", "varchar(max)", "nvarchar(max)" };
        int [] typemax = { 8000, 8000, 4000, 4000, Int32.MaxValue,
            Int32.MaxValue/2, Int32.MaxValue, Int32.MaxValue/2 };
        int [] charsize = { 1, 1, 2, 2, 1, 2, 1, 2 };
        bool [] varsize = { false, true, false, true, true, true, true, true };
        bool [] sizeparam = { true, true, true, true, false, false, false,
            false };
        bool [] lenok = { true, true, true, true, false, false, true, true };

        string lipsum_text = read_lipsum();

        WVASSERT(lipsum_text.Length >= sizes[sizes.Length-1]);

        for (int i=0; i < types.Length; i++) {
            for (int j=0; j < sizes.Length && sizes[j] <= typemax[i]; j++) {
                if (sizeparam[i]) {
                    WVASSERT(Exec(string.Format("CREATE TABLE #test1 "
                                    + "(data {0}({1}), roworder int not null)",
                                    types[i], sizes[j])));
                } else {
                    WVASSERT(Exec(string.Format("CREATE TABLE #test1 "
                                    + "(data {0}, roworder int not null)",
                                    types[i])));
                    j = sizes.Length-1;
                }

                for (int k=0; k <= j; k++) {
                    WVASSERT(Exec(string.Format(
                                    "INSERT INTO #test1 VALUES ('{0}', {1})",
                                    lipsum_text.Substring(0,
                                        sizes[k]).Replace("'", "''"), k)));
                    /* This doesn't work because it truncates to 4000 chars
                     * regardless of if it's a nchar/nvarchar or plain
                     * char/varchar.
                    WVASSERT(Insert("#test1",
                                new SqlString(
                                    lipsum_text.Substring(0, sizes[k])), k));
                                    */
                }

                WVASSERT(Insert("#test1", DBNull.Value, j+1));

                WVASSERT(SetupOutputTable("#test1out"));

                WVASSERT(RunSucker("#test1out",
                            "SELECT * FROM #test1 ORDER BY roworder", true));

                WVASSERT(Exec("DROP TABLE #test1"));

                SqlDataReader reader;

                if (lenok[i]) {
                    WVASSERT(Reader("SELECT LEN(data), DATALENGTH(data), data "
                                + "FROM #test1out ORDER BY _",
                                out reader));
                } else {
                    WVASSERT(Reader("SELECT -1, "
                                + "DATALENGTH(data), data FROM #test1out "
                                + "ORDER BY _",
                                out reader));
                }

                using (reader) {
                    for (int k=0; k <= j; k++) {
                        WVASSERT(reader.Read());

                        if (lenok[i])
                            WVPASSEQ(GetInt64(reader, 0), sizes[k]);

                        WVPASSEQ(GetInt64(reader, 1),
                                sizes[varsize[i] ? k : j]*charsize[i]);
                        WVPASSEQ(reader.GetString(2).Substring(0, sizes[k]),
                                lipsum_text.Substring(0, sizes[k]));
                    }

                    WVASSERT(reader.Read());
                    WVPASS(reader.IsDBNull(2));

                    WVFAIL(reader.Read());
                }

                WVASSERT(Exec("DROP TABLE #test1out"));
            }
        }
    }

    [Test, Category("Data")]
    public void VerifyDateTime()
    {
        // datetime, smalldatetime
        // Insert 7 rows: max, a date in the future, now, a date in the past,
        // datetime epoch, min, null
        //
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)
        //
        // The actual dates don't really matter, but:
        // - The date in the past is adewhurst's birthday (approximately, PST)
        // - The date in the future is 1 second past the signed 32-bit overflow
        //   of seconds since the unix epoch (UTC)
        // - The datetime epoch is January 1 1900 at midnight
        //
        // Other notes:
        // - The min/max values of SqlDateTime are supposed to correspond to the
        //   min/max values of the SQL Server datetime type, except Mono doesn't
        //   quite have the semantics right, so the min/max values are
        //   hard-coded in instead. Bug filed with Mono.
        // - All smalldatetime values are rounded down to the nearest minute,
        //   since it only has per-minute granularity
        
        SqlDateTime epoch = new SqlDateTime(0, 0);
        SqlDateTime smallMin = epoch;
        SqlDateTime smallMax = new SqlDateTime(2079, 6, 6, 23, 59, 0, 0);

        SqlDateTime dtMin = new SqlDateTime(1753, 1, 1, 0, 0, 0, 0);
        // This is wrong, but mono seems to have trouble with the fractional
        // parts.
        SqlDateTime dtMax = new SqlDateTime(9999, 12, 31, 23, 59, 59, 0);

        SqlDateTime pastDate = new SqlDateTime(1984, 12, 2, 3, 0, 0, 0);
        SqlDateTime pastDateSmall = new SqlDateTime(1984, 12, 2, 3, 0, 0, 0);
        SqlDateTime futureDate = new SqlDateTime(2038, 6, 19, 3, 14, 8, 0);
        SqlDateTime futureDateSmall = new SqlDateTime(2038, 6, 19, 3, 14, 0, 0);

        // Mono has difficulties converting DateTime to SqlDateTime directly, so
        // take it down to per-second precision, which works reliably
        // Bug filed with Mono.
        DateTime now = DateTime.Now;
        SqlDateTime sqlNow = new SqlDateTime(now.Year, now.Month, now.Day,
                now.Hour, now.Minute, now.Second);
        SqlDateTime sqlNowSmall = new SqlDateTime(now.Year, now.Month, now.Day,
                now.Hour, now.Minute, 0);

        WVASSERT(Exec("CREATE TABLE #test1 (dt datetime, sdt smalldatetime, "
                    + "roworder int not null)"));

        WVASSERT(Insert("#test1", dtMin, smallMin, 1));
        WVASSERT(Insert("#test1", epoch, epoch, 2));
        WVASSERT(Insert("#test1", pastDate, pastDateSmall, 3));
        WVASSERT(Insert("#test1", sqlNow, sqlNowSmall, 4));
        WVASSERT(Insert("#test1", futureDate, futureDateSmall, 5));
        WVASSERT(Insert("#test1", dtMax, smallMax, 6));
        WVASSERT(Insert("#test1", DBNull.Value, DBNull.Value, 7));

        WVASSERT(RunSucker("#suckout", "SELECT * FROM #test1"));

        WVASSERT(Exec("DROP TABLE #test1"));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT dt, sdt FROM #suckout ORDER BY roworder",
                    out reader));

        using (reader) {
            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), dtMin);
            WVPASSEQ(reader.GetSqlDateTime(1), smallMin);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), epoch);
            WVPASSEQ(reader.GetSqlDateTime(1), epoch);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), pastDate);
            WVPASSEQ(reader.GetSqlDateTime(1), pastDateSmall);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), sqlNow);
            WVPASSEQ(reader.GetSqlDateTime(1), sqlNowSmall);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), futureDate);
            WVPASSEQ(reader.GetSqlDateTime(1), futureDateSmall);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlDateTime(0), dtMax);
            WVPASSEQ(reader.GetSqlDateTime(1), smallMax);

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));
            WVPASS(reader.IsDBNull(1));

            WVFAIL(reader.Read());
        }
    }

    [Test, Category("Data")]
    public void VerifyDecimal()
    {
        // decimal(38,0), decimal(38,38), decimal(18,0), decimal(1,0),
        // decimal(1,1), numeric as same types
        // Insert 6 rows: max, something positive, 0, something negative, min,
        // nulls
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)

        Byte [,] sizes = {
            // {precision, scale}
            {38, 0},
            {38, 38},
            {18, 0},
            {1, 0},
            {1, 1}
        };

        // Construct all of the things we will insert
        // These are all strings because attempting to use the SqlDecimal class
        // just leads to no end of problems. Even Microsoft's .NET
        // implementation seems to have issues with the max/min value ones.
        object [,] values = {
            {
                "99999999999999999999999999999999999999",
                "0.99999999999999999999999999999999999999",
                "999999999999999999",
                "9",
                "0.9"
            }, {
                "123456",
                "0.12345600000000000000000000000000000000",
                "123456",
                "1",
                "0.1"
            }, {
                /*
                 * The "zero" data set actually makes Mono's TDS library croak.
                 * But that's not a SqlSucker bug. The other data sets should
                 * give reasonable confidence in SqlSucker anyway.
                 * Bug filed with Mono.
                "0",
                "0.00000000000000000000000000000000000000",
                "0",
                "0",
                "0.0"
            }, {
                */
                "-654321",
                "-0.65432100000000000000000000000000000000",
                "-654321",
                "-1",
                "-0.1"
            }, {
                "-99999999999999999999999999999999999999",
                "-0.99999999999999999999999999999999999999",
                "-999999999999999999",
                "-9",
                "-0.9"
            }, {
                DBNull.Value,
                DBNull.Value,
                DBNull.Value,
                DBNull.Value,
                DBNull.Value
            }
        };

        // Make sure that the data is specified correctly here
        WVPASSEQ(sizes.GetLength(0), values.GetLength(1));

        // Make the table we're going to create
        System.Text.StringBuilder schema = new System.Text.StringBuilder(
                "CREATE TABLE #test1 (");

        // Make one of each decimal and numeric column. These are in fact
        // identical, but since either may show up in real-world tables, testing
        // both is a good plan
        for (int i=0; i < sizes.GetLength(0); i++) {
            schema.AppendFormat("d{0}_{1} decimal({0},{1}), "
                    + "n{0}_{1} numeric({0},{1}), ", sizes[i,0], sizes[i,1]);
        }

        schema.Append("roworder int not null)");

        WVASSERT(Exec(schema.ToString()));

        // Now insert them
        object [] insertParams = new object[2*values.GetLength(1)+1];

        for (int i=0; i < values.GetLength(0); i++) {
            insertParams[insertParams.Length-1] = i;
            for (int j=0; j < insertParams.Length-1; j++) {
                insertParams[j] = values[i,j/2];
            }
            WVASSERT(Insert("#test1", insertParams));
        }

        WVASSERT(RunSucker("#suckout", "SELECT * FROM #test1"));

        WVASSERT(Exec("DROP TABLE #test1"));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT * FROM #suckout ORDER BY roworder",
                    out reader));

        using (reader) {
            for (int i=0; i < values.GetLength(0); i++) {
                WVASSERT(reader.Read());

                for (int j=0; j < insertParams.Length-1; j++) {
                    if (values[i,j/2] is DBNull) {
                        WVPASS(reader.IsDBNull(j));
                    } else {
                        // The preprocessor doesn't like the comma in the array
                        // subscripts
                        string val = (string)values[i,j/2];
                        string fromdb = reader.GetSqlDecimal(j).ToString();

                        // Mono produces ".1" and "-.1"
                        // Microsoft .NET produces "0.1" and "-0.1"
                        // Deal with that here.
                        // Bug filed with Mono.
                        if (val[0] == '0' && fromdb[0] == '.') {
                            WVPASSEQ(fromdb, val.Substring(1));
                        } else if (val[0] == '-' && val[1] == '0'
                                && fromdb[0] == '-' && fromdb[1] == '.') {
                            WVPASSEQ(fromdb, "-" + val.Substring(2));
                        } else {
                            WVPASSEQ(fromdb, val);
                        }
                    }
                }
            }

            WVFAIL(reader.Read());
        }
    }

    [Test, Category("Data")]
    public void VerifyFloat()
    {
        // float(53), float(24), real
        // Insert 8 rows: max, something positive, smallest positive, 0,
        // smallest negative, something negative, min, nulls
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)
        //
        // Specifically, infinity, -infinity and NaN are not tested here because
        // SQL Server appears to reject them as values for float columns

        // Construct all of the things we will insert
        object [,] values = {
            {
                /* Can't use SqlDouble.MaxValue et al. because there are
                 * rounding issues in Mono somewhere that make it reject the
                 * exact maximum value. These numbers come from the SQL Server
                 * 2005 reference for the float data type
                 * Bug filed with Mono.
                SqlDouble.MaxValue,
                SqlSingle.MaxValue,
                SqlSingle.MaxValue */
                1.79E+308d,
                3.40E+38f,
                3.40E+38f
            }, {
                /* Mono has problems with sending Math.E in a way that is
                 * roundtrip-able
                 * Bug filed with Mono.
                (double)Math.E,
                (float)Math.E,
                (float)Math.E */
                2.71828182845905d,
                2.718282f,
                2.718282f
            }, {
                /* Can't use Double.Epsilon or Single.Epsilon because SQL server
                 * complains, even on the Microsoft .NET implementation
                 * These numbers come from the SQL Server 2005 reference for the
                 * float data type
                Double.Epsilon,
                Single.Epsilon,
                Single.Epsilon */
                2.23E-308d,
                1.18E-38f,
                1.18E-38f
            }, {
                0.0d,
                0.0f,
                0.0f
            }, {
                /*
                -Double.Epsilon,
                -Single.Epsilon,
                -Single.Epsilon */
                -2.23E-308d,
                -1.18E-38f,
                -1.18E-38f
            }, {
                -127.001d,
                -1270.01f,
                -12700.1f
            }, {
                /*
                SqlDouble.MinValue,
                SqlSingle.MinValue,
                SqlSingle.MinValue */
                -1.79E+308d,
                -3.40E+38f,
                -3.40E+38f
            }, {
                DBNull.Value,
                DBNull.Value,
                DBNull.Value
            }
        };

        WVASSERT(Exec("CREATE TABLE #test1 (f53 float(53), f24 float(24), "
                    + "r real, roworder int not null)"));

        // Now insert them
        object [] insertParams = new object[values.GetLength(1)+1];

        for (int i=0; i < values.GetLength(0); i++) {
            insertParams[insertParams.Length-1] = i;
            for (int j=0; j < insertParams.Length-1; j++) {
                insertParams[j] = values[i,j];
            }
            WVASSERT(Insert("#test1", insertParams));
        }

        WVASSERT(RunSucker("#suckout", "SELECT * FROM #test1"));

        WVASSERT(Exec("DROP TABLE #test1"));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT * FROM #suckout ORDER BY roworder",
                    out reader));

        using (reader) {
            for (int i=0; i < values.GetLength(0); i++) {
                WVASSERT(reader.Read());

                for (int j=0; j < insertParams.Length-1; j++) {
                    // The preprocessor doesn't like the comma in the array
                    // subscripts
                    object val = values[i,j];

                    if (val is DBNull) {
                        WVPASS(reader.IsDBNull(j));
                    } else if (val is double) {
                        WVPASSEQ(reader.GetDouble(j), (double)val);
                    } else if (val is float) {
                        WVPASSEQ(reader.GetFloat(j), (float)val);
                    } else {
                        // If we get here, a data type was used in the values
                        // array that's not handled by one of the above cases
                        bool test_is_broken = true;
                        WVFAIL(test_is_broken);
                    }
                }
            }

            WVFAIL(reader.Read());
        }
    }

    [Test, Category("Data")]
    public void VerifyMoney()
    {
        // money, smallmoney
        // Insert 6 rows: max, a positive amount, 0, a negative amount, min,
        // null
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)

        WVASSERT(Exec("CREATE TABLE #test1 (m money, sm smallmoney, "
                    + "roworder int not null)"));

        WVASSERT(Insert("#test1", SqlMoney.MaxValue, 214748.3647m, 1));
        WVASSERT(Insert("#test1", 1337.42m, 1337.42m, 2));
        WVASSERT(Insert("#test1", 0.0m, 0.0m, 3));
        WVASSERT(Insert("#test1", -3.141m, -3.141m, 5));
        WVASSERT(Insert("#test1", SqlMoney.MinValue, -214748.3648m, 6));
        WVASSERT(Insert("#test1", DBNull.Value, DBNull.Value, 7));

        WVASSERT(RunSucker("#suckout", "SELECT * FROM #test1"));

        WVASSERT(Exec("DROP TABLE #test1"));

        SqlDataReader reader;
        // Cast the return type because Mono doesn't properly handle negative
        // money amounts
        // Bug filed with Mono.
        WVASSERT(Reader("SELECT CAST(m as decimal(20,4)),"
                    + "CAST(sm as decimal(20,4)) "
                    + "FROM #suckout ORDER BY roworder", out reader));

        using (reader) {
            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetDecimal(0), SqlMoney.MaxValue.ToDecimal());
            WVPASSEQ(reader.GetDecimal(1), 214748.3647m);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetDecimal(0), 1337.42m);
            WVPASSEQ(reader.GetDecimal(1), 1337.42m);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetDecimal(0), 0m);
            WVPASSEQ(reader.GetDecimal(1), 0m);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetDecimal(0), -3.141m);
            WVPASSEQ(reader.GetDecimal(1), -3.141m);

            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetDecimal(0), SqlMoney.MinValue.ToDecimal());
            WVPASSEQ(reader.GetDecimal(1), -214748.3648m);

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));
            WVPASS(reader.IsDBNull(1));

            WVFAIL(reader.Read());
        }
    }

    [Test, Category("Data")]
    public void VerifyTimestamp()
    {
        // Create a table with a timestamp column, create a bunch of rows in a
        // particular order, then check that they match up after copying

        // This permutation strategy is discussed in the RowOrdering test
        const int numElems = 101;
        const int prime1 = 47;

        WVASSERT(Exec("CREATE TABLE #test1 (ts timestamp, "
                    + "roworder int not null)"));

        for (int i=0, j=0; i < numElems; i++, j = (i*prime1) % numElems) {
            Insert("#test1", DBNull.Value, j);
        }

        SqlDataReader reader;
        WVASSERT(Reader("SELECT ts,roworder FROM #test1 ORDER BY roworder",
                    out reader));
        
        SqlBinary [] tsdata = new SqlBinary[numElems];

        using (reader) {
            for (int i=0; i < numElems; i++) {
                WVASSERT(reader.Read());
                WVPASSEQ(reader.GetInt32(1), i);
                tsdata[i] = reader.GetSqlBinary(0);
            }

            WVFAIL(reader.Read());
        }

        WVASSERT(RunSucker("#suckout",
                    "SELECT ts,roworder from #test1 ORDER BY ts", true));

        WVASSERT(Exec("DROP TABLE #test1"));

        WVASSERT(Reader("SELECT ts,roworder FROM #suckout ORDER BY _",
                    out reader));

        using (reader) {
            for (int i=0, j=0; i < numElems; i++, j = (i*prime1) % numElems) {
                WVASSERT(reader.Read());
                WVPASSEQ(reader.GetInt32(1), j);
                WVPASSEQ(reader.GetSqlBinary(0), tsdata[j]);
            }

            WVFAIL(reader.Read());
        }
    }

    [Test, Category("Data")]
    public void VerifyUniqueIdentifier()
    {
        // uniqueidentifier
        // Insert 2 rows: a valid number, null
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)

        SqlGuid guid = new SqlGuid("6F9619FF-8B86-D011-B42D-00C04FC964FF");

        WVASSERT(Exec("CREATE TABLE #test1 (u uniqueidentifier, "
                    + "roworder int not null)"));

        WVASSERT(Insert("#test1", guid, 1));
        WVASSERT(Insert("#test1", DBNull.Value, 2));

        WVASSERT(RunSucker("#suckout",
                    "SELECT u from #test1 ORDER BY roworder", true));

        WVASSERT(Exec("DROP TABLE #test1"));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT u FROM #suckout ORDER BY _",
                    out reader));

        using (reader) {
            WVASSERT(reader.Read());
            WVPASSEQ(reader.GetSqlGuid(0), guid);

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));

            WVFAIL(reader.Read());
        }
    }

    [Test, Category("Data")]
    public void VerifyVarBinaryMax()
    {
        // varbinary(max), image

        // This must be sorted
        long [] sizes = { 1, 10, 50, 255, 4000, 8000, 8040, 8192, 16080, 16384,
            24120, 32160, 32768, 40200, 65536, 131072, 262144, 524288, 1048576,
            2097152, 3076506 };

        string [] types = { "varbinary(max)", "image" };

        Byte [] image_data = read_image();

        WVASSERT(image_data.Length >= sizes[sizes.Length-1]);

        foreach (string type in types) {
            WVASSERT(Exec(string.Format("CREATE TABLE #test1 "
                            + "(data {0}, roworder int not null)", type)));

            for (int k=0; k < sizes.Length; k++) {
                Byte [] data = new byte[sizes[k]];
                Array.Copy(image_data, data, sizes[k]);

                WVASSERT(Insert("#test1", new SqlBinary(data), k));
            }

            WVASSERT(Insert("#test1", DBNull.Value, sizes.Length));

            WVASSERT(SetupOutputTable("#test1out"));

            WVASSERT(RunSucker("#test1out",
                        "SELECT * FROM #test1 ORDER BY roworder", true));

            WVASSERT(Exec("DROP TABLE #test1"));

            SqlDataReader reader;
            WVASSERT(Reader("SELECT DATALENGTH(data), data FROM "
                        + "#test1out ORDER BY _",
                        out reader));

            using (reader) {
                for (int k=0; k < sizes.Length; k++) {
                    Byte [] data = new byte[sizes[k]];
                    Array.Copy(image_data, data, sizes[k]);

                    WVASSERT(reader.Read());

                    WVPASSEQ(GetInt64(reader, 0), sizes[k]);
                    WVPASSEQ(reader.GetSqlBinary(1), new SqlBinary(data));
                }

                WVASSERT(reader.Read());
                WVPASS(reader.IsDBNull(1));

                WVFAIL(reader.Read());
            }

            WVASSERT(Exec("DROP TABLE #test1out"));
        }
    }

    [Test, Category("Data")]
    public void VerifyXML()
    {
        // xml
        // Insert 2 rows: some sample XML, null
        // Then check that they were copied correctly
        // Assume that the schema of the output table is correct (tested
        // elsewhere)
        // This isn't very exhaustive, so improvements are welcome.
        // This was going to use SqlXml instead of using a string, but Mono
        // doesn't support that very well.

        // This MUST not have any extra whitespace, as it will be stripped by
        // some SQL parser and won't be reproduced when it comes back out.
        // This is the style that Microsoft's .NET returns
        string xml =
            "<outside><!--hi--><element1 />Text<element2 type=\"pretty\" />"
            + "</outside>";
        // This is the style that Mono returns
        string altxml =
            "<outside><!--hi--><element1/>Text<element2 type=\"pretty\"/>"
            + "</outside>";

        WVASSERT(Exec("CREATE TABLE #test1 (x xml, "
                    + "roworder int not null)"));

        WVASSERT(Insert("#test1", xml, 1));
        WVASSERT(Insert("#test1", DBNull.Value, 2));

        WVASSERT(RunSucker("#suckout",
                    "SELECT x from #test1 ORDER BY roworder", true));

        WVASSERT(Exec("DROP TABLE #test1"));

        SqlDataReader reader;
        WVASSERT(Reader("SELECT x FROM #suckout ORDER BY _",
                    out reader));

        using (reader) {
            WVASSERT(reader.Read());
            // Sigh. 
            if (reader.GetString(0) == altxml) {
                WVPASSEQ(reader.GetString(0), altxml);
            } else {
                WVPASSEQ(reader.GetString(0), xml);
            }

            WVASSERT(reader.Read());
            WVPASS(reader.IsDBNull(0));

            WVFAIL(reader.Read());
        }
    }

    [Test, Category("Data")]
    public void Unicode()
    {
        // nchar, nvarchar (in-row or max), ntext
        // Using lots of non-ascii characters
        
        string unicode_text = read_unicode();

        int [] sizes = { 4000, unicode_text.Length };
        WVASSERT(unicode_text.Length >= sizes[0]);

        string [] types = { "nchar", "nvarchar", "ntext", "nvarchar(max)" };
        int [] typemax = { 4000, 4000, Int32.MaxValue/2, Int32.MaxValue/2 };
        int [] charsize = { 2, 2, 2, 2 };
        bool [] varsize = { false, true, true, true };
        bool [] sizeparam = { true, true, false, false };
        bool [] lenok = { true, true, false, true };

        for (int i=0; i < types.Length; i++) {
            for (int j=0; j < sizes.Length && sizes[j] <= typemax[i]; j++) {
                if (sizeparam[i]) {
                    WVASSERT(Exec(string.Format("CREATE TABLE #test1 "
                                    + "(data {0}({1}), roworder int not null)",
                                    types[i], sizes[j])));
                } else {
                    WVASSERT(Exec(string.Format("CREATE TABLE #test1 "
                                    + "(data {0}, roworder int not null)",
                                    types[i])));
                    j = sizes.Length-1;
                }

                for (int k=0; k <= j; k++) {
                    WVASSERT(Exec(string.Format(
                                    "INSERT INTO #test1 VALUES (N'{0}', {1})",
                                    unicode_text.Substring(0,
                                        sizes[k]).Replace("'", "''"), k)));
                }

                WVASSERT(SetupOutputTable("#test1out"));

                WVASSERT(RunSucker("#test1out",
                            "SELECT * FROM #test1 ORDER BY roworder", true));

                WVASSERT(Exec("DROP TABLE #test1"));

                SqlDataReader reader;

                if (lenok[i]) {
                    WVASSERT(Reader("SELECT LEN(data), DATALENGTH(data), data "
                                + "FROM #test1out ORDER BY _",
                                out reader));
                } else {
                    WVASSERT(Reader("SELECT -1, "
                                + "DATALENGTH(data), data FROM #test1out "
                                + "ORDER BY _",
                                out reader));
                }

                using (reader) {
                    for (int k=0; k <= j; k++) {
                        WVASSERT(reader.Read());

                        if (lenok[i])
                            WVPASSEQ(GetInt64(reader, 0), sizes[k]);

                        WVPASSEQ(GetInt64(reader, 1),
                                sizes[varsize[i] ? k : j]*charsize[i]);
                        WVPASSEQ(reader.GetString(2).Substring(0, sizes[k]),
                                unicode_text.Substring(0, sizes[k]));
                    }

                    WVFAIL(reader.Read());
                }

                WVASSERT(Exec("DROP TABLE #test1out"));
            }
        }
    }

    [Test, Category("Running")]
    public void Recursion()
    {
        // Check that SqlSucker can be called recursively

        // This will need redesigning to recurse past 25 levels because we run
        // out of alphabet (primes list will need to be extended too)
        // No, there is no particular reason that this has to use primes.
        // Also, the default recursion depth of 32 limits recurse_lvl to 9
        const int recurse_lvl = 9;
        char colname_base = 'A';
        int [] primes = { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43,
            47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101 };

        WVASSERT(Exec(string.Format("CREATE PROCEDURE #sucker_test0\n"
            + "AS BEGIN\n"
            + "SELECT {0} as {1}\n"
            + "END", primes[0], colname_base)));

        System.Text.StringBuilder colnames = new System.Text.StringBuilder();

        for (int i=1; i <= recurse_lvl; i++) {
            if (i > 1) {
                colnames.Append(", ");
            }
            colnames.AppendFormat("{0}", (char)(colname_base + i - 1));

            WVASSERT(Exec(string.Format("CREATE PROCEDURE #sucker_test{0}\n"
                    + "AS BEGIN\n"
                    + "CREATE TABLE #out{1} (_ int)\n"
                    + "EXEC SqlSucker '#out{1}','EXEC #sucker_test{1}',false\n"
                    + "SELECT {2}, {3} as {4} FROM #out{1}\n"
                    + "DROP TABLE #out{1}\n"
                    + "END", i, i-1, colnames, primes[i],
                    (char)(colname_base + i))));

        }

        WVASSERT(RunSucker("#suckout",
                    string.Format("EXEC #sucker_test{0}", recurse_lvl)));

        for (int i=recurse_lvl; i >= 0; i--) {
            WVASSERT(Exec(string.Format("DROP PROCEDURE #sucker_test{0}", i)));
        }

        SqlDataReader reader;
        WVASSERT(Reader("SELECT * FROM #suckout", out reader));
        using (reader) {
            WVPASSEQ(reader.FieldCount, recurse_lvl+1);

            for (int i=0; i <= recurse_lvl; i++) {
                WVPASSEQ(reader.GetName(i),
                        string.Format("{0}", (char)(colname_base + i)));
            }

            WVASSERT(reader.Read());
            for (int i=0; i <= recurse_lvl; i++) {
                WVPASSEQ(reader.GetInt32(i), primes[i]);
            }

            WVFAIL(reader.Read());
        }
    }
}

}

