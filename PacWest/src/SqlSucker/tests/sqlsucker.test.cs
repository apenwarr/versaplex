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
	using (SqlCommand execCmd = new SqlCommand(query, con)) {
	    execCmd.ExecuteNonQuery();
	}

	return true;
    }

    bool Scalar(string query, out object result)
    {
	using (SqlCommand execCmd = new SqlCommand(query, con)) {
	    result = execCmd.ExecuteScalar();
	}

	return true;
    }

    bool Reader(string query, out SqlDataReader result)
    {
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
	Exec(string.Format(
		    "EXEC SqlSucker '{0}', '{1}'",
		    table.Replace("'", "''"),
		    query.Replace("'", "''")));
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

	if (con != null)
	    con.Dispose();
	con = null;
    }

    [Test]
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
	}
    }

    [Test]
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
}

}

