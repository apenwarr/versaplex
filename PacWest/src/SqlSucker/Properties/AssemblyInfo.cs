using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.SqlServer.Server;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;

// General Information about an assembly is controlled through the following
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("SqlSucker")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Versabanq")]
[assembly: AssemblyProduct("SqlSucker")]
[assembly: AssemblyCopyright("Copyright © Versabanq 2007")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

[assembly: ComVisible(false)]

//
// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version
//      Build Number
//      Revision
//
// You can specify all the values or you can default the Revision and Build
// Numbers by using the '*' as shown below:
[assembly: AssemblyVersion("1.0.*")]

public class SqlSuckerProc
{
    private const string tempServer = "localhost";
    private const string tempUser = "asta";
    private const string tempPassword = "m!ddle-tear";
    // Make sure tempTable has square bracket quoting if it contains spaces
    private const string tempTable = "#SuckerTemp";
    // Set tempDatabase to null to use database name of context connection
    private const string tempDatabase = null;

    private const bool sendDebugInfo = false;

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void SqlSucker(SqlString TableName, SqlString SQL)
    {
        if (TableName.IsNull) {
            throw new System.Exception("Table name must not be null");
        }
        if (TableName.ToString().Length == 0) {
            throw new System.Exception("Table name must not be empty");
        }
        string outTable = string.Format("[{0}]",
            TableName.ToString().Replace("]", "]]"));

        if (SQL.IsNull) {
            throw new System.Exception("SQL string must not be null");
        }
        if (SQL.ToString().Length == 0) {
            throw new System.Exception("SQL string must not be empty");
        }

        using (SqlConnection ctxCon = new SqlConnection(
            "context connection=true"))
        using (SqlConnection tmpCon = new SqlConnection()) {
            ctxCon.Open();

            tmpCon.ConnectionString = string.Format(
                "Server={0};UID={1};Password={2};Database={3}",
                tempServer, tempUser, tempPassword,
                tempDatabase == null ? ctxCon.Database : tempDatabase);

            SqlCommand ctxConCmd = ctxCon.CreateCommand();
            SqlCommand tmpConCmd = tmpCon.CreateCommand();

            ValidateOutputTable(ctxCon, outTable);

            string colDefs;
            bool anyRows;

            ctxConCmd.CommandText = SQL.ToString();

            using (SqlDataReader reader = ctxConCmd.ExecuteReader()) {
                if (!reader.HasRows || reader.FieldCount <= 0) {
                    throw new System.Exception(
                        "Query returned no usable results");
                }

                colDefs = GenerateSchema(reader);

                if (sendDebugInfo)
                    SqlContext.Pipe.Send(colDefs);

                anyRows = reader.Read();

                if (anyRows) {
                    tmpCon.Open();

                    try {
                        tmpConCmd.CommandText = string.Format(
                            "CREATE TABLE {0} ({1})", tempTable, colDefs);
                        tmpConCmd.ExecuteNonQuery();
                    } catch (SqlException e) {
                        throw new System.Exception(
                            "Could not create intermediate table", e);
                    }

                    CopyReader(reader, tmpCon, tempTable);
                }
            }

            SetupOutTable(outTable, ctxCon, colDefs);

            if (!anyRows) {
                // We're done because there's no data to copy
                return;
            }

            tmpConCmd.CommandText = string.Format(
                "SELECT * FROM {0}", tempTable);

            using (SqlDataReader reader = tmpConCmd.ExecuteReader()) {
                if (!reader.Read()) {
                    throw new System.Exception(
                        "Data was expected in intermediate table; none found");
                }

                CopyReader(reader, ctxCon, outTable);
            }

            DropTempTable(tmpCon);
        }
    }

    // Needs reader to be positioned on the first row
    private static void CopyReader(SqlDataReader reader, SqlConnection outCon,
        string outTable)
    {
        using (SqlCommand outCmd = outCon.CreateCommand())
        try {
            System.Text.StringBuilder insertCmdStr =
                new System.Text.StringBuilder();

            insertCmdStr.AppendFormat(
                "INSERT INTO {0} VALUES (@col0", outTable);
            // There is always at least 1 column,
            // so start loop at 1 instead of 0
            for (int i = 1; i < reader.FieldCount; i++) {
                insertCmdStr.AppendFormat(", @col{0}", i);
            }
            insertCmdStr.Append(")");

            SqlParameter[] insertParams =
                new SqlParameter[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++) {
                insertParams[i] = new SqlParameter();
                insertParams[i].ParameterName = string.Format("@col{0}", i);
                insertParams[i].SqlValue = reader[i];
                outCmd.Parameters.Add(insertParams[i]);
            }

            outCmd.CommandText = insertCmdStr.ToString();

            // It doesn't like this for some reason
            // outCmd.Prepare();

            // We already filled in the first row
            outCmd.ExecuteNonQuery();

            if (sendDebugInfo)
                SqlContext.Pipe.Send(string.Format("Record sent to {0}",
                        outTable));

            while (reader.Read()) {
                for (int i = 0; i < reader.FieldCount; i++) {
                    insertParams[i].SqlValue = reader[i];
                }
                outCmd.ExecuteNonQuery();

                if (sendDebugInfo)
                    SqlContext.Pipe.Send(string.Format("Record sent to {0}",
                            outTable));
            }
        } catch (SqlException e) {
            throw new System.Exception(
                string.Format("Could not send data to {0}", outTable), e);
        }
    }

    private static void SetupOutTable(string outTable, SqlConnection con, string colDefs)
    {
        using (SqlCommand cmd = con.CreateCommand())
        try {
            // Empty it
            cmd.CommandText = string.Format("TRUNCATE TABLE {0}", outTable);
            cmd.ExecuteNonQuery();

            // Add new columns
            cmd.CommandText = string.Format("ALTER TABLE {0} ADD {1}",
                outTable, colDefs);
            cmd.ExecuteNonQuery();

            // Delete the dummy column
            cmd.CommandText = string.Format("ALTER TABLE {0} DROP COLUMN _",
                outTable);
            cmd.ExecuteNonQuery();
        } catch (SqlException e) {
            throw new System.Exception("Could not set up output table", e);
        }
    }

    private static void DropTempTable(SqlConnection con)
    {
        using (SqlCommand cmd = con.CreateCommand())
        try {
            cmd.CommandText = string.Format("DROP TABLE {0}",
                tempTable);
            cmd.ExecuteNonQuery();
        } catch (SqlException e) {
            throw new System.Exception(
                "Could not clean up intermediate table", e);
        }
    }

    private static string GenerateSchema(SqlDataReader reader)
    {
        System.Text.StringBuilder colDefs = new System.Text.StringBuilder();

        using (DataTable schema = reader.GetSchemaTable()) {
            bool first = true;

            foreach (DataRowView col in schema.DefaultView) {
                if (!first) {
                    colDefs.Append(", ");
                }

                string typeParams = "";

                // Ew. Gross.
                System.Type dt =
                    (System.Type)col["ProviderSpecificDataType"];

                if (dt == typeof(SqlString) || dt == typeof(SqlBinary)) {
                    if ((bool)col["IsLong"]) {
                        typeParams = "(max)";
                    } else {
                        typeParams = string.Format("({0})", col["ColumnSize"]);
                    }
                } else if (dt == typeof(SqlDecimal)) {
                    typeParams = string.Format("({0},{1})",
                        col["NumericPrecision"], col["NumericScale"]);
                }

                colDefs.AppendFormat("[{0}] {1}{2}",
                    col["ColumnName"].ToString().Replace("]", "]]"),
                    col["DataTypeName"], typeParams);

                first = false;
            }
        }

        return colDefs.ToString();
    }

    private static void ValidateOutputTable(SqlConnection con, string outTable)
    {
        using (SqlCommand cmd = con.CreateCommand())
        try {
            // Get a column list in the output table
            cmd.CommandText = string.Format("SELECT * FROM {0} WHERE 1=2",
                outTable);

            using (SqlDataReader reader = cmd.ExecuteReader()) {
                if (reader.FieldCount != 1 || reader.GetName(0) != "_") {
                    throw new System.Exception(
                        "Output table has incorrect schema");
                }
            }
        } catch (SqlException e) {
            throw new System.Exception("Error with output table", e);
        }
    }
}