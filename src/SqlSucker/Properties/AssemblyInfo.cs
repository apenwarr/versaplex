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
    // A required parameter to SqlSucker_GetConInfo to deter users from trying
    // to call it directly. Although if they call it anyway, it won't hurt
    // anything.
    private const int conInfoMagic = 83926752;

    private static object conInitLock = new object();
    private static object conInfoLock = new object();
    private static bool conInfoOk = false;
    private static string tempServer;
    private static string tempUser;
    private static string tempPassword;
    private static string tempTable;
    private static string tempDatabase;

    // Name to use for output table's ordering column
    private const string orderingColumn = "_";
    // Name to use for input table's dummy column
    private const string dummyColumn = "_";

    private const bool sendDebugInfo = false;

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void SqlSucker_GetConInfo(SqlInt32 magic)
    {
        if (conInfoMagic != magic.Value) {
            throw new System.Exception(
                "This procedure is internal to SqlSucker. "
                + "Do not call it directly.");
        }

        lock (conInfoLock) {
            conInfoOk = false;

            using (SqlConnection ctxCon = new SqlConnection(
                "context connection=true"))
            using (SqlCommand ctxConCmd = new SqlCommand(
                "SELECT server, username, password, tablename, dbname "
                + "FROM SqlSuckerConfig", ctxCon)) {

                ctxCon.Open();

                using (SqlDataReader reader = ctxConCmd.ExecuteReader()) {
                    if (!reader.Read()) {
                        throw new System.Exception(
                            "No rows in SqlSuckerConfig");
                    }

                    tempServer = reader.GetString(0);
                    tempUser = reader.GetString(1);
                    tempPassword = reader.GetString(2);
                    tempTable = reader.GetString(3);
                    tempDatabase = reader.GetString(4);
                    conInfoOk = true;
                }
            }
        }
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void SqlSucker(SqlString TableName, SqlString SQL, bool ordered)
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

            SqlCommand ctxConCmd = ctxCon.CreateCommand();
            SqlCommand tmpConCmd = tmpCon.CreateCommand();

            SqlConnectionStringBuilder tmpConStr =
                new SqlConnectionStringBuilder();
            tmpConStr.Enlist = false;

            lock (conInitLock) {
                ctxConCmd.CommandText = string.Format(
                    "EXEC SqlSucker_GetConInfo {0}", conInfoMagic);
                ctxConCmd.ExecuteNonQuery();

                lock (conInfoLock) {
                    if (!conInfoOk) {
                        throw new System.Exception(
                            "Can't load temporary connection info");
                    }

                    tmpConStr.DataSource = tempServer;
                    tmpConStr.UserID = tempUser;
                    tmpConStr.Password = tempPassword;
                    tmpConStr.InitialCatalog = tempDatabase;
                }
            }

            tmpCon.ConnectionString = tmpConStr.ConnectionString;

            // Check the output table before doing much so that we bail out
            // early with minimal side-effects
            ValidateOutputTable(ctxCon, outTable);

            string colDefs;
            SqlParameter[] paramList;
            bool anyRows;

            ctxConCmd.CommandText = SQL.ToString();

            using (SqlDataReader reader = ctxConCmd.ExecuteReader()) {
                if (reader.FieldCount <= 0) {
                    throw new System.Exception(
                        "Query returned no usable results");
                }

                colDefs = GenerateSchema(reader, out paramList);

                if (sendDebugInfo)
                    SqlContext.Pipe.Send(colDefs);

                anyRows = reader.Read();

                if (anyRows) {
                    try {
                        tmpCon.Open();
                    } catch (System.Exception e) {
                        throw new System.Exception(
                            "Could not open temporary output connection", e);
                    }

                    SetupTempTable(tmpCon, colDefs, ordered);
                    CopyReader(reader, tmpCon, tempTable, paramList);
                }
            }

            // The executed SQL statement may have side-effects, so check the
            // output table again
            ValidateOutputTable(ctxCon, outTable);

            SetupOutTable(outTable, ctxCon, colDefs, ordered);

            if (!anyRows) {
                // We're done because there's no data to copy
                return;
            }

            if (ordered) {
                tmpConCmd.CommandText = string.Format(
                    "SELECT * FROM {0} ORDER BY {1}", tempTable,
                    orderingColumn);
            } else {
                tmpConCmd.CommandText = string.Format(
                    "SELECT * FROM {0}", tempTable);
            }

            using (SqlDataReader reader = tmpConCmd.ExecuteReader()) {
                if (!reader.Read()) {
                    throw new System.Exception(
                        "Data was expected in intermediate table; none found");
                }

                CopyReader(reader, ctxCon, outTable, paramList);
            }

            DropTempTable(tmpCon);
        }
    }

    // Needs reader to be positioned on the first row
    private static void CopyReader(SqlDataReader reader, SqlConnection outCon,
        string outTable, SqlParameter[] paramList)
    {
        using (SqlCommand outCmd = outCon.CreateCommand())
        try {
            System.Text.StringBuilder insertCmdStr =
                new System.Text.StringBuilder();

            insertCmdStr.AppendFormat("INSERT INTO {0} VALUES (", outTable);

            for (int i = 0; i < paramList.GetLength(0); i++) {
                if (i > 0)
                    insertCmdStr.Append(", ");

                insertCmdStr.AppendFormat("@col{0}", i);
                outCmd.Parameters.Add(paramList[i]);
            }

            insertCmdStr.Append(")");

            outCmd.CommandText = insertCmdStr.ToString();

            // If this throws, the most likely culprit is one (or more) of the
            // SqlParameters being generated incorrectly in GenerateSchema()
            outCmd.Prepare();

            do {
                for (int i = 0; i < paramList.GetLength(0); i++) {
                    paramList[i].SqlValue = reader.GetSqlValue(i);
                }
                outCmd.ExecuteNonQuery();

                if (sendDebugInfo)
                    SqlContext.Pipe.Send(string.Format("Record sent to {0}",
                            outTable));
            } while (reader.Read());

            outCmd.Parameters.Clear();
        } catch (SqlException e) {
            throw new System.Exception(
                string.Format("Could not send data to {0}", outTable), e);
        }
    }

    private static void SetupOutTable(string outTable, SqlConnection con,
        string colDefs, bool ordered)
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
            cmd.CommandText = string.Format("ALTER TABLE {0} DROP COLUMN {1}",
                outTable, dummyColumn);
            cmd.ExecuteNonQuery();

            if (ordered)
                AddOrderingColumn(con, outTable);
        } catch (SqlException e) {
            throw new System.Exception("Could not set up output table", e);
        }
    }

    private static void AddOrderingColumn(SqlConnection con, string table)
    {
        using (SqlCommand cmd = con.CreateCommand()) {
            cmd.CommandText = string.Format(
                "ALTER TABLE {0} ADD {1} bigint IDENTITY NOT NULL PRIMARY KEY",
                table, orderingColumn);
            cmd.ExecuteNonQuery();
        }
    }

    private static void SetupTempTable(SqlConnection con, string colDefs, bool ordered)
    {
        using (SqlCommand cmd = con.CreateCommand())
        try {
            cmd.CommandText = string.Format("CREATE TABLE {0} ({1})", tempTable,
                colDefs);
            cmd.ExecuteNonQuery();

            if (ordered)
                AddOrderingColumn(con, tempTable);
        } catch (SqlException e) {
            throw new System.Exception(
                "Could not create intermediate table", e);
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

    private static string GenerateSchema(SqlDataReader reader,
        out SqlParameter[] paramList)
    {
        System.Text.StringBuilder colDefs = new System.Text.StringBuilder();
        paramList = new SqlParameter[reader.FieldCount];

        int i = 0;

        using (DataTable schema = reader.GetSchemaTable()) {
            bool first = true;

            foreach (DataRowView col in schema.DefaultView) {
                bool isLong = (bool)col["IsLong"];

                // First set up the parameter list entry; keep the parsed enum
                // around so that we can use it later.
                paramList[i] = new SqlParameter();
                paramList[i].ParameterName = string.Format("@col{0}", i);

                // This makes me cry, but if there is actually a better way to
                // do this, it's hard to find. Extra crying for the try/catches
                // with empty catch blocks.
                string dbtStr = (string)col["DataTypeName"];
                
                if (dbtStr.ToLower() == "sql_variant") {
                    // The parse-the-string-as-enum-value trick works except
                    // that sql_variant becomes Variant. Close. However,
                    // sql_variant types seem to be difficult to handle
                    // properly and thus are not supported.
                    throw new System.Exception("Columns of type sql_variant "
                        + "are not supported by SqlSucker at this time");
                }

                SqlDbType dbt = (SqlDbType)System.Enum.Parse(typeof(SqlDbType),
                        dbtStr, true);

                if (isLong) {
                    // Clearly, when we have a long type, the SQL server should
                    // keep calling it one thing, while we use a different enum
                    // value for SqlParameter (with the name of a deprecated
                    // data type!). Anything else would just make too much
                    // sense!
                    switch (dbt) {
                    case SqlDbType.NVarChar:
                        paramList[i].SqlDbType = SqlDbType.NText;
                        break;
                    case SqlDbType.VarChar:
                        paramList[i].SqlDbType = SqlDbType.Text;
                        break;
                    case SqlDbType.VarBinary:
                        paramList[i].SqlDbType = SqlDbType.Image;
                        break;
                    default:
                        paramList[i].SqlDbType = dbt;
                        break;
                    }
                } else {
                    paramList[i].SqlDbType = dbt;
                }
                paramList[i].Direction = ParameterDirection.Input;

                // So the story behind the rest of these is that they don't
                // really have to be set for all data types, but it seems
                // exceedingly horrible to exhaustively figure out which
                // situations they're required for. Instead, we try to set
                // them and ignore if it doesn't work. If something went wrong
                // that matters, outCmd.Prepare() will throw in CopyReader()
                // anyway.
                try {
                    // XXX: Apparently long datatypes set ColumnSize to 2^31-1.
                    // This is fine for varchar and binary, but nvarchar needs
                    // it to be 2^30-1. So hack around that.
                    if (dbt == SqlDbType.Xml) {
                        // Um, this appears to magically work.
                        paramList[i].Size = -1;
                    } else if (isLong && dbt == SqlDbType.NVarChar) {
                        paramList[i].Size = ((int)col["ColumnSize"])/2;
                    } else {
                        paramList[i].Size = (int)col["ColumnSize"];
                    }
                } catch { }
                try {
                    paramList[i].Precision =
                        (byte)((System.Int16)col["NumericPrecision"]);
                } catch { }
                try {
                    paramList[i].Scale =
                        (byte)((System.Int16)col["NumericScale"]);
                } catch { }

                if (!first) {
                    colDefs.Append(", ");
                }

                string typeParams = "";

                // Ew. Gross.
                System.Type dt =
                    (System.Type)col["ProviderSpecificDataType"];

                if (dt == typeof(SqlString) || dt == typeof(SqlBinary)) {
                    if (isLong) {
                        switch (dbt) {
                        case SqlDbType.Image:
                        case SqlDbType.NText:
                        case SqlDbType.Text:
                            // These are deprecated type names that are
                            // identical to varbinary(max), nvarchar(max)
                            // and varchar(max) (respectively), except that
                            // you don't have to specify a size. In fact, if
                            // you do, then it croaks with an error.
                            break;
                        default:
                            typeParams = "(max)";
                            break;
                        }
                    } else {
                        switch (dbt) {
                        case SqlDbType.Timestamp:
                            // This turns into a varbinary(8)
                            typeParams = "(8)";
                            break;
                        default:
                            typeParams = string.Format("({0})",
                                col["ColumnSize"]);
                            break;
                        }
                    }
                } else if (dt == typeof(SqlDecimal)) {
                    typeParams = string.Format("({0},{1})",
                        col["NumericPrecision"], col["NumericScale"]);
                }

                if (col["ColumnName"].ToString().Length == 0) {
                    throw new System.Exception(string.Format(
                        "Result set column {0} needs to have a name",
                        col["ColumnOrdinal"]));
                }

                // Magical special cases that need to be handled
                switch (dbt) {
                case SqlDbType.Timestamp:
                    // timestamp is probably not what we actually want to use
                    // in the results because it'll become a timestamp for the
                    // new temporary table. Instead, store what the value was
                    // in a varbinary(8) column (the 8 is set above).
                    dbtStr = "varbinary";
                    break;
                }

                colDefs.AppendFormat("[{0}] {1}{2}",
                    col["ColumnName"].ToString().Replace("]", "]]"),
                    dbtStr, typeParams);

                if (sendDebugInfo)
		            SqlContext.Pipe.Send(col["ColumnName"].ToString()+" "
			            +dbtStr+" size "+col["ColumnSize"]);

                first = false;
                i++;
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
                if (reader.FieldCount != 1 || reader.GetName(0) != dummyColumn) {
                    throw new System.Exception(
                        "Output table has incorrect schema");
                }
            }
        } catch (SqlException e) {
            throw new System.Exception("Error with output table", e);
        }
    }
}
