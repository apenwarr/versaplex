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
                    tmpCon.Open();

                    try {
                        tmpConCmd.CommandText = string.Format(
                            "CREATE TABLE {0} ({1})", tempTable, colDefs);
                        tmpConCmd.ExecuteNonQuery();
                    } catch (SqlException e) {
                        throw new System.Exception(
                            "Could not create intermediate table", e);
                    }

                    CopyReader(reader, tmpCon, tempTable, paramList);
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

            insertCmdStr.AppendFormat(
                "INSERT INTO {0} VALUES (@col0", outTable);
            // There is always at least 1 column,
            // so start loop at 1 instead of 0
            for (int i = 1; i < reader.FieldCount; i++) {
                insertCmdStr.AppendFormat(", @col{0}", i);
            }
            insertCmdStr.Append(")");

            for (int i = 0; i < reader.FieldCount; i++) {
                outCmd.Parameters.Add(paramList[i]);
            }

            outCmd.CommandText = insertCmdStr.ToString();

            // If this throws, the most likely culprit is one (or more) of the
            // SqlParameters being generated incorrectly in GenerateSchema()
            outCmd.Prepare();

            do {
                for (int i = 0; i < reader.FieldCount; i++) {
                    paramList[i].SqlValue = reader[i];
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

                switch (dbt) {
                case SqlDbType.Timestamp:
                    // Timestamp columns can not have a value explicitly set
                    // into them, so they can't be copied and thus are not
                    // supported (if necessary, perhaps it could be converted
                    // into a datetime?)
                    throw new System.Exception("Columns of type timestamp "
                        + "are not supported by SqlSucker at this time");
                }

                if (isLong) {
                    // Clearly, when we have a long type, the SQL server should
                    // keep calling it one thing, while we use a different enum
                    // value for SqlParameter (with the name of a deprecated
                    // data type!). Anything else would just make too much
                    // sense!
                    switch (dbt) {
                    case SqlDbType.NVarChar:
                    case SqlDbType.NText:
                        paramList[i].SqlDbType = SqlDbType.NText;
                        break;
                    case SqlDbType.VarChar:
                    case SqlDbType.Text:
                        paramList[i].SqlDbType = SqlDbType.Text;
                        break;
                    case SqlDbType.VarBinary:
                    case SqlDbType.Image:
                        paramList[i].SqlDbType = SqlDbType.Image;
                        break;
                    case SqlDbType.Xml:
                        paramList[i].SqlDbType = SqlDbType.Xml;
                        break;
                    default:
                        throw new System.Exception(string.Format(
                            "Unknown long column type {0} detected",
                            col["DataTypeName"]));
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
                        paramList[i].Size = (1 << 30) - 1;
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
                            // This is interpreted as a SqlBinary but it is
                            // an error to specify a size.
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

                colDefs.AppendFormat("[{0}] {1}{2}",
                    col["ColumnName"].ToString().Replace("]", "]]"),
                    col["DataTypeName"], typeParams);

                if (sendDebugInfo)
		            SqlContext.Pipe.Send(col["ColumnName"].ToString()+" "
			            +col["DataTypeName"]+" size "+col["ColumnSize"]);

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
