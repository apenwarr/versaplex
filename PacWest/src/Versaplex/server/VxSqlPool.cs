using System.Data.SqlClient;

namespace versabanq.Versaplex.Server {

public static class VxSqlPool
{
    public static SqlConnection TakeConnection()
    {
        SqlConnectionStringBuilder conStr =
            new SqlConnectionStringBuilder();

        // Mono doesn't support this
        //conStr.Enlist = false;
        conStr.DataSource = "amsdev";
        conStr.UserID = "asta";
        conStr.Password = "m!ddle-tear";
        conStr.InitialCatalog = "adrian_test";

        SqlConnection con = new SqlConnection(conStr.ConnectionString);

        // FIXME: Exceptions
        con.Open();

        return con;
    }

    public static void ReleaseConnection(SqlConnection c)
    {
        c.Close();
    }
}

}
