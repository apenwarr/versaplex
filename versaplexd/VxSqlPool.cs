using System.Data.SqlClient;
using System.Collections.Generic;

namespace versabanq.Versaplex.Server {

public static class VxSqlPool
{
    private static IDictionary<string, SqlConnectionStringBuilder> conninfo 
        = new Dictionary<string, SqlConnectionStringBuilder>();

    private static bool GetConnInfoFromConnId(string connid, 
        out SqlConnectionStringBuilder conStr)
    {
        conStr = new SqlConnectionStringBuilder();

        // Mono doesn't support this
        //conStr.Enlist = false;
        // FIXME: Get this from DBus
        conStr.DataSource = "amsdev";
        conStr.UserID = "asta";
        conStr.Password = "m!ddle-tear";
        conStr.InitialCatalog = "adrian_test";

        System.Console.Write("Faked conStr, about to add connection info");

        // Save it for later so we don't have to keep asking DBus
        conninfo.Add(connid, conStr);

        // FIXME: Need to check if we actually got the info and added
        // it to the list.
        return true;
    }

    public static SqlConnection TakeConnection(string connid)
    {
        SqlConnectionStringBuilder conStr;
        
        if (!conninfo.TryGetValue(connid, out conStr))
        {
            if (!GetConnInfoFromConnId(connid, out conStr))
            {
                System.Console.Write("TakeConnection: No value found, aborting");
                return null;
            }
        }

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
