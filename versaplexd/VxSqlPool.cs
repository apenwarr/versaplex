using System.Data.SqlClient;
using System.Collections;
using System.Collections.Generic;
using Wv.Utils;

namespace versabanq.Versaplex.Server {

public static class VxSqlPool
{
    private static Ini inifile = new Ini("versaplexd.ini");

    private static bool GetConnInfoFromConnId(string connid, 
        out SqlConnectionStringBuilder conStr)
    {
        conStr = new SqlConnectionStringBuilder();

        // Mono doesn't support this
        //conStr.Enlist = false;

        // At the moment, a connection ID is just a username
        string dbname = inifile["User Map"][connid];
        if (dbname == null)
            return false;

        string cfgval = inifile["Connections"][dbname];
        if (cfgval == null)
            return false;

        string moniker_name = "mssql:";
        if (cfgval.IndexOf(moniker_name) == 0)
            conStr.ConnectionString = cfgval.Substring(moniker_name.Length);
        else
            return false;

        System.Console.Write("Connection string: {0}", conStr.ConnectionString);

        return true;
    }

    public static SqlConnection TakeConnection(string connid)
    {
        SqlConnectionStringBuilder conStr;
        System.Console.WriteLine("TakeConnection {0}, starting", connid);
        
        if (!GetConnInfoFromConnId(connid, out conStr))
        {
            System.Console.WriteLine(
                "TakeConnection: No value found, aborting");
            return null;
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
