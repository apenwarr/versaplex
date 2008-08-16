using System.Data.SqlClient;
using System.Collections;
using System.Collections.Generic;
using System;
using Wv;

public static class VxSqlPool
{
    static WvLog log = new WvLog("VxSqlPool", WvLog.L.Debug2);
    private static WvIni inifile;

    public static void SetIniFile(string filepath)
    {
	inifile = new WvIni(filepath);
    }

    private static SqlConnectionStringBuilder GetConnInfoFromConnId(
        string connid)
    {
        SqlConnectionStringBuilder conStr = new SqlConnectionStringBuilder();

        // Mono doesn't support this
        //conStr.Enlist = false;

        // At the moment, a connection ID is just a username
        string dbname = inifile["ConnId Map"][connid];
	if (dbname == null)
	    dbname = inifile["ConnId Map"]["*"]; // try default
        if (dbname == null)
            throw new VxConfigException(
		String.Format("No conn id '{0}' found.",
                connid));

        string cfgval = inifile["Connections"][dbname];
        if (cfgval == null)
            throw new VxConfigException(String.Format(
                "No connection found for conn id {0}", connid));

        string moniker_name = "mssql:";
        if (cfgval.IndexOf(moniker_name) == 0)
            conStr.ConnectionString = cfgval.Substring(moniker_name.Length);
        else
            throw new VxConfigException(String.Format(
                "Malformed connection string '{0}'.", cfgval));

        log.print("Connection string: {0}\n", conStr.ConnectionString);

        return conStr;
    }

    public static SqlConnection TakeConnection(string connid)
    {
        log.print("TakeConnection {0}, starting\n", connid);
        
        SqlConnectionStringBuilder conStr = GetConnInfoFromConnId(connid);
	
	try
	{
	    SqlConnection con = new SqlConnection(conStr.ConnectionString);
	    con.Open();
	    return con;
	}
	catch (SqlException e)
	{
	    throw new VxConfigException("Connect: " + e.Message);
	}
    }

    public static void ReleaseConnection(SqlConnection c)
    {
        c.Close();
    }
}
