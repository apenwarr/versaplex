using System.Data.SqlClient;
using System.Collections;
using System.Collections.Generic;
using System;
using Wv;

public static class VxSqlPool
{
    private static WvIni inifile;

    public static void SetIniFile(string filepath)
    {
	inifile = new WvIni(filepath);
    }

    private static string find_connection_moniker(string connid)
    {
        // At the moment, a connection ID is just a username
        string dbname = inifile["User Map"][connid];
	if (dbname == null)
	    dbname = inifile["User Map"]["*"]; // try default
        if (dbname == null)
            throw new VxConfigException(
		String.Format("No user '{0}' found.",
                connid));

        string cfgval = inifile["Connections"][dbname];
        if (cfgval == null)
            throw new VxConfigException(String.Format(
                "No connection found for user {0}", connid));

	return cfgval;
    }

    public static string GetUsernameForCert(string cert)
    {
	string username = inifile["Cert Map"][cert];
	if (username == null)
            throw new VxConfigException(String.Format(
                "No user found for cert with fingerprint '{0}'.", cert));

	return username;
    }

    public static WvDbi create(string connid)
    {
	try
	{
	    return WvDbi.create(find_connection_moniker(connid));
	}
	catch (SqlException e)
	{
	    throw new VxConfigException("Connect: " + e.Message);
	}
    }
}
