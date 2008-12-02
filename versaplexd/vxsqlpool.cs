using System;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Wv;
using Wv.Extensions;

public static class VxSqlPool
{
    private static WvIni inifile;

    public static void SetIniFile(string path)
    {
	if (path.e() || !File.Exists(path))
	    throw new Exception
	        (wv.fmt("Could not find config file '{0}'", path));
	inifile = new WvIni(path);
    }

    private static string find_connection_moniker(string connid)
    {
        // At the moment, a connection ID is just a username
        string dbname = inifile.get("User Map", connid);
	if (dbname == null)
	    dbname = inifile.get("User Map", "*"); // try default
        if (dbname == null)
            throw new VxConfigException(
		String.Format("No user '{0}' found.",
                connid));

        string cfgval = inifile.get("Connections", dbname);
        if (cfgval == null)
            throw new VxConfigException(String.Format(
                "No connection found for user {0}", connid));

	return cfgval;
    }

    public static string GetUsernameForCert(string cert)
    {
	string username = inifile.get("Cert Map", cert);
	if (username == null)
            throw new VxConfigException(String.Format(
                "No user found for cert with fingerprint '{0}'.", cert));

	return username;
    }

    static Dictionary<string, int> userpermissions =
    	new Dictionary<string, int>();

    public static int access_restrictions(string connid)
    {
    	int security_level;
	if (!userpermissions.TryGetValue(connid, out security_level))
	{
	    string iniseclevel = inifile.get("Security Level", connid);

	    // If we found *nothing* perhaps we should evaluate a judgement
	    // on security, see if '*' has values... assuming we're  not
	    // already looking for '*''s security.
	    if (iniseclevel == null)
	    {
		security_level = 2;
		if (connid != "*")
		{
		    iniseclevel = inifile.get("Security Level", "*");

		    if (iniseclevel != null &&
			(iniseclevel == "0" ||
			iniseclevel == "1"))
			security_level = Convert.ToInt32(iniseclevel);
		}
	    }
	    else
		security_level = !(iniseclevel == "0" ||
		    iniseclevel == "1") ? 2 :
			Convert.ToInt32(iniseclevel);

	    userpermissions[connid] = security_level;
	}
	return security_level;
    }

    public static WvDbi create(string connid)
    {
	try
	{
	    return WvDbi.create(find_connection_moniker(connid));
	}
	catch (DbException e)
	{
	    throw new VxConfigException("Connect: " + e.Message);
	}
    }
}
