using System.Data.Common;
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

    static Dictionary<string, bool> userpermissions =
    	new Dictionary<string, bool>();

    public static bool is_allowed_unrestricted_queries(string connid)
    {
    	bool is_allowed;
	if (!userpermissions.TryGetValue(connid, out is_allowed))
	{
	    string is_maybe_allowed = inifile.get("Security Level", connid);

	    // If we found *nothing* perhaps we should evaluate a judgement
	    // on security, see if '*' has values... assuming we're  not
	    // already looking for '*''s security.
	    if (is_maybe_allowed == null)
	    {
		is_allowed = false;
		if (connid != "*")
		{
		    is_maybe_allowed = inifile.get("Security Level", "*");

		    //FIXME:  Numeric or boolean scheme?
		    is_allowed = !(is_maybe_allowed == null ||
				    is_maybe_allowed == "0" ||
				    is_maybe_allowed == "false");
		}
	    }
	    else
		is_allowed = !(is_maybe_allowed == "0" ||
				is_maybe_allowed == "false");

	    userpermissions[connid] = is_allowed;
	}
	return is_allowed;
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
