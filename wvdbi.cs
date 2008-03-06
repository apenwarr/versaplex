using System;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Collections.Specialized;
using Wv;

namespace Wv.Dbi
{
    public class Db
    {
	static WvIni settings = new WvIni("wvodbc.ini");
	IDbConnection db;
	WvLog log = new WvLog("WvDbi");
	bool fake_bind = false;
	
	public Db(string odbcstr)
	{
	    string real;
	    bool use_mssql = false;
	    
	    if (settings[odbcstr].Count > 0)
	    {
		StringDictionary sect = settings[odbcstr];
		
		if (sect["driver"] == "SqlClient")
		{
		    use_mssql = true;
		    fake_bind = true;
		    real = wv.fmt("server={0};database={1};"
				  + "User ID={2};Password={3};",
				  sect["server"],
				  sect["database"],
				  sect["user"], sect["password"]);
		}
		else
		    real = wv.fmt("driver={{{0}}};server={1};database={2};"
				  + "uid={3};pwd={4};",
				  sect["driver"], sect["server"],
				  sect["database"],
				  sect["user"], sect["password"]);
		log.print("Generated ODBC string: {0}", real);
	    }
	    else if (String.Compare(odbcstr, 0, "dsn=", 0, 4, true) == 0)
		real = odbcstr;
	    else if (String.Compare(odbcstr, 0, "driver=", 0, 7, true) == 0)
		real = odbcstr;
	    else
		throw new ArgumentException
		   ("unrecognized odbc string '" + odbcstr + "'");
	    if (use_mssql)
		db = new SqlConnection(real);
	    else
		db = new OdbcConnection(real);
	    db.Open();
	}
	
	IDbCommand prepare(string sql, int nargs)
	{
	    IDbCommand cmd = db.CreateCommand();
	    cmd.CommandText = sql;
	    if (!fake_bind && nargs == 0)
	       cmd.Prepare();
	    return cmd;
	}
	
	// FIXME: if fake_bind, this only works the first time for a given
	// IDBCommand object!  Don't try to recycle them.
	void bind(IDbCommand cmd, params object[] args)
	{
	    if (fake_bind)
	    {
		object[] list = new object[args.Length];
		for (int i = 0; i < args.Length; i++)
		{
		    if (args[i] == null)
			list[i] = "null";
		    else if (args[i] is int)
			list[i] = (int)args[i];
		    else
			list[i] = wv.fmt("'{0}'", args[i].ToString());
		}
		cmd.CommandText = wv.fmt(cmd.CommandText, list);
		log.print("fake_bind: '{0}'", cmd.CommandText);
		return;
	    }
	    
	    bool need_add = (cmd.Parameters.Count < args.Length);
	    
	    for (int i = 0; i < args.Length; i++)
	    {
		object a = args[i];
		IDataParameter param;
		if (cmd.Parameters.Count <= i)
		    cmd.Parameters.Add(param = cmd.CreateParameter());
		else
		    param = (IDataParameter)cmd.Parameters[i];
		if (a is DateTime)
		    param.DbType = DbType.DateTime;
		else
		    param.DbType = DbType.Int32; // I sure hope so...
		param.Value = a;
	    }
	    
	    if (need_add)
		cmd.Prepare();
	}
	
	public IDataReader select(string sql, params object[] args)
	{
	    return select(prepare(sql, args.Length), args);
	}
	
	public IDataReader select(IDbCommand cmd, params object[] args)
	{
	    bind(cmd, args);
	    return cmd.ExecuteReader();
	}
	
	public int execute(string sql, params object[] args)
	{
	    return execute(prepare(sql, args.Length), args);
	}
	
	public int execute(IDbCommand cmd, params object[] args)
	{
	    bind(cmd, args);
	    return cmd.ExecuteNonQuery();
	}
	
	public int try_execute(string sql, params object[] args)
	{
	    try
	    {
		return execute(sql, args);
	    }
	    catch (OdbcException)
	    {
		// well, I guess no rows were affected...
		return 0;
	    }
	}
    }
}

