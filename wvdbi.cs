using System;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Collections.Specialized;
using System.Collections.Generic;
using System.Linq;
using Wv;
using Wv.Extensions;

namespace Wv
{
    public class WvDbi: IDisposable
    {
	static WvIni settings = new WvIni("wvodbc.ini");
	protected static WvLog log = new WvLog("WvDbi", WvLog.L.Debug1);
	IDbConnection _db;
	protected IDbConnection db 
	    { get { return _db; } }
	public IDbConnection fixme_db 
	    { get { return _db; } }
	static bool registered = false;
	
        // MSSQL freaks out if there are more than 100 connections open at a
        // time.  Give ourselves a safety margin.
        static int num_active = 0;
        static int max_active = 50;
	
	public static WvDbi create(string moniker)
	{
	    if (!registered)
	    {
		registered = true;
		WvDbi_MSSQL.register();
		WvDbi_ODBC.register();
	    }
	    
	    log.print("Creating '{0}'\n", moniker);
	    
	    if (!moniker.Contains(":") && settings[moniker].Count > 0)
	    {
		StringDictionary sect = settings[moniker];
		
		if (sect["driver"] == "SqlClient")
		    return create(wv.fmt("mssql:"
					 + "server={0};database={1};"
					 + "User ID={2};Password={3};",
					 sect["server"],
					 sect["database"],
					 sect["user"], sect["password"]));
		else
		    return create(wv.fmt("ado:"
					 + "driver={0};server={1};database={2};"
					 + "uid={3};pwd={4};",
					 sect["driver"], sect["server"],
					 sect["database"],
					 sect["user"], sect["password"]));
	    }
	    
	    if (moniker.StartsWith("dsn=") || moniker.StartsWith("driver="))
		return create("ado:" + moniker);
	    
	    return WvMoniker<WvDbi>.create(moniker);
	}
	
	protected WvDbi()
	{
            wv.assert(num_active < max_active, "Too many open connections");
            num_active++;
	}

	~WvDbi()
	{
	    wv.assert(false, "A WvDbi object was not Dispose()d");
	}
	
	protected void opendb(IDbConnection db)
	{
	    this._db = db;
            if ((db.State & System.Data.ConnectionState.Open) == 0)
                db.Open();
	}

        public IDbConnection Conn
        {
            get { return db; }
        }
	
	// Implement IDisposable.
	public void Dispose() 
	{
            num_active--;
	    db.Dispose();
	    GC.SuppressFinalize(this); 
	}
	
	protected virtual IDbCommand prepare(string sql, int nargs)
	{
	    IDbCommand cmd = db.CreateCommand();
	    cmd.CommandText = sql;
	    if (nargs == 0)
	       cmd.Prepare();
	    return cmd;
	}
	
	// Note: this is overridden in the MSSQL version because the binding
	// stuff doesn't actually work in that one under mono.  Sigh.
	protected virtual void bind(IDbCommand cmd, params object[] args)
	{
	    bool need_add = (cmd.Parameters.Count < args.Length);
	    
	    // This is the safe one, because we use normal bind() and thus
	    // the database layer does our escaping for us.
	    for (int i = 0; i < args.Length; i++)
	    {
		object a = args[i];
		IDataParameter param;
		if (cmd.Parameters.Count <= i)
		    cmd.Parameters.Add(param = cmd.CreateParameter());
		else
		    param = (IDataParameter)cmd.Parameters[i];
		if (a is DateTime)
		{
		    param.DbType = DbType.DateTime;
		    param.Value = a;
		}
		else
		{
		    param.DbType = DbType.String; // I sure hope so...
		    param.Value = a.ToString();
		}
	    }
	    
	    if (need_add)
		cmd.Prepare();
	}

	// WvSqlRows know their schema.  But, what if you get no rows back,
	// and you REALLY need that schema information?  THEN you call this.
	public IEnumerable<WvColInfo>
	    statement_schema(string sql, params object[] args)
	{
	    IDbCommand cmd = prepare(sql, args.Length);
	    if (args.Count() > 0)
		bind(cmd, args);

	    // Kill that data reader in case it tries to stick around
	    using (IDataReader e = cmd.ExecuteReader())
	    {
		// We have to use ToArray() here because as we return, the
		// parent IDataReader will get destroyed, thus potentially
		// destroying the SchemaTable too.
		return WvColInfo.FromDataTable(e.GetSchemaTable()).ToArray();
	    }
	}
	
	public IEnumerable<WvSqlRow> select(string sql,
					       params object[] args)
	{
	    return select(prepare(sql, args.Length), args);
	}
	
	public IEnumerable<WvSqlRow> select(IDbCommand cmd,
						params object[] args)
	{
            if (args.Count() > 0)
                bind(cmd, args);
	    return cmd.ExecuteToWvAutoReader();
	}
	
	public WvSqlRow select_onerow(string sql, params object[] args)
	{
	    // only iterates a single row, if it exists
	    foreach (WvSqlRow r in select(sql, args))
		return r; // only return the first one
	    return null;
	}
	
	public WvAutoCast select_one(string sql, params object[] args)
	{
	    var a = select_onerow(sql, args);
	    if (a != null && a.Length > 0)
		return a[0];
	    else
		return WvAutoCast._null;
	}
	
	public int execute(string sql, params object[] args)
	{
	    return execute(prepare(sql, args.Length), args);
	}
	
	public int execute(IDbCommand cmd, params object[] args)
	{
            if (args.Count() > 0)
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
    
    public class WvDbi_ODBC : WvDbi
    {
	public static void register()
	{
	    WvMoniker<WvDbi>.register("ado",
		 (string m, object o) => new WvDbi_ODBC(m));
	    WvMoniker<WvDbi>.register("odbc",
		 (string m, object o) => new WvDbi_ODBC(m));
	}
	
	public WvDbi_ODBC(string moniker)
	{
	    string real;
	    if (moniker.StartsWith("dsn=") || moniker.StartsWith("driver="))
		real = moniker;
	    else
	    {
		// try to parse it as an URL
		WvUrl url = new WvUrl(moniker);
		if (url.path.StartsWith("/"))
		    url.path = url.path.Substring(1);
		real = wv.fmt("driver={0};server={1};database={2};"
			      + "User ID={3};uid={3};Password={4};pwd={4}",
			      url.method, url.host, url.path,
			      url.user, url.password);
	    }
	    
	    log.print("ODBC create: '{0}'\n", real);
	    opendb(new OdbcConnection(real));
	}
	
    }
    
    public class WvDbi_MSSQL : WvDbi
    {
	public static void register()
	{
	    WvMoniker<WvDbi>.register("mssql",
		 (string m, object o) => new WvDbi_MSSQL(m));
	}
	
        public WvDbi_MSSQL(SqlConnection conn)
        {
	    opendb(conn);
        }
	
	public WvDbi_MSSQL(string moniker)
	{
	    string real;
	    if (!moniker.StartsWith("//"))
		real = moniker;
	    else
	    {
		// try to parse it as an URL
		WvUrl url = new WvUrl(moniker);
		if (url.path.StartsWith("/"))
		    url.path = url.path.Substring(1);
		real = wv.fmt("server={0};database={1};"
			      + "User ID={2};Password={3};",
			      url.host, url.path, url.user, url.password);
	    }
	    
	    log.print("MSSQL create: '{0}'\n", real);
	    opendb(new SqlConnection(real));
	}
	
	protected override IDbCommand prepare(string sql, int nargs)
	{
	    IDbCommand cmd = db.CreateCommand();
	    cmd.CommandText = sql;
	    return cmd;
	}
	
	// FIXME: this only works the first time for a given
	// IDBCommand object!  Don't try to recycle them.
	protected override void bind(IDbCommand cmd, params object[] args)
	{
	    object[] list = new object[args.Length];
	    for (int i = 0; i < args.Length; i++)
	    {
		// FIXME!!!  This doesn't escape SQL strings!!
		if (args[i] == null)
		    list[i] = "null";
		else if (args[i] is int)
		    list[i] = (int)args[i];
		else
		    list[i] = wv.fmt("'{0}'", args[i].ToString());
	    }
	    cmd.CommandText = wv.fmt(cmd.CommandText, list);
	    log.print("fake_bind: '{0}'\n", cmd.CommandText);
	}
    }
}

