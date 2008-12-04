using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using Wv;
using Wv.Extensions;
using Wv.NDesk.Options;

public static class SchemamaticCli
{
    static WvLog log = new WvLog("sm");
    static WvLog err = log.split(WvLog.L.Error);
    
    static int ShowHelp()
    {
        Console.Error.WriteLine(
@"Usage: sm [--dry-run] [--force] [--verbose] <command> <moniker> <dir>
  Schemamatic: copy database schemas between a database server and the
  current directory.

  Valid commands: push, pull, dpush, dpull, pascalgen

  pascalgen usage:
    sm [--verbose] [--global-syms=<syms>] pascalgen <classname> <dir>

  --dry-run/-n: lists the files that would be changed but doesn't modify them.
  --force/-f: performs potentially destructive database update operations.
  --verbose/-v: Increase the verbosity of the log output.  Can be specified 
        multiple times.
  --global-syms/-g: Move <syms> from parameters to members.  Symbols can be
        separated by any one of comma, semicolon, or colon, or this option 
        can be specified multiple times. (pascalgen only)
");
	return 99;
    }

    static int do_pull(ISchemaBackend remote, string dir, VxCopyOpts opts)
    {
        log.print("Pulling schema.\n");
        VxDiskSchema disk = new VxDiskSchema(dir);

        VxSchemaErrors errs 
	    = VxSchema.CopySchema(remote, disk, opts | VxCopyOpts.Verbose);

	int code = 0;
        foreach (var p in errs)
	{
            foreach (var err in p.Value)
            {
                Console.WriteLine("{0} applying {1}: {2} ({3})", 
                    err.level, err.key, err.msg, err.errnum);
                code = 1;
            }
	}
       
	return code;
    }

    static int do_push(ISchemaBackend remote, string dir, VxCopyOpts opts)
    {
        log.print("Pushing schema.\n");
        VxDiskSchema disk = new VxDiskSchema(dir);

        VxSchemaErrors errs = VxSchema.CopySchema(disk, remote, opts);

	int code = 0;
        foreach (var p in errs)
	{
            foreach (var err in p.Value)
            {
                Console.WriteLine("{0} applying {1}: {2} ({3})", 
                    err.level, err.key, err.msg, err.errnum);
                code = 1;
            }
	}
	
	return code;
    }

    static char[] whitespace = {' ', '\t'};

    // Remove any leading or trailing whitespace, and any comments.
    static string FixLine(string line)
    {
        string retval = line.Trim();
        int comment_pos = retval.IndexOf('#');
        if (comment_pos >= 0)
            retval = retval.Remove(comment_pos);

        return retval;
    }

    public class Command
    {
        public int pri;
        public string cmd;
        public string table;
        public string where;
    }

    // Parses a command line of the form "sequence_num command table ..."
    // A sequence number is a 5-digit integer, zero-padded if needed.
    // The current commands are:
    // zap - "00001 zap table_name" or "00002 zap *"
    //  Adds table_name to the list of tables to clear in 00001-zap.sql
    //  If the table name is *, zaps all tables.
    // export - "00003 export foo" or "00004 export foo where condition"
    static Command ParseCommand(string line)
    {
        string[] parts = line.Split(whitespace, 5, 
            StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length < 2)
        {
            err.print("Invalid command. ({0})\n", line);
            return null;
        }

        Command newcmd = new Command();

        newcmd.pri = wv.atoi(parts[0]);
        if (newcmd.pri < 0)
            return null;

        newcmd.cmd = parts[1];
        if (newcmd.cmd == "export")
        {
            // You can say "12345 export foo" or "12345 export foo where bar"
            if (parts.Length == 3)
                newcmd.table = parts[2];
            else if (parts.Length == 5)
            {
                string w = parts[3];
                if (w != "where")
                {
                    err.print("Invalid 'export' syntax. " + 
                        "('{0}' should be 'where' in '{1}')\n", w, line);
                    return null;
                }
                newcmd.table = parts[2];
                newcmd.where = parts[4];
            }
            else
            {
                err.print("Invalid 'export' syntax. ({0})", line);
                return null;
            }
        }
        else if (newcmd.cmd == "zap")
        {
            if (parts.Length < 3)
            {
                err.print("Syntax: 'zap <tables...>'. ({0})\n", line);
                return null;
            }
            string[] tables = line.Split(whitespace, 3, 
                StringSplitOptions.RemoveEmptyEntries);
            newcmd.table = tables[2];
        }
        else
        {
            err.print("Command '{0}' unknown. ({1})\n", newcmd.cmd, line);
            return null;
        }

        return newcmd;
    }

    static IEnumerable<Command> ParseCommands(string[] command_strs)
    {
        List<Command> commands = new List<Command>();
        int last_pri = -1;
        foreach (string _line in command_strs)
        {
            string line = FixLine(_line);
            if (line.Length == 0)
                continue;

            Command cmd = ParseCommand(line);
            if (cmd == null)
                return null;

            if (last_pri >= cmd.pri)
            {
                err.print("Priority code '{0}' <= previous '{1}'. ({2})\n", 
                    cmd.pri, last_pri, line);
                return null;
            }
            last_pri = cmd.pri;

            commands.Add(cmd);
        }
        return commands;
    }

    static int do_dpull(ISchemaBackend remote, string dir,
			 VxCopyOpts opts)
    {
        log.print("Pulling data.\n");

        string cmdfile = Path.Combine(dir, "data-export.txt");
        if (!File.Exists(cmdfile))
        {
            err.print("Missing command file: {0}\n", cmdfile);
            return 5;
        }

        log.print("Retrieving schema checksums from database.\n");
        VxSchemaChecksums dbsums = remote.GetChecksums();

        log.print("Reading export commands.\n");
        string[] cmd_strings = File.ReadAllLines(cmdfile);
        
        Dictionary<string,string> replaces = new Dictionary<string,string>();
        List<string> skipfields = new List<string>();
        string tablefield, replacewith;
        int i = 0;
        foreach (string s in cmd_strings)
        {
            if (s.StartsWith("replace "))
            {
                //take replace off
                replacewith = s.Substring(8);
                
                //take table.fieldname
                tablefield = replacewith.Substring(0,
                                    s.IndexOf(" with ")-8).Trim().ToLower();
                
                //take the value
                replacewith = replacewith.Substring(
                                    replacewith.IndexOf(" with ")+6).Trim();
                if (replacewith.ToLower() == "null")
                    replaces.Add(tablefield,null);
                else
                    replaces.Add(tablefield,
                               replacewith.Substring(1,replacewith.Length-2));
                
                cmd_strings[i] = "";
            }
            else if (s.StartsWith("skipfield "))
            {
                skipfields.Add(s.Substring(10).Trim().ToLower());
                cmd_strings[i] = "";
            }
                
            i++;
        }

        log.print("Parsing commands.\n");
        IEnumerable<Command> commands = ParseCommands(cmd_strings);

        foreach (Command cmd in commands)
        {
            if (cmd.cmd != "zap" && !dbsums.ContainsKey("Table/" + cmd.table))
            {
                err.print("Table doesn't exist: {0}\n", cmd.table);
                return 4;
            }
        }

        if (commands == null)
            return 3;

        string datadir = Path.Combine(dir, "DATA");
        log.print("Cleaning destination directory '{0}'.\n", datadir);
        Directory.CreateDirectory(datadir);
        foreach (string path in Directory.GetFiles(datadir, "*.sql"))
        {
            if ((opts & VxCopyOpts.DryRun) != 0)
                log.print("Would have deleted '{0}'\n", path);
            else
                File.Delete(path);
        }

        log.print("Processing commands.\n");
        foreach (Command cmd in commands)
        {
            StringBuilder data = new StringBuilder();
            if (cmd.cmd == "export")
            {
                data.Append(wv.fmt("TABLE {0}\n", cmd.table));
                data.Append(remote.GetSchemaData(cmd.table, cmd.pri, cmd.where,
                                                 replaces, skipfields));
            }
            else if (cmd.cmd == "zap")
            {
                foreach (string table in cmd.table.Split(whitespace, 
                    StringSplitOptions.RemoveEmptyEntries))
                {
                    if (table != "*")
                        data.Append(wv.fmt("DELETE FROM [{0}]\n\n", table));
                    else
                    {
                        List<string> todelete = new List<string>();
                        foreach (var p in dbsums)
                        {
                            string type, name;
                            VxSchema.ParseKey(p.Value.key, out type, out name);
                            if (type == "Table")
                                todelete.Add(name);
                        }
                        todelete.Sort(StringComparer.Ordinal);
                        foreach (string name in todelete)
                            data.Append(wv.fmt("DELETE FROM [{0}]\n", name));
                    }
                }
                cmd.table = "ZAP";
            }

            string outname = Path.Combine(datadir, 
                wv.fmt("{0:d5}-{1}.sql", cmd.pri, cmd.table));
            if ((opts & VxCopyOpts.DryRun) != 0)
                log.print("Would have written '{0}'\n", outname);
            else
                File.WriteAllBytes(outname, data.ToString().ToUTF8());
        }
	
	return 0;
    }

    // Extracts the table name and priority out of a path.  E.g. for 
    // "/foo/12345-bar.sql", returns "12345" and "bar" as the priority and 
    // table name.  Returns -1/null if the parse fails.
    static void ParsePath(string pathname, out int seqnum, 
        out string tablename)
    {
        FileInfo info = new FileInfo(pathname);
        seqnum = -1;
        tablename = null;

        int dashidx = info.Name.IndexOf('-');
        if (dashidx < 0)
            return;

        string pristr = info.Name.Remove(dashidx);
        string rest = info.Name.Substring(dashidx + 1);
        int pri = wv.atoi(pristr);

        if (pri < 0)
            return;

        if (info.Extension.ToLower() == ".sql")
            rest = rest.Remove(rest.ToLower().LastIndexOf(".sql"));
        else
            return;

        seqnum = pri;
        tablename = rest;

        return;
    }

    static int ApplyFiles(string[] files, ISchemaBackend remote,
			  VxCopyOpts opts)
    {
        int seqnum;
        string tablename;

        Array.Sort(files);
        foreach (string file in files)
        {
            string data = File.ReadAllText(file, Encoding.UTF8);
            ParsePath(file, out seqnum, out tablename);

            if (tablename == "ZAP")
                tablename = "";

            log.print("Applying data from {0}\n", file);
            if ((opts & VxCopyOpts.DryRun) != 0)
                continue;

            if (seqnum > 0)
            {
                remote.PutSchemaData(tablename, data, seqnum);
            }
            else
            {
                // File we didn't generate, try to apply it anyway.
                remote.PutSchemaData("", data, 0);
            }
        }
	
	return 0;
    }

    static int do_dpush(ISchemaBackend remote, string dir, VxCopyOpts opts)
    {
        log.print("Pushing data.\n");
	
        if (!Directory.Exists(dir))
            return 5; // nothing to do

        string datadir = Path.Combine(dir, "DATA");
        if (Directory.Exists(datadir))
	    dir = datadir;
	
	var files = 
	    from f in Directory.GetFiles(dir)
	    where !f.EndsWith("~")
	    select f;
	return ApplyFiles(files.ToArray(), remote, opts);
    }

    // The Pascal type information for a single stored procedure argument.
    struct PascalArg
    {
        public string spname;
        public string varname;
        public List<string> pascaltypes;
        public string call;
        public string defval;

        // Most of the time we just care about the first pascal type.  
        // Any other types are helper types to make convenience functions.
        public string pascaltype
        {
            get { return pascaltypes[0]; }
        }

        public PascalArg(string _spname, SPArg arg)
        {
            spname = _spname;
            varname = arg.name;
            call = "m_" + varname;
            defval = arg.defval;
            pascaltypes = new List<string>();

            string type = arg.type.ToLower();
            string nulldef;
            if (type.Contains("char") || type.Contains("text") ||
                type.Contains("binary") || type.Contains("sysname"))
            {
                pascaltypes.Add("string");
                nulldef = "'__!NIL!__'";
            }
            else if (type.Contains("int"))
            {
                pascaltypes.Add("integer");
                nulldef = "low(integer)+42";
            }
            else if (type.Contains("bit") || type.Contains("bool"))
            {
                pascaltypes.Add("integer");
                pascaltypes.Add("boolean");
                nulldef = "low(integer)+42";
            }
            else if (type.Contains("float") || type.Contains("decimal") || 
                type.Contains("real") || type.Contains("numeric"))
            {
                pascaltypes.Add("double");
                nulldef = "1e-42";
            }
            else if (type.Contains("date") || type.Contains("time"))
            {
                pascaltypes.Add("TDateTime");
                call = wv.fmt("TPwDateWrap.Create(m_{0})", arg.name);
                nulldef = "low(integer)+42";
                // Non-null default dates not supported.
                if (defval.ne())
                    defval = "";
            }
            else if (type.Contains("money"))
            {
                pascaltypes.Add("currency");
                nulldef = "-90000000000.0042";
            }
            else if (type.Contains("image"))
            {
                pascaltypes.Add("TBlobField");
                call = wv.fmt("TBlobField(m_{0})", varname);
                nulldef = "nil";
            }
            else
            {
                throw new ArgumentException(wv.fmt(
                    "Unknown parameter type '{0}' (parameter @{1})", 
                    arg.type, arg.name));
            }

            if (defval.e() || defval.ToLower() == "null")
                defval = nulldef;
        }

        public string GetMemberDecl()
        {
            // The first type is the main one, and gets the declaration.
            return wv.fmt("m_{0}: {1};", varname, pascaltypes[0]);
        }

        public string GetMethodDecl()
        {
            var l = new List<string>();
            foreach (string type in pascaltypes)
            {
                l.Add(wv.fmt("function set{0}(v: {1}): _T{2}; {3}inline;",
                    varname, type, spname, 
                    pascaltypes.Count > 1 ? "overload; " : ""));
            }
            return l.join("\n    ");
        }

        public string GetDecl()
        {
            string extra = defval.ne() ? " = " + defval : "";

            return "p_" + varname + ": " + pascaltypes[0] + extra;
        }

        public string GetDefine()
        {
            return "p_" + varname + ": " + pascaltypes[0];
        }

        public string GetCtor()
        {
            return wv.fmt(".set{0}(p_{0})", varname);
        }

        public string GetSetters()
        {
            var sb = new StringBuilder();
            bool did_first = false;
            foreach (string type in pascaltypes)
            {
                // Types past the first get sent through ord()
                sb.Append(wv.fmt("function _T{0}.set{1}(v: {2}): _T{0};\n" + 
                    "begin\n" +
                    "  m_{1} := {3};\n" +
                    "  result := self;\n" + 
                    "end;\n\n",
                    spname, varname, type, did_first ? "ord(v)" : "v"));
                did_first = true;
            }
            return sb.ToString();
        }
    }

    static void do_pascalgen(string classname, string dir, 
        Dictionary<string,string> global_syms, string outfile)
    {
        if (!classname.StartsWith("T"))
        {
            Console.Error.Write("Classname must start with T.\n");
            return;
        }
        // Replace leading 'T' with a 'u'
        string unitname = "u" + classname.Remove(0, 1);

        VxDiskSchema disk = new VxDiskSchema(dir);
        VxSchema schema = disk.Get(null);

        var types = new List<string>();
        var iface = new List<string>();
        var impl = new List<string>();
        var setters = new List<string>();

        var keys = schema.Keys.ToList();
        keys.Sort(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            var elem = schema[key];
            if (elem.type != "Procedure")
                continue;

            var sp = new StoredProcedure(elem.text);

            var pargs = new List<PascalArg>();
            foreach (SPArg arg in sp.args)
                pargs.Add(new PascalArg(elem.name, arg));

            var decls = new List<string>();
            var impls = new List<string>();
            var ctors = new List<string>();
            foreach (PascalArg parg in pargs)
            {
                if (!global_syms.ContainsKey(parg.varname.ToLower()))
                {
                    decls.Add(parg.GetDecl());
                    impls.Add(parg.GetDefine());
                    ctors.Add(parg.GetCtor());
                }
                else
                {
                    string old = global_syms[parg.varname.ToLower()];
                    if (old.ne() && old.ToLower() != parg.pascaltype.ToLower())
                    {
                        log.print("Definition for global '{0}' differs!  " + 
                            "old '{1}', new '{2}'\n", 
                            parg.varname, old, parg.pascaltype);
                    }
                    else
                    {
                        // The global declaration supplants the local
                        // declaration and implementation, but not the ctor.
                        global_syms[parg.varname.ToLower()] = parg.pascaltype;
                        ctors.Add(parg.GetCtor());
                    }
                }
            }

            // Factory function that produces builder objects
            iface.Add(wv.fmt("function {0}\n"
                + "       ({1}): _T{0};\n",
                elem.name, decls.join(";\n        ")));

            // Actual implementation of the factory function
            impl.Add(wv.fmt(
                "function {0}.{1}\n"
                + "       ({2}): _T{1};\n"
                + "begin\n"
                + "    result := _T{1}.Create(db)\n"
                + "        {3};\n"
                + "end;\n\n",
                classname, elem.name, 
                impls.join(";\n        "),
                ctors.join("\n        ")
                ));

            var memberdecls = new List<string>();
            var methoddecls = new List<string>();
            foreach (PascalArg parg in pargs)
            {
                memberdecls.Add(parg.GetMemberDecl());
                methoddecls.Add(parg.GetMethodDecl());
            }

            // Declaration for per-procedure builder class
            types.Add("_T" + elem.name + " = class(TPwDataCmd)\n"
                + "  private\n"
                + "    " + memberdecls.join("\n    ") + "\n"
                + "  public\n"
                + "    function MakeRawSql: string; override;\n"
                + "    " + methoddecls.join("\n    ") + "\n"
                + "  end;\n"
                );

            // Member functions of the builder classes

            var argstrs = new List<string>();
            var argcalls = new List<string>();
            foreach (PascalArg parg in pargs)
            {
                argstrs.Add(wv.fmt("'{0}'", parg.varname));
                argcalls.Add(parg.call);
            }

            setters.Add(wv.fmt(
                "function _T{0}.MakeRawSql: string;\n"
                + "begin\n"
                + "    result := TPwDatabase.ExecStr('{0}',\n"
                + "       [{1}],\n"
                + "       [{2}]);\n"
                + "end;\n\n",
                elem.name, 
                argstrs.join( ",\n        "), 
                argcalls.join(",\n        ")));

            foreach (PascalArg parg in pargs)
                setters.Add(parg.GetSetters());
        }

        var sb = new StringBuilder();
        sb.Append("(*\n"
            + " * THIS FILE IS AUTOMATICALLY GENERATED BY sm.exe\n"
            + " * DO NOT EDIT!\n"
            + " *)\n"
            + "unit " + unitname + ";\n\n");

        var global_syms_keys = global_syms.Keys.ToList();
        global_syms_keys.Sort();
        var globalfields = new List<string>();
        var globalprops = new List<string>();
        foreach (var sym in global_syms_keys)
        {
            string type = global_syms[sym];
            if (type.e())
            {
                log.print(WvLog.L.Error, 
                    "Global symbol '{0}' is never used in any procedure!\n", 
                    sym);
                return;
            }
            globalfields.Add(wv.fmt("p_{0}: {1};", sym, type));
            globalprops.Add(wv.fmt("property {0}: {1}  read p_{0} write p_{0};",
                sym, type));
        }

        sb.Append("interface\n\n"
            + "uses uPwTemp, uPwData, Db;\n"
            + "\n"
            + "{$M+}\n"
            + "type\n"
            + "  " + types.join("\n  ")
            + "  \n"
            + "  " + classname + " = class(TObject)\n"
            + "  private\n"
            + "    fDb: TPwDatabase;\n"
            + "    " + globalfields.join("\n    ") + "\n"
            + "  published\n"
            + "    property db: TPwDatabase  read fDb write fDb;\n"
            + "    " + globalprops.join("\n    ") + "\n"
            + "  public\n"
            + "    constructor Create; overload;\n"
            + "    constructor Create(db: TPwDatabase); overload;\n"
            + "    " + iface.join("    ")
            + "  end;\n\n");

        sb.Append("implementation\n"
            + "\n"
            + "constructor " + classname + ".Create;\n"
            + "begin\n"
            + "    self.db := nil;\n"
            + "end;\n"
            + "\n"
            + "constructor " + classname + ".Create(db: TPwDatabase);\n"
            + "begin\n"
            + "    self.db := db;\n"
            + "end;\n"
            + "\n"
            + impl.join("")
            );

        sb.Append(setters.join(""));

        sb.Append("\n\nend.\n");

	if (outfile.e())
	    Console.Write(sb.ToString());
	else
	{
	    using (var f = new FileStream(outfile,
			  FileMode.Create, FileAccess.Write))
	    {
		f.write(sb.ToUTF8());
	    }
	}
    }

    private static ISchemaBackend GetBackend(string moniker)
    {
        log.print("Connecting to '{0}'\n", moniker);
        return VxSchema.create(moniker);
    }

    public static int Main(string[] args)
    {
        try {
            return _Main(args);
        }
        catch (Exception e) {
            wv.printerr("schemamatic: {0}\n", e.Message);
            return 99;
        }
    }
    
    public static int pascalgen_Main(List<string> extra,
				     Dictionary<string,string> global_syms,
				     string outfile)
    {
	if (extra.Count != 3)
	{
	    ShowHelp();
	    return 98;
	}
	
	string classname = extra[1];
	string dir       = extra[2];
	
	do_pascalgen(classname, dir, global_syms, outfile);
	return 0;
    }
    
    public static int _Main(string[] args)
    {
	// command line options
	VxCopyOpts opts = VxCopyOpts.Verbose;
    
	int verbose = (int)WvLog.L.Info;
	string outfile = null;
        var global_syms = new Dictionary<string,string>();
        var extra = new OptionSet()
            .Add("n|dry-run", delegate(string v) { opts |= VxCopyOpts.DryRun; } )
            .Add("f|force", delegate(string v) { opts |= VxCopyOpts.Destructive; } )
            .Add("v|verbose", delegate(string v) { verbose++; } )
            .Add("g|global-sym=", delegate(string v) 
                { 
                    var splitopts = StringSplitOptions.RemoveEmptyEntries;
                    char[] splitchars = {',', ';', ':'};
                    if (v.ne()) 
                        foreach (var sym in v.Split(splitchars, splitopts))
                            global_syms.Add(sym.ToLower(), null); 
                } )
	    .Add("o|output-file=", delegate(string v) { outfile = v; })
            .Parse(args);

	WvLog.maxlevel = (WvLog.L)verbose;
	
	if (extra.Count < 1)
	{
            ShowHelp();
            return 97;
	}
	
	string cmd = extra[0];
	if (cmd == "pascalgen")
	    return pascalgen_Main(extra, global_syms, outfile);
	
	WvIni bookmarks = new WvIni(
		    wv.PathCombine(wv.getenv("HOME"), ".wvdbi.ini"));
	
	string moniker = extra.Count > 1 
	    ? extra[1] : bookmarks.get("Defaults", "url", null);
        string dir     = extra.Count > 2 ? extra[2] : ".";
	
	if (moniker.e())
	{
	    ShowHelp();
	    return 96;
	}
	
	// look up a bookmark if it exists, else use the provided name as a
	// moniker
	moniker = bookmarks.get("Bookmarks", moniker, moniker);

	// provide a default username/password if they weren't provided
	// FIXME: are there URLs that should have a blank username/password?
	WvUrl url = new WvUrl(moniker);
	if (url.user.e())
	    url.user = bookmarks.get("Defaults", "user");
	if (url.password.e())
	    url.password = bookmarks.get("Defaults", "password");
	    
	using (var backend = GetBackend(url.ToString()))
	{
	    bookmarks.set("Defaults", "url", moniker);
	    bookmarks.maybeset("Defaults", "user", url.user);
	    bookmarks.maybeset("Defaults", "password", url.password);
	    
	    string p = url.path.StartsWith("/") 
		? url.path.Substring(1) : url.path;
	    bookmarks.set("Bookmarks", p, moniker);
	    
	    try {
		bookmarks.save();
	    } catch (IOException) {
		// not a big deal if we can't save our bookmarks.
	    }
	    
	    if (cmd == "remote")
		return 0; // just saved the bookmark, so we're done
	    else if (cmd == "pull")
		do_pull(backend, dir, opts);
	    else if (cmd == "push")
		do_push(backend, dir, opts);
	    else if (cmd == "dpull")
		do_dpull(backend, dir, opts);
	    else if (cmd == "dpush")
		do_dpush(backend, dir, opts);
	    else
	    {
		Console.Error.WriteLine("\nUnknown command '{0}'\n", cmd);
		ShowHelp();
	    }
	}
	
	return 0;
    }
}
