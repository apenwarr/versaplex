using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using Wv;
using Wv.Extensions;
using Wv.NDesk.Options;
using NDesk.DBus;

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
    sm [--verbose] pascalgen <classname> <dir>

  --dry-run: lists the files that would be changed but doesn't modify them.
  --force/-f: performs potentially destructive database update operations.
  --verbose/-v: Increase the verbosity of the log output.  Can be specified 
        multiple times.
");
	return 99;
    }

    static int DoExport(ISchemaBackend remote, string dir, VxCopyOpts opts)
    {
        log.print("Pulling schema.\n");
        VxDiskSchema disk = new VxDiskSchema(dir);

        VxSchemaErrors errs 
	    = VxSchema.CopySchema(remote, disk, opts | VxCopyOpts.Verbose);

	int code = 0;
        foreach (var p in errs)
            foreach (var err in p.Value)
            {
                Console.WriteLine("{0} applying {1}: {2} ({3})", 
                    err.level, err.key, err.msg, err.errnum);
                code = 1;
            }
	
	return code;
    }

    static int DoApply(ISchemaBackend remote, string dir, VxCopyOpts opts)
    {
        log.print("Pushing schema.\n");
        VxDiskSchema disk = new VxDiskSchema(dir);

        VxSchemaErrors errs = VxSchema.CopySchema(disk, remote, opts);

	int code = 0;
        foreach (var p in errs)
            foreach (var err in p.Value)
            {
                Console.WriteLine("{0} applying {1}: {2} ({3})", 
                    err.level, err.key, err.msg, err.errnum);
                code = 1;
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

    static int DoGetData(ISchemaBackend remote, string dir,
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
                data.Append(wv.fmt("DELETE FROM [{0}];\n", cmd.table));
                data.Append(remote.GetSchemaData(cmd.table, cmd.pri, cmd.where));
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
                        todelete.Sort();
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
            string data = File.ReadAllText(file, System.Text.Encoding.UTF8);
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

    static int DoApplyData(ISchemaBackend remote, string dir, VxCopyOpts opts)
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

        public PascalArg(string _spname, SPArg arg)
        {
            spname = _spname;
            varname = arg.name;
            call = "m_" + varname;
            defval = arg.defval;
            pascaltypes = new List<string>();

            string type = arg.type.ToLower();
            string nulldef;
            if (type.Contains("char") || type.Contains("text"))
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

            if (defval.e())
                defval = nulldef;
        }

        public string GetMemberDecl()
        {
            var l = new List<string>();
            foreach (string type in pascaltypes)
                l.Add(wv.fmt("m_{0}: {1};", varname, type));
            return l.Join("\n    ");
        }

        public string GetMethodDecl()
        {
            var l = new List<string>();
            foreach (string type in pascaltypes)
            {
                l.Add(wv.fmt("function set{0}(v: {1}): _T{2}; {3}inline;",
                    varname, type, spname, 
                    pascaltypes.Count > 1 ? "override " : ""));
            }
            return l.Join("\n    ");
        }

        public string GetDecl()
        {
            string extra = defval.ne() ? " = " + defval : "";

            var l = new List<string>();
            foreach (string type in pascaltypes)
                l.Add("p_" + varname + ": " + type + extra);
            return l.Join("\n    ");
        }

        public string GetDefine()
        {
            var l = new List<string>();
            foreach (string type in pascaltypes)
                l.Add("p_" + varname + ": " + type);
            return l.Join("\n    ");
        }

        public string GetCtor()
        {
            return wv.fmt(".set{0}(p_{0})", varname);
        }

        public string GetSetters()
        {
            var sb = new StringBuilder();
            foreach (string type in pascaltypes)
            {
                sb.Append(wv.fmt("function _T{0}.set{1}(v: {2}): _T{0};\n" + 
                    "begin\n" +
                    "  m_{1} := v;\n" +
                    "  result := self;\n" + 
                    "end;\n\n",
                    spname, varname, type));
            }
            return sb.ToString();
        }
    }

    static void DoPascalGen(string classname, string dir)
    {
        if (!classname.StartsWith("T"))
        {
            System.Console.Error.Write("Classname must start with T.\n");
            return;
        }
        // Replace leading 'T' with a 'u'
        string unitname = "u" + classname.Remove(0, 1);

        VxDiskSchema disk = new VxDiskSchema(dir);
        VxSchema schema = disk.Get(null);

        var types = new List<string>();
        var iface = new List<string>();
        var impl = new List<string>();

        foreach (var elem in schema)
        {
            if (elem.Value.type != "Procedure")
                continue;

            var sp = new StoredProcedure(elem.Value.text);

            var pargs = new List<PascalArg>();
            foreach (SPArg arg in sp.args)
                pargs.Add(new PascalArg(sp.name, arg));

            var decls = new List<string>();
            var impls = new List<string>();
            var ctors = new List<string>();
            foreach (PascalArg parg in pargs)
            {
                decls.Add(parg.GetDecl());
                impls.Add(parg.GetDefine());
                ctors.Add(parg.GetCtor());
            }

            // Factory function that produces builder objects
            iface.Add(wv.fmt("function {0}\n"
                + "       ({1}): _T{0};\n",
                sp.name, decls.Join(";\n        ")));

            // Actual implementation of the factory function
            impl.Add(wv.fmt(
                "function {0}.{1}\n"
                + "       ({2}): _T{1};\n"
                + "begin\n"
                + "    result := _T{1}.Create(db)\n"
                + "        {3};\n"
                + "end;\n\n",
                classname, sp.name, 
                impls.Join(";\n        "),
                ctors.Join("\n        ")
                ));

            var memberdecls = new List<string>();
            var methoddecls = new List<string>();
            foreach (PascalArg parg in pargs)
            {
                memberdecls.Add(parg.GetMemberDecl());
                methoddecls.Add(parg.GetMethodDecl());
            }

            // Declaration for per-procedure builder class
            types.Add("_T" + sp.name + " = class(TPwDataCmd)\n"
                + "  private\n"
                + "    " + memberdecls.Join("\n    ") + "\n"
                + "  public\n"
                + "    function MakeRawSql: string; override;\n"
                + "    " + methoddecls.Join("\n    ") + "\n"
                + "  end;\n"
                );
        }

        var sb = new StringBuilder();
        sb.Append("(*\n"
            + " * THIS FILE IS AUTOMATICALLY GENERATED BY sm.exe\n"
            + " * DO NOT EDIT!\n"
            + " *)\n"
            + "unit " + unitname + ";\n\n");

        // FIXME: Global fields
        // FIXME: Global properties

        sb.Append("interface\n\n"
            + "uses uPwTemp, uPwData, Db;\n"
            + "\n"
            + "{$M+}\n"
            + "type\n"
            + "  " + types.Join("\n  ")
            + "  \n"
            + "  " + classname + " = class(TObject)\n"
            + "  private\n"
            + "    fDb: TPwDatabase;\n"
            // FIXME: Add global fields
            + "  published\n"
            + "    property db: TPwDatabase  read fDb write fDb;\n"
            // FIXME: Add global properties
            + "  public\n"
            + "    constructor Create; overload;\n"
            + "    constructor Create(db: TPwDatabase); overload;\n"
            + "    " + iface.Join("    ")
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
            + impl.Join("")
            );

        // FIXME: Add setters here

        sb.Append("\n\nend.\n");

        // FIXME: Write file
        System.Console.Write(sb.ToString());
    }

    private static ISchemaBackend GetBackend(string moniker)
    {
        log.print("Connecting to '{0}'\n", moniker);
        return VxSchema.create(moniker);
    }

    public static void Main(string[] args)
    {
	// command line options
	VxCopyOpts opts = VxCopyOpts.Verbose;
    
	int verbose = (int)WvLog.L.Info;
        var extra = new OptionSet()
            .Add("dry-run", delegate(string v) { opts |= VxCopyOpts.DryRun; } )
            .Add("f|force", delegate(string v) { opts |= VxCopyOpts.Destructive; } )
            .Add("v|verbose", delegate(string v) { verbose++; } )
            .Parse(args);

	WvLog.maxlevel = (WvLog.L)verbose;
	
        if (extra.Count != 3)
        {
            ShowHelp();
            return;
        }

	string cmd     = extra[0];
	string moniker = extra[1];
        string dir     = extra[2];

	if (cmd == "pull")
	    DoExport(GetBackend(moniker), dir, opts);
	else if (cmd == "push")
	    DoApply(GetBackend(moniker), dir, opts);
	else if (cmd == "dpull")
	    DoGetData(GetBackend(moniker), dir, opts);
	else if (cmd == "dpush")
	    DoApplyData(GetBackend(moniker), dir, opts);
	else if (cmd == "pascalgen")
        {
            string classname = extra[1];
	    DoPascalGen(classname, dir);
        }
	else
	{
	    Console.Error.WriteLine("\nUnknown command '{0}'\n", cmd);
	    ShowHelp();
	}
    }
}
