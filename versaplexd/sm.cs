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
    
    // command line options
    static string bus = null;
    static VxCopyOpts opts = VxCopyOpts.Verbose;
    
    static int ShowHelp()
    {
        Console.Error.WriteLine(
@"Usage: sm [-b dbus-moniker] [--dry-run] <push|pull|dpush|dpull> <dir>
  Schemamatic: copy database schemas between a database server and the
  current directory.

  -b: specifies the dbus moniker to connect to.  If not provided, uses
      DBUS_SESSION_BUS_ADDRESS.
  --dry-run: lists the files that would be changed but doesn't modify them.
");
	return 99;
    }

    static int DoExport(string bus_moniker, string dir, VxCopyOpts dry_run)
    {
        log.print("Pulling from '{0}'\n", bus_moniker);
	
        VxDbusSchema dbus = new VxDbusSchema(bus_moniker);
        VxDiskSchema disk = new VxDiskSchema(dir);

        VxSchemaErrors errs 
	    = VxSchema.CopySchema(dbus, disk, opts | VxCopyOpts.Verbose);

	int code = 0;
        foreach (var p in errs)
        {
            VxSchemaError err = p.Value;
            Console.WriteLine("Error applying {0}: {1} ({2})", 
                err.key, err.msg, err.errnum);
	    code = 1;
        }
	
	return code;
    }

    static int DoApply(string bus_moniker, string dir, VxCopyOpts opts)
    {
        log.print("Pushing to '{0}'\n", bus_moniker);
	
        VxDbusSchema dbus = new VxDbusSchema(bus_moniker);
        VxDiskSchema disk = new VxDiskSchema(dir);

        VxSchemaErrors errs = VxSchema.CopySchema(disk, dbus, opts);

	int code = 0;
        foreach (var p in errs)
        {
            VxSchemaError err = p.Value;
            Console.WriteLine("Error applying {0}: {1} ({2})", 
                err.key, err.msg, err.errnum);
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

    static int DoGetData(string bus_moniker, string dir,
			 VxCopyOpts opts)
    {
        log.print("Pulling data from '{0}'\n", bus_moniker);
        VxDbusSchema dbus = new VxDbusSchema(bus_moniker);

        string cmdfile = Path.Combine(dir, "data-export.txt");
        if (!File.Exists(cmdfile))
        {
            err.print("Missing command file: {0}\n", cmdfile);
            return 5;
        }

        log.print("Retrieving schema checksums from database.\n");
        VxSchemaChecksums dbsums = dbus.GetChecksums();

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
                data.Append(dbus.GetSchemaData(cmd.table, cmd.pri, cmd.where));
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
                            VxSchema.ParseKey(p.Value.name, out type, out name);
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

    static int ApplyFiles(string[] files, VxDbusSchema dbus, VxCopyOpts opts)
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
                dbus.PutSchemaData(tablename, data, seqnum);
            }
            else
            {
                // File we didn't generate, try to apply it anyway.
                dbus.PutSchemaData("", data, 0);
            }
        }
	
	return 0;
    }

    static int DoApplyData(string bus_moniker, string dir, VxCopyOpts opts)
    {
        if (!Directory.Exists(dir))
            return 5; // nothing to do

        string datadir = Path.Combine(dir, "DATA");
        if (Directory.Exists(datadir))
	    dir = datadir;
	
        log.print("Pushing data to '{0}'\n", bus_moniker);
	
        VxDbusSchema dbus = new VxDbusSchema(bus_moniker);

	var files = 
	    from f in Directory.GetFiles(dir)
	    where !f.EndsWith("~")
	    select f;
	return ApplyFiles(files.ToArray(), dbus, opts);
    }

    public static void Main(string[] args)
    {
	int verbose = (int)WvLog.L.Info;
        var extra = new OptionSet()
            .Add("b=|bus=", delegate(string v) { bus = v; } )
            .Add("dry-run", delegate(string v) { opts |= VxCopyOpts.DryRun; } )
            .Add("f|force", delegate(string v) { opts |= VxCopyOpts.Destructive; } )
            .Add("v|verbose", delegate(string v) { verbose++; } )
            .Parse(args);

	WvLog.maxlevel = (WvLog.L)verbose;
	
        if (extra.Count != 2)
        {
            ShowHelp();
            return;
        }

	string cmd = extra[0];
        string dir = extra[1];

        if (bus.e())
            bus = Address.Session;
        if (bus.e())
        {
            log.print
                ("DBUS_SESSION_BUS_ADDRESS not set and no -b option given.\n");
            ShowHelp();
        }

        log.print("Connecting to '{0}'\n", bus);
	if (cmd == "pull")
	    DoExport(bus, dir, opts);
	else if (cmd == "push")
	    DoApply(bus, dir, opts);
	else if (cmd == "dpull")
	    DoGetData(bus, dir, opts);
	else if (cmd == "dpush")
	    DoApplyData(bus, dir, opts);
	else
	{
	    Console.Error.WriteLine("\nUnknown command '{0}'\n", cmd);
	    ShowHelp();
	}
    }
}
