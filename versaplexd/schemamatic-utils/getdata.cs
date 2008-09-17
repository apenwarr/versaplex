using System;
using System.Text;
using System.Collections.Generic;
using System.IO;
using Wv;
using Wv.Extensions;
using Wv.NDesk.Options;
using NDesk.DBus;

public static class GetData
{
    static WvLog log = new WvLog("GetData");
    static WvLog err = new WvLog("GetData", WvLog.L.Error);

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

    // Parses a 5-digit positive number from pri.  Returns -1 on error.
    static int ParsePriority(string pri)
    {
        int charcount = 0;
        foreach (char c in pri)
        {
            charcount++;
            if (!Char.IsDigit(c))
            {
                charcount = -1;
                break;
            }
        }
        if (charcount != 5 || pri.Length != 5)
        {
            err.print("Priority code '{0}' must be a 5-digit number.\n", pri);
            return -1;
        }

        return Int32.Parse(pri);
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

        newcmd.pri = ParsePriority(parts[0]);
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

    static void DoGetData(string bus_moniker, string exportdir, bool dry_run)
    {
        VxDbusSchema dbus = new VxDbusSchema(bus_moniker);

        string cmdfile = Path.Combine(exportdir, "data-export.txt");
        if (!File.Exists(cmdfile))
        {
            err.print("Missing command file: {0}\n", cmdfile);
            return;
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
                return;
            }
        }

        if (commands == null)
            return;

        string datadir = Path.Combine(exportdir, "DATA");
        log.print("Cleaning destination directory '{0}'.\n", datadir);
        Directory.CreateDirectory(datadir);
        foreach (string path in Directory.GetFiles(datadir, "*.sql"))
        {
            if (dry_run)
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
            if (dry_run)
                log.print("Would have written '{0}'\n", outname);
            else
                File.WriteAllBytes(outname, data.ToString().ToUTF8());
        }
    }

    static void ShowHelp()
    {
        Console.Error.WriteLine(
            @"Usage: getdata [-b dbus-moniker] [--dry-run] <outputdir>
  Updates an SQL schema from Versaplex into the outputdir.

  -b: specifies the dbus moniker to connect to.  If not provided, uses
      DBUS_SESSION_BUS_ADDRESS.
  --dry-run: lists the files that would be changed but doesn't modify them.
");
    }

    public static void Main(string[] args)
    {
        string bus = null;
        string exportdir = null;
        bool dry_run = false;

        var extra = new OptionSet()
            .Add("b=|bus=", delegate(string v) { bus = v; } )
            .Add("dry-run", delegate(string v) { dry_run = (v != null); } )
            .Parse(args);

        if (extra.Count != 1)
        {
            ShowHelp();
            return;
        }

        exportdir = extra[0];

        if (bus == null)
            bus = Address.Session;

        if (bus == null)
        {
            log.print
                ("DBUS_SESSION_BUS_ADDRESS not set and no -b option given.\n");
            ShowHelp();
        }

        log.print("Exporting to '{0}'\n", exportdir);
        log.print("Connecting to '{0}'\n", bus);

        DoGetData(bus, exportdir, dry_run);
    }
}
