using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Wv;
using Wv.Extensions;

struct SPArg
{
    public string name;
    public string type;
    public string defval;

    public SPArg(string _name, string _type, string _defval)
    {
        name = _name;
        type = _type;
        defval = _defval;
    }
}

class StoredProcedure
{
    public List<SPArg> args;
    public string name;

    // These are mindnumbingly long to type out.  Use the perl shorthands.
    private static RegexOptions i = RegexOptions.IgnoreCase;
    private static RegexOptions x = RegexOptions.IgnorePatternWhitespace;
    private static RegexOptions m = RegexOptions.Multiline;
    private static RegexOptions s = RegexOptions.Singleline;
    private static RegexOptions compiled = RegexOptions.Compiled;

    static Regex commentline_re = new Regex(@"--.*$", m|compiled);
    static Regex comment_re = new Regex(@"/\* .*? \*/", x|s|compiled);
    static Regex decl_re = new Regex(
        @"CREATE \s+ (PROC|PROCEDURE) \s+ (\S*) (.*?) \s+ AS \s+", 
        x|s|i|compiled);
    static Regex args_re = new Regex(@"
        @(\S+) \s+                                  # name
        ([^,=]+ (?: \([\d,]*\))? \s*)               # type
        ((?: = \s* (?: '[^']*')|(?: [^,]+) )?)      # default
        ,                                           # trailing comma
        ", x|s|i|compiled);
    static Regex output_re = new Regex(@"\s+ OUTPUT \s* $", x|i|compiled);

    public StoredProcedure()
    {
        args = new List<SPArg>();
        name = null;
    }

    public StoredProcedure(string sp)
    {
        name = null;
        Parse(sp);
    }

    public static string StripComments(string sp)
    {
        sp = commentline_re.Replace(sp, "");
        sp = comment_re.Replace(sp, "");

        return sp;
    }

    public static string FixDefaultValue(string def)
    {
        if (def.e())
            return def;

        // Strip leading spaces and equal signs, and any "output" qualifiers
        def = Regex.Replace(def, @"^ \s* = \s*", "", x);
        def = output_re.Replace(def, "");

        return def;
    }

    public void Parse(string sp)
    {
        args = new List<SPArg>();

        var log = new WvLog("SP.Parse", WvLog.L.Debug2);
        log.print("Parsing {0}\n", sp);

        sp = StripComments(sp);

        var decl_match = decl_re.Match(sp);
        if (decl_match.Success)
        {
            // Group 0 is the whole thing, group 1 is PROC or PROCEDURE.
            this.name = decl_match.Groups[2].Value;
            string args_str = decl_match.Groups[3].Value + ",";
            log.print("name={0}, args_str={1}\n", name, args_str);

            // Match the arguments
            foreach (Match m in args_re.Matches(args_str))
            {
                log.print("Checking new match, val={0}\n", m.Value);
                string paramname = m.Groups[1].Value.Trim();
                string paramtype = m.Groups[2].Value.Trim();
                string defval = m.Groups[3].Value.Trim();

                paramtype = output_re.Replace(paramtype, "");
                defval = FixDefaultValue(defval);
                args.Add(new SPArg(paramname, paramtype, defval));
            }
        }
        else
            log.print("Not matched\n");
    }
}
