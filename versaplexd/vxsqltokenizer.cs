
using System;
using System.Collections.Generic;

public class VxSqlToken
{
    public enum TokenType { None, Unquoted, SingleQuoted, DoubleQuoted,
    				Delimited, LParen, RParen, Comma, Semicolon,
				Keyword, DelimitedComment, Comment,
				Relop, Equals, Not, Bitop, Addop, Multop,
				Numeric, Scientific, Period, ERROR_UNKNOWN };
    public TokenType type;
    public string name;

    public VxSqlToken(TokenType t, string n)
    {
	type = t;
	name = n;
    }

    public bool IsUnquotedAndLowerCaseEq(string eq)
    {
	return type == TokenType.Unquoted && name.ToLower() == eq.ToLower();
    }

    public bool IsKeyWordEq(string key)
    {
	return type == TokenType.Keyword && key.ToUpper() == name.ToUpper();
    }

    public override string ToString()
    {
	if (type == TokenType.SingleQuoted)
	    return String.Format("'{0}'", name);
	if (type == TokenType.DoubleQuoted || type == TokenType.Delimited)
	    return String.Format("[{0}]", name);
	if (type == TokenType.DelimitedComment || type == TokenType.Comment)
	    return String.Format("/* {0} */", name);
	if (type == TokenType.Keyword)
	    return name.ToUpper();
	return name;
    }
}

public class VxSqlTokenizer
{
    private static string[] sqlkeywords = {
				    "ADD", "EXCEPT", "PERCENT",
				    "ALL", "EXEC", "PLAN",
				    "ALTER", "EXECUTE", "PRECISION",
				    "AND", "EXISTS", "PRIMARY",
				    "ANY", "EXIT", "PRINT",
				    "AS", "FETCH", "PROC",
				    "ASC", "FILE", "PROCEDURE",
				    "AUTHORIZATION", "FILLFACTOR", "PUBLIC",
				    "BACKUP", "FOR", "RAISERROR",
				    "BEGIN", "FOREIGN", "READ",
				    "BETWEEN", "FREETEXT", "READTEXT",
				    "BREAK", "FREETEXTTABLE", "RECONFIGURE",
				    "BROWSE", "FROM", "REFERENCES",
				    "BULK", "FULL", "REPLICATION",
				    "BY", "FUNCTION", "RESTORE",
				    "CASCADE", "GOTO", "RESTRICT",
				    "CASE", "GRANT", "RETURN",
				    "CHECK", "GROUP", "REVOKE",
				    "CHECKPOINT", "HAVING", "RIGHT",
				    "CLOSE", "HOLDLOCK", "ROLLBACK",
				    "CLUSTERED", "IDENTITY", "ROWCOUNT",
				    "COALESCE", "IDENTITY_INSERT", "ROWGUIDCOL",
				    "COLLATE", "IDENTITYCOL", "RULE",
				    "COLUMN", "IF", "SAVE",
				    "COMMIT", "IN", "SCHEMA",
				    "COMPUTE", "INDEX", "SELECT",
				    "CONSTRAINT", "INNER", "SESSION_USER",
				    "CONTAINS", "INSERT", "SET",
				    "CONTAINSTABLE", "INTERSECT", "SETUSER",
				    "CONTINUE", "INTO", "SHUTDOWN",
				    "CONVERT", "IS", "SOME",
				    "CREATE", "JOIN", "STATISTICS",
				    "CROSS", "KEY", "SYSTEM_USER",
				    "CURRENT", "KILL", "TABLE",
				    "CURRENT_DATE", "LEFT", "TEXTSIZE",
				    "CURRENT_TIME", "LIKE", "THEN",
				    "CURRENT_TIMESTAMP", "LINENO", "TO",
				    "CURRENT_USER", "LOAD", "TOP",
				    "CURSOR", "NATIONAL", "TRAN",
				    "DATABASE", "NOCHECK", "TRANSACTION",
				    "DBCC", "NONCLUSTERED", "TRIGGER",
				    "DEALLOCATE", "NOT", "TRUNCATE",
				    "DECLARE", "NULL", "TSEQUAL",
				    "DEFAULT", "NULLIF", "UNION",
				    "DELETE", "OF", "UNIQUE",
				    "DENY", "OFF", "UPDATE",
				    "DESC", "OFFSETS", "UPDATETEXT",
				    "DISK", "ON", "USE",
				    "DISTINCT", "OPEN", "USER",
				    "DISTRIBUTED", "OPENDATASOURCE", "VALUES",
				    "DOUBLE", "OPENQUERY", "VARYING",
				    "DROP", "OPENROWSET", "VIEW",
				    "DUMMY", "OPENXML", "WAITFOR",
				    "DUMP", "OPTION", "WHEN",
				    "ELSE", "OR", "WHERE",
				    "END", "ORDER", "WHILE",
				    "ERRLVL", "OUTER", "WITH",
				    "ESCAPE", "OVER", "WRITETEXT",
    };

    private List<VxSqlToken> tokens;

    string cur;
    VxSqlToken.TokenType curstate;

    private void reset_state()
    {
	cur = "";
	curstate = VxSqlToken.TokenType.None;
    }

    private void save_and_reset_state()
    {
	tokens.Add(new VxSqlToken(curstate, cur));
	reset_state();
    }

    private VxSqlToken.TokenType get_singletoken_state(char c)
    {
	switch (c)
	{
	case '(':
	    return VxSqlToken.TokenType.LParen;
	case ')':
	    return VxSqlToken.TokenType.RParen;
	case '&':
	    return VxSqlToken.TokenType.Bitop;
	case '|':
	    return VxSqlToken.TokenType.Bitop;
	case '^':
	    return VxSqlToken.TokenType.Bitop;
	case '~':
	    return VxSqlToken.TokenType.Bitop;
	case '*':
	    return VxSqlToken.TokenType.Multop;
	case '/':
	    return VxSqlToken.TokenType.Multop;
	case '%':
	    return VxSqlToken.TokenType.Multop;
	case '+':
	    return VxSqlToken.TokenType.Addop;
	case '-':
	    return VxSqlToken.TokenType.Addop;
	case '=':
	    return VxSqlToken.TokenType.Equals;
	case '>':
	    return VxSqlToken.TokenType.Relop;
	case '<':
	    return VxSqlToken.TokenType.Relop;
	case '!':
	    return VxSqlToken.TokenType.Not;
	case ',':
	    return VxSqlToken.TokenType.Comma;
	case ';':
	    return VxSqlToken.TokenType.Semicolon;
	case '.':
	    return VxSqlToken.TokenType.Period;
	default:
	    return VxSqlToken.TokenType.None;
	}
    }

    private void decipher_unquoted_state()
    {
	string curupper = cur.ToUpper();
	foreach (string word in sqlkeywords)
	    if (curupper == word)
	    {
		curstate = VxSqlToken.TokenType.Keyword;
		break;
	    }
    }

    public VxSqlTokenizer(string query)
    {
	tokens = new List<VxSqlToken>();
	string q = query.Trim();

	reset_state();

	for (int i = 0; i < q.Length; ++i)
	{
	    char c = q[i];
	    char peek = i < q.Length - 1 ? q[i + 1] : '\0';
	    switch (curstate)
	    {
	    case VxSqlToken.TokenType.None:
		VxSqlToken.TokenType singletoken_state = get_singletoken_state(c);
		if (isalpha(c) || c == '_' || c == '@' || c == '#')
		{
		    curstate = VxSqlToken.TokenType.Unquoted;
		    cur += c;
		    if (!isidentifierchar(peek))
		    {
			decipher_unquoted_state();
			save_and_reset_state();
		    }
		}
		else if (c == '\'')
		    curstate = VxSqlToken.TokenType.SingleQuoted;
		else if (c == '"')
		    curstate = VxSqlToken.TokenType.DoubleQuoted;
		else if (c == '[')
		    curstate = VxSqlToken.TokenType.Delimited;
		else if (c == '-' && peek == '-')
		{
		    ++i;
		    curstate = VxSqlToken.TokenType.Comment;
		}
		else if (c == '/' && peek == '*')
		{
		    ++i;
		    curstate = VxSqlToken.TokenType.DelimitedComment;
		}
		else if (isdigit(c))
		{
		    curstate = VxSqlToken.TokenType.Numeric;
		    cur += c;
		    if (!isnumericchar(peek))
			save_and_reset_state();
		}
		else if (c == '.' && isdigit(peek))
		{
		    curstate = VxSqlToken.TokenType.Numeric;
		    cur += c;
		}
		else if (singletoken_state != VxSqlToken.TokenType.None)
		{
		    cur += c;
		    curstate = singletoken_state;

		    // account for '<=', '>=' and '<>'
		    if (curstate == VxSqlToken.TokenType.Relop &&
			(peek == '=' || (c == '<' && peek == '>')))
		    {
		        ++i;
			cur += peek;
		    }

		    save_and_reset_state();
		}
		else if (!isspace(c))
		{
		    cur += c;
		    curstate = VxSqlToken.TokenType.ERROR_UNKNOWN;
		    save_and_reset_state();
		}
		break;
	    case VxSqlToken.TokenType.Unquoted:
		cur += c;
		if (!isidentifierchar(peek))
		{
		    decipher_unquoted_state();
		    save_and_reset_state();
		}
		break;
	    case VxSqlToken.TokenType.SingleQuoted:
		if (c == '\'')
		{
		    if (peek == '\'')
		    {
			//FIXME:  Single single-quote, or two of them?
			cur += "''";
			++i;
		    }
		    else
			save_and_reset_state();
		}
		else
		    cur += c;
		break;
	    case VxSqlToken.TokenType.DoubleQuoted:
		if (c == '"')
		{
		    if (peek == '"')
		    {
			cur += "\"";
			++i;
		    }
		    else
		    {
			//Double-quotes and square brackets are identical in
			//T-Sql syntax, except that square brackets don't
			//require escaping of double-quotes.  So, for brevity
			//and cleanliness, treat double-quoted identifiers as
			//being encapsulated in square brackets
			curstate = VxSqlToken.TokenType.Delimited;
			save_and_reset_state();
		    }
		}
		else
		    cur += c;
		break;
	    case VxSqlToken.TokenType.Delimited:
		if (c == ']')
		    save_and_reset_state();
		else
		    cur += c;
		break;
	    case VxSqlToken.TokenType.Comment:
		cur += c;
		break;
	    case VxSqlToken.TokenType.DelimitedComment:
		// NOTE:  In T-Sql, a 'GO' command within a comment generates an
		// error, but our tokenizer doesn't need to care about this.
		if (c == '*' && peek == '/')
		{
		    ++i;
		    // For sanity, we'll have only a single 'comment' token
		    curstate = VxSqlToken.TokenType.Comment;
		    save_and_reset_state();
		}
		else
		    cur += c;
		break;
	    case VxSqlToken.TokenType.Numeric:
		if (c == 'E' || c == 'e')
		{
		    curstate = VxSqlToken.TokenType.Scientific;
		    cur += 'e';
		    if (peek == '-' || peek == '+')
		    {
			++i;
			cur += peek;
		    }
		    else if (!isdigit(peek))
		    {
			curstate = VxSqlToken.TokenType.Numeric;
			save_and_reset_state();
		    }
		}
		else
		{
		    cur += c;
		    if (!isnumericchar(peek))
			save_and_reset_state();
		}
		break;
	    case VxSqlToken.TokenType.Scientific:
		cur += c;
		if (!isdigit(peek))
		{
		    curstate = VxSqlToken.TokenType.Numeric;
		    save_and_reset_state();
		}
		break;
	    default:
		break;
	    }
	}

	if (curstate != VxSqlToken.TokenType.None)
	    save_and_reset_state();

/*
	Console.WriteLine("GOT A REQUEST FROM VERSAPLEX:");
	Console.WriteLine("Original Query: {0}", rawquery);
	Console.WriteLine("Broken down:");
	foreach (VxSqlToken t in tokens)
	{
	    Console.WriteLine("Token: {0}: {1}", t.type.ToString(), t.name);
	}
*/
    }

    public List<VxSqlToken> getlist()
    {
	return tokens;
    }

    public static string form_query(List<VxSqlToken> toks)
    {
	string ret = "";
	foreach (VxSqlToken token in toks)
	    ret += token + " ";
	
	return ret;
    }

    private static bool isalpha(char c)
    {
	return char.IsLetter(c);
    }

    private static bool isidentifierchar(char c)
    {
	return char.IsLetterOrDigit(c) || c == '@' || c == '#' || c == '_' ||
		c == '$';
    }

    private static bool isnumericchar(char c)
    {
	return isdigit(c) || c == '.' || c == 'e' || c == 'E';
    }

    private static bool isspace(char c)
    {
	return char.IsWhiteSpace(c);
    }

    private static bool isdigit(char c)
    {
	return char.IsDigit(c);
    }
}

/*
public class Maintenance
{
    public static void Main(string[] args)
    {
	VxSqlTokenizer me = new VxSqlTokenizer("create table : [zoo] (foo varchar(20), zoo int); insert into [zoo] values (\"fuckhat\", .e); select zoo.foo from [zoo] where zoo ! <> 2");

	foreach (VxSqlToken t in me.getlist())
	{
	    Console.WriteLine("VxSqlToken: {0}: {1}", t.type.ToString(), t.name);
	}
    }
}
*/
