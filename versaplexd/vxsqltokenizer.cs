
using System;
using System.Collections.Generic;

public class VxSqlToken
{
    public enum TokenType { None, Unquoted, SingleQuoted, DoubleQuoted,
    				Delimited, LParen, RParen, Comma, Semicolon,
				Keyword, DelimitedComment, Comment,
				Relop, Not, Bitop, Addop, Multop,
				Numeric, Scientific, Period, ERROR_UNKNOWN };
    public TokenType type;
    public string name;
    public string leading_space;
    public string trailing_space;

    public VxSqlToken(TokenType t, string n, string l)
    {
	type = t;
	name = n;
	leading_space = l;
	trailing_space = "";
    }

    public bool NotQuotedAndLowercaseEq(string eq)
    {
	return type != TokenType.SingleQuoted && type != TokenType.DoubleQuoted
		&& type != TokenType.Delimited && type != TokenType.Comment
		&& type != TokenType.DelimitedComment
		&& name.ToLower() == eq.ToLower();
    }

    public bool IsKeyWordEq(string key)
    {
	return type == TokenType.Keyword && key.ToUpper() == name.ToUpper();
    }

    public override string ToString()
    {
	string cool = name;
	if (type == TokenType.SingleQuoted)
	    cool = String.Format("'{0}'", name);
	else if (type == TokenType.DoubleQuoted)
	    cool = String.Format("\"{0}\"", name);
	else if (type == TokenType.Delimited)
	    cool = String.Format("[{0}]", name);
	else if (type == TokenType.DelimitedComment)
	    cool = String.Format("/*{0}*/", name);
	else if (type == TokenType.Comment)
	    cool = String.Format("--{0}", name);
	//if (type == TokenType.Keyword)
	//    return name.ToUpper();
	return leading_space + cool + trailing_space;
    }

    public static implicit operator string(VxSqlToken t)
    {
	return t.ToString();
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
    private VxSqlToken last;

    string cur;
    string curspace;
    VxSqlToken.TokenType curstate;

    private void reset_state()
    {
	cur = "";
	curspace = "";
	curstate = VxSqlToken.TokenType.None;
    }

    private void save_and_reset_state()
    {
	last = new VxSqlToken(curstate, cur, curspace);
	tokens.Add(last);
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
	    return VxSqlToken.TokenType.Relop;
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

    public void tokenize(string q)
    {
	last = null;
	tokens = new List<VxSqlToken>();

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
		    save_and_reset_state();
		}
		else if (!isspace(c))
		{
		    cur += c;
		    curstate = VxSqlToken.TokenType.ERROR_UNKNOWN;
		    save_and_reset_state();
		}
		else //whitespace
		    curspace += c;
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
			cur += "\"\"";
			++i;
		    }
		    else
			save_and_reset_state();
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
		// NOTE:  In T-Sql, a 'GO' command within a comment generates an
		// error, but our tokenizer doesn't need to care about this.
		if (c == '\n')
		{
		    save_and_reset_state();
		    curspace += c;
		}
		else
		    cur += c;
		break;
	    case VxSqlToken.TokenType.DelimitedComment:
		// NOTE:  In T-Sql, a 'GO' command within a comment generates an
		// error, but our tokenizer doesn't need to care about this.
		if (c == '*' && peek == '/')
		{
		    ++i;
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
	if (curspace != "")
	{
	    if (last != null)
		last.trailing_space = curspace;
	    else
	    {
		last = new VxSqlToken(VxSqlToken.TokenType.None, "", curspace);
		tokens.Add(last);
	    }
	}

/*
	Console.WriteLine("GOT A REQUEST FROM VERSAPLEX:");
	Console.WriteLine("Original Query: {0}", q);
	Console.WriteLine("Broken down:");
	foreach (VxSqlToken t in tokens)
	{
	    Console.WriteLine("Token: {0}: {1}", t.type.ToString(), t.name);
	}
*/
    }

    public VxSqlTokenizer(string query)
    {
	tokenize(query);
    }

    public VxSqlTokenizer()
    {
    }

    public List<VxSqlToken> gettokens()
    {
	return tokens;
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
	VxSqlTokenizer me = new VxSqlTokenizer();
	me.tokenize("create procedure Func1 as select 'Hello, world, this is Func1!'\n");

	foreach (VxSqlToken t in me.gettokens())
	{
	    Console.WriteLine("VxSqlToken: {0}: {1}", t.type, t);
	}
    }
} */
