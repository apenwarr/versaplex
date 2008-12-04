/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
#include "wvtest.cs.h"

using System;
using System.Data;
using System.Collections.Generic;
using Wv;
using Wv.Test;


[TestFixture]
class VxExecChunkRecordsetTests
{
    [Test, Category("VxSqlTokenizer")]
    public void TokenizingTests()
    {
	WvLog.maxlevel = WvLog.L.Debug5;
	VxSqlTokenizer tok = new VxSqlTokenizer();

	// This actual string tripped up the tokenizer on 28.10.08, because
	// a stupidly placed Trim() removed the trailing '\n' from the string.
	// Let's make sure we never do that again.
	tok.tokenize("create procedure Func1 as select '" +
			"Hello, world, this is Func1!'\n");

	VxSqlToken[] tokens = tok.gettokens().ToArray();
	WVPASSEQ(tokens.Length, 6);

	string[] expecteds = { "create", " procedure", " Func1", " as",
				" select", " 'Hello, world, this is Func1!'\n"
			    };
	VxSqlToken.TokenType[] expectedt = { VxSqlToken.TokenType.Keyword,
					    VxSqlToken.TokenType.Keyword,
					    VxSqlToken.TokenType.Unquoted,
					    VxSqlToken.TokenType.Keyword,
					    VxSqlToken.TokenType.Keyword,
					    VxSqlToken.TokenType.SingleQuoted
					    };

	for (int i = 0; i < tokens.Length; ++i)
	{
	    VxSqlToken t = tokens[i];
	    WVPASS(t.type == expectedt[i]);
	    WVPASSEQ(t.ToString(), expecteds[i]);
	}
    }


    public static void Main()
    {
	WvTest.DoMain();
    }
}
