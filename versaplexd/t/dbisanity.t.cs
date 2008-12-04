/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
#include "wvtest.cs.h"

using System;
using System.Linq;
using Wv;
using Wv.Test;

[TestFixture]
class WvDbiSanity
{
    [Test]
    public void dbisanity()
    {
	using (VersaplexTester t = new VersaplexTester())
	{
	    WvDbi dbi = t.dbi;
	    
	    dbi.try_exec("drop table dbisanity");
	    dbi.exec("create table dbisanity (name varchar(80), age integer)");
	    dbi.exec("insert into dbisanity values ('frog', 12)");
	    dbi.exec("insert into dbisanity values ('bog', 13)");
	    dbi.exec("insert into dbisanity values ('hog', 14)");
	    
	    using (var r = dbi.select("select * from dbisanity"))
	    {
		WVPASSEQ(r.columns.Count(), 2);
		WVPASSEQ(r.columns.Count(), 2);
		WVPASSEQ(r.columns.Count(), 2);
		WVPASSEQ(r.Count(), 3);
	    }

	    using (var r = dbi.select("select * from dbisanity"))
	    {
		WVPASSEQ(r.columns.Count(), 2);
		WVPASSEQ(r.columns.Count(), 2);
		WVPASSEQ(r.columns.Count(), 2);
		WVPASSEQ(r.Count(), 3);
	    }
	    
	    dbi.try_exec("drop table dbisanity");
	}
    }

    public static void Main()
    {
	WvTest.DoMain();
    }
}
