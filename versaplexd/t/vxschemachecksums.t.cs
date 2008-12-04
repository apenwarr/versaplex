/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
#include "wvtest.cs.h"
// Test the VxSchemaChecksums and VxSchemaChecksum classes

using System;
using System.Linq;
using System.Collections.Generic;
using Wv;
using Wv.Test;

// Note: This doesn't inherit from VersaplexTester, since it just tests some 
// standalone static methods.
[TestFixture]
class VxSchemaChecksumTester 
{
    [Test, Category("Schemamatic"), Category("VxSchemaChecksum")]
    public void TestParseSumString()
    {
        IEnumerable<ulong> list;
        list = VxSchemaChecksum.ParseSumString(null);
        WVPASSEQ(list.Count(), 0);

        list = VxSchemaChecksum.ParseSumString("");
        WVPASSEQ(list.Count(), 0);

        list = VxSchemaChecksum.ParseSumString(" ");
        WVPASSEQ(list.Count(), 0);

        list = VxSchemaChecksum.ParseSumString("  ");
        WVPASSEQ(list.Count(), 0);

        list = VxSchemaChecksum.ParseSumString("0x10");
        WVPASSEQ(list.Count(), 1);
        WVPASSEQ(list.First(), 16);

        list = VxSchemaChecksum.ParseSumString("10");
        WVPASSEQ(list.Count(), 1);
        WVPASSEQ(list.First(), 16);

        list = VxSchemaChecksum.ParseSumString("0x10 0x20");
        WVPASSEQ(list.Count(), 2);
        WVPASSEQ(list.ElementAt(0), 16);
        WVPASSEQ(list.ElementAt(1), 32);

        list = VxSchemaChecksum.ParseSumString("20 10");
        WVPASSEQ(list.Count(), 2);
        WVPASSEQ(list.ElementAt(0), 32);
        WVPASSEQ(list.ElementAt(1), 16);

        list = VxSchemaChecksum.ParseSumString("0x10 20 0X3a 0x4B ");
        WVPASSEQ(list.Count(), 4);
        WVPASSEQ(list.ElementAt(0), 16);
        WVPASSEQ(list.ElementAt(1), 32);
        WVPASSEQ(list.ElementAt(2), 58);
        WVPASSEQ(list.ElementAt(3), 75);

        // Test ignoring invalid elements
        list = VxSchemaChecksum.ParseSumString("0x10 0xasdf 0x20");
        WVPASSEQ(list.Count(), 2);
        WVPASSEQ(list.ElementAt(0), 16);
        WVPASSEQ(list.ElementAt(1), 32);
    }

    [Test, Category("Schemamatic"), Category("VxSchemaChecksum")]
    public void TestChecksumEquals()
    {
        VxSchemaChecksum sum1 = new VxSchemaChecksum("foo", 1);
        VxSchemaChecksum sum2 = new VxSchemaChecksum("foo", 1);
        VxSchemaChecksum sum3 = new VxSchemaChecksum("bar", 1);
        // Make sure to get a value greater than 2^32 in the mix.
        ulong[] list4 = {1, 2, (0x10UL<<32)};
        VxSchemaChecksum sum4 = new VxSchemaChecksum("bar", list4);

        WVFAIL(sum1.Equals(null));
        WVFAIL(sum1.Equals(new object()));

        // Test symmetry (a==b => b==a) and reflexivity (a==a) in Equals()
        WVPASS(sum1.Equals(sum1));
        WVPASS(sum1.Equals(sum2));
        WVPASS(!sum1.Equals(sum3));
        WVPASS(!sum1.Equals(sum4));

        WVPASS(sum2.Equals(sum1));
        WVPASS(sum2.Equals(sum2));
        WVPASS(!sum2.Equals(sum3));
        WVPASS(!sum2.Equals(sum4));

        WVPASS(!sum3.Equals(sum1));
        WVPASS(!sum3.Equals(sum2));
        WVPASS(sum3.Equals(sum3));
        WVPASS(!sum3.Equals(sum4));

        WVPASS(!sum4.Equals(sum1));
        WVPASS(!sum4.Equals(sum2));
        WVPASS(!sum4.Equals(sum3));
        WVPASS(sum4.Equals(sum4));

        // The hash codes only depend on the list of checksums.
        WVPASSEQ(sum1.GetHashCode(), 1);
        WVPASSEQ(sum2.GetHashCode(), 1);
        WVPASSEQ(sum3.GetHashCode(), 1);
        WVPASSEQ(sum4.GetHashCode(), 0x13);

        // Check that Equals() being true implies GetHashCode() matches
        VxSchemaChecksum[] sumarr = {sum1, sum2, sum3, sum4};
        foreach (var i in sumarr)
            foreach (var j in sumarr)
            {
                int ihash = i.GetHashCode();
                int jhash = j.GetHashCode();
                WVPASS(!i.Equals(j) || (ihash == jhash));
            }
    }

    public static void Main()
    {
        WvTest.DoMain();
    }
}
