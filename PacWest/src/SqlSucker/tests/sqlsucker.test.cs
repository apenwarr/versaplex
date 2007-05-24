#include "wvtest.cs.h"

using System;
using NUnit.Framework;
using Wv.Test;

namespace SqlSucker.Test
{

[TestFixture]
public class SqlSuckerTest {
    [SetUp]
    public void init() {
    }

    [TearDown]
    public void cleanup() {
    }

    [Test]
    public void test1() {
	WVPASSEQ(1, 1);
    }
}

}

