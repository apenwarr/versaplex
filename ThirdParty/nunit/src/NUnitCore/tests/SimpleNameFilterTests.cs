// ****************************************************************
// Copyright 2007, Charlie Poole
// This is free software licensed under the NUnit license. You may
// obtain a copy of the license at http://nunit.org/?p=license&r=2.4
// ****************************************************************

using System;
using System.Collections;
using NUnit.Framework;
using NUnit.Tests.Assemblies;
using NUnit.Core.Builders;
using NUnit.Core.Filters;
using NUnit.TestUtilities;

namespace NUnit.Core.Tests
{
    [TestFixture]
    public class SimpleNameFilterTests
    {
        private TestSuite testSuite;
        private NUnit.Core.TestCase mock3;

        [SetUp]
        public void SetUp()
        {
            testSuite = new TestSuite("Mock Test Suite");
            testSuite.Add(TestBuilder.MakeFixture(typeof(MockTestFixture)));
            mock3 = (NUnit.Core.TestCase)TestFinder.Find("MockTest3", testSuite);
        }

        [Test]
        public void SingleNameMatch()
        {
            string fullName = "NUnit.Tests.Assemblies.MockTestFixture.MockTest3";
            Assert.AreEqual(fullName, mock3.TestName.FullName);
            SimpleNameFilter filter = new SimpleNameFilter(fullName);
            Assert.IsTrue(filter.Pass(mock3), "Name Filter did not pass test case");
            Assert.AreEqual("NUnit.Tests.Assemblies.MockTestFixture", ((TestSuite)testSuite.Tests[0]).TestName.FullName);
            Assert.IsTrue(filter.Pass((TestSuite)testSuite.Tests[0]), "Name Filter did not pass test suite");
        }

        [Test]
        public void MultipleNameMatch()
        {
            NUnit.Core.TestCase mock1 = (NUnit.Core.TestCase)TestFinder.Find("MockTest1", testSuite);
            SimpleNameFilter filter = new SimpleNameFilter();
            filter.Add("NUnit.Tests.Assemblies.MockTestFixture.MockTest3");
            filter.Add("NUnit.Tests.Assemblies.MockTestFixture.MockTest1");
            Assert.IsTrue(filter.Pass(mock3), "Name Filter did not pass test case");
            Assert.IsTrue(filter.Pass(mock1), "Name Filter did not pass test case");
            Assert.IsTrue(filter.Pass((TestSuite)testSuite.Tests[0]), "Name Filter did not pass test suite");
        }

        [Test]
        public void SuiteNameMatch()
        {
            NUnit.Core.TestSuite mockTest = (NUnit.Core.TestSuite)TestFinder.Find("MockTestFixture", testSuite);
            SimpleNameFilter filter = new SimpleNameFilter("NUnit.Tests.Assemblies.MockTestFixture");
            Assert.IsTrue(filter.Pass(mock3), "Name Filter did not pass test case");
            Assert.IsTrue(filter.Pass(mockTest), "Suite did not pass test case");
            Assert.IsTrue(filter.Pass(testSuite), "Suite did not pass test case");
        }

        [Test]
        public void TestDoesNotMatch()
        {
            SimpleNameFilter filter = new SimpleNameFilter("NUnit.Tests.Assemblies.MockTestFixture.MockTest1");
            Assert.IsFalse(filter.Pass(mock3), "Name Filter did pass test case");
            Assert.IsTrue(filter.Pass(testSuite), "Name Filter did not pass test suite");
        }

        [Test]
        public void HighLevelSuite()
        {
            NUnit.Core.TestSuite mockTest = (NUnit.Core.TestSuite)TestFinder.Find("MockTestFixture", testSuite);
            SimpleNameFilter filter = new SimpleNameFilter("NUnit.Tests.Assemblies.MockTestFixture");
            Assert.AreEqual(true, filter.Pass(mock3), "test case");
            Assert.AreEqual(true, filter.Pass(mockTest), "middle suite");
            Assert.AreEqual(true, filter.Pass(testSuite), "test suite");
        }

        [Test]
        public void ExplicitTestCaseDoesNotMatchWhenNotSelectedDirectly()
        {
            NUnit.Core.TestCase explicitTest = (NUnit.Core.TestCase)TestFinder.Find("ExplicitlyRunTest", testSuite);
            SimpleNameFilter filter = new SimpleNameFilter("Mock Test Suite");
            Assert.AreEqual(false, filter.Pass(explicitTest));
        }

        [Test]
        public void ExplicitTestCaseMatchesWhenSelectedDirectly()
        {
            NUnit.Core.TestCase explicitTest = (NUnit.Core.TestCase)TestFinder.Find("ExplicitlyRunTest", testSuite);
            SimpleNameFilter filter = new SimpleNameFilter("NUnit.Tests.Assemblies.MockTestFixture.ExplicitlyRunTest");
            Assert.IsTrue(filter.Pass(explicitTest), "Name Filter did not pass on test case");
            Assert.IsTrue(filter.Pass(testSuite), "Name Filter did not pass on test suite");
        }

        [Test]
        public void ExplicitTestSuiteDoesNotMatchWhenNotSelectedDirectly()
        {
            NUnit.Core.TestSuite mockTest = (NUnit.Core.TestSuite)TestFinder.Find("MockTestFixture", testSuite);
			mockTest.RunState = RunState.Explicit;
            SimpleNameFilter filter = new SimpleNameFilter("Mock Test Suite");
            Assert.AreEqual(false, filter.Pass(mock3), "descendant of explicit suite should not match");
            Assert.AreEqual(false, filter.Pass(mockTest), "explicit suite should not match");
        }

        [Test]
        public void ExplicitTestSuiteMatchesWhenSelectedDirectly()
        {
            NUnit.Core.TestSuite mockTest = (NUnit.Core.TestSuite)TestFinder.Find("MockTestFixture", testSuite);
			mockTest.RunState = RunState.Explicit;
            SimpleNameFilter filter = new SimpleNameFilter("NUnit.Tests.Assemblies.MockTestFixture");
            Assert.AreEqual(true, filter.Pass(mock3), "test case");
            Assert.AreEqual(true, filter.Pass(mockTest), "middle suite");
            Assert.AreEqual(true, filter.Pass(testSuite), "test suite");
        }
    }
}
