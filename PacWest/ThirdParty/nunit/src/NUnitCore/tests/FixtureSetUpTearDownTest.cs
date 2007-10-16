// ****************************************************************
// This is free software licensed under the NUnit license. You
// may obtain a copy of the license as well as information regarding
// copyright ownership at http://nunit.org/?p=license&r=2.4.
// ****************************************************************

using System;
using NUnit.Framework;
using NUnit.Core.Builders;
using NUnit.Util;
using NUnit.TestUtilities;
using NUnit.TestData.FixtureSetUpTearDown;

namespace NUnit.Core.Tests
{
	[TestFixture]
	public class FixtureSetupTearDownTest
	{
		private TestSuiteResult RunTestOnFixture( object fixture )
		{
			TestSuite suite = TestBuilder.MakeFixture( fixture.GetType() );
			suite.Fixture = fixture;
			return (TestSuiteResult)suite.Run( NullListener.NULL );
		}

		[Test]
		public void MakeSureSetUpAndTearDownAreCalled()
		{
			SetUpAndTearDownFixture fixture = new SetUpAndTearDownFixture();
			RunTestOnFixture( fixture );

			Assert.AreEqual(1, fixture.setUpCount, "SetUp");
			Assert.AreEqual(1, fixture.tearDownCount, "TearDown");
		}

		[Test]
		public void MakeSureSetUpAndTearDownAreCalledOnExplicitFixture()
		{
			ExplicitSetUpAndTearDownFixture fixture = new ExplicitSetUpAndTearDownFixture();
			RunTestOnFixture( fixture );

			Assert.AreEqual(1, fixture.setUpCount, "SetUp");
			Assert.AreEqual(1, fixture.tearDownCount, "TearDown");
		}

		[Test]
		public void CheckInheritedSetUpAndTearDownAreCalled()
		{
			InheritSetUpAndTearDown fixture = new InheritSetUpAndTearDown();
			RunTestOnFixture( fixture );

			Assert.AreEqual(1, fixture.setUpCount);
			Assert.AreEqual(1, fixture.tearDownCount);
		}

		[Test]
		public void CheckInheritedSetUpAndTearDownAreNotCalled()
		{
			DefineInheritSetUpAndTearDown fixture = new DefineInheritSetUpAndTearDown();
			RunTestOnFixture( fixture );

			Assert.AreEqual(0, fixture.setUpCount);
			Assert.AreEqual(0, fixture.tearDownCount);
			Assert.AreEqual(1, fixture.derivedSetUpCount);
			Assert.AreEqual(1, fixture.derivedTearDownCount);
		}

		[Test]
		public void HandleErrorInFixtureSetup() 
		{
			MisbehavingFixture fixture = new MisbehavingFixture();
			fixture.blowUpInSetUp = true;
			TestSuiteResult result = (TestSuiteResult)RunTestOnFixture( fixture );

			Assert.AreEqual( 1, fixture.setUpCount, "setUpCount" );
			Assert.AreEqual( 0, fixture.tearDownCount, "tearDownCOunt" );

			// should have one suite and one fixture
			ResultSummarizer summ = new ResultSummarizer(result);
			Assert.AreEqual(1, summ.ResultCount);
			Assert.AreEqual(0, summ.TestsNotRun);
			Assert.AreEqual(0, summ.SuitesNotRun);
			
			Assert.IsTrue(result.Executed, "Suite should have executed");
			Assert.IsTrue(result.IsFailure, "Suite should have failed");
			Assert.AreEqual("System.Exception : This was thrown from fixture setup", result.Message, "TestSuite Message");
			Assert.IsNotNull(result.StackTrace, "TestSuite StackTrace should not be null");

			TestResult testResult = ((TestResult)result.Results[0]);
			Assert.IsTrue(testResult.Executed, "Testcase should have executed");
			Assert.AreEqual("TestFixtureSetUp failed in MisbehavingFixture", testResult.Message, "TestSuite Message");
            Assert.AreEqual(FailureSite.Parent, testResult.FailureSite);
			Assert.AreEqual(testResult.StackTrace, testResult.StackTrace, "TestCase stackTrace should match TestSuite stackTrace" );
		}

		[Test]
		public void RerunFixtureAfterSetUpFixed() 
		{
			MisbehavingFixture fixture = new MisbehavingFixture();
			fixture.blowUpInSetUp = true;
			TestSuiteResult result = RunTestOnFixture( fixture );

			// should have one suite and one fixture
			ResultSummarizer summ = new ResultSummarizer(result);
			Assert.AreEqual(1, summ.ResultCount);
			Assert.AreEqual(0, summ.TestsNotRun);
			Assert.AreEqual(0, summ.SuitesNotRun);
			Assert.IsTrue(result.Executed, "Suite should have executed");

			//fix the blow up in setup
			fixture.Reinitialize();
			result = RunTestOnFixture( fixture );

			Assert.AreEqual( 1, fixture.setUpCount, "setUpCount" );
			Assert.AreEqual( 1, fixture.tearDownCount, "tearDownCOunt" );

			// should have one suite and one fixture
			summ = new ResultSummarizer(result);
			Assert.AreEqual(1, summ.ResultCount);
			Assert.AreEqual(0, summ.TestsNotRun);
			Assert.AreEqual(0, summ.SuitesNotRun);
		}

		[Test]
		public void HandleIgnoreInFixtureSetup() 
		{
			IgnoreInFixtureSetUp fixture = new IgnoreInFixtureSetUp();
			TestSuiteResult result = RunTestOnFixture( fixture );

			// should have one suite and one fixture
			ResultSummarizer summ = new ResultSummarizer(result);
			Assert.AreEqual(0, summ.ResultCount);
			Assert.AreEqual(1, summ.TestsNotRun);
			Assert.AreEqual(1, summ.SuitesNotRun);
			Assert.IsFalse(result.Executed, "Suite should not have executed");
			Assert.AreEqual("TestFixtureSetUp called Ignore", result.Message);
			Assert.IsNotNull(result.StackTrace, "StackTrace should not be null");

			TestResult testResult = ((TestResult)result.Results[0]);
			Assert.IsFalse(testResult.Executed, "Testcase should not have executed");
			Assert.AreEqual("TestFixtureSetUp called Ignore", testResult.Message );
		}

		[Test]
		public void HandleErrorInFixtureTearDown() 
		{
			MisbehavingFixture fixture = new MisbehavingFixture();
			fixture.blowUpInTearDown = true;
			TestSuiteResult result = RunTestOnFixture( fixture );
			Assert.AreEqual(1, result.Results.Count);
			Assert.IsTrue(result.Executed, "Suite should have executed");
			Assert.IsTrue(result.IsFailure, "Suite should have failed" );

			Assert.AreEqual( 1, fixture.setUpCount, "setUpCount" );
			Assert.AreEqual( 1, fixture.tearDownCount, "tearDownCOunt" );

			Assert.AreEqual("This was thrown from fixture teardown", result.Message);
			Assert.IsNotNull(result.StackTrace, "StackTrace should not be null");

			// should have one suite and one fixture
			ResultSummarizer summ = new ResultSummarizer(result);
			Assert.AreEqual(1, summ.ResultCount);
			Assert.AreEqual(0, summ.TestsNotRun);
			Assert.AreEqual(0, summ.SuitesNotRun);
		}

		[Test]
		public void HandleExceptionInFixtureConstructor()
		{
			TestSuite suite = TestBuilder.MakeFixture( typeof( ExceptionInConstructor ) );
			TestSuiteResult result = (TestSuiteResult)suite.Run( NullListener.NULL );

			// should have one suite and one fixture
			ResultSummarizer summ = new ResultSummarizer(result);
			Assert.AreEqual(1, summ.ResultCount);
			Assert.AreEqual(0, summ.TestsNotRun);
			Assert.AreEqual(0, summ.SuitesNotRun);
			
			Assert.IsTrue(result.Executed, "Suite should have executed");
			Assert.IsTrue(result.IsFailure, "Suite should have failed");
			Assert.AreEqual("System.Exception : This was thrown in constructor", result.Message, "TestSuite Message");
			Assert.IsNotNull(result.StackTrace, "TestSuite StackTrace should not be null");

			TestResult testResult = ((TestResult)result.Results[0]);
			Assert.IsTrue(testResult.Executed, "Testcase should have executed");
			Assert.AreEqual("TestFixtureSetUp failed in ExceptionInConstructor", testResult.Message, "TestSuite Message");
            Assert.AreEqual(FailureSite.Parent, testResult.FailureSite);
			Assert.AreEqual(testResult.StackTrace, testResult.StackTrace, "TestCase stackTrace should match TestSuite stackTrace" );
		}

		[Test]
		public void RerunFixtureAfterTearDownFixed() 
		{
			MisbehavingFixture fixture = new MisbehavingFixture();
			fixture.blowUpInTearDown = true;
			TestSuiteResult result = RunTestOnFixture( fixture );
			Assert.AreEqual(1, result.Results.Count);

			// should have one suite and one fixture
			ResultSummarizer summ = new ResultSummarizer(result);
			Assert.AreEqual(1, summ.ResultCount);
			Assert.AreEqual(0, summ.TestsNotRun);
			Assert.AreEqual(0, summ.SuitesNotRun);

			fixture.Reinitialize();
			result = RunTestOnFixture( fixture );

			Assert.AreEqual( 1, fixture.setUpCount, "setUpCount" );
			Assert.AreEqual( 1, fixture.tearDownCount, "tearDownCOunt" );

			summ = new ResultSummarizer(result);
			Assert.AreEqual(1, summ.ResultCount);
			Assert.AreEqual(0, summ.TestsNotRun);
			Assert.AreEqual(0, summ.SuitesNotRun);
		}

		[Test]
		public void HandleSetUpAndTearDownWithTestInName()
		{
			SetUpAndTearDownWithTestInName fixture = new SetUpAndTearDownWithTestInName();
			RunTestOnFixture( fixture );

			Assert.AreEqual(1, fixture.setUpCount);
			Assert.AreEqual(1, fixture.tearDownCount);
		}

		[Test]
		public void RunningSingleMethodCallsSetUpAndTearDown()
		{
			SetUpAndTearDownFixture fixture = new SetUpAndTearDownFixture();
			TestSuite suite = TestBuilder.MakeFixture( fixture.GetType() );
			suite.Fixture = fixture;
			NUnit.Core.TestCase testCase = (NUnit.Core.TestCase)suite.Tests[0];
			
			suite.Run(NullListener.NULL, new Filters.NameFilter( testCase.TestName ) );

			Assert.AreEqual(1, fixture.setUpCount);
			Assert.AreEqual(1, fixture.tearDownCount);
		}

		[Test]
		public void IgnoredFixtureShouldNotCallFixtureSetUpOrTearDown()
		{
			IgnoredFixture fixture = new IgnoredFixture();
			TestSuite suite = new TestSuite("IgnoredFixtureSuite");
			TestSuite fixtureSuite = TestBuilder.MakeFixture( fixture.GetType() );
			suite.Fixture = fixture;
			NUnit.Core.TestCase testCase = (NUnit.Core.TestCase)fixtureSuite.Tests[0];
			suite.Add( fixtureSuite );
			
			fixtureSuite.Run(NullListener.NULL);
			Assert.IsFalse( fixture.setupCalled, "TestFixtureSetUp called running fixture" );
			Assert.IsFalse( fixture.teardownCalled, "TestFixtureTearDown called running fixture" );

			suite.Run(NullListener.NULL);
			Assert.IsFalse( fixture.setupCalled, "TestFixtureSetUp called running enclosing suite" );
			Assert.IsFalse( fixture.teardownCalled, "TestFixtureTearDown called running enclosing suite" );

			testCase.Run(NullListener.NULL);
			Assert.IsFalse( fixture.setupCalled, "TestFixtureSetUp called running a test case" );
			Assert.IsFalse( fixture.teardownCalled, "TestFixtureTearDown called running a test case" );
		}

		[Test]
		public void FixtureWithNoTestsShouldNotCallFixtureSetUpOrTearDown()
		{
			FixtureWithNoTests fixture = new FixtureWithNoTests();
			RunTestOnFixture( fixture );		
			Assert.IsFalse( fixture.setupCalled, "TestFixtureSetUp called running fixture" );
			Assert.IsFalse( fixture.teardownCalled, "TestFixtureTearDown called running fixture" );
		}

        [Test]
        public void DisposeCalledWhenFixtureImplementsIDisposable()
        {
            DisposableFixture fixture = new DisposableFixture();
            RunTestOnFixture(fixture);
            Assert.IsTrue(fixture.disposeCalled);
        }
	}
}
