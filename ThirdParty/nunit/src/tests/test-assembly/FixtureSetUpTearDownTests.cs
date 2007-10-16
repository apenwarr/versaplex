// ****************************************************************
// Copyright 2007, Charlie Poole
// This is free software licensed under the NUnit license. You may
// obtain a copy of the license at http://nunit.org/?p=license&r=2.4
// ****************************************************************

using System;
using NUnit.Framework;

namespace NUnit.TestData.FixtureSetUpTearDown
{
	[TestFixture]
	public class SetUpAndTearDownFixture
	{
		public int setUpCount = 0;
		public int tearDownCount = 0;

		[TestFixtureSetUp]
		public virtual void Init()
		{
			setUpCount++;
		}

		[TestFixtureTearDown]
		public virtual void Destroy()
		{
			tearDownCount++;
		}

		[Test]
		public void Success(){}

		[Test]
		public void EvenMoreSuccess(){}
	}

	[TestFixture,Explicit]
	public class ExplicitSetUpAndTearDownFixture
	{
		public int setUpCount = 0;
		public int tearDownCount = 0;

		[TestFixtureSetUp]
		public virtual void Init()
		{
			setUpCount++;
		}

		[TestFixtureTearDown]
		public virtual void Destroy()
		{
			tearDownCount++;
		}

		[Test]
		public void Success(){}

		[Test]
		public void EvenMoreSuccess(){}
	}

	[TestFixture]
	public class InheritSetUpAndTearDown : SetUpAndTearDownFixture
	{
		[Test]
		public void AnotherTest(){}

		[Test]
		public void YetAnotherTest(){}
	}

	[TestFixture]
	public class DefineInheritSetUpAndTearDown : SetUpAndTearDownFixture
	{
		public int derivedSetUpCount;
		public int derivedTearDownCount;

		[TestFixtureSetUp]
		public override void Init()
		{
			derivedSetUpCount++;
		}

		[TestFixtureTearDown]
		public override void Destroy()
		{
			derivedTearDownCount++;
		}

		[Test]
		public void AnotherTest(){}

		[Test]
		public void YetAnotherTest(){}
	}

	[TestFixture]
	public class MisbehavingFixture 
	{
		public bool blowUpInSetUp = false;
		public bool blowUpInTearDown = false;

		public int setUpCount = 0;
		public int tearDownCount = 0;

		public void Reinitialize()
		{
			setUpCount = 0;
			tearDownCount = 0;

			blowUpInSetUp = false;
			blowUpInTearDown = false;
		}

		[TestFixtureSetUp]
		public void BlowUpInSetUp() 
		{
			setUpCount++;
			if (blowUpInSetUp)
				throw new Exception("This was thrown from fixture setup");
		}

		[TestFixtureTearDown]
		public void BlowUpInTearDown()
		{
			tearDownCount++;
			if ( blowUpInTearDown )
				throw new Exception("This was thrown from fixture teardown");
		}

		[Test]
		public void nothingToTest() 
		{
		}
	}

	[TestFixture]
	public class ExceptionInConstructor
	{
		public ExceptionInConstructor()
		{
			throw new Exception( "This was thrown in constructor" );
		}

		[Test]
		public void nothingToTest()
		{
		}
	}

	[TestFixture]
	public class IgnoreInFixtureSetUp
	{
		[TestFixtureSetUp]
		public void SetUpCallsIgnore() 
		{
			Assert.Ignore( "TestFixtureSetUp called Ignore" );
		}

		[Test]
		public void nothingToTest() 
		{
		}
	}

	[TestFixture]
	public class SetUpAndTearDownWithTestInName
	{
		public int setUpCount = 0;
		public int tearDownCount = 0;

		[TestFixtureSetUp]
		public virtual void TestFixtureSetUp()
		{
			setUpCount++;
		}

		[TestFixtureTearDown]
		public virtual void TestFixtureTearDown()
		{
			tearDownCount++;
		}

		[Test]
		public void Success(){}

		[Test]
		public void EvenMoreSuccess(){}
	}

	[TestFixture, Ignore( "Do Not Run This" )]
	public class IgnoredFixture
	{
		public bool setupCalled = false;
		public bool teardownCalled = false;

		[TestFixtureSetUp]
		public virtual void ShouldNotRun()
		{
			setupCalled = true;
		}

		[TestFixtureTearDown]
		public virtual void NeitherShouldThis()
		{
			teardownCalled = true;
		}

		[Test]
		public void Success(){}

		[Test]
		public void EvenMoreSuccess(){}
	}

	[TestFixture]
	public class FixtureWithNoTests
	{
		public bool setupCalled = false;
		public bool teardownCalled = false;

		[TestFixtureSetUp]
		public virtual void Init()
		{
			setupCalled = true;
		}

		[TestFixtureTearDown]
		public virtual void Destroy()
		{
			teardownCalled = true;
		}
	}

    [TestFixture]
    public class DisposableFixture : IDisposable
    {
        public bool disposeCalled = false;

        [Test]
        public void OneTest() { }

        public void Dispose()
        {
            disposeCalled = true;
        }
    }
}
