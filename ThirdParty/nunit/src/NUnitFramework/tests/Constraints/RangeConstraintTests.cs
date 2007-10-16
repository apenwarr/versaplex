using System;

namespace NUnit.Framework.Constraints.Tests
{
	/// <summary>
	/// Summary description for RangeConstraintTests.
	/// </summary>
	[TestFixture]
	public class RangeConstraintTests : ConstraintTestBase
	{
		[SetUp]
		public void SetUp()
		{
			Matcher = new RangeConstraint( 40, 49 );
			GoodValues = new object[] { 40, 42, 48 };
			BadValues = new object [] { 39, 49, 50, "42", null };
			Description = "between 40 and 49";
		}
	}
}
