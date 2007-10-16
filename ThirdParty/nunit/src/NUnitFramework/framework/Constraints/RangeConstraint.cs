using System;

namespace NUnit.Framework.Constraints
{
	/// <summary>
	/// Summary description for BetweenConstraint.
	/// </summary>
	public class RangeConstraint : Constraint
	{
		IComparable low;
		IComparable high;
		bool includeLow = true;
		bool includeHigh = false;

		public RangeConstraint( IComparable low, IComparable high, bool includeLow, bool includeHigh )
		{
			this.low = low;
			this.high = high;
			this.includeLow = includeLow;
			this.includeHigh = includeHigh;
		}

		public RangeConstraint( IComparable low, IComparable high, bool inclusive )
			: this( low, high, inclusive, inclusive ) { }

		public RangeConstraint( IComparable low, IComparable high )
			: this( low, high, true, false ) { }

		public override bool Matches(object actual)
		{
			this.actual = actual;

			if ( actual == null )
				return false;
			
			Type actualType = actual.GetType();
			if ( actualType != low.GetType() || actualType != high.GetType() )
				return false;

			int lowCompare = low.CompareTo( actual );
			if ( lowCompare > 0 || !includeLow && lowCompare == 0 )
				return false;

			int highCompare = high.CompareTo( actual );
			return highCompare > 0 || includeHigh && highCompare == 0;
		}

		public override void WriteDescriptionTo(MessageWriter writer)
		{
			writer.WritePredicate( "between" );
			writer.WriteExpectedValue( low );
			writer.WriteConnector( "and" );
			writer.WriteExpectedValue( high );
		}
	}
}
