// ****************************************************************
// Copyright 2007, Charlie Poole
// This is free software licensed under the NUnit license. You may
// obtain a copy of the license at http://nunit.org/?p=license&r=2.4
// ****************************************************************

using System;
using System.Collections;

namespace NUnit.Framework.Constraints
{
	/// <summary>
	/// EmptyConstraint tests a whether a string or collection is empty,
	/// postponing the decision about which test is applied until the
	/// type of the actual argument is known.
	/// </summary>
	public class EmptyConstraint : Constraint
	{
        /// <summary>
        /// Test whether the constraint is satisfied by a given value
        /// </summary>
        /// <param name="actual">The value to be tested</param>
        /// <returns>True for success, false for failure</returns>
		public override bool Matches(object actual)
		{
			this.actual = actual;

			return actual is string && (string)actual == string.Empty
				|| actual is ICollection && ((ICollection)actual).Count == 0;
		}

        /// <summary>
        /// Write the constraint description to a MessageWriter
        /// </summary>
        /// <param name="writer">The writer on which the description is displayed</param>
		public override void WriteDescriptionTo(MessageWriter writer)
		{
			writer.Write( "<empty>" );
		}
	}
}
