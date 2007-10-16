// ****************************************************************
// Copyright 2007, Charlie Poole
// This is free software licensed under the NUnit license. You may
// obtain a copy of the license at http://nunit.org/?p=license&r=2.4
// ****************************************************************

using System;

namespace NUnit.Framework.Constraints
{
	/// <summary>
	/// Abstract base class used for prefixes
	/// </summary>
	public abstract class AbstractPrefixConstraint : Constraint
	{
		/// <summary>
		/// The base constraint
		/// </summary>
		protected Constraint baseConstraint;

		/// <summary>
		/// Construct given a base constraint
		/// </summary>
		/// <param name="baseConstraint"></param>
		protected AbstractPrefixConstraint( Constraint baseConstraint )
		{
			this.baseConstraint = baseConstraint;
		}

		/// <summary>
		/// Set all modifiers applied to the prefix into
		/// the base constraint before matching
		/// </summary>
		protected void PassModifiersToBase()
		{
			if ( this.caseInsensitive )
				baseConstraint = baseConstraint.IgnoreCase;
			if ( this.tolerance != null )
				baseConstraint = baseConstraint.Within( tolerance );
			if ( this.compareAsCollection )
				baseConstraint = baseConstraint.AsCollection;
			if ( this.compareWith != null )
				baseConstraint = baseConstraint.Comparer( compareWith );
		}
	}
}