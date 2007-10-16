// ****************************************************************
// Copyright 2007, Charlie Poole
// This is free software licensed under the NUnit license. You may
// obtain a copy of the license at http://nunit.org/?p=license&r=2.4
// ****************************************************************

using System;
using NUnit.Framework.Constraints;

namespace NUnit.Framework.SyntaxHelpers
{
	/// <summary>
	/// SyntaxHelper is the abstract base class for all
	/// syntax helpers.
	/// </summary>
	public abstract class SyntaxHelper
	{
		#region Prefix Operators
		/// <summary>
		/// Not returns a ConstraintBuilder that negates
		/// the constraint that follows it.
		/// </summary>
		public static ConstraintBuilder Not
		{
			get { return new ConstraintBuilder().Not; }
		}

		/// <summary>
		/// All returns a ConstraintBuilder, which will apply
		/// the following constraint to all members of a collection,
		/// succeeding if all of them succeed.
		/// </summary>
		public static ConstraintBuilder AllItems
		{
			get { return new ConstraintBuilder().All; }
		}

		/// <summary>
		/// SomeItem returns a ConstraintBuilder, which will apply
		/// the following constraint to all members of a collection,
		/// succeeding if any of them succeed.
		/// </summary>
		public static ConstraintBuilder SomeItem
		{
			get { return new ConstraintBuilder().Some; }
		}

		/// <summary>
		/// NoItem returns a ConstraintBuilder, which will apply
		/// the following constraint to all members of a collection,
		/// succeeding only if none of them succeed.
		/// </summary>
		public static ConstraintBuilder NoItem
		{
			get { return new ConstraintBuilder().None; }
		}
		#endregion
	}
}
