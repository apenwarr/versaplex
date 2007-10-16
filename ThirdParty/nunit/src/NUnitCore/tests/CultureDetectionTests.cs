using System;
using System.Globalization;
using NUnit.Framework;

namespace NUnit.Core.Tests
{
	/// <summary>
	/// Summary description for CultureDetectionTests.
	/// </summary>
	[TestFixture]
	public class CultureDetectionTests
	{
		private CultureDetector detector = new CultureDetector("fr-FR");

		private void ExpectMatch( string culture )
		{
			if ( !detector.IsCultureSupported( culture ) )
				Assert.Fail( string.Format( "Failed to match \"{0}\"" , culture ) );
		}

		private void ExpectMatch( CultureAttribute attr )
		{
			if ( !detector.IsCultureSupported( attr ) )
				Assert.Fail( string.Format( "Failed to match attribute with Include=\"{0}\",Exclude=\"{1}\"", attr.Include, attr.Exclude ) );
		}

		private void ExpectFailure( string culture )
		{
			if ( detector.IsCultureSupported( culture ) )
				Assert.Fail( string.Format( "Should not match \"{0}\"" , culture ) );
			Assert.AreEqual( "Only supported under culture " + culture, detector.Reason );
		}

		private void ExpectFailure( CultureAttribute attr, string msg )
		{
			if ( detector.IsCultureSupported( attr ) )
				Assert.Fail( string.Format( "Should not match attribute with Include=\"{0}\",Exclude=\"{1}\"",
					attr.Include, attr.Exclude ) );
			Assert.AreEqual( msg, detector.Reason );
		}

		[Test]
		public void CanMatchStrings()
		{
			ExpectMatch( "fr-FR" );
			ExpectMatch( "fr" );
			ExpectMatch( "fr-FR,fr-BE,fr-CA" );
			ExpectMatch( "en,de,fr,it" );
			ExpectFailure( "en-GB" );
			ExpectFailure( "en" );
			ExpectFailure( "fr-CA" );
			ExpectFailure( "fr-BE,fr-CA" );
			ExpectFailure( "en,de,it" );
		}

		[Test]
		public void CanMatchAttributeWithInclude()
		{
			ExpectMatch( new CultureAttribute( "fr-FR" ) );
			ExpectMatch( new CultureAttribute( "fr-FR,fr-BE,fr-CA" ) );
			ExpectFailure( new CultureAttribute( "en" ), "Only supported under culture en" );
			ExpectFailure( new CultureAttribute( "en,de,it" ), "Only supported under culture en,de,it" );
		}

		[Test]
		public void CanMatchAttributeWithExclude()
		{
			CultureAttribute attr = new CultureAttribute();
			attr.Exclude = "en";
			ExpectMatch( attr );
			attr.Exclude = "en,de,it";
			ExpectMatch( attr );
			attr.Exclude = "fr";
			ExpectFailure( attr, "Not supported under culture fr");
			attr.Exclude = "fr-FR,fr-BE,fr-CA";
			ExpectFailure( attr, "Not supported under culture fr-FR,fr-BE,fr-CA" );
		}

		[Test]
		public void CanMatchAttributeWithIncludeAndExclude()
		{
			CultureAttribute attr = new CultureAttribute( "en,fr,de,it" );
			attr.Exclude="fr-CA,fr-BE";
			ExpectMatch( attr );
			attr.Exclude = "fr-FR";
			ExpectFailure( attr, "Not supported under culture fr-FR" );
		}
	}
}
