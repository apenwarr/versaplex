using System;
using System.IO;
using System.Xml.Serialization;
using NUnit.Framework;

namespace NUnit.Util.Tests
{
	/// <summary>
	/// Summary description for NUnitProjectFileTests.
	/// </summary>
	[TestFixture]
	public class NUnitProjectFileTests
	{
		[Test]
		public void SerializeProject()
		{
			NUnitProjectFile project = new NUnitProjectFile();

			XmlSerializer serializer = new XmlSerializer( typeof( NUnitProjectFile ) );
			StringWriter writer = new StringWriter();
			serializer.Serialize( writer, project );
			Console.WriteLine( writer );
		}
	}

	[Serializable]
	public class NUnitProjectFile
	{
		public struct ProjectSettings
		{
			[XmlAttribute]
			public string appbase;

			[XmlAttribute]
			public string activeConfig;

			public ProjectSettings( string appbase, string activeConfig )
			{
				this.appbase = appbase;
				this.activeConfig = activeConfig;
			}
		}

		public struct ProjectConfig
		{
			[XmlAttribute]
			public string name;

			[XmlAttribute]
			public string appbase;

			[XmlAttribute]
			public string configfile;

			[XmlAttribute]
			public string binpath;

			[XmlAttribute]
			public string binpathtype;

			[XmlElement]
			public AssemblyItem[] assembly;
		}

		public ProjectSettings Settings = new ProjectSettings( null, "Release" );

		[XmlElement("Config")]
		public ProjectConfig[] Configs;

		public NUnitProjectFile()
		{
			Configs = new ProjectConfig[2];

			Configs[0].name = "Debug";
			Configs[0].binpathtype = "Auto";
			Configs[0].assembly = new AssemblyItem[2];
			Configs[0].assembly[0].path = @"NUnitFramework\tests\bin\Debug\nunit.framework.tests.dll";
			Configs[0].assembly[1].path = @"NUnitCore\tests\bin\Debug\nunit.core.tests.dll";

			Configs[1].name = "Release";
			Configs[1].binpathtype = "Auto";
			Configs[1].assembly = new AssemblyItem[2];
			Configs[1].assembly[0].path = @"NUnitFramework\tests\bin\Release\nunit.framework.tests.dll";
			Configs[1].assembly[1].path = @"NUnitCore\tests\bin\Release\nunit.core.tests.dll";
		}
	}
}
