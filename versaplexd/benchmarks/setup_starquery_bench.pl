#!/usr/bin/perl -w

# A script to initialize an MSSQL database for performance testing using
# StarQuery (comparing VxODBC vs. native MSSQL TDS).  Run with "-c" to clean
# up the database without initializing it again.

use strict;

use DBI;

my $dbh = DBI->connect("DBI:Sybase:server=testdb;database=testdb", "sa", "scs",
	{PrintError => 1}) || die "No!";

# Employee:
# 	int
# 	nvarchar(x), nvarchar(x)
# 	currency (salary)
# 	datetime (date of hire)
# 	bit (gender)
# 	double (%discontentment)
#
# BloodType:
# 	int
# 	nchar(n)
#
# LifeStory:
# 	int
# 	nvarchar(max)
#
# Department:
# 	int
# 	varchar(x)
# 	char (some_dumb_code)
#	decimal (yearly budget)
#
# Obituary
# 	int
# 	varchar(max)

my @cleanup = (
	"DROP TABLE Employee",
	"DROP TABLE Department",
	"DROP TABLE BloodType",
	"DROP TABLE LifeStory",
	"DROP TABLE Obituary"
);

my @setup = (
	"CREATE TABLE Employee \
		(id INT NOT NULL PRIMARY KEY IDENTITY(1,1), \
		 last_name NVARCHAR(30) NOT NULL, \
		 first_name NVARCHAR(30) NOT NULL, \
		 salary MONEY NOT NULL, \
		 date_of_hire DATETIME NOT NULL, \
		 gender BIT NOT NULL, \
		 discontentment REAL, \
		 blood_type INT, \
		 lifestory INT, \
		 department INT, \
		 obituary INT)",
	"CREATE TABLE BloodType \
		(type INT NOT NULL PRIMARY KEY IDENTITY(1,1), \
		 description NCHAR(15) NOT NULL)",
	"CREATE TABLE LifeStory \
		(story INT NOT NULL PRIMARY KEY IDENTITY(1,1),
		 full_text NVARCHAR(MAX) NOT NULL)",
	"CREATE TABLE Department \
		(department INT NOT NULL PRIMARY KEY IDENTITY(1,1), \
		 name VARCHAR(40) NOT NULL, \
		 some_dumb_code CHAR(5), \
		 yearly_budget DECIMAL(19, 2))",
	"CREATE TABLE Obituary \
		(obituary INT NOT NULL PRIMARY KEY IDENTITY(1,1), \
		 english_version VARCHAR(MAX) NOT NULL)"
);

my @blood_values = (
	"INSERT INTO BloodType VALUES ('A')",
	"INSERT INTO BloodType VALUES ('B')",
	"INSERT INTO BloodType VALUES ('AB')",
	"INSERT INTO BloodType VALUES ('Ö')",
	"INSERT INTO BloodType VALUES ('Mutant-Hybrid')",
	"INSERT INTO BloodType VALUES ('Sample 33B')",
	"INSERT INTO BloodType VALUES ('Corrósive Acid')",
	"INSERT INTO BloodType VALUES ('Français')"
);

my @life_values = (
	"INSERT INTO LifeStory VALUES ('I went intó the foręst ąnd contracted Pąul Buńyan diśęąśę')",
	"INSERT INTO LifeStory VALUES ('Be picked up along the Québec border by marauding 中国 Super Über Mother Mutants, Yarr!')",
	"INSERT INTO LifeStory VALUES ('Found amongst the Tyrąnid Hordęs, slaughtering gôy left and right')"
);

my @dept_values = (
	"INSERT INTO Department VALUES ('Secret Research', 'S', '4353245.23')",
	"INSERT INTO Department VALUES ('Death Squads', 'DS', '34645523.10')",
	"INSERT INTO Department VALUES ('Pirate Speakers', 'SKULL', '0.01')",
	"INSERT INTO Department VALUES ('Bee Keeping', 'BEE', '100.00')",
	"INSERT INTO Department VALUES ('Recruitment', 'R', '25000.25')",
	"INSERT INTO Department VALUES ('Palindromes', 'OAO', '1110001.11')"
);

my @obituary_values = (
	"INSERT INTO Obituary VALUES ('Yarr, he be goin to Davy Jones locker!')",
	"INSERT INTO Obituary VALUES ('Trampled upon by a giant Mastodon.')",
	"INSERT INTO Obituary VALUES ('Eaten by a grue.')",
	"INSERT INTO Obituary VALUES ('Had all bones in body crushed via violent, passionate sex with an entire tribe of dark elves.')",
	"INSERT INTO Obituary VALUES ('Crushed by an anvil and ate 100 bullets after accidentally being injected with a strange virus which mutated this poor bastard into a half-dog half-orc monstrosity who could climb walls and ate raw human flesh.')",
);

my @female_names = (
"Adelheid",
"Adolpha",
"Afra",
"Agatha",
"Agnes",
"Alexandra",
"Alfonsine",
"Alheyd",
"Amelia",
"Anastasia",
"Andrea",
"Angelica",
"Angelina",
"Anna",
"Annalesa",
"Annemarie",
"Antonia",
"Apollonia",
"Augusta",
"Barbara",
"Beatrijs",
"Berta",
"Blanca",
"Bleckerynne",
"Brigitta",
"Carlotta",
"Catharina",
"Christiane",
"Christina",
"Clara",
"Condradine",
"Cornelia",
"Cosima",
"Crescentia",
"Dorothea",
"Druidgen",
"Edeltraut",
"Eleanor",
"Elfrieda",
"Elisabeth",
"Elsa",
"Elsbeth",
"Elspeth",
"Elss",
"Ennel",
"Erica",
"Ermingard",
"Eva",
"Felicitas",
"Fernanda",
"Else",
"Flora",
"Francesca",
"Frederica",
"Frieda",
"Fritzi",
"Fya",
"Fygen",
"Gabriela",
"Galena",
"Genoveva",
"Georgia",
"Gerda",
"Gisela",
"Grete",
"Gretel",
"Grietgen",
"Grietkin",
"Gruner",
"Gysela",
"Haiga",
"Hanna",
"Hans",
"Hedwig",
"Helena",
"Henrietta",
"Huberta",
"Ida",
"Ilse,Ilsa",
"Irmgard",
"Irwina",
"Isolde",
"Jaquetta",
"Joan",
"Johanna",
"Josepha",
"Josephina",
"Julia",
"Kaila",
"Karyssa",
"Katarina",
"Katharina",
"Katherine",
"Kunigund",
"Kunigunde",
"Laura",
"Leissl",
"Leonora",
"Leopoldine",
"Liesel",
"Liselotte",
"Lucia",
"Louisa",
"Lucretia",
"Madela",
"Magdalena",
"Margarethe",
"Margrethe",
"Maria",
"Marretta",
"Martha",
"Martina",
"Mathilda",
"Mayken",
"Mechthild",
"Michaela",
"Nicole",
"Niesgin",
"Octavia",
"Orthey",
"Ottilia",
"Paula",
"Petra",
"Petronella",
"Phillipa",
"Phyllis",
"Rosina",
"Rychterin",
"Sabina",
"Sibalda",
"Sibylla",
"Sibylle",
"Sigfreda",
"Sigrid",
"Solvig",
"Stephana",
"Stephanie",
"Susanna",
"Susanne",
"Termeckerin",
"Theodora",
"Thomasina",
"Uda",
"Ulrica",
"Ursula",
"Valborga",
"Verena",
"Veronica",
"Walburga",
"Wilhelmina",
"Zymburg"
);

my @male_names = (
"Abraham",
"Achatius",
"Achaz",
"Achim",
"Adam",
"Adolf",
"Albrecht",
"Alexander",
"Alfred",
"Alphonso",
"Ambrosius",
"Andreas",
"Anthony",
"Anton",
"Appolonius",
"Arnold",
"Arthur",
"Artus",
"August",
"Augustin",
"Augustus",
"Azmus",
"Balthazar",
"Barend",
"Barholomaeus",
"Barnabas",
"Barthel",
"Bartholomous",
"Bernhard",
"Bernward",
"Berthold",
"Bonaventure",
"Boris",
"Carl",
"Casimir",
"Caspar",
"Christian",
"Christof",
"Christoph",
"Claus",
"Clovis",
"Conrad",
"Conz",
"Cornelius",
"Cosmo",
"Danel",
"David",
"De Vos",
"Derich",
"Desiderius",
"Diepolt",
"Dieric",
"Dieter",
"Dietrich",
"Dirk",
"Dominic",
"Eberhard",
"Ebolt",
"Egidius",
"Egidius",
"Eitel",
"Elias",
"Eligius",
"Endres",
"Eobanus",
"Erasmus",
"Ergot",
"Erhard",
"Erich",
"Ernst",
"Eugen",
"Eustace",
"Ewalt",
"Eytlfridrich",
"Fedor",
"Felix",
"Ferdinand",
"Ferdinand",
"Florian",
"Franz",
"Franziskus",
"Friedrich",
"Fritz",
"Gallin",
"Geiler",
"Geoffrey",
"Georg",
"Gerd",
"Gabriel",
"Gerhard",
"Gero",
"Gerrit",
"Gilbert",
"Gilg",
"Gothard",
"Gottfried",
"Gotz",
"Gregor",
"Guldemunde",
"Günter",
"Günther",
"Gustav",
"Hans",
"Hanskarl",
"Hansl",
"Hartmann",
"Hartmut",
"Heinrich",
"Heintz",
"Heinz",
"Hektor",
"Helmut",
"Helmuth",
"Hendrich",
"Hermann",
"Heyg",
"Hieronymus",
"Hoffman",
"Hubert",
"Hubertus",
"Hugo",
"Ignatius",
"Israhel",
"Jacob",
"Jakob",
"James",
"Jan",
"Janos",
"Jeckel",
"Jeremias",
"Jesse",
"Joachim",
"Jobst",
"Johann",
"Johannes",
"Joos",
"Jorg",
"Joris",
"Joseph",
"Joss",
"Jost",
"Juan",
"Julius",
"Jurgen",
"Justus",
"Karl",
"Kaspar",
"Klaus",
"Kolomon",
"Konrad",
"Konstantin",
"Kunz",
"Lanz",
"Lazarus",
"Lennard",
"Lenz",
"Leo",
"Leonard",
"Leonhard",
"Leonhart",
"Leopold",
"Lettel",
"Lienhard",
"Lienhart",
"Linhart",
"Loflinger",
"Lorenz",
"Lothar",
"Louis",
"Lucas",
"Ludwig",
"Luiz",
"Lynssl",
"Marc",
"Mang",
"Marquart",
"Martin",
"Marx",
"Maternus",
"Mathis",
"Mathius",
"Matthaus",
"Matthew",
"Maximillian",
"Melchior",
"Meyer",
"Michael",
"Moritz",
"Nicholas",
"Nicodemas",
"Nicolas",
"Niklas",
"Nils",
"Nisin",
"Oswald",
"Oswolt",
"Ottheinrich",
"Otto",
"Pankratz",
"Paul",
"Pengel",
"Pepin",
"Peter",
"Petrus",
"Phillip",
"Piotr",
"Quentin",
"Rap",
"Raymond",
"Reinhard",
"Reinhart",
"Reinprecht",
"Rickhart",
"Roger",
"Rolf",
"Rudolph",
"Ruprecht",
"Sebald",
"Sebaldus",
"Sebastian",
"Segfried",
"Severin",
"Sigismund",
"Sigmund",
"Silvester",
"Simon",
"Sixt",
"Sixten",
"Spagorl",
"Steffan",
"Stephan",
"Terrence",
"Theobald",
"Theodor",
"Theodorich",
"Thomas",
"Thorsten",
"Timotheus",
"Tobias",
"Tringen",
"Tristan",
"Ulin",
"Ulrich",
"Urbanus",
"Urs",
"Valentin",
"Valerius",
"Veit",
"Vinzenz",
"Virgil",
"Vogte",
"Volk",
"Walter",
"Weiner",
"Werner",
"Wilhelm",
"Willi",
"Wllibald",
"Wolf",
"Wolfgang",
"Zorg"
);

my @last_names = (
"Aleander",
"Altdorfer",
"Althusius",
"Amman",
"Anckenreuter",
"Asper",
"Baldung",
"Bebel",
"Beck",
"Behaim",
"Beham",
"Bosch",
"Brahe",
"Brantner",
"Braun",
"Brenz",
"Breu",
"Breugel",
"Breytenbach",
"Brunfels",
"Bucer",
"Buchner",
"Bugenhagen",
"Burckhardt",
"Burgkmair",
"Capito",
"Carlstadt",
"Cock",
"Coeck",
"Commenius",
"Cranach",
"Cuspinian",
"Denk",
"Durer",
"Ebwein",
"Eck",
"Eckhart",
"Eisner",
"Falkensteiner",
"Feyerabend",
"Franck",
"Friedank",
"Frühauf",
"Fuch",
"Fugger",
"Gassel",
"Grebel",
"Grünewald",
"Hagenberg",
"Halder",
"Hanse",
"Harsherin",
"Hechinger",
"Hetzer",
"Hofhaimer",
"Holbein",
"Hollywars",
"Jamnitzer",
"Höchstetter",
"Huber",
"Humpis",
"Hut",
"Imhot",
"Institoris",
"Kappeler",
"Keppler",
"Khlesl",
"Kinsfelt",
"Koberger",
"Kölderer",
"Krantz",
"Lang",
"Luther",
"Massmünster",
"Massys",
"May",
"Melanchthon",
"Melber",
"Müllner",
"Münzer",
"Myer",
"Nützel",
"Offenberg",
"Ortelius",
"Osiander",
"Oterle",
"Pacher",
"Peutinger",
"Pirkenheimer",
"Pirkheimer",
"Plarer",
"Pollak",
"Quadt",
"Reichlich",
"Reisch",
"Reuchlin",
"Reuss",
"Reuwich",
"Rörl",
"Sachs",
"Salesar",
"Schäufelein",
"Schenck",
"Scheurl",
"Schiering",
"Schöner",
"Schwartz",
"Schwenkfeld",
"Scwartz",
"Simons",
"Slakany",
"Spalatin",
"Sprenger",
"Schongauer",
"Springinklee",
"Stradanus",
"Sundler",
"Suso",
"Tauler",
"Teschitz",
"Teuschel",
"Thurn",
"Trautsun",
"Treitzsaur",
"Truchsess",
"Tucher",
"Valera",
"van Aelst",
"van Yfan",
"Vesalius",
"Veter",
"Volk",
"von Altenhaus",
"von Bora",
"von Burtenbach",
"von Cili",
"von Eulenhaus",
"von Frundsberg",
"von Greyssen",
"von Hohenegg",
"von Hutten",
"von Keiserberg",
"von Kinckelbach",
"von Kökeritz",
"von Miltitz",
"von Raug",
"von Rot",
"von Ruppa",
"von Sandrart",
"von Sickingen",
"von Slandersberg",
"von Trimberg",
"von Waldeck",
"Wallenstein",
"Wanner",
"Weigel",
"Wein",
"Welser",
"Weydehart",
"Weytmulner",
"Wilke",
"Winter",
"Wrangel",
"Wunderlich",
"Zainer",
"Zebitz",
"Zuberle",
"Zucker",
"Zwingli"
);

my @commands = @cleanup;
if (!defined($ARGV[0]) || $ARGV[0] ne "-c") {
	push(@commands, @setup, @blood_values, @life_values, @dept_values,
			@obituary_values);
}

foreach (@commands) {
	$dbh->do($_);
}

exit 0 if (defined($ARGV[0] && $ARGV[0] eq "-c"));

# Employee
#	(id INT NOT NULL PRIMARY KEY IDENTITY(1,1), \
#	 last_name NVARCHAR(30) NOT NULL, \
#	 first_name NVARCHAR(30) NOT NULL, \
#	 salary MONEY NOT NULL, \
#	 date_of_hire DATETIME NOT NULL, \
#	 gender BIT NOT NULL, \
#	 discontentment REAL, \
#	 blood_type INT, \
#	 lifestory INT, \
#	 department INT, \
#	 obituary INT)",

my @genders = (\@male_names, \@female_names);
for (my $i = 0; $i < 10000; ++$i) {
	my $gender = int(rand(2));
	my $first_name = $genders[$gender][int(rand($#{$genders[$gender]} + 1))];
	my $last_name = $last_names[int(rand(scalar(@last_names)))];
	my $salary = rand(500000);
	my $discontentment = rand(100);
	my $blood_type = int(rand(scalar(@blood_values))) + 1;
	my $lifestory = int(rand(scalar(@life_values))) + 1;
	my $department = int(rand(scalar(@dept_values))) + 1;
	my $obituary = int(rand(scalar(@obituary_values))) + 1;
	my $month_of_hire = int(rand(12)) + 1;
	my $day_of_hire = int(rand(28)) + 1;
	my $year_of_hire = sprintf("%02d", int(rand(100)));
	my $hiredata = "$month_of_hire/$day_of_hire/19$year_of_hire";

	my $statement = "INSERT INTO Employee VALUES ('$last_name', \
		'$first_name', '$salary', '$hiredata', $gender, \
		'$discontentment', $blood_type, $lifestory, $department, \
		$obituary)";

	$dbh->do($statement) || die "$statement";
}

exit 0;

my $sth = $dbh->prepare("SELECT * from Obituary");
if ($sth->execute) {
	while (my @row = $sth->fetchrow) {
		print "GOT A ROW:  ";
		foreach (@row) {
			print;
			print ", ";
		}
		print "\n";
	}
}
$sth->finish;

$dbh->disconnect;
