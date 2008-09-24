#!/usr/bin/perl -w
#
# Versaplex speed/bandwidth/latency benchmark.  See file README for more
# information.
#
use warnings;
use strict;

use DBI;
use Net::DBus;
use Net::DBus::Reactor;
use Time::HiRes;
use File::Temp;

use Net::DBus::Annotation qw(:call);

################################################################################
# Variables... edit these in versabench.conf.
################################################################################

# Which tests do you want to run? (1 = run, 0 = don't run)
our $test_mssql = 1;
our $test_dbus = 1;
our $test_vxodbc = 1;

# #tests to run for large and small queries
our $num_large_row_tests = 20;
our $num_small_row_tests = 500;
our $num_small_insert_tests = 200;
our $num_parallel_insert_tests = 100;

# Interfaces we use to talk to the corresponding data sources;
# to capture packets on, per-test
our $sql_if = "vmnet1";
our $dbus_if = "lo";
our $vxodbc_if = $dbus_if;

# The name of the database on your SQL server that Versaplexd talks to, and
# which we'll now let MSSQL mangle.
our $sqlserver = "testdb";
our $dbname = "testdb";

# Connection goodies per test
our $sql_port = 1433;
our $dbus_moniker = "tcp:host=127.0.0.1,port=5556";
our $vxodbc_moniker = "gzip:tcp:127.0.0.1:5555";

# Username and password to connect to the database
our $user = "sa";
our $pw = "scs";

# Tcpdump behaviour governing
# Tcpdump doesn't get the packets out of the kernel right away, you have to
# give it a second or two to get on its feet.  This governs how many seconds
# you give it before killing it to analyze its packet stream.
our $stupid_tcpdump_timeout = 2;
#1500 seems not to grab all the data.  Hmm.  We use a larger packet size for
#tcpdump captures.
our $max_tcp_packet_size = 2000;
# Want to see messages like "tcpdump starting on interface blah blah?"  Me
# neither, but just in case, set this to 1 and you will.
our $view_tcpdump_status = 0;


do 'versabench.conf'
    or die("Can't use versabench.conf: $!\n");

################################################################################
# Constants auto-generated from variables above. No touchy (unless you want to)!
################################################################################

my $tredir = "2>/dev/null";
if ($view_tcpdump_status) {
	$tredir = "2>&1";
}
my @runbools = ($test_dbus, $test_vxodbc, $test_mssql);
my @runfuncs = ();

################################################################################
# Constants not even generated, just assigned.
################################################################################

my $silly_text = "This is becoming a speech.  You are the captain sir, you are entitled.  I am not entitled to ramble on about something everyone knows.  Captain Jean-Luc Picard of the USS Enterprise.  Captain Jean-Luc Picard of the USS Enterprise. M-M-M-M Make it so.  Make it so.  M-M-M-M Make it so.  Make it so.  He just kept talking in one looooonggg... incredibly unbroken sentence moving from topic to topic very fast so that no one had a chance to interrupt it was really quite hypnotic.  Um.  I am not dressed properly.  There is this theory of the Moebius.  A... rift in the fabric of space, where time becomes a loop.  Where time becomes a loop.  Where time (time) becomes a loop.  The first guiding principle of any Starfleet officer is to the truth.  Be it personal truth, or historical truth.  If you can not find it within yourself to stand up and tell the truth, then you do not deserve to wear that uniform!!!1111  Electric Barbarella is such an awesome tune, it is stuckin my head right now and I CAN NOT GET IT OUT!";

my $tempfile = File::Temp::mktemp("vxtests.XXXX");

# Theoretically, putting this in an 'END' block means that even if DBus throws
# an error we don't handle, this file will be removed.
END { unlink $tempfile; }

my $kill_tcpdump = "kill `pgrep tcpdump`";

my $small_row_query = "select 1";
my $large_row_query = "select * from testbitch";
my $insert_query = "INSERT INTO testbitch (numcol, testcol1, testcol2, " .
		"testcol3, testcol4) VALUES (" .
		"1, 'foo foo foo fooma fooma f', 'foo foo foo fooma fooma f'," .
		" 'foo foo foo fooma fooma f', 'foo foo foo fooma fooma f')";
my @printtables = ();

################################################################################
# Utility functions
################################################################################

# Count # round trips to get data from the server.  The routine takes the
# tempfile (which is raw dumped TCP packets matching the port we're
# communicating on) and filters out all SYN, ACK, and FIN packets (those with
# no data).  Then, the first packet is the initiator of the session, and gives
# us a caller and a callee; the remaining chain of packets are all responses.
# If we see another change in caller/callee, that's a new round-trip.
sub count_roundtrips
{
	my $num = shift;
	my $data = shift;

	open my $myfh, "tcpdump -s $max_tcp_packet_size -r $tempfile '(((ip[2:2] - ((ip[0]&0xf)<<2)) - ((tcp[12]&0xf0)>>2)) != 0)' $tredir |";

	my $convs = 1;
	my $curhost = "";
	my $curdest = "";
	{
		my $line1 = <$myfh>;
		$line1 =~ m/IP ([^\s]+) > ([^\s:]+)/;
		($curhost, $curdest) = ($1, $2);
	}
	while (<$myfh>) {
		m/IP ([^\s]+) > ([^\s:]+)/;
		if ($1 ne $curdest || $2 ne $curhost) {
			$curhost = $1;
			$curdest = $2;
			++$convs;
		}
	}

	push(@{$data}, $convs / $num);
	close $myfh;
}

# Count #bytes transmitted.  Just counts the #bytes in the raw dump file.
sub count_size
{
	my $num = shift;
	my $data = shift;

	open my ($myfh), $tempfile;
	local $/;
	push (@{$data}, sprintf("%.2f", length(<$myfh>) / $num));
	close $myfh;
}

# Call count_roundtrips and count_size.
sub perform_counts
{
	my $num = shift;
	my $data = shift;

	count_size($num, $data);
	count_roundtrips($num, $data);
}

# Get the dbus port for tcpdump to monitor from the DBus moniker
sub get_dbus_port
{
	my $moniker = shift;

	if ($moniker =~ /port\s*=\s*(\d+)/) {
		return $1;
	} elsif ($moniker =~ /tcp(:[^:]+)?:(\d+)/) {
		return $2;
	}
}

# Test SQL/VxODBC via DBI.
# @param $mydbh = handle to db
# @param $query = duh
# @param $num = number of queries to perform
# @param $if = the network inteface to monitor activity on
# @param $port = the port that matters for monitoring
# @param $data = the array we attach data to
sub sql_executor
{
	my $mydbh = shift;
	my $query = shift;
	my $num = shift;
	my $if = shift;
	my $port = shift;
	my $data = shift;
	
	system("tcpdump -w $tempfile -s $max_tcp_packet_size -i $if 'tcp port $port' $tredir &");
	sleep 1;  #Need to give tcpdump a sec to start up

	my $elapsed = 0;
	my @querya = split(/ /, $query);
	if ($querya[0] eq "INSERT") {
		for (my $i = 0; $i < $num; ++$i) {
			my $t = [Time::HiRes::gettimeofday];
			my $rv = $mydbh->do($query);
			$elapsed += Time::HiRes::tv_interval($t);
		}
	} else {
		for (my $i = 0; $i < $num; ++$i) {
			my $t = [Time::HiRes::gettimeofday];
			my $sth = $mydbh->prepare($query);

			if ($sth->execute) {
				while(my @dat = $sth->fetchrow) { # do nothing
				}
			}
			$sth->finish;
			$elapsed += Time::HiRes::tv_interval($t);
		}
	}
	push(@{$data}, sprintf("%.7f", $elapsed / $num));
	sleep $stupid_tcpdump_timeout;
	system($kill_tcpdump);
	if ($querya[0] ne "INSERT") {
		perform_counts($num, $data);
	}
}

# Test DBus.
# @param $dbus_handle = Handle to a DBus object to execute ExecChunkRecordset.
# @param $query = duh
# @param $num = number of queries to perform
# @param $data = the array we attach data to
sub dbus_executor
{
	my $dbus_handle = shift;
	my $query = shift;
	my $num = shift;
	my $data = shift;

	my $elapsed;
	for (my $i = 0; $i < $num; ++$i) {
		my $t = [Time::HiRes::gettimeofday];
		my $response = $dbus_handle->ExecChunkRecordset($query);
		$elapsed += Time::HiRes::tv_interval($t);
	}
	push(@{$data}, sprintf("%.7f", $elapsed / $num));
	#print "DBUS: for statement: $query, time elapsed is: $elapsed\n";
}

################################################################################
# Initial set-up
################################################################################

#It sucks, but we have to connect to *something* right now, to input the
#initial testing data
my $dbh = DBI->connect("DBI:Sybase:server=$sqlserver;database=$dbname",
                         $user, $pw, {PrintError => 0}) 
    or die "Unable to connect to SQL server ($sqlserver, db=$dbname)";

# Create initial testing data
$dbh->do("DROP TABLE testbitch");
$dbh->do("CREATE TABLE testbitch (numcol int, testcol1 TEXT NULL, testcol2 TEXT NULL, testcol3 TEXT NULL, testcol4 TEXT NULL, testcol5 TEXT NULL, testcol6 TEXT NULL, testcol7 TEXT NULL, testcol8 TEXT NULL, testcol9 TEXT NULL, testcol10 TEXT NULL)");

my $large_datasize = 0;
for (my $i = 0; $i < 105; ++$i) {
	$dbh->do("INSERT INTO testbitch (numcol) values ($i)");
	for (my $j = 1; $j <= 10; ++$j) {
		$dbh->do("UPDATE testbitch SET testcol$j = '$silly_text' where NUMCOL = $i");
		$large_datasize += length($silly_text) + 4;
	}
}
# Warm up DB-cache
for (my $i = 0; $i < 10; ++$i) {
	my $sth = $dbh->prepare("select * from testbitch limit 5");

	if ($sth->execute) {
		while(my @dat = $sth->fetchrow) { # do nothing
		}
	}
	$sth->finish;
}

################################################################################
# Perl-DBus tests start
################################################################################

sub test_dbus
{
	my @dbus_data = ("Perl-DBus");
	push(@printtables, \@dbus_data);

	my $dbus_port = get_dbus_port($dbus_moniker);
	$ENV{DBUS_SESSION_BUS_ADDRESS} = $dbus_moniker;

	my $bus = Net::DBus->session;
	my $versaplex = $bus->get_service("vx.versaplexd");
	my $db = $versaplex->get_object("/db", "vx.db");

	$db->connect_to_signal("ChunkRecordsetSig", sub {
				#Do nothing, just prove that we could
				my ($colinfo, $data, $nullity, $reply) = @_;
				});

	my $reactor = Net::DBus::Reactor->main();

	my $tcpdump_dbus = "tcpdump -w $tempfile -s $max_tcp_packet_size " .
			"-i $dbus_if 'tcp port $dbus_port' $tredir &";

	system($tcpdump_dbus);
	sleep 1;  #Give tcpdump a chance to get on its feet

	my $runtest = 1;
	my $del;
	my $big = $reactor->add_timeout(500, Net::DBus::Callback->new(
		method => sub {
		if ($runtest != 1) {
			return;
		}
		$runtest = 2;
		dbus_executor($db, $large_row_query, $num_large_row_tests,
				\@dbus_data);
		$reactor->toggle_timeout($del, 1);
	}));

	my $small = $reactor->add_timeout(500, Net::DBus::Callback->new(
		method => sub {
		if ($runtest != 2) {
			return;
		}
		$runtest = 3;
		dbus_executor($db, $small_row_query, $num_small_row_tests,
				\@dbus_data);
		$reactor->toggle_timeout($del, 1);
	}));
	$reactor->toggle_timeout($small, 0);

	my $runtimes = 0;
	$del = $reactor->add_timeout($stupid_tcpdump_timeout * 1000 + 500,
		Net::DBus::Callback->new(method => sub {
		system($kill_tcpdump);
		my $num;
		if (++$runtimes == 1) {
			$reactor->remove_timeout($big);
			$reactor->toggle_timeout($small, 1);
			$num = $num_large_row_tests;
		} else {
			$reactor->remove_timeout($small);
			$num = $num_small_row_tests;
		}
		perform_counts($num, \@dbus_data);
		if ($runtimes == 1) {
			pop(@dbus_data);  # Remove # round-trips, nobody cares?
			system($tcpdump_dbus);
			sleep 1;
		} elsif ($runtimes == 2) {
			$reactor->shutdown;
		}
	}));
	$reactor->toggle_timeout($del, 0);

	$reactor->run;

	#For the insert, *I* know that in the background, we're just going to
	#call ExecRecordset instead of ExecChunkrecordset.  As such, no need for
	#the DBus::reactor and such, no more signals to check for
	dbus_executor($db, $insert_query, $num_small_insert_tests, \@dbus_data);

	# We certainly want to find a way to test this with VxODBC, but really
	# can't for now.  Pipelined inserts; note the dbus_call_noreply, which
	# means we don't care and don't listen for responses, just keep firing.
	my $elapsed = 0;
	for (my $i = 0; $i < $num_parallel_insert_tests; ++$i) {
		my $t = [Time::HiRes::gettimeofday];
		$db->ExecChunkRecordset(dbus_call_noreply, $insert_query);
		$elapsed += Time::HiRes::tv_interval($t);
	}
	push(@dbus_data,sprintf("%.7f", $elapsed / $num_parallel_insert_tests));
}

push(@runfuncs, ["Perl-DBus", \&test_dbus]);

################################################################################
# VxODBC tests start
################################################################################

sub test_vxodbc
{
	my $vxodbc_port = get_dbus_port($vxodbc_moniker);
	$ENV{DBUS_SESSION_BUS_ADDRESS} = $vxodbc_moniker;

	my @vx_data = ("VxODBC\t");
	push(@printtables, \@vx_data);
	
	my $dbh_vx = DBI->connect("DBI:ODBC:testdb", $user, $pw, {PrintError => 1}) || die "Unable to connect to SQL server";

	sql_executor($dbh_vx, $large_row_query, $num_large_row_tests,
			$vxodbc_if, $vxodbc_port, \@vx_data);
	pop(@vx_data);  # Remove round-trips, nobody is interested?
	sql_executor($dbh_vx, $small_row_query, $num_small_row_tests,
			$vxodbc_if, $vxodbc_port, \@vx_data);
	sql_executor($dbh_vx, $insert_query, $num_small_insert_tests,
			$vxodbc_if, $vxodbc_port, \@vx_data);

	push(@vx_data, "N/A");
}

push(@runfuncs, ["VxODBC", \&test_vxodbc]);

################################################################################
# MSSQL tests start
################################################################################

sub test_mssql
{
	my @ms_data = ("Native MS SQL");
	unshift(@printtables, \@ms_data);

	sql_executor($dbh, $large_row_query, $num_large_row_tests,
			$sql_if, $sql_port, \@ms_data);
	pop(@ms_data);  # Remove round-trips, nobody is interested?
	sql_executor($dbh, $small_row_query, $num_small_row_tests,
			$sql_if, $sql_port, \@ms_data);
	sql_executor($dbh, $insert_query, $num_small_insert_tests,
			$sql_if, $sql_port, \@ms_data);

	push(@ms_data, "N/A");
}

push(@runfuncs, ["Native MS SQL", \&test_mssql]);

################################################################################
# Run this thing
################################################################################

for (my $i = 0; $i < scalar(@runfuncs); ++$i) {
	next if (!$runbools[$i]);

	print "RUNNING ", $runfuncs[$i][0], " tests...\n";
	&{$runfuncs[$i][1]};
}
		

################################################################################
# Clean-up
################################################################################

$dbh->do("DROP TABLE testbitch");

$dbh->disconnect;

################################################################################
# Generate table data
################################################################################

my @rowtitles = ("\t\t\t\t|",
		"Multirow SELECT time\t\t|", "(secs/request)\t\t\t|",
		"Multirow SELECT bandwidth\t|", "(bytes/request)\t\t|",
		"Tiny SELECT time\t\t|", "(secs/request)\t\t\t|",
		"Tiny SELECT bandwidth\t\t|", "(bytes/request)\t\t|",
		"Tiny SELECT round-trips\t|", "(trips/request)\t\t|",
		"Single-row INSERT speed\t|", "(secs/request)\t\t\t|",
		"Pipelined INSERT speed\t\t|", "(secs/request)\t\t\t|"
		);

print "Ah, the part you've been waiting for... table data!\n";
print "Results are compiled from averaging capture data over:\n";
print " - $num_large_row_tests large multi-row request(s) (",
	"~$large_datasize bytes/request)\n";
print " - $num_small_row_tests small 4-byte request(s)\n";
print " - $num_small_insert_tests 104-byte row insertion(s)\n";
print " - $num_parallel_insert_tests simultaneous 104-byte row insertion(s)\n";

print "\n";

sub print_dashline
{
	my $endpoint = 81 - (3 - scalar(@printtables)) * 16;
	$endpoint = 80 if ($endpoint == 81);
	print "-" x $endpoint;
}

print_dashline;
print "\n|";
print (shift @rowtitles);
foreach (@printtables) {
	print shift(@{$_}), "\t|";
}
print "\n";

print_dashline;
print "\n";

for (my $i = 0; $i < scalar(@rowtitles); ++$i) {
	print "|", $rowtitles[$i];
	if ($i % 2) {
		foreach (@printtables) {
			print "\t\t|";
		}
		print "\n";
		print_dashline;
	} else {
		foreach (@printtables) {
			my $p = $_->[$i / 2];
			print $p, "\t";
			if (length($p) <= 6) {
				print "\t";
			}
			print "|";
		}
	}
	print "\n";
}
