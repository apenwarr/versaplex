WVDOTNET=.
include rules.mk
include monorules.mk

all: wv.dll 

tests: all streamtest servtest httpservtest htmlgentest t/tests

PKGS += /r:System.Data /r:System.Web /r:Mono.Data.SqliteClient

servtest.exe: PKGS += /r:System.ServiceProcess /r:System.Configuration.Install

streamtest.exe servtest.exe httpservtest.exe htmlgentest.exe: wv.dll

wv.dll: assemblyinfo.cs \
	wvutils.cs wvtest.cs wvdata.cs wvmoniker.cs wvurl.cs \
	wvdbi.cs wvini.cs \
	wveventer.cs wvbuf.cs \
	wvstream.cs wvstreamstream.cs wvsockstream.cs \
	wvloopback.cs wvlog.cs wvhexdump.cs \
	wvextensions.cs wvweb.cs wvhtml.cs wvhttpserver.cs \
	wvcsv.cs \
	ndesk-options.cs mono-getline.cs

test: tests t/test

clean:: t/clean
	rm -f *.pass servtest streamtest httpservtest htmlgentest
