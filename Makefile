include rules.mk
include monorules.mk

all: wv.dll 

tests: all streamtest servtest httpservtest htmlgentest

PKGS += /r:System.Data /r:System.Web

servtest.exe: PKGS += /r:System.ServiceProcess /r:System.Configuration.Install

streamtest.exe servtest.exe httpservtest.exe htmlgentest.exe: wv.dll

wv.dll: assemblyinfo.cs \
	wvutils.cs wvtest.cs wvdata.cs wvdbi.cs wvini.cs \
	wveventer.cs wvbuf.cs wvstream.cs wvlog.cs wvhexdump.cs \
	wvextensions.cs wvweb.cs wvhtml.cs wvhttpserver.cs \
	ndesk-options.cs

test: tests t/test

clean:: t/clean
	rm -f *.pass servtest streamtest httpservtest htmlgentest
