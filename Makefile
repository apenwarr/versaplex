include rules.mk
include monorules.mk

all: wv.dll streamtest servtest httpservtest

PKGS += /r:System.Data /r:System.Web

servtest.exe: PKGS += /r:System.ServiceProcess /r:System.Configuration.Install

streamtest.exe servtest.exe httpservtest.exe: wv.dll

wv.dll: assemblyinfo.cs \
	wvutils.cs wvtest.cs wvweb.cs wvdbi.cs wvini.cs \
	wveventer.cs wvbuf.cs wvstream.cs wvlog.cs wvhexdump.cs \
	wvextensions.cs wvhttpserver.cs \
	ndesk-options.cs

test: all t/test

clean:: t/clean
	rm -f *.pass servtest streamtest httpservtest
