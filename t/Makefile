include ../monorules.mk

all: test

PKGS += /r:System.Data /r:System.Web
CPPFLAGS += -I..

wvtest.t.exe: wvtest.t.cs.E ../wv.dll

wvutils.t.exe: wvutils.t.cs.E ../wv.dll

wvhttpserver.t.exe: wvhttpserver.t.cs.E ../wv.dll

test: wvtest.t.pass wvutils.t.pass wvhttpserver.t.pass

clean::
	rm -f *.pass
