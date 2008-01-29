include ../rules.mk
include ../monorules.mk

all: wv.dll

PKGS += /r:System.Data /r:System.Web

wv.dll: wvutils.cs wvtest.cs wvweb.cs wvdbi.cs wvini.cs assemblyinfo.cs

wvtest.t.exe: wvtest.t.cs.E wv.dll

wvutils.t.exe: wvutils.t.cs.E wv.dll

test: wvtest.t.pass wvutils.t.pass

clean::
	rm -f *.pass
