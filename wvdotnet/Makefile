include ../rules.mk
include ../monorules.mk

streams: FORCE

all: wv.dll streams

PKGS += /r:System.Data /r:System.Web

wv.dll: wvutils.cs wvtest.cs wvweb.cs wvdbi.cs wvini.cs assemblyinfo.cs

test: t/test

clean::
	rm -f *.pass
