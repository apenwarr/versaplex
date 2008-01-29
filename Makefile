include ../monorules.mk

all: wv.dll

PKGS=/r:System.Data /r:System.Web

wv.dll: wvutils.cs wvtest.cs wvweb.cs wvdbi.cs wvini.cs assemblyinfo.cs

LIBTESTFILES = wvtest.t.cs.E wvutils.t.cs.E

TESTS = $(patsubst %.cs.E,%.exe,$(LIBTESTFILES))

test: wv.dll $(TESTS) $(addsuffix .pass,$(TESTS))

$(addsuffix .pass,$(TESTS)): %.pass: %
	rm -f $@
	./$<
	touch $@

$(TESTS): wv.dll

clean::
	rm -f *.pass
