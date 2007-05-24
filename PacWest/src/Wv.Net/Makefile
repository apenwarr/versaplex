.PHONY: all clean test

DLL = wv.dll

all: $(DLL)

SHELL=/bin/bash
CPP=cc -E
# cc -E tries to guess by extension what to do with the file.
# Override that for C# (.cs files).
CSCPP=$(CPP) -x c
CPPFLAGS=

CSFLAGS=-warn:4 -debug
#CSFLAGS += -warnaserror
PKGS=-r:System.Data -r:System.Web -pkg:nunit 

LIBFILES = \
	wvutils.cs wvtest.cs wvweb.cs wvdbi.cs wvini.cs assemblyinfo.cs

LIBTESTFILES = \
	wvtest.t.cs.E wvutils.t.cs.E \

FILES = $(LIBFILES) $(LIBTESTFILES)

test: $(DLL)
	nunit-console /nologo /labels $^

$(patsubst %.cs.E,%.d,$(filter %.cs.E,$(FILES))): %.d: %.cs
	@echo Generating dependency file $@ for $<
	@set -e; set -o pipefail; rm -f $@; (\
	    ($(CSCPP) -M -MM -MQ '$@' $(CPPFLAGS) $< && echo Makefile) \
		| paste -s -d ' ' - && \
	    $(CSCPP) -M -MM -MQ '$<'.E $(CPPFLAGS) $< \
	) > $@ \
	|| (rm -f $@ && echo "Error generating dependency file." && exit 1)

include $(patsubst %.cs.E,%.d,$(filter %.cs.E,$(FILES)))

%.cs.E: %.cs
	@rm -f $@
	set -o pipefail; $(CSCPP) $(CPPFLAGS) -C -P -dI $< \
		| expand -8 \
		| sed -e 's,^#include,//#include,' \
		>$@ || (rm -f $@ && exit 1)

$(DLL): $(FILES)
	gmcs $(PKGS) $(CSFLAGS) -target:library -out:$@ $(filter %.cs.E %.cs,$^)

clean:
	rm -f *~ *.E *.d *.exe *.dll TestResult.xml *.mdb
