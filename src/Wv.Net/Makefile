.PHONY: all clean test

DLL = wv.dll

all: $(DLL)

SHELL=/bin/bash
# cc -E tries to guess by extension what to do with the file.
# And it does other weird things. cpp seems to Just Work(tm), so use that for
# our C# (.cs) files
CSCPP=cpp
CPPFLAGS=

ifeq ($(OS),Windows_NT)
CSC?=csc
PKGS=
else
CSC?=gmcs
PKGS=/r:System.Data /r:System.Web
endif

CSFLAGS=/warn:4 /debug
#CSFLAGS += /warnaserror

LIBFILES = \
	wvutils.cs wvtest.cs wvweb.cs wvdbi.cs wvini.cs assemblyinfo.cs

LIBTESTFILES = \
	wvtest.t.cs.E wvutils.t.cs.E \

TESTS = $(patsubst %.cs,%.exe,$(patsubst %.E,%,$(LIBTESTFILES)))

FILES = $(LIBFILES) $(LIBTESTFILES)

test: $(DLL) $(TESTS) $(addsuffix .pass,$(TESTS))

$(addsuffix .pass,$(TESTS)): %.pass: %
	./$<
	touch $@

$(TESTS): %.exe: %.cs.E $(DLL)
	$(CSC) $(PKGS) $(CSFLAGS) /r:wv.dll /target:exe /out:$@ $(filter %.cs.E %.cs,$^)

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
	set -o pipefail; $(CSCPP) $(CPPFLAGS) -C -dI $< \
		| expand -8 \
		| sed -e 's,^#include,//#include,' \
		| grep -v '^# [0-9]' \
		>$@ || (rm -f $@ && exit 1)

$(DLL): $(LIBFILES)
	$(CSC) $(PKGS) $(CSFLAGS) /target:library /out:$@ $(filter %.cs.E %.cs,$^)

clean:
	rm -f *~ *.E *.d *.exe *.dll TestResult.xml *.mdb *.pdb *.pass
