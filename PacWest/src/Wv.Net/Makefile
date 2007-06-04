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
PKGS=/r:nunit.framework.dll
NUNIT=nunit.framework.dll
NUNITPATH=/cygdrive/c/Program\ Files/NUnit\ 2.4.1/bin

nunit.framework.dll: %.dll: $(NUNITPATH)/%.dll
	rm -f "$@"
	cp "$<" "$@"
else
CSC?=gmcs
NUNIT=
PKGS=/r:System.Data /r:System.Web /pkg:nunit
endif

CSFLAGS=/warn:4 /debug
#CSFLAGS += /warnaserror

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
	set -o pipefail; $(CSCPP) $(CPPFLAGS) -C -dI $< \
		| expand -8 \
		| sed -e 's,^#include,//#include,' \
		| grep -v '^# [0-9]' \
		>$@ || (rm -f $@ && exit 1)

$(DLL): $(FILES) $(NUNIT)
	$(CSC) $(PKGS) $(CSFLAGS) /target:library /out:$@ $(filter %.cs.E %.cs,$^)

clean:
	rm -f *~ *.E *.d *.exe *.dll TestResult.xml *.mdb *.pdb
