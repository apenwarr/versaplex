.PHONY: all clean test

DLL = versaplex.test.dll

all: $(DLL)

SHELL=/bin/bash
WVPATH=../../Wv.Net
DBUSPATH=../../../ThirdParty/dbus-sharp/src
# cc -E tries to guess by extension what to do with the file.
# And it does other weird things. cpp seems to Just Work(tm), so use that for
# our C# (.cs) files
CSCPP=cpp
CPPFLAGS=-I$(WVPATH)

ifeq ($(OS),Windows_NT)
CSC?=csc
PKGS=/r:nunit.framework.dll /r:wv.dll /r:NDesk.DBus.dll
NUNIT=nunit.framework.dll
NUNITPATH=/cygdrive/c/Program\ Files/NUnit\ 2.4.1/bin

nunit.framework.dll: %.dll: $(NUNITPATH)/%.dll
	rm -f "$@"
	cp "$<" "$@"

wv.dll: %.dll: $(WVPATH)/%.dll
	rm -f "$@"
	cp "$<" "$@"

NDesk.DBus.dll: %.dll: $(DBUSPATH)/%.dll
	rm -f "$@"
	cp "$<" "$@"
else
CSC?=/usr/local/mono-svn/bin/gmcs
NUNIT=
PKGS=/r:System.Data /r:System.Web /r:wv /pkg:nunit /r:NDesk.DBus.dll /r:Mono.Posix

wv.dll: %.dll: $(WVPATH)/%.dll
	rm -f "$@"
	ln -s "$<" "$@"
	test -f "$@"

NDesk.DBus.dll: %.dll: $(DBUSPATH)/%.dll
	rm -f "$@"
	ln -s "$<" "$@"
	test -f "$@"
endif

CSFLAGS=/warn:4 /debug
#CSFLAGS += /warnaserror

FILES = versaplex.test.cs.E

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

$(DLL): $(FILES) wv.dll NDesk.DBus.dll $(NUNIT)
	$(CSC) $(PKGS) $(CSFLAGS) /target:library /out:$@ $(filter %.cs.E %.cs,$^)

clean:
	rm -f *~ *.E *.d *.exe *.dll TestResult.xml *.mdb *.pdb
