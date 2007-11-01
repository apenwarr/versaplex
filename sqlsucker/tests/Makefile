.PHONY: all clean test

all: test

SHELL=/bin/bash
WVPATH=../../Wv.Net
# cc -E tries to guess by extension what to do with the file.
# And it does other weird things. cpp seems to Just Work(tm), so use that for
# our C# (.cs) files
CSCPP=cpp
CPPFLAGS=-I$(WVPATH)

ifeq ($(OS),Windows_NT)
CSC?=csc
PKGS=/r:wv.dll

wv.dll: %.dll: $(WVPATH)/%.dll
	rm -f "$@"
	cp "$<" "$@"
else
CSC?=gmcs
PKGS=/r:System.Data /r:System.Web /r:wv

wv.dll: %.dll: $(WVPATH)/%.dll
	rm -f "$@"
	ln -s "$<" "$@"
	test -f "$@"
endif

CSFLAGS=/warn:4 /debug
#CSFLAGS += /warnaserror

TESTS=sqlsucker.test.exe

test: wv.dll $(TESTS) $(addsuffix .pass,$(TESTS))

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

clean:
	rm -f *~ *.E *.d *.exe *.dll TestResult.xml *.mdb *.pdb
