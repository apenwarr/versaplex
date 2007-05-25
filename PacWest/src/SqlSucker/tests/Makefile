.PHONY: all clean test

DLL = sqlsucker.test.dll

all: $(DLL)

SHELL=/bin/bash
WVPATH=../../Wv.Net
# cc -E tries to guess by extension what to do with the file.
# And it does other weird things. cpp seems to Just Work(tm), so use that for
# our C# (.cs) files
CSCPP=cpp
CPPFLAGS=-I$(WVPATH)

CSFLAGS=-warn:4 -debug
#CSFLAGS += -warnaserror
PKGS=-r:System.Data -pkg:nunit -r:wv

FILES = sqlsucker.test.cs.E

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

$(DLL): $(FILES) wv.dll
	gmcs $(PKGS) $(CSFLAGS) -target:library -out:$@ $(filter %.cs.E %.cs,$^)

wv.dll:
	rm -f $@
	ln -s $(WVPATH)/wv.dll wv.dll
	test -f wv.dll

clean:
	rm -f *~ *.E *.d *.exe *.dll TestResult.xml *.mdb
