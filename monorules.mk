.PHONY: all clean test

default: all

SHELL=/bin/bash

# cc -E tries to guess by extension what to do with the file.
# And it does other weird things. cpp seems to Just Work(tm), so use that for
# our C# (.cs) files
CSCPP=cpp

PKGS=/r:System.Data

# Cygwin supports symlinks, but they aren't actually useful outside cygwin,
# so let's just copy instead.  We also use Microsoft's .net compiler instead
# of mono.
ifeq ($(OS),Windows_NT)
  CSC?=csc
  SYMLINK=cp
else
  CSC?=gmcs
  SYMLINK=ln -s
  PKGS += /r:Mono.Posix
endif

CSFLAGS=/warn:4 /debug
#CSFLAGS += /warnaserror

test: all
	$(MAKE) -C t $@

# Rules for generating autodependencies on header files
$(patsubst %.cs.E,%.d,$(filter %.cs.E,$(FILES))): %.d: %.cs
	@echo Generating dependency file $@ for $<
	@set -e; set -o pipefail; rm -f $@; (\
	    ($(CSCPP) -M -MM -MQ '$@' $(CPPFLAGS) $< && echo Makefile) \
		| paste -s -d ' ' - && \
	    $(CSCPP) -M -MM -MQ '$<'.E $(CPPFLAGS) $< \
	) > $@ \
	|| (rm -f $@ && echo "Error generating dependency file." && exit 1)

include $(patsubst %.cs.E,%.d,$(filter %.cs.E,$(FILES)))

# Rule for actually preprocessing source files with headers
%.cs.E: %.cs
	@rm -f $@
	set -o pipefail; $(CSCPP) $(CPPFLAGS) -C -dI $< \
		| expand -8 \
		| sed -e 's,^#include,//#include,' \
		| grep -v '^# [0-9]' \
		>$@ || (rm -f $@ && exit 1)

%.dll: assemblyinfo.cs
	$(CSC) $(CSFLAGS) /target:library /out:$@ \
		$(PKGS) \
		$(filter %.cs.E %.cs,$^) \
		$(patsubst %.dll,/r:%.dll,$(filter %.dll,$^))

%.exe: %.cs
	for d in $(filter ../%.dll,$^); do \
		rm -f $$(basename $$d); \
		$(SYMLINK) -v $$d .; \
	done
	$(CSC) $(CSFLAGS) /target:exe /out:$@ \
		$(PKGS) \
		$(filter %.cs.E %.cs,$^) \
		$(patsubst %.dll,/r:%.dll,$(filter %.dll,$^))

%: %.exe
	rm -f $@
	$(SYMLINK) $< $@

clean::
	rm -f *~ *.E *.d *.exe *.dll *.mdb *.pdb
