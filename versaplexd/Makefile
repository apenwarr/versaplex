.PHONY: all clean test

EXE = versaplex.exe

all: $(EXE)

SHELL=/bin/bash
WVPATH=../wvdotnet
DBUSPATH=../dbus-sharp/src
# cc -E tries to guess by extension what to do with the file.
# And it does other weird things. cpp seems to Just Work(tm), so use that for
# our C# (.cs) files
CSCPP=cpp
CPPFLAGS=-I$(WVPATH)

ifeq ($(OS),Windows_NT)
CSC?=csc
PKGS=/r:NDesk.DBus.dll

NDesk.DBus.dll: %.dll: $(DBUSPATH)/%.dll
	rm -f "$@"
	cp "$<" "$@"

# Not needed at this time
#PKGS=/r:nunit.framework.dll /r:wv.dll
#NUNIT=nunit.framework.dll
#NUNITPATH=/cygdrive/c/Program\ Files/NUnit\ 2.4.1/bin
#
#nunit.framework.dll: %.dll: $(NUNITPATH)/%.dll
#	rm -f "$@"
#	cp "$<" "$@"
#
#wv.dll: %.dll: $(WVPATH)/%.dll
#	rm -f "$@"
#	cp "$<" "$@"
else
CSC?=gmcs
PKGS=/r:System.Data /r:NDesk.DBus.dll /r:Mono.Posix

NDesk.DBus.dll: %.dll: $(DBUSPATH)/%.dll
	rm -f "$@"
	ln -s "$<" "$@"
	test -f "$@"

# Not needed at this time
#NUNIT=
#PKGS=/r:System.Data /r:System.Web /r:wv /pkg:nunit
#
#wv.dll: %.dll: $(WVPATH)/%.dll
#	rm -f "$@"
#	ln -s "$<" "$@"
#	test -f "$@"
endif

CSFLAGS=/warn:4 /debug
#CSFLAGS += /warnaserror

FILES = PriorityQueue.cs VxBufferStream.cs VxDbApi.cs VxDbus.cs VxEvent.cs \
    VxEventLoop.cs VxNotifySocket.cs VxSocket.cs \
    VxSqlPool.cs Main.cs

#test: $(DLL)
#	nunit-console /nologo /labels $^

test: all
	$(MAKE) -C t $@

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

$(EXE): $(FILES) NDesk.DBus.dll
	$(CSC) $(PKGS) $(CSFLAGS) /target:exe /out:$@ $(filter %.cs.E %.cs,$^)

clean:
	rm -f *~ *.E *.d *.exe *.dll TestResult.xml *.mdb *.pdb
