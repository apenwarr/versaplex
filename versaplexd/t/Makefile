include ../../monorules.mk

CPPFLAGS += -I../../wvdotnet
PKGS += /r:System.Data

all: test

test: versaplex.t.pass

$(addsuffix .pass,$(TESTS)): %.pass: %
	./$<
	touch $@
	
versaplex.t.exe: versaplex.t.cs.E VxDbInterface.cs.E \
	../wv.dll ../NDesk.DBus.dll
