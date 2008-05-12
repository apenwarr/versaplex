include ../../monorules.mk

CPPFLAGS += -I../../wvdotnet
PKGS += /r:System.Data

all: test

test: versaplex.t.pass

$(addsuffix .pass,$(TESTS)): %.pass: %
	./$<
	touch $@
	
versaplex.t.exe: versaplex.t.cs.E dbuserror.cs.E \
	../dodgytransport.cs ../vxnotifysocket.cs ../vxsocket.cs \
	../vxcolumninfo.cs ../vxdbusdatetime.cs \
	../wv.dll ../NDesk.DBus.dll
