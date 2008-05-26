include ../../monorules.mk

CPPFLAGS += -I../../wvdotnet
PKGS += /r:System.Data

all: test

test: versaplex.t.pass vxsanity.t.pass verifydata.t.pass schemamatic.t.pass

versaplex.t.exe: versaplex.t.cs.E versaplextester.cs.E dbuserror.cs \
	../dodgytransport.cs ../vxnotifysocket.cs ../vxsocket.cs \
	../vxcolumninfo.cs ../vxdbusdatetime.cs \
	../wv.dll ../NDesk.DBus.dll

vxsanity.t.exe: vxsanity.t.cs.E versaplextester.cs.E dbuserror.cs \
	../dodgytransport.cs ../vxnotifysocket.cs ../vxsocket.cs \
	../vxcolumninfo.cs ../vxdbusdatetime.cs \
	../wv.dll ../NDesk.DBus.dll

verifydata.t.exe: verifydata.t.cs.E versaplextester.cs.E dbuserror.cs \
	../dodgytransport.cs ../vxnotifysocket.cs ../vxsocket.cs \
	../vxcolumninfo.cs ../vxdbusdatetime.cs \
	../wv.dll ../NDesk.DBus.dll

schemamatic.t.exe: schemamatic.t.cs.E versaplextester.cs.E dbuserror.cs \
	../dodgytransport.cs ../vxnotifysocket.cs ../vxsocket.cs \
	../vxcolumninfo.cs ../vxdbusdatetime.cs ../vxschemachecksums.cs \
	../vxschema.cs \
	../wv.dll ../NDesk.DBus.dll

clean::
	rm -f *.pass
