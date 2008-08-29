include ../../rules.mk
include ../../monorules.mk

BINS = export.exe apply.exe getdata.exe

PKGS += /r:System.Data

SCHEMAMATIC_SOURCES = \
    ../dodgytransport.cs ../ischemabackend.cs ../vxdbusutils.cs \
    ../vxexceptions.cs ../vxnotifysocket.cs  ../vxsocket.cs \
    ../vxdiskschema.cs ../vxdbusschema.cs ../vxschema.cs \
    ../vxschemachecksums.cs ../vxschemadiff.cs ../vxschemaerrors.cs

LIBS = ../wv.dll ../NDesk.DBus.dll

all: $(BINS)

export.exe: $(SCHEMAMATIC_SOURCES) $(LIBS)
apply.exe: $(SCHEMAMATIC_SOURCES) $(LIBS)
getdata.exe: $(SCHEMAMATIC_SOURCES) $(LIBS)

clean:: 
	rm -f $(BINS)
