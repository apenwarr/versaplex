include ../../rules.mk
include ../../monorules.mk

BINS = export.exe

# FIXME: Move dbuserror.cs from ../t/ to ../
SCHEMAMATIC_SOURCES = \
    ../dodgytransport.cs ../ischemabackend.cs ../vxdbusutils.cs \
    ../t/dbuserror.cs ../vxnotifysocket.cs  ../vxsocket.cs \
    ../vxdiskschema.cs ../vxdbusschema.cs ../vxschema.cs \
    ../vxschemachecksums.cs ../vxschemadiff.cs ../vxschemaerrors.cs

LIBS = ../wv.dll ../NDesk.DBus.dll

all: $(BINS)

export.exe: $(SCHEMAMATIC_SOURCES) $(LIBS)

clean:: 
	rm -f $(BINS)
