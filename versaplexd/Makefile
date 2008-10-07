WVDOTNET=../wvdotnet
include ../rules.mk
include ../monorules.mk

all: versaplexd sm vxcli

PKGS += /unsafe /r:System.Data

test: all t/test
	
tests: all t/all

t/all: all

versaplexd.exe: versaplexd.cs \
    ischemabackend.cs \
    vxapi.cs \
    vxcolumninfo.cs vxdbusdatetime.cs vxdbschema.cs \
    vxexceptions.cs \
    vxschema.cs vxschemachecksums.cs vxschemadiff.cs vxschemaerrors.cs \
    vxsqlpool.cs wvdbusserver.cs \
    ../dbus-sharp/wvdbus-sharp.dll \
    ../wvdotnet/wv.dll

sm.exe: \
    ischemabackend.cs storedprocedure.cs \
    vxexceptions.cs \
    vxdiskschema.cs vxdbschema.cs vxdbusschema.cs vxschema.cs \
    vxschemachecksums.cs vxschemadiff.cs vxschemaerrors.cs \
    ../dbus-sharp/wvdbus-sharp.dll \
    ../wvdotnet/wv.dll
    
vxcli.exe: vxcli.cs \
    vxexceptions.cs \
    ../dbus-sharp/wvdbus-sharp.dll \
    ../wvdotnet/wv.dll

clean:: t/clean
	rm -f versaplexd
