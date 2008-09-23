WVDOTNET=../wvdotnet
include ../rules.mk
include ../monorules.mk

all: versaplexd sm vxcli

PKGS += /unsafe /r:System.Data

test: all t/test
	
tests: all t/all

t/all: all

versaplexd.exe: versaplexd.cs \
    dodgytransport.cs ischemabackend.cs PriorityQueue.cs \
    vxapi.cs vxbufferstream.cs \
    vxcolumninfo.cs vxdbus.cs vxdbusdatetime.cs vxdbschema.cs \
    vxevent.cs vxeventloop.cs vxexceptions.cs vxnotifysocket.cs \
    vxschema.cs vxschemachecksums.cs vxschemadiff.cs vxschemaerrors.cs \
    vxsocket.cs vxsqlpool.cs wvdbusserver.cs \
    ../dbus-sharp/src/NDesk.DBus.dll \
    ../wvdotnet/wv.dll

sm.exe: \
    dodgytransport.cs ischemabackend.cs vxdbusutils.cs \
    vxexceptions.cs vxnotifysocket.cs  vxsocket.cs \
    vxdiskschema.cs vxdbschema.cs vxdbusschema.cs vxschema.cs \
    vxschemachecksums.cs vxschemadiff.cs vxschemaerrors.cs \
    ../dbus-sharp/src/NDesk.DBus.dll \
    ../wvdotnet/wv.dll
    
vxcli.exe: vxcli.cs \
    dodgytransport.cs vxsocket.cs vxnotifysocket.cs vxdbusutils.cs \
    vxexceptions.cs \
    ../dbus-sharp/src/NDesk.DBus.dll \
    ../wvdotnet/wv.dll

clean:: t/clean
	rm -f versaplexd
