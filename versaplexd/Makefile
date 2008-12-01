WVDOTNET=../wvdotnet
include ../rules.mk
include ../monorules.mk

all: versaplexd versaplexd-svc sm vxcli

PKGS += /r:System.Data

test: all t/test
	
tests: all t/all

t/all: all

versaplexd.exe versaplexd-svc.exe: versaplexd.cs \
    ischemabackend.cs \
    vxapi.cs vxsqltokenizer.cs \
    vxcolumninfo.cs vxdbusdatetime.cs vxdbschema.cs \
    vxexceptions.cs \
    vxschema.cs vxschemachecksums.cs vxschemadiff.cs vxschemaerrors.cs \
    vxsqlpool.cs wvdbusserver.cs \
    ../wvdbus-sharp/wvdbus-sharp.dll \
    ../wvdotnet/wv.dll
    
versaplexd.exe: main.cs
versaplexd-svc.exe: versaplexd-svc.cs
versaplexd-svc.exe: \
	PKGS+=/r:System.Configuration.Install /r:System.ServiceProcess
    
sm.exe: \
    ischemabackend.cs storedprocedure.cs \
    vxexceptions.cs \
    vxdiskschema.cs vxdbschema.cs vxdbusschema.cs vxschema.cs \
    vxschemachecksums.cs vxschemadiff.cs vxschemaerrors.cs \
    ../wvdbus-sharp/wvdbus-sharp.dll \
    ../wvdotnet/wv.dll
    
vxcli.exe: vxcli.cs \
    vxexceptions.cs \
    ../wvdbus-sharp/wvdbus-sharp.dll \
    ../wvdotnet/wv.dll

clean:: t/clean
	rm -f versaplexd
