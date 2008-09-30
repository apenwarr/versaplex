WVDOTNET=../wvdotnet
include $(WVDOTNET)/monorules.mk
include $(WVDOTNET)/rules.mk

all: wvdbus-sharp.dll

CSFLAGS+=/unsafe

wv.dll:

wvdbus-sharp.dll: \
	assemblyinfo.cs \
	Address.cs \
	Authentication.cs \
	Bus.cs \
	BusObject.cs \
	Connection.cs \
	DataConverter.cs \
	DBus.cs \
	DProxy.cs \
	ExportObject.cs \
	Introspection.cs \
	Mapper.cs \
	MatchRule.cs \
	Message.cs \
	MessageWriter.cs \
	msgiter.cs \
	PendingCall.cs \
	Protocol.cs \
	Server.cs \
	Signature.cs \
	SocketTransport.cs \
	Transport.cs \
	TypeImplementer.cs \
	UnixMonoTransport.cs \
	UnixNativeTransport.cs \
	UnixTransport.cs \
	Wrapper.cs \
	$(WVDOTNET)/wv.dll

tests: all t/tests

test: tests t/test

clean:: t/clean
