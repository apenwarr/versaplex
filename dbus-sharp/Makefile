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
	Connection.cs \
	DataConverter.cs \
	MatchRule.cs \
	Message.cs \
	MessageWriter.cs \
	msgiter.cs \
	Protocol.cs \
	SocketTransport.cs \
	Transport.cs \
	UnixNativeTransport.cs \
	$(WVDOTNET)/wv.dll

tests: all t/tests

test: tests t/test

clean:: t/clean
