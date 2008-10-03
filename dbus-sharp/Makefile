WVDOTNET=../wvdotnet
include $(WVDOTNET)/monorules.mk
include $(WVDOTNET)/rules.mk

all: wvdbus-sharp.dll

CSFLAGS+=/unsafe

wv.dll:

wvdbus-sharp.dll: \
	assemblyinfo.cs \
	Authentication.cs \
	Connection.cs \
	DataConverter.cs \
	dodgytransport.cs \
	MatchRule.cs \
	Message.cs \
	MessageWriter.cs \
	msgiter.cs \
	Protocol.cs \
	$(WVDOTNET)/wv.dll

tests: all t/tests

test: tests t/test

clean:: t/clean
