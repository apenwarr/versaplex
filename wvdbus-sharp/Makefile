WVDOTNET=../wvdotnet
include $(WVDOTNET)/monorules.mk
include $(WVDOTNET)/rules.mk

all: wvdbus-sharp.dll

CSFLAGS+=/unsafe

wv.dll:

wvdbus-sharp.dll: \
	assemblyinfo.cs \
	auth.cs \
	conn.cs \
	DataConverter.cs \
	matchrule.cs \
	msg.cs \
	msgwriter.cs \
	msgiter.cs \
	protocol.cs \
	$(WVDOTNET)/wv.dll

tests: all t/tests

test: tests t/test

clean:: t/clean
