WVDOTNET=../../wvdotnet
include $(WVDOTNET)/monorules.mk

CPPFLAGS += -I$(WVDOTNET)

all: tests

tests: all.t

test: all.t.pass

all.t.exe: \
	all.t.cs.E \
	../wvdbus-sharp.dll \
	$(WVDOTNET)/wv.dll
