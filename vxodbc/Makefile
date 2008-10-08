include ../config.mk
WVSTREAMS:=$(shell pwd)/../wv/wvstreams
include $(WVSTREAMS)/wvrules.mk

ifndef BUILD_TARGET 
$(error Please run the "configure" or "configure-mingw32" script in ..)
endif

WVPORTS=../wv/wvports
XPLC=$(WVPORTS)/xplc/build/xplc
OPENSSL=$(WVPORTS)/openssl/build/openssl
DBUS=$(WVPORTS)/dbus/build/dbus/dbus/.libs
BOOST=$(WVPORTS)/boost/build/boost
ZLIB=$(WVPORTS)/zlib/build/zlib
CXXFLAGS+=-w \
	-DUNICODE_SUPPORT -DODBCVER=0x0351 \
	-I$(WVSTREAMS)/include -I$(XPLC)/include -I$(BOOST)

OBJS=\
	bind.o \
	columninfo.o \
	connection.o \
	convert.o \
	dlg_specific.o \
	dlg_wingui.o \
	drvconn.o \
	environ.o \
	execute.o \
	info.o \
	info30.o \
	misc.o \
	mylog.o \
	pgapi30.o \
	multibyte.o \
	options.o \
	parse.o \
	pgtypes.o \
	psqlodbc.o \
	qresult.o \
	results.o \
	statement.o \
	tuple.o \
	odbcapi.o \
	odbcapi30.o \
	descriptor.o \
	odbcapiw.o \
	odbcapi30w.o \
	inouealc.o \
	win_unicode.o \
	wvlogger.o \
	wvssl_necessities.o \
	vxhelpers.o

# Files made by configure
CONFIGUREFILES=\
	configure \
	config.status \
	config.h \
	config.h.in

distclean: clean
	rm -f $(CONFIGUREFILES)
	rm -rf autom4te.cache

clean:
	rm -f *.o *.so *~ *.res *.dll *.a .*.d *.d 
	$(MAKE) -C t $@

config.h.in: configure.ac aclocal.m4
	autoheader

configure: configure.ac config.h.in
	autoconf

include $(wildcard .*.d)

test tests:
	$(MAKE) -C t $@

ifeq ($(BUILD_TARGET),win32)
include Makefile-win32
else
include Makefile-linux
endif
