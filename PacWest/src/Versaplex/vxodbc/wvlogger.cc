#include "wvlogger.h"
#include <wvlog.h>
#include <wvlogfile.h>
#include <wvlogstream.h>
#include <wvmoniker.h>
#include <wvlinkerhack.h>
#include <wvistreamlist.h>
#include <wvcrash.h>
#include "psqlodbc.h"

static WvLog *log;
static WvLogRcv *rcv1, *rcv2, *rcv3;

WV_LINK_TO(WvTCPConn);
WV_LINK_TO(WvSSLStream);

#ifdef _MSC_VER
static int getpid()
{
    return GetCurrentProcessId();
}
#endif

void wvlog_open()
{
#ifdef _MSC_VER
    setup_console_crash();
#endif
    rcv1 = new WvLogConsole(dup(2), WvLog::Debug5);
    rcv2 = new WvLogFile(MYLOGDIR "/vxodbc.log", WvLog::Debug4);
    IWvStream *s = wvcreate<IWvStream>("tcp:averyp-server:4444");
    assert(s);
    WvIStreamList::globallist.append(s, false, "tcp logger");
    rcv3 = new WvLogStream(s, WvLog::Debug4);
    log = new WvLog(getpid(), WvLog::Debug);
}


void wvlog_print(const char *file, int line, const char *s)
{
    if (!log)
	wvlog_open();
    while (WvIStreamList::globallist.select(0))
	WvIStreamList::globallist.callback();
    WvString ss("%s:%s: %s", file, line, s);
    log->print(ss);
}


void wvlog_close()
{
    if (log) delete log;
    if (rcv1) delete rcv1;
    if (rcv2) delete rcv2;
    if (rcv3) delete rcv3;
    log = NULL;
    rcv1 = rcv2 = rcv3 = NULL;
}
