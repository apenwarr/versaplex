#include "wvlogger.h"
#include <wvlog.h>
#include <wvlogfile.h>
#include <wvlogstream.h>
#include <wvmoniker.h>
#include <wvlinkerhack.h>
#include <wvistreamlist.h>
#include <wvcrash.h>
#include "psqlodbc.h"

static WvLog *wvlog = NULL;
static WvLogRcv *rcv = NULL;

char log_level[2];
char log_moniker[255];

WV_LINK_TO(WvTCPConn);
WV_LINK_TO(WvSSLStream);
WV_LINK_TO(WvGzipStream);
#ifndef _MSC_VER
WV_LINK_TO(WvUnixConn);
#endif

void wvlog_open()
{
#ifdef _MSC_VER
    setup_console_crash();
#endif
    WvLog::LogLevel priii = WvLog::Info;
    if (log_level != '\0')
    {
	int prii = atoi(log_level);
	if (prii >= (int)WvLog::NUM_LOGLEVELS)
	    priii = WvLog::Debug5;
	else if (prii >= (int)WvLog::Info)
	    priii = (WvLog::LogLevel)prii;
    }

    if (rcv)
	delete rcv;

    if (log_moniker[0] != '\0')
    {
	IWvStream *s = wvcreate<IWvStream>(log_moniker);
	assert(s);
	WvIStreamList::globallist.append(s, false, "VxODBC logger");
	rcv = new WvLogStream(s, priii);
    }
    else
    {
	rcv = new WvLogConsole(dup(2), priii);
    }
	
    if (!wvlog)
	wvlog = new WvLog(getpid(), WvLog::Debug);
}


void wvlog_print(const char *file, int line, const char *s)
{
    if (!wvlog)
	return;
    while (WvIStreamList::globallist.select(0))
	WvIStreamList::globallist.callback();
    WvString ss("%s:%s: %s", file, line, s);
    wvlog->print(ss);
}


void wvlog_close()
{
    if (wvlog) delete wvlog;
    if (rcv) delete rcv;
    wvlog = NULL;
    rcv = NULL;
}
