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

int log_level = 0;
WvString log_moniker;

WV_LINK_TO(WvTCPConn);
WV_LINK_TO(WvSSLStream);
WV_LINK_TO(WvGzipStream);
#ifndef _MSC_VER
WV_LINK_TO(WvUnixConn);
#endif

struct pstring wvlog_get_moniker()
{
    const int safe_size = 1024;
    log_moniker.setsize(safe_size);
    struct pstring ret = { log_moniker.edit(), safe_size };
    return ret;
}

int wvlog_isset()
{
    return !log_moniker.isnull() && *(log_moniker.cstr());
}

void wvlog_open()
{
#ifdef _MSC_VER
    setup_console_crash();
#endif
    WvLog::LogLevel pri = WvLog::Info;
    if (log_level)
    {
	if (log_level >= (int)WvLog::NUM_LOGLEVELS)
	    pri = WvLog::Debug5;
	else if (log_level >= (int)WvLog::Info)
	    pri = (WvLog::LogLevel)log_level;
    }

    if (rcv)
	delete rcv;

    if (wvlog_isset())
    {
	IWvStream *s = wvcreate<IWvStream>(log_moniker);
	assert(s);
	WvIStreamList::globallist.append(s, false, "VxODBC logger");
	rcv = new WvLogStream(s, pri);
    }
    else
    {
	rcv = new WvLogConsole(dup(2), pri);
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
