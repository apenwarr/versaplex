#include "wvlogger.h"
#include <wvlog.h>
#include <wvlogfile.h>
#include <wvlogstream.h>
#include <wvmoniker.h>
#include <wvlinkerhack.h>
#include <wvistreamlist.h>
#include <wvcrash.h>
#include "psqlodbc.h"
#include <wvlogrcv.h>

static WvLog *wvlog = NULL;
static WvLogRcv *rcv = NULL;

int log_level = 0;
static WvString log_moniker;

WV_LINK_TO(WvConStream);
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
    return !!log_moniker;
}

class WvNullRcv : public WvLogRcv
{
public:
    WvNullRcv() : WvLogRcv(WvLog::Info) {}
    ~WvNullRcv() {}

protected:
    virtual void _mid_line(const char *str, size_t len) {}
};

void wvlog_open()
{
    if (rcv)
	wvlog_close();
#ifdef _MSC_VER
    else  // This will only be true once; when we first call this function
	setup_console_crash();
#endif
    
    if (wvlog_isset())
    {
	WvLog::LogLevel pri;
	if (log_level >= (int)WvLog::NUM_LOGLEVELS)
	    pri = WvLog::Debug5;
	else if (log_level >= 1)
	    pri = (WvLog::LogLevel)log_level;
	else
	    pri = WvLog::Info;

	IWvStream *s = wvcreate<IWvStream>(log_moniker);
	assert(s);
	rcv = new WvLogStream(s, pri);
    	if (!wvlog)
	    wvlog = new WvLog(getpid(), WvLog::Debug);

	(*wvlog)(WvLog::Notice, "Log initialized. (log_level=%s)\n",
		 log_level);
    }
    else // We want this to also capture (and eliminate) DBus messages.
	rcv = new WvNullRcv();
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
    if (wvlog) (*wvlog)(WvLog::Info, "Log closing.\n");
    if (wvlog) delete wvlog;
    if (rcv) delete rcv;
    wvlog = NULL;
    rcv = NULL;
}
