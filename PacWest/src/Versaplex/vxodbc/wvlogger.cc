#include "wvlogger.h"
#include <wvwin32-sanitize.h>
#include <wvlog.h>
#include <wvlogfile.h>
#include <wvlogstream.h>
#include <wvmoniker.h>
#include <wvlinkerhack.h>
#include <wvistreamlist.h>
#include <wvdbusconn.h>

static WvLog *log;
static WvLogRcv *rcv1, *rcv2, *rcv3;
static WvDBusConn *conn;

WV_LINK_TO(WvTCPConn);
WV_LINK_TO(WvSSLStream);


void wvlog_open()
{
    rcv1 = new WvLogConsole(dup(2));
    rcv2 = new WvLogFile("c:\\temp\\vxodbc.log");
    IWvStream *s = wvcreate<IWvStream>("tcp:averyp-server:4444");
    assert(s);
    WvIStreamList::globallist.append(s, false, "tcp logger");
    rcv3 = new WvLogStream(s, WvLog::Debug4);
    conn = new WvDBusConn("tcp:averyp-server:5555");
    WvIStreamList::globallist.append(conn, false);
    log = new WvLog(GetCurrentProcessId(), WvLog::Debug);
}


void wvlog_print(const char *file, int line, const char *s)
{
    if (!log)
	wvlog_open();
    while (WvIStreamList::globallist.select(0))
	WvIStreamList::globallist.callback();
    WvString ss("%s:%s: %s", file, line, s);
    log->print(ss);
    conn->send(WvDBusMsg("com.Versabanq.Log", "/com/Versabanq/Log",
			     "com.Versabanq.Log", "Out").append(ss));
}


void wvlog_close()
{
    if (log) delete log;
    if (rcv1) delete rcv1;
    if (rcv2) delete rcv2;
    if (rcv3) delete rcv3;
    if (conn) delete conn;
    log = NULL;
    rcv1 = rcv2 = rcv3 = NULL;
    conn = NULL;
}
