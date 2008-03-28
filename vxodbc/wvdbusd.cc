#include "wvdbusserver.h"
#include "wvassert.h"
#include "wvlinkerhack.h"

WV_LINK_TO(WvTCPListener);
WV_LINK_TO(WvUnixListener);
WV_LINK_TO(WvSSLStream);

static WvDBusServer *s;

extern "C" void wvdbusd_start()
{
    wvassert(!s);
    s = new WvDBusServer();
}

extern "C" void wvdbusd_stop()
{
    WVRELEASE(s);
    s = NULL;
}

extern "C" void wvdbusd_listen(const char *moniker)
{
    s->listen(moniker);
}

extern "C" void wvdbusd_runonce()
{
    // FIXME: Should use runonce(-1) here, but then the .net thread we
    // run in won't be able to get interrupted to check want_to_die.
    // Ideally we'd hand them a socket or something that they can ping us
    // with.
    s->runonce(1000);
}
