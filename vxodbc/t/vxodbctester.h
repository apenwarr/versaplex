#ifndef VXODBCTESTER_H
#define VXODBCTESTER_H

#include "wvstring.h"
#include "wvlog.h"
#include "fileutils.h"
#include "wvdbusserver.h"
#include "wvdbusconn.h"

#define WVPASS_SQL(sql) \
    do \
    { \
        if (!WvTest::start_check(__FILE__, __LINE__, #sql, SQL_SUCCEEDED(sql)))\
            ReportError(#sql, __LINE__, __FILE__); \
    } while (0)
#define WVPASS_SQL_EQ(x, y) do { if (!WVPASSEQ((x), (y))) { CheckReturn(); } } while (0)

class Table;
class WvDBusConn;
class WvDBusMsg;

class TestDBusServer
{
public:
    WvString moniker;
    WvDBusServer *s;

    TestDBusServer()
    {
        fprintf(stderr, "Creating a test DBus server.\n");
        WvString smoniker("unix:tmpdir=%s.dir",
                         wvtmpfilename("wvdbus-sock-"));
        s = new WvDBusServer(smoniker);
        moniker = s->get_addr();
        fprintf(stderr, "Server address is '%s'\n", moniker.cstr());
        WvIStreamList::globallist.append(s, false);
    }

    ~TestDBusServer()
    {
        delete s;
    }
};

class VxOdbcTester
{
public:
    TestDBusServer dbus_server;
    WvDBusConn vxserver_conn;
    Table *t;
    WvString expected_query;
    static int num_names_registered;
    WvLog log;

    VxOdbcTester();
    ~VxOdbcTester();

    static bool name_request_cb(WvDBusMsg &msg); 
    bool msg_received(WvDBusMsg &msg);
};

#endif // VXODBCTESTER_H
