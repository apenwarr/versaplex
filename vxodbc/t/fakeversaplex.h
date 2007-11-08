#include "wvdbusconn.h"
#include "wvstring.h"
#include "wvistreamlist.h"
#include "wvlog.h"

#define WVPASS_SQL(sql) \
    do \
    { \
        if (!WvTest::start_check(__FILE__, __LINE__, #sql, SQL_SUCCEEDED(sql)))\
            ReportError(#sql, __LINE__, __FILE__); \
    } while (0)
#define WVPASS_SQL_EQ(x, y) do { if (!WVPASSEQ((x), (y))) { CheckReturn(); } } while (0)

class Table;

class FakeVersaplexServer
{
public:
    WvDBusConn vxserver_conn;
    Table *t;
    WvString expected_query;
    static int num_names_registered;
    WvLog log;

    // FIXME: Use a private bus when we can tell VxODBC where to find it.
    // Until then, just use the session bus and impersonate Versaplex, 
    // hoping that no other Versaplex server is running.
    FakeVersaplexServer() : 
        vxserver_conn("dbus:session"),
        t(NULL),
        log("Fake Versaplex", WvLog::Debug1)
    {
        WvString use_real(getenv("USE_REAL_VERSAPLEX"));
        if (!use_real || use_real == "0") 
        {
            WvIStreamList::globallist.append(&vxserver_conn, false);

            log("*** Registering com.versabanq.versaplex\n");
            vxserver_conn.request_name("com.versabanq.versaplex", &name_request_cb);
            while (num_names_registered < 1)
                WvIStreamList::globallist.runonce();

            WvDBusCallback cb(wv::bind(
                &FakeVersaplexServer::msg_received, this, _1));
            vxserver_conn.add_callback(WvDBusConn::PriNormal, cb, this);
        }
    }

    static bool name_request_cb(WvDBusMsg &msg) 
    {
        WvLog log("name_request_cb", WvLog::Debug1);
        num_names_registered++;
        // FIXME: Sensible logging
        // FIXME: Do something useful if the name was already registered
        log("*** A name was registered: %s\n", (WvString)msg);
        return true;
    }

    bool msg_received(WvDBusMsg &msg);
};

