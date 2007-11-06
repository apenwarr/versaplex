#include "wvdbusconn.h"
#include "wvstring.h"
#include "wvistreamlist.h"

class Table;

class FakeVersaplexServer
{
public:
    WvDBusConn vxserver_conn;
    Table *t;
    WvString expected_query;
    static int num_names_registered;

    // FIXME: Use a private bus when we can tell VxODBC where to find it.
    // Until then, just use the session bus and impersonate Versaplex, 
    // hoping that no other Versaplex server is running.
    FakeVersaplexServer() : vxserver_conn("dbus:session"),
        t(NULL)
    {
        WvIStreamList::globallist.append(&vxserver_conn, false);

        fprintf(stderr, "*** Registering com.versabanq.versaplex\n");
        vxserver_conn.request_name("com.versabanq.versaplex", &name_request_cb);
        while (num_names_registered < 1)
            WvIStreamList::globallist.runonce();

        WvDBusCallback cb(wv::bind(
            &FakeVersaplexServer::msg_received, this, _1));
        vxserver_conn.add_callback(WvDBusConn::PriNormal, cb, this);
    }

    static bool name_request_cb(WvDBusMsg &msg) 
    {
        num_names_registered++;
        // FIXME: Sensible logging
        // FIXME: Do something useful if the name was already registered
        fprintf(stderr, "*** A name was registered: %s\n", ((WvString)msg).cstr());
        return true;
    }

    bool msg_received(WvDBusMsg &msg);
};

