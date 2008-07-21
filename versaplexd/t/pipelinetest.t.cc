#include <wvdbusconn.h>
#include <wvdbusmsg.h>
#include <wvtest.h>
#include <list>

bool reply_cb(std::list<uint32_t> &serials, WvDBusMsg &msg)
{
    WVFAIL(msg.iserror());
    if (!WVPASS(!serials.empty()))
        return true;
    // Non-replies won't have a replyserial
    if (!WVPASS(msg.is_reply()))
        return true;

    WVPASSEQ(serials.front(), msg.get_replyserial());
    serials.pop_front();
    return true;
}

WVTEST_MAIN("DBus pipelining")
{
    WvDBusConn conn("dbus:session");
    WVPASS(conn.isok());

    std::list<uint32_t> serials;
    for (int i = 0; i < 10; i++)
    {
        WvDBusMsg testmsg("vx.versaplexd", "/db", "vx.db", "Test");
        conn.send(testmsg, wv::bind(&reply_cb, wv::ref(serials), _1), -1);
        serials.push_back(testmsg.get_serial());
    }

    while (serials.size() != 0)
        conn.runonce();
}
