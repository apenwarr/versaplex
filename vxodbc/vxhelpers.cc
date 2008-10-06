#include "vxhelpers.h"
#include <list>

static std::map<unsigned int, VxResultSet *> signal_returns;

static void update_sigrets(unsigned int key, VxResultSet *value)
{
    signal_returns[key] = value;
}

static bool signal_sorter(WvDBusMsg &msg)
{
    WvString member = msg.get_member();
    // We have a signal, and it's a signal carrying data we want!
    if (!!member && member == "ChunkRecordsetSig")
    {
    	WvDBusMsg::Iter top(msg);
	unsigned int reply_serial =
	    (unsigned int)top.getnext().getnext().getnext().getnext().get_int();
	if (signal_returns[reply_serial])
	{
	    signal_returns[reply_serial]->process_msg(msg);
	    return true;
	}
    }

    return false;
}

static std::map<WvDBusConn *, bool> callbacked_conns;

static void erase_conn(WvStream &s)
{
    WvDBusConn *c = (WvDBusConn *)&s;
    callbacked_conns.erase(c);
}

void VxResultSet::process_msg(WvDBusMsg &msg)
{
    WvDBusMsg::Iter top(msg);
    WvDBusMsg::Iter colinfo(top.getnext().open());
    WvDBusMsg::Iter data(top.getnext().open().getnext().open());
    WvDBusMsg::Iter flags(top.getnext().open());

    if (process_colinfo)
    {
	int num_cols;
	// Sadly there's no better way to get the number of columns
	for (num_cols = 0; colinfo.next(); ++num_cols) { }
	colinfo.rewind();
	// Allocate space for the column info data.
	QR_set_num_fields(res, num_cols);
	maxcol = num_cols - 1;

	for (int colnum = 0; colinfo.next(); ++colnum)
	{
	    WvDBusMsg::Iter i(colinfo.open());

	    int colsize = i.getnext();
	    WvString colname = i.getnext();
	    int coltype = vxtype_to_pgtype(i.getnext());

	    set_field_info(colnum, colname, coltype, colsize);
	}
    	process_colinfo = false;
    }
    
    for (data.rewind(); data.next(); )
    {
	TupleField *tuple = QR_AddNew(res);

	WvDBusMsg::Iter cols(data.open());
	for (int colnum = 0; cols.next() && colnum < numcols(); colnum++)
	    set_tuplefield_string(&tuple[colnum], *cols);
    }
}
    
void VxResultSet::runquery(WvDBusConn &conn, const char *func,
			    const char *query)
{
    WvDBusMsg msg("vx.versaplexd", "/db", "vx.db", func);
    msg.append(query);
    if (!callbacked_conns[&conn])
    {
        conn.add_callback(WvDBusConn::PriNormal, signal_sorter);
	conn.setclosecallback(wv::bind(&erase_conn, wv::ref(conn)));
	callbacked_conns[&conn] = true;
    }
    process_colinfo = true;
    WvDBusMsg reply = conn.send_and_wait(msg, 50000,
    			wv::bind(&update_sigrets, _1, this));

    if (reply.iserror())
	    mylog("DBus error: '%s'\n", ((WvString)reply).cstr());
    else
    {
    	// Method return
	WvDBusMsg::Iter top(reply);
	if (top.next() &&
	    top.get_str() != "ChunkRecordset sent you all your data!")
	    process_msg(reply);
    }

    uint32_t reply_serial = reply.get_replyserial();
    if (signal_returns[reply_serial])
	signal_returns.erase(reply_serial);
}
