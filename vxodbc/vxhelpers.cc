#include "vxhelpers.h"
#include "wvistreamlist.h"
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
    while (WvIStreamList::globallist.select(0))
	WvIStreamList::globallist.callback();
    WvDBusMsg reply = conn.send_and_wait(msg, 50000,
    			wv::bind(&update_sigrets, _1, this));

    if (reply.iserror())
	mylog("DBus error: '%s'\n", ((WvString)reply).cstr());
    else // Method return
	process_msg(reply);

    uint32_t reply_serial = reply.get_replyserial();
    if (signal_returns[reply_serial])
	signal_returns.erase(reply_serial);
}

void VxResultSet::return_versaplex_db()
{
    // Allocate space for the column info data... see below.
    // const int my_cols = 5;
    const int my_cols = 1;
    QR_set_num_fields(res, my_cols);
    maxcol = my_cols - 1;

    // Column info... the names and types of the columns here were stolen from
    // tracing the result of a 'list tables' call to the server.
    set_field_info(0, "TABLE_QUALIFIER", 1043, 128);
    //FIXME:  The below 4 are commented out, because StarQuery happens to work
    //        with only one return column.  Will other applications that also
    //        expect some kind of standard reply to what catalogs are available?
    //set_field_info(1, "TABLE_OWNER", 1043, 128);
    //set_field_info(2, "TABLE_NAME", 1043, 128);
    //set_field_info(3, "TABLE_TYPE", 1043, 32);
    //set_field_info(4, "REMARKS", 1043, 254);

    // set up fake data, just one row, with the TABLE_QUALIFIER being
    // a fake '__VERSAPLEX' database, and the rest of the columns being null.
    TupleField *tuple = QR_AddNew(res);

    set_tuplefield_string(&tuple[0], "__VERSAPLEX");
    //FIXME:  See above.
    //for (int i = 1; i < my_cols; ++i)
    //	set_tuplefield_string(&tuple[i], NULL);
}
