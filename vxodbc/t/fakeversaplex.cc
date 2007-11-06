#include "fakeversaplex.h"

#include "table.h"
#include "wvtest.h"

#include <vector>

int FakeVersaplexServer::num_names_registered = 0;

bool FakeVersaplexServer::msg_received(WvDBusMsg &msg)
{
    if (msg.get_dest() != "com.versabanq.versaplex")
        return false;

    if (msg.get_path() != "/com/versabanq/versaplex/db") 
        return false;

    if (msg.get_interface() != "com.versabanq.versaplex.db") 
        return false;

    // The message was for us

    fprintf(stdout, "*** Received message %s\n", ((WvString)msg).cstr());
    printf("*** Got argstr '%s'\n", msg.get_argstr().cstr());

    printf("sender:%s\ndest:%s\npath:%s\niface:%s\nmember:%s\n",
        msg.get_sender().cstr(), msg.get_dest().cstr(), msg.get_path().cstr(), 
        msg.get_interface().cstr(), msg.get_member().cstr());

#if 0
    WvDBusMsg::Iter ii(msg);
    for (ii.rewind(); ii.next(); )
    {
        printf("**** Contents: %s\n", ii
    }
#endif

    if (msg.get_member() == "ExecRecordset")
    {
        printf("Processing ExecRecordSet\n");
        WvString query(msg.get_argstr());
        if (query == "use pmccurdy")
        {
            printf("*** Sending error\n");
            WvDBusError(msg, "System.ArgumentOutOfRangeException", 
                "Argument is out of range.").send(vxserver_conn);
            return false;
        }
        else if (query == expected_query)
        {
            printf("*** Sending reply\n");
            WvDBusMsg reply = msg.reply();
            std::vector<Column>::iterator it;

            reply.array_start(WvString("(%s)", ColumnInfo::getDBusSignature()));
            for (it = t->cols.begin(); it != t->cols.end(); ++it)
                it->info.writeHeader(reply);
            reply.array_end();

            // Write the body signature
            WvString sig(t->getDBusTypeSignature());
            reply.varray_start(WvString("(%s)", sig)).struct_start(sig);
            // Write the body
            for (it = t->cols.begin(); it != t->cols.end(); ++it)
            {
                it->addDataTo(reply);
            }
            reply.struct_end().varray_end();

            // Nullity
            reply.array_start("ay").array_start("y");
            for (it = t->cols.begin(); it != t->cols.end(); ++it)
                reply.append(it->info.nullable);
            reply.array_end().array_end();

            reply.send(vxserver_conn);
        }
        else
        {
            WvDBusError(msg, "System.NotImplemented", 
                "Not yet implemented.  Try again later.").send(vxserver_conn);
        }
    }
    else
    {
        WvDBusError(msg, "System.NotImplemented", 
            "Not yet implemented.  Try again later.").send(vxserver_conn);
    }
    return true;
}
