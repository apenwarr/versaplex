#include "wvstring.h"
#include "wvdbusserver.h"
#include "wvdbusconn.h"
#include "wvtest.h"
#include "fileutils.h"

#include <stdlib.h>
#include <string.h>

#include "common.h"

#include <vector>

// FIXME: This needs a namespace prefix
enum ColumnType {
    Int64 = 0,
    Int32,
    Int16,
    UInt8,
    Bool,
    Double,
    Uuid,
    Binary,
    String,
    DateTime,
    Decimal,
    ColumnTypeMax
};

// FIXME: This probably shouldn't be global
WvString gColTypeNames[ColumnTypeMax + 1] = {
    "Int64",
    "Int32",
    "Int16",
    "UInt8",
    "Bool",
    "Double",
    "Uuid",
    "Binary",
    "String",
    "DateTime",
    "Decimal",
    WvString::null
};

WvString gColTypeDBusType[ColumnTypeMax + 1] = {
    "x",
    "i",
    "s",
    "y",
    "b",
    "d",
    "s",
    // FIXME: Binary data will need additional type information to be sent 
    // across DBus.  Maybe use an array of bytes?
    "v",
    "s",
    "(ii)",
    "s"
};

class ColumnInfo
{
public:
    int size;
    WvString colname;
    ColumnType coltype;
    short precision;
    short scale;
    unsigned char nullable;

    static WvString getDBusSignature()
    {
        return "issnny";
    }

    ColumnInfo(WvString _colname, ColumnType _coltype, bool _nullable,
            int _size, short _precision, short _scale) :
        size(_size),
        colname(_colname), 
        coltype(_coltype), 
        precision(_precision),
        scale(_scale),
        nullable(_nullable)
    {
    }

    void writeHeader(WvDBusMsg &msg)
    {
        msg.struct_start(getDBusSignature());
        msg.append(size);
        msg.append(colname);
        msg.append(gColTypeNames[coltype]);
        msg.append(precision);
        msg.append(scale);
        msg.append(nullable);
        msg.struct_end();
    }

    // Note: These use the MSSQL names for data types.
    // FIXME: It may make sense to specify the SQL datatypes in the enum, and
    // convert them to more DBussy types later, IOW flip this around.
    WvString getSqlColtype()
    {
        switch(coltype) 
        {
        case Int64:
            return "bigint";
        case Int32:
            return "int";
        case Int16:
            return "smallint";
        case UInt8:
            return "tinyint";
        case Bool:
            return "bit";
        case Double:
            return "float";
        case Uuid:
            return "uniqueidentifier";
        case Binary:
            return "binary";
        case String:
            if (size > 0)
                return WvString("varchar(%s)", size);
            else
                return WvString("text");
        case DateTime:
            return "datetime";
        case Decimal:
            return WvString("numeric(%s,%s)", precision, scale);
        case ColumnTypeMax:
        default:
            WVFAILEQ(WvString("Unknown SQL type %d", coltype), WvString::null);
            return "Unknown";
        }
    }
};

class Column
{
public:
    ColumnInfo info;
    // A bunch of malloc'd data.  Cast it back to whatever is appropriate.
    std::vector<void *> data;

    Column(ColumnInfo _info) : info(_info)
    {
    }

    ~Column()
    {
        std::vector<void *>::iterator it;
        for (it = data.begin(); it != data.end(); ++it)
        {
            free(*it);
        }
    }

    // FIXME: Warn if not of the right type
    Column& append(WvStringParm str)
    {
        char *newstr = (char *)malloc(str.len() + 1);
        strcpy(newstr, str.cstr());
        data.push_back(newstr);
        return *this;
    }

    // FIXME: It might be nice to template this.
    Column& append(long long element)
    {
        long long *newelem = (long long *)malloc(sizeof(element));
        *newelem = element;
        data.push_back(newelem);
        return *this;
    }

    Column& append(int element)
    {
        int *newelem = (int *)malloc(sizeof(element));
        *newelem = element;
        data.push_back(newelem);
        return *this;
    }

    Column& append(short element)
    {
        short *newelem = (short *)malloc(sizeof(element));
        *newelem = element;
        data.push_back(newelem);
        return *this;
    }

    Column& append(unsigned char element)
    {
        unsigned char *newelem = (unsigned char *)malloc(sizeof(element));
        *newelem = element;
        data.push_back(newelem);
        return *this;
    }

    Column& append(signed char element)
    {
        signed char *newelem = (signed char *)malloc(sizeof(element));
        *newelem = element;
        data.push_back(newelem);
        return *this;
    }

    Column& append(double element)
    {
        double *newelem = (double *)malloc(sizeof(element));
        *newelem = element;
        data.push_back(newelem);
        return *this;
    }

    void addDataTo(WvDBusMsg &reply)
    {
        switch (info.coltype)
        {
        case Int64:
            reply.append(*(long long *)data[0]);
            break;
        case Int32:
            reply.append(*(int *)data[0]);
            break;
        case Int16:
            reply.append(*(short *)data[0]);
            break;
        case UInt8:
            reply.append(*(unsigned char *)data[0]);
            break;
        case Bool:
            reply.append(*(bool *)data[0]);
            break;
        case Double:
            reply.append(*(double *)data[0]);
            break;
        case Uuid:
            reply.append((char *)data[0]);
            break;
        case Binary:
            // FIXME: Binary data needs a type signature or something.
            break;
        case String:
            reply.append((char *)data[0]);
            break;
        case DateTime:
            reply.struct_start("ii");
            reply.append(*(int *)data[0]);
            reply.append(*(((int *)data[0])+1));
            reply.struct_end();
            break;
        case Decimal:
            reply.append((char *)data[0]);
            break;
        case ColumnTypeMax:
        default:
            WVFAILEQ(WvString("Unknown SQL type %d", info.coltype), WvString::null);
            break;
        }
        return;
    }
};

class Table
{
public:
    std::vector<Column> cols;
    WvString name;

    Table(WvStringParm _name) : name(_name)
    {
    }

    ~Table()
    {
    }

    void addCol(WvStringParm colname, ColumnType coltype, bool nullable,
            int size, short precision, short scale)
    {
        cols.push_back(
            Column(ColumnInfo(colname, coltype, nullable, size, precision, scale)));
    }

    // String lenth of 0 means use unlimited length (TEXT as opposed to
    // VARCHAR(N)
    void addStringCol(WvStringParm colname, int size, bool nullable)
    {
        addCol(colname, String, nullable, size, 0, 0);
    }

    WvString getCreateTableStmt()
    {
        WvString result("create table %s (", name);

        std::vector<Column>::iterator it;
        for (it = cols.begin(); it != cols.end(); ++it)
        {
            if (it != cols.begin()) 
                result.append(",");

            Column &col = *it;
            result.append("%s %s%s", 
                col.info.colname,
                col.info.getSqlColtype(),
                col.info.nullable ? "" : " not null");
        }
        result.append(")");
        return result;
    }

    WvString getDBusTypeSignature()
    {
        WvString result("");
        std::vector<Column>::iterator it;
        for (it = cols.begin(); it != cols.end(); ++it)
        {
            result.append(gColTypeDBusType[it->info.coltype]);
        }
        return result;
    }
};

class TestDBusServer
{
public:
    WvString moniker;
    WvDBusServer *s;

    TestDBusServer()
    {
        fprintf(stderr, "Creating a test DBus server.\n");
        WvString smoniker("unix:tmpdir=%s.dir", wvtmpfilename("wvdbus-sock-"));
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

class FakeVersaplexServer
{
public:
    TestDBusServer dbus;
    WvDBusConn vxserver_conn;
    Table *t;
    WvString expected_query;

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

    static int num_names_registered;
    static bool name_request_cb(WvDBusMsg &msg) 
    {
        num_names_registered++;
        // FIXME: Sensible logging
        // FIXME: Do something useful if the name was already registered
        fprintf(stderr, "*** A name was registered: %s\n", ((WvString)msg).cstr());
        return true;
    }

    bool msg_received(WvDBusMsg &msg) 
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
};

int FakeVersaplexServer::num_names_registered = 0;

// FIXME: Move this to a header, and use a more sensible error checking
// function
#define WVPASS_SQL(sql) \
    do \
    { \
        if (!WvTest::start_check(__FILE__, __LINE__, #sql, SQL_SUCCEEDED(sql)))\
            ReportError(#sql, __LINE__, __FILE__); \
    } while (0)
#define WVPASS_SQL_EQ(x, y) do { if (!WVPASSEQ((x), (y))) { CheckReturn(); } } while (0)

int main(int argc, char *argv[])
{
    FakeVersaplexServer v;
    WvString command;

    Connect();

    WVPASS_SQL(CommandWithResult(Statement, "drop table odbctestdata"));

    bool not_null = 0;
    Table t("odbctestdata");
    t.addCol("col1", String, not_null, 30, 0, 0);
    t.addCol("col2", Int32, not_null, 4, 0, 0);
    t.addCol("col3", Double, not_null, 8, 0, 0);
    t.addCol("col4", Decimal, not_null, 0, 18, 6);
    t.addCol("col5", DateTime, not_null, 8, 0, 0);
    t.addCol("col6", String, not_null, 0, 0, 0);
    v.t = &t;

    // Send the CREATE TABLE statement even though we've already created it 
    // behind the scenes; this lets us also run against a real DB backend for
    // sanity checking.
    WVPASS_SQL(CommandWithResult(Statement, t.getCreateTableStmt()));

    std::vector<Column>::iterator it;
    it = t.cols.begin();
    it->append("ABCDEFGHIJKLMNOP");
    (++it)->append(123456);
    (++it)->append(1234.56);
    (++it)->append("123456.78");
    // Note: this doesn't match the value inserted, but nobody actually checks.
    (++it)->append(123456).append(0);
    (++it)->append("just to check returned length...");

    WVPASS(it == t.cols.end());
    command = "insert dbo.odbctestdata values ("
        "'ABCDEFGHIJKLMNOP',"
        "123456," "1234.56," "123456.78," "'Sep 11 2001 10:00AM'," 
        "'just to check returned length...')";
    WVPASS_SQL(CommandWithResult(Statement, command)); 

    v.expected_query = "select * from odbctestdata";
    WVPASS_SQL(CommandWithResult(Statement, v.expected_query));

    WVPASS_SQL(SQLFetch(Statement));

    for (int i = 1; i <= 6; i++) {
        SQLLEN cnamesize;
        SQLCHAR output[256];

        WVPASS_SQL(SQLGetData(Statement, i, SQL_C_CHAR, 
                                output, sizeof(output), &cnamesize));

        WVFAILEQ((char *)output, WvString::null);
        WVPASSEQ((int)cnamesize, strlen((char *)output));
    }

    WVPASS_SQL_EQ(SQLFetch(Statement), SQL_NO_DATA);
    WVPASS_SQL(SQLCloseCursor(Statement));

    WVPASS_SQL(CommandWithResult(Statement, "drop table odbctestdata"));

    Disconnect();

    printf("Done.\n");
    return 0;
}
