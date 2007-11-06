
#ifndef COLUMN_H
#define COLUMN_H

#include "wvtest.h"
#include "wvstring.h"
#include "wvdbusmsg.h"

#include <vector>

class ColumnInfo
{
public:
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

    int size;
    WvString colname;
    ColumnInfo::ColumnType coltype;
    short precision;
    short scale;
    unsigned char nullable;

    static WvString ColTypeNames[ColumnTypeMax + 1];
    static WvString ColTypeDBusType[ColumnTypeMax + 1];

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

    void writeHeader(WvDBusMsg &msg);
    WvString getSqlColtype();
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

    void addDataTo(WvDBusMsg &reply);

    Column& append(WvStringParm element);
    Column& append(long long element);
    Column& append(int element);
    Column& append(short element);
    Column& append(unsigned char element);
    Column& append(signed char element);
    Column& append(double element);
};

#endif
