
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
        case ColumnInfo::Int64:
            reply.append(*(long long *)data[0]);
            break;
        case ColumnInfo::Int32:
            reply.append(*(int *)data[0]);
            break;
        case ColumnInfo::Int16:
            reply.append(*(short *)data[0]);
            break;
        case ColumnInfo::UInt8:
            reply.append(*(unsigned char *)data[0]);
            break;
        case ColumnInfo::Bool:
            reply.append(*(bool *)data[0]);
            break;
        case ColumnInfo::Double:
            reply.append(*(double *)data[0]);
            break;
        case ColumnInfo::Uuid:
            reply.append((char *)data[0]);
            break;
        case ColumnInfo::Binary:
            // FIXME: Binary data needs a type signature or something.
            break;
        case ColumnInfo::String:
            reply.append((char *)data[0]);
            break;
        case ColumnInfo::DateTime:
            reply.struct_start("ii");
            reply.append(*(int *)data[0]);
            reply.append(*(((int *)data[0])+1));
            reply.struct_end();
            break;
        case ColumnInfo::Decimal:
            reply.append((char *)data[0]);
            break;
        case ColumnInfo::ColumnTypeMax:
        default:
            WVFAILEQ(WvString("Unknown SQL type %d", info.coltype), WvString::null);
            break;
        }
        return;
    }
};

#endif
