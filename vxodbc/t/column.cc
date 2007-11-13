#include "column.h"

#include "wvstring.h"

WvString ColumnInfo::ColTypeNames[ColumnTypeMax + 1] = {
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

WvString ColumnInfo::ColTypeDBusType[ColumnTypeMax + 1] = {
    "x",
    "i",
    "n",
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

// Note: These use the MSSQL names for data types.
// FIXME: It may make sense to specify the SQL datatypes in the enum, and
// convert them to more DBussy types later, IOW flip this around.
WvString ColumnInfo::getSqlColtype()
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

void ColumnInfo::writeHeader(WvDBusMsg &msg)
{
    msg.struct_start(getDBusSignature());
    msg.append(size);
    msg.append(colname);
    msg.append(ColumnInfo::ColTypeNames[coltype]);
    msg.append(precision);
    msg.append(scale);
    msg.append(nullable);
    msg.struct_end();
}

void Column::addDataTo(WvDBusMsg &reply)
{
    if (data.size() < 1)
        return;

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
        // FIXME: Each element in the vector should be a complete entry
        WVPASS(data.size() >= 2);
        reply.append(*(int *)data[0]);
        reply.append(*(int *)data[1]);
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

Column& Column::append(WvStringParm str)
{
    char *newstr = (char *)malloc(str.len() + 1);
    strcpy(newstr, str.cstr());
    data.push_back(newstr);
    return *this;
}

// FIXME: It might be nice to template this.
Column& Column::append(long long element)
{
    long long *newelem = (long long *)malloc(sizeof(element));
    *newelem = element;
    data.push_back(newelem);
    return *this;
}

Column& Column::append(int element)
{
    WVFAILEQ(element, -234);
    int *newelem = (int *)malloc(sizeof(element));
    *newelem = element;
    data.push_back(newelem);
    return *this;
}

Column& Column::append(short element)
{
    short *newelem = (short *)malloc(sizeof(element));
    *newelem = element;
    data.push_back(newelem);
    return *this;
}

Column& Column::append(unsigned char element)
{
    unsigned char *newelem = (unsigned char *)malloc(sizeof(element));
    *newelem = element;
    data.push_back(newelem);
    return *this;
}

Column& Column::append(signed char element)
{
    signed char *newelem = (signed char *)malloc(sizeof(element));
    *newelem = element;
    data.push_back(newelem);
    return *this;
}

Column& Column::append(double element)
{
    double *newelem = (double *)malloc(sizeof(element));
    *newelem = element;
    data.push_back(newelem);
    return *this;
}

