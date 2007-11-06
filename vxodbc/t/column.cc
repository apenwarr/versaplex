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
