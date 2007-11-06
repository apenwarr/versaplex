#ifndef TABLE_H
#define TABLE_H

#include <vector>
#include "column.h"

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

    void addCol(WvStringParm colname, ColumnInfo::ColumnType coltype, 
            bool nullable, int size, short precision, short scale)
    {
        cols.push_back(
            Column(ColumnInfo(colname, coltype, nullable, size, precision, scale)));
    }

    // String lenth of 0 means use unlimited length (TEXT as opposed to
    // VARCHAR(N)
    void addStringCol(WvStringParm colname, int size, bool nullable)
    {
        addCol(colname, ColumnInfo::String, nullable, size, 0, 0);
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
            result.append(ColumnInfo::ColTypeDBusType[it->info.coltype]);
        }
        return result;
    }
};

#endif
