Using SqlSucker:
1.  CREATE TABLE #SuckOut (_ int)
2.  EXEC SqlSucker @TableName='#SuckOut', @SQL='EXEC storedProcedure',
        @Ordered=true

If @ordered is true, then the last column of the output table will be
"_ bigint NOT NULL PRIMARY KEY", containing the order of the records returned
from the stored procedure (i.e. running "SELECT * FROM #SuckOut ORDER BY _"
will do what you want).

If @ordered is false (or not given; its default value is false), then the rows
are returned in no particular order. They will tend to be in the right order if
you "SELECT * FROM #SuckOut", but there is no guarantee of this.

@TableName may specify a global or a temporary table. It will modify the schema
of the ouput table to match that of the record set returned by the SQL
statement. When SqlSucker is called, it must contain only a single column,
named "_".

@SQL is an arbitrary SQL statement to execute. It must return a record set
(even one with zero rows), or SqlSucker will throw an error. This could be, for
example, a SELECT statement or an EXEC statement for a stored procedure that
returns a recordset.

--

Installation instructions:
1.  Compile SqlSucker.dll. Easiest way to do this is using Visual Studio
2.  Deploy to the SQL server. Options:
    a.  Copy SqlSucker.dll to the SQL Server, then
        CREATE ASSEMBLY SqlSucker FROM 'c:\path\to\SqlSucker.dll'
            WITH PERMISSION_SET = UNSAFE
    b.  Use Visual Studio to upload it automatically
        This appears to convert the contents of the file into a long hexadecmial
        string and then CREATE ASSEMBLY SqlSucker FROM 0x4D5A9000030000000...
        WITH PERMISSION_SET = UNSAFE
3.  CREATE PROCEDURE SqlSucker_GetConInfo
        @magic int
        WITH EXECUTE AS SELF
        AS EXTERNAL NAME SqlSucker.SqlSuckerProc.SqlSucker_GetConInfo
4.  CREATE PROCEDURE SqlSucker
        @TableName nvarchar(4000),
        @SQL nvarchar(4000),
        @ordered bit = 0
        WITH EXECUTE AS CALLER
        AS EXTERNAL NAME SqlSucker.SqlSuckerProc.SqlSucker
5.  CREATE TABLE SqlSuckerConfig (
        server varchar(512) NOT NULL,
        username nvarchar(512) NOT NULL,
        password nvarchar(512) NOT NULL,
        tablename varchar(512) NOT NULL,
        dbname varchar(512) NOT NULL
    )
6.  INSERT INTO SqlSuckerConfig VALUES
        ('localhost', 'username', 'password', '#SuckerTemp', 'dbname')
    replacing values as appropriate. Tablename should be an arbitrary temporary
    table name. Dbname does not need to be the same database as where SqlSucker
    is being called from, since it just uses this to store data in a temporary
    table briefly and then removes it immediately after.

Notes:
1.  SqlSuckerConfig should be restricted so that it is only be accessible to
    the user that SqlSucker_GetConInfo runs as. SqlSucker_GetConInfo passes the
    connection information onto SqlSucker using static variables within
    SqlSucker.dll, allowing SqlSucker to run with the permissions of the caller
    while still reading connection information that the caller can not see.
2.  Unless the SqlSucker.dll assembly is signed with a trusted key or the
    database is set to trustworthy, SQL Server will not let the CREATE ASSEMBLY
    statement execute with the PERMISSION_SET = UNSAFE (or EXTERNAL_ACCESS).
    To set the database to trustworthy:
        ALTER DATABASE dbname SET TRUSTWORTHY ON

--

Tests:
Tests are in the tests/ subdirectory. They run on Linux under Mono and on
Windows (under cygwin). On Windows, the Makefile may require tweaking if nunit
is not installed in the default directory or a version other than 2.4.1 is
installed.

Wv.Net must first be compiled into an assembly in ../Wv.Net

In the tests directory, run "make" to build the assembly.
-    On Linux, run "make test" to run the tests through nunit-console
-    On Windows, open SqlSucker.test.dll in the nunit GUI and run the tests

Known problems on Windows with Microsoft's .NET implementation:
-    VerifyDecimal test fails because Microsoft prepends a "0" in front of
     values in the range (-1, 0) and (0,1), but Mono does not.
-    VerifyXML test fails because Microsoft applies whitespace differently than
     Mono.