#include "common.h"

#include <unistd.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>

#ifndef WIN32
#include "tds_sysdep_private.h"
#else
#define TDS_SDIR_SEPARATOR "\\"
#endif

HENV Environment;
HDBC Connection;
HSTMT Statement;

bool HACKY_GLOBAL_VARS_INITED = false;
char USER[512];
char SERVER[512];
char PASSWORD[512];
char DATABASE[512];
char DRIVER[1024];

/* some platforms do not have setenv, define a replacement */
#if !HAVE_SETENV
void
odbc_setenv(const char *name, const char *value, int overwrite)
{
#if HAVE_PUTENV
	char buf[1024];

	sprintf(buf, "%s=%s", name, value);
	putenv(buf);
#endif
}
#endif

void set_odbcini_info(WvStringParm server, WvStringParm driver, 
	WvStringParm database, WvStringParm user, WvStringParm pass, 
	WvStringParm dbus)
{
	fprintf(stderr, "*** In set_odbcini_info\n");
	
	strcpy(SERVER, server.cstr());
	strcpy(DRIVER, driver.cstr());
	strcpy(DATABASE, database.cstr());
	strcpy(USER, user.cstr());
	strcpy(PASSWORD, pass.cstr());

	HACKY_GLOBAL_VARS_INITED = true;

	/* craft out odbc.ini, avoid to read wrong one */
	FILE *in = fopen("odbc.ini", "w");
	if (in) {
		fprintf(in, "[%s]\nDriver = %s\nDatabase = %s\n"
                        "Servername = %s\nUsername = %s\nDBus Connection = %s\n", 
                        SERVER, DRIVER, DATABASE, SERVER, USER, dbus.cstr());
		fclose(in);
		setenv("ODBCINI", "./odbc.ini", 1);
		setenv("SYSODBCINI", "./odbc.ini", 1);
	}
}

void
ReportError(const char *errmsg, int line, const char *file)
{
	SQLSMALLINT handletype;
	SQLHANDLE handle;
	SQLRETURN ret;
	unsigned char sqlstate[6];
	unsigned char msg[256];


	if (Statement) {
		handletype = SQL_HANDLE_STMT;
		handle = Statement;
	} else if (Connection) {
		handletype = SQL_HANDLE_DBC;
		handle = Connection;
	} else {
		handletype = SQL_HANDLE_ENV;
		handle = Environment;
	}
	if (errmsg[0]) {
		if (line)
			fprintf(stdout, "%s:%d %s\n", file, line, errmsg);
		else
			fprintf(stdout, "%s\n", errmsg);
	}
	ret = SQLGetDiagRec(handletype, handle, 1, sqlstate, NULL, msg, sizeof(msg), NULL);
	if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
		fprintf(stdout, "SQL error %s -- %s (%s:%d)\n", sqlstate, msg, file, line);
}

int
Connect(void)
{
	if (!HACKY_GLOBAL_VARS_INITED)
	{
		printf("No connection information provided\n");
		return 1;
	}

	if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &Environment) != SQL_SUCCESS) {
		printf("Unable to allocate env\n");
		exit(1);
	}

        SQLSetEnvAttr(Environment, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) (SQL_OV_ODBC3), SQL_IS_UINTEGER);

	if (SQLAllocHandle(SQL_HANDLE_DBC, Environment, &Connection) != SQL_SUCCESS) {
		printf("Unable to allocate connection\n");
                CheckReturn();
		SQLFreeHandle(SQL_HANDLE_ENV, Environment);
		exit(1);
	}
	printf("odbctest\n--------\n\n");
	printf("connection parameters:\nserver:   '%s'\nuser:     '%s'\npassword: '%s'\ndatabase: '%s'\n",
	       SERVER, USER,  PASSWORD /* "????" */ , DATABASE);

	int res = SQLConnect(Connection, (SQLCHAR *) SERVER, SQL_NTS, (SQLCHAR *) USER, SQL_NTS, (SQLCHAR *) PASSWORD, SQL_NTS);
	if (!SQL_SUCCEEDED(res)) {
		printf("Unable to open data source (ret=%d)\n", res);
		CheckReturn();
		exit(1);
	}

	if (SQLAllocHandle(SQL_HANDLE_STMT, Connection, &Statement) != SQL_SUCCESS) {
		printf("Unable to allocate statement\n");
		CheckReturn();
		exit(1);
	}

	char command[512];
	sprintf(command, "use %s", DATABASE);
	printf("%s\n", command);

	if (!SQL_SUCCEEDED(SQLExecDirect(Statement, (SQLCHAR *) command, SQL_NTS))) {
		printf("Unable to execute statement\n");
		CheckReturn();
		exit(1);
	}
	return 0;
}

int
Disconnect(void)
{
	if (Statement) {
		SQLFreeStmt(Statement, SQL_DROP);
		Statement = SQL_NULL_HSTMT;
	}

	if (Connection) {
		SQLDisconnect(Connection);
		SQLFreeHandle(SQL_HANDLE_DBC, Connection);
		Connection = SQL_NULL_HDBC;
	}

	if (Environment) {
		SQLFreeHandle(SQL_HANDLE_ENV, Environment);
		Environment = SQL_NULL_HENV;
	}
	return 0;
}

bool
Command(HSTMT stmt, const char *command)
{
	int result = 0;

	fprintf(stderr, "%s\n", command);
	result = SQLExecDirect(stmt, (SQLCHAR *) command, SQL_NTS);
	if (result != SQL_SUCCESS && result != SQL_NO_DATA) {
		fprintf(stderr, "Unable to execute statement\n");
		CheckReturn();
		return false;
	}
        return true;
}

SQLRETURN
CommandWithResult(HSTMT stmt, const char *command)
{
	printf("%s\n", command);
	return SQLExecDirect(stmt, (SQLCHAR *) command, SQL_NTS);
}

static int ms_db = -1;
int
db_is_microsoft(void)
{
	char buf[64];
	SQLSMALLINT len;
	int i;

	if (ms_db < 0) {
		buf[0] = 0;
		SQLGetInfo(Connection, SQL_DBMS_NAME, buf, sizeof(buf), &len);
		for (i = 0; buf[i]; ++i)
			buf[i] = tolower(buf[i]);
		ms_db = (strstr(buf, "microsoft") != NULL);
	}
	return ms_db;
}

static int freetds_driver = -1;
int
driver_is_freetds(void)
{
	char buf[64];
	SQLSMALLINT len;
	int i;

	if (freetds_driver < 0) {
		buf[0] = 0;
		SQLGetInfo(Connection, SQL_DRIVER_NAME, buf, sizeof(buf), &len);
		for (i = 0; buf[i]; ++i)
			buf[i] = tolower(buf[i]);
		freetds_driver = (strstr(buf, "tds") != NULL);
	}
	return freetds_driver;
}

void
CheckCols(int n, int line, const char * file)
{
	SQLSMALLINT cols;
	SQLRETURN res;

	res = SQLNumResultCols(Statement, &cols);
	if (res != SQL_SUCCESS) {
		if (res == SQL_ERROR && n < 0)
			return;
		fprintf(stderr, "%s:%d: Unable to get column numbers\n", file, line);
		CheckReturn();
		Disconnect();
		exit(1);
	}

	if (cols != n) {
		fprintf(stderr, "%s:%d: Expected %d columns returned %d\n", file, line, n, (int) cols);
		Disconnect();
		exit(1);
	}
}

void
CheckRows(int n, int line, const char * file)
{
	SQLLEN rows;
	SQLRETURN res;

	res = SQLRowCount(Statement, &rows);
	if (res != SQL_SUCCESS) {
		if (res == SQL_ERROR && n < -1)
			return;
		fprintf(stderr, "%s:%d: Unable to get row\n", file, line);
		CheckReturn();
		Disconnect();
		exit(1);
	}

	if (rows != n) {
		fprintf(stderr, "%s:%d: Expected %d rows returned %d\n", file, line, n, (int) rows);
		Disconnect();
		exit(1);
	}
}

void
ResetStatement(void)
{
	SQLFreeStmt(Statement, SQL_DROP);
	Statement = SQL_NULL_HSTMT;
	if (SQLAllocHandle(SQL_HANDLE_STMT, Connection, &Statement) != SQL_SUCCESS)
		ODBC_REPORT_ERROR("Unable to allocate statement");
}

