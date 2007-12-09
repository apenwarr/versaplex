#ifdef WIN32
#include <windows.h>
#include <direct.h>
#endif

#if HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */

#include <stdio.h>

#if HAVE_STDLIB_H
#include <stdlib.h>
#endif /* HAVE_STDLIB_H */

#if HAVE_STRING_H
#include <string.h>
#endif /* HAVE_STRING_H */

#include <sqltypes.h>
#include <sql.h>
#include <sqlext.h>

#include "wvstring.h"


#ifndef HAVE_SQLLEN
#ifndef SQLULEN
#define SQLULEN SQLUINTEGER
#endif
#ifndef SQLLEN
#define SQLLEN SQLINTEGER
#endif
#endif

extern HENV Environment;
extern HDBC Connection;
extern HSTMT Statement;
extern int use_odbc_version3;

extern char USER[512];
extern char SERVER[512];
extern char PASSWORD[512];
extern char DATABASE[512];
extern char DRIVER[1024];

void set_odbcini_info(WvStringParm server, WvStringParm driver, 
	WvStringParm database, WvStringParm user, WvStringParm pass,
        WvStringParm dbus);
#define CheckReturn() ReportError("", __LINE__, __FILE__)

void ReportError(const char *msg, int line, const char *file);

void CheckCols(int n, int line, const char * file);
void CheckRows(int n, int line, const char * file);
#define CHECK_ROWS(n) CheckRows(n, __LINE__, __FILE__)
#define CHECK_COLS(n) CheckCols(n, __LINE__, __FILE__)
void ResetStatement(void);

#define ODBC_REPORT_ERROR(msg) ReportError(msg, __LINE__, __FILE__)
int Connect(void);
int Disconnect(void);
// Returns true if the statement returns SQL_SUCCESS or SQL_NO_DATA.
// Returns false and prints an error message otherwise.
bool Command(HSTMT stmt, const char *command);
SQLRETURN CommandWithResult(HSTMT stmt, const char *command);
int db_is_microsoft(void);
int driver_is_freetds(void);

#if !HAVE_SETENV
void odbc_setenv(const char *name, const char *value, int overwrite);

#define setenv odbc_setenv
#endif

