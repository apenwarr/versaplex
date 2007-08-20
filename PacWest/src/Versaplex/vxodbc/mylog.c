/*
 * Description:	This module contains miscellaneous routines
 *		such as for debugging/logging and string functions.
 */

#include "psqlodbc.h"
#include "wvlogger.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>

#ifndef WIN32
#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#define	GENERAL_ERRNO		(errno)
#define	GENERAL_ERRNO_SET(e)	(errno = e)
#else
#define	GENERAL_ERRNO		(GetLastError())
#define	GENERAL_ERRNO_SET(e)	SetLastError(e)
#endif

#if defined(WIN_MULTITHREAD_SUPPORT)
static CRITICAL_SECTION qlog_cs, mylog_cs;
#elif defined(POSIX_MULTITHREAD_SUPPORT)
static pthread_mutex_t qlog_cs, mylog_cs;
#endif				/* WIN_MULTITHREAD_SUPPORT */
static int mylog_on = 0, qlog_on = 0;

int get_mylog(void)
{
    return mylog_on;
}

int get_qlog(void)
{
    return qlog_on;
}

void logs_on_off(int cnopen, int mylog_onoff, int qlog_onoff)
{
    static int mylog_on_count = 0,
	mylog_off_count = 0, qlog_on_count = 0, qlog_off_count = 0;

    ENTER_MYLOG_CS;
    ENTER_QLOG_CS;
    if (mylog_onoff)
	mylog_on_count += cnopen;
    else
	mylog_off_count += cnopen;
    if (mylog_on_count > 0)
    {
	if (mylog_onoff > mylog_on)
	    mylog_on = mylog_onoff;
	else if (mylog_on < 1)
	    mylog_on = 1;
    } else if (mylog_off_count > 0)
	mylog_on = 0;
    else
	mylog_on = 1;
    if (qlog_onoff)
	qlog_on_count += cnopen;
    else
	qlog_off_count += cnopen;
    if (qlog_on_count > 0)
	qlog_on = 1;
    else if (qlog_off_count > 0)
	qlog_on = 0;
    else
	qlog_on = 1;
    LEAVE_QLOG_CS;
    LEAVE_MYLOG_CS;
}

static void _vmylog(const char *file, int line,
		    const char *fmt, va_list args)
{
    char buf[1024];
    int gerrno;
    
    gerrno = GENERAL_ERRNO;
    ENTER_MYLOG_CS;

    vsnprintf(buf, sizeof(buf)-1, fmt, args);
    buf[sizeof(buf)-1] = 0;
    wvlog_print(file, line, buf);

    LEAVE_MYLOG_CS;
    GENERAL_ERRNO_SET(gerrno);
}


#ifdef MY_LOG

void _mylog(const char *file, int line, const char *fmt, ...)
{
    va_list args;

//      if (!mylog_on)  return;
    
    va_start(args, fmt);
    _vmylog(file, line, fmt, args);
    va_end(args);
}

void _forcelog(const char *file, int line, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    _vmylog(file, line, fmt, args);
    va_end(args);
}
static void mylog_initialize()
{
    INIT_MYLOG_CS;
}
static void mylog_finalize()
{
    mylog_on = 0;
    wvlog_close();
    DELETE_MYLOG_CS;
}
#else
void MyLog(char *fmt, ...)
{
}
static void mylog_initialize()
{
}
static void mylog_finalize()
{
}
#endif				/* MY_LOG */


#ifdef Q_LOG
void _qlog(const char *file, int line, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    _vmylog(file, line, fmt, args);
    va_end(args);
}
static void qlog_initialize()
{
    INIT_QLOG_CS;
}
static void qlog_finalize()
{
    qlog_on = 0;
    DELETE_QLOG_CS;
}
#else
static void qlog_initialize()
{
}
static void qlog_finalize()
{
}
#endif				/* Q_LOG */

void InitializeLogging()
{
    mylog_initialize();
    qlog_initialize();
}

void FinalizeLogging()
{
    mylog_finalize();
    qlog_finalize();
}
