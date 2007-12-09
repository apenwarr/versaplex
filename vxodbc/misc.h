/* File:			misc.h
 *
 * Description:		See "misc.c"
 *
 * Comments:		See "notice.txt" for copyright and license information.
 *
 */

#ifndef __MISC_H__
#define __MISC_H__

/* Define a type for defining a constant string expression. 
 * Note: needed by psqlodbc.h
 */
#define CSTR static const char * const

#include "psqlodbc.h"

#include <stdio.h>
#ifndef  WIN32
#include <unistd.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif
/*	Uncomment MY_LOG define to compile in the mylog() statements.
	Then, debug logging will occur if 'Debug' is set to 1 in the ODBCINST.INI
	portion of the registry.  You may have to manually add this key.
	This logfile is intended for development use, not for an end user!
*/
#define MY_LOG


/*	Uncomment Q_LOG to compile in the qlog() statements (Communications log, i.e. CommLog).
	This logfile contains serious log statements that are intended for an
	end user to be able to read and understand.  It is controlled by the
	'CommLog' flag in the ODBCINST.INI portion of the registry (see above),
	which is manipulated on the setup/connection dialog boxes.
*/
#define Q_LOG

#if defined(WIN_MULTITHREAD_SUPPORT)
#define	INIT_QLOG_CS	InitializeCriticalSection(&qlog_cs)
#define	ENTER_QLOG_CS	EnterCriticalSection(&qlog_cs)
#define	LEAVE_QLOG_CS	LeaveCriticalSection(&qlog_cs)
#define	DELETE_QLOG_CS	DeleteCriticalSection(&qlog_cs)
#define	INIT_MYLOG_CS	InitializeCriticalSection(&mylog_cs)
#define	ENTER_MYLOG_CS	EnterCriticalSection(&mylog_cs)
#define	LEAVE_MYLOG_CS	LeaveCriticalSection(&mylog_cs)
#define	DELETE_MYLOG_CS	DeleteCriticalSection(&mylog_cs)
#elif defined(POSIX_MULTITHREAD_SUPPORT)
#define	INIT_QLOG_CS	pthread_mutex_init(&qlog_cs,0)
#define	ENTER_QLOG_CS	pthread_mutex_lock(&qlog_cs)
#define	LEAVE_QLOG_CS	pthread_mutex_unlock(&qlog_cs)
#define	DELETE_QLOG_CS	pthread_mutex_destroy(&qlog_cs)
#define	INIT_MYLOG_CS	pthread_mutex_init(&mylog_cs,0)
#define	ENTER_MYLOG_CS	pthread_mutex_lock(&mylog_cs)
#define	LEAVE_MYLOG_CS	pthread_mutex_unlock(&mylog_cs)
#define	DELETE_MYLOG_CS	pthread_mutex_destroy(&mylog_cs)
#else
#define	INIT_QLOG_CS
#define	ENTER_QLOG_CS
#define	LEAVE_QLOG_CS
#define	DELETE_QLOG_CS
#define	INIT_MYLOG_CS
#define	ENTER_MYLOG_CS
#define	LEAVE_MYLOG_CS
#define	DELETE_MYLOG_CS
#endif /* WIN_MULTITHREAD_SUPPORT */

#ifndef WIN32
#define MYLOGDIR			"/tmp"
#else
#define MYLOGDIR			"c:\\temp"
#endif /* WIN32 */

#ifdef MY_LOG
# define mylog(fmt, args...)       _mylog(__func__, __LINE__, fmt, ## args)
# define forcelog(fmt, args...) _forcelog(__func__, __LINE__, fmt, ## args)
  extern int _mylog(const char *file, int line, const char *fmt, ...);
  extern int _forcelog(const char *file, int line, const char *fmt, ...);
#else /* !MY_LOG */
# define mylog(fmt, args...)
#endif /* MY_LOG */
    
#define	inolog	if (get_mylog() > 1) mylog /* for really temporary debug */

#ifdef Q_LOG
# define qlog(fmt, args...)  _qlog(__func__, __LINE__, fmt, ## args)
  extern int _qlog(const char *file, int line, const char *fmt, ...);
#else /* !Q_LOG */
# define qlog(fmt, args...)
#endif
    
#define	inoqlog	qlog
int	get_qlog(void);
int	get_mylog(void);

#ifndef WIN32
#define DIRSEPARATOR		"/"
#else
#define DIRSEPARATOR		"\\"
#endif

#ifdef WIN32
#define PG_BINARY			O_BINARY
#define PG_BINARY_R			"rb"
#define PG_BINARY_W			"wb"
#define PG_BINARY_A			"ab"
#else
#define PG_BINARY			0
#define PG_BINARY_R			"r"
#define PG_BINARY_W			"w"
#define PG_BINARY_A			"a"
#endif


void	InitializeLogging();
void	FinalizeLogging();

void remove_newlines(char *string);
char *strncpy_null(char *dst, const char *src, ssize_t len);
char *trim(char *string);
char *make_string(const unsigned char *s, ssize_t len, char *buf, size_t bufsize);
char *make_lstring_ifneeded(ConnectionClass *, const void *s, ssize_t len, BOOL);
char *my_strcat(char *buf, const char *fmt, const char *s, ssize_t len);
char *schema_strcat(char *buf, const char *fmt, const char *s, ssize_t len,
		    const char *, int, ConnectionClass *conn);
char *my_strcat1(char *buf, const char *fmt, const char *s1,
		 const char *s, ssize_t len);
char *schema_strcat1(char *buf, const char *fmt, const char *s1,
		     const char *s, ssize_t len,
		     const char *, int, ConnectionClass *conn);
int	snprintf_add(char *buf, size_t size, const char *format, ...);
size_t	snprintf_len(char *buf, size_t size, const char *format, ...);
/* #define	GET_SCHEMA_NAME(nspname) 	(stricmp(nspname, "public") ? nspname : "") */
#define	GET_SCHEMA_NAME(nspname) 	(nspname)

/* defines for return value of my_strcpy */
#define STRCPY_SUCCESS		1
#define STRCPY_FAIL			0
#define STRCPY_TRUNCATED	(-1)
#define STRCPY_NULL			(-2)

ssize_t my_strcpy(char *dst, ssize_t dst_len, const char *src, ssize_t src_len);

#ifdef __cplusplus
}
#endif
#endif /* __MISC_H__ */
