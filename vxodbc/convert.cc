/*
 * Description: This module contains routines related to
 *	   converting parameters and columns into requested data types.
 *	   Parameters are converted from their SQL_C data types into
 *	   the appropriate postgres type.  Columns are converted from
 *	   their postgres type (SQL type) into the appropriate SQL_C
 *	   data type.
 */
/* Multibyte support  Eiji Tokuya	2001-03-15	*/

#include "convert.h"
#ifdef	WIN32
#include <float.h>
#endif				/* WIN32 */

#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include "multibyte.h"

#include <time.h>
#ifdef HAVE_LOCALE_H
#include <locale.h>
#endif
#include <math.h>
#include <stdlib.h>
#include "statement.h"
#include "qresult.h"
#include "bind.h"
#include "pgtypes.h"
#include "connection.h"
#include "catfunc.h"
#include "pgapifunc.h"

#if defined(UNICODE_SUPPORT) && defined(WIN32)
#define	WIN_UNICODE_SUPPORT
#endif

CSTR NAN_STRING = "NaN";
CSTR INFINITY_STRING = "Infinity";
CSTR MINFINITY_STRING = "-Infinity";

#ifdef	__CYGWIN__
#define TIMEZONE_GLOBAL _timezone
#elif	defined(WIN32) || defined(HAVE_INT_TIMEZONE)
#define TIMEZONE_GLOBAL timezone
#endif

/*
 *	How to map ODBC scalar functions {fn func(args)} to Postgres.
 *	This is just a simple substitution.  List augmented from:
 *	http://www.merant.com/datadirect/download/docs/odbc16/Odbcref/rappc.htm
 *	- thomas 2000-04-03
 */
char *mapFuncs[][2] = {
/*	{ "ASCII",		 "ascii"	  }, built_in */
    {"CHAR", "chr($*)"},
    {"CONCAT", "textcat($*)"},
/*	{ "DIFFERENCE", "difference" }, how to ? */
    {"INSERT",
     "substring($1 from 1 for $2 - 1) || $4 || substring($1 from $2 + $3)"},
    {"LCASE", "lower($*)"},
    {"LEFT", "ltrunc($*)"},
    {"%2LOCATE", "strpos($2,  $1)"},	/* 2 parameters */
    {"%3LOCATE", "strpos(substring($2 from $3), $1) + $3 - 1"},	/* 3 parameters */
    {"LENGTH", "char_length($*)"},
/*	{ "LTRIM",		 "ltrim"	  }, built_in */
    {"RIGHT", "rtrunc($*)"},
    {"SPACE", "repeat('' '', $1)"},
/*	{ "REPEAT",		 "repeat"	  }, built_in */
/*	{ "REPLACE", "replace" }, ??? */
/*	{ "RTRIM",		 "rtrim"	  }, built_in */
/*	{ "SOUNDEX", "soundex" }, how to ? */
    {"SUBSTRING", "substr($*)"},
    {"UCASE", "upper($*)"},

/*	{ "ABS",		 "abs"		  }, built_in */
/*	{ "ACOS",		 "acos"		  }, built_in */
/*	{ "ASIN",		 "asin"		  }, built_in */
/*	{ "ATAN",		 "atan"		  }, built_in */
/*	{ "ATAN2",		 "atan2"	  }, bui;t_in */
    {"CEILING", "ceil($*)"},
/*	{ "COS",		 "cos" 		  }, built_in */
/*	{ "COT",		 "cot" 		  }, built_in */
/*	{ "DEGREES",		 "degrees" 	  }, built_in */
/*	{ "EXP",		 "exp" 		  }, built_in */
/*	{ "FLOOR",		 "floor" 	  }, built_in */
    {"LOG", "ln($*)"},
    {"LOG10", "log($*)"},
/*	{ "MOD",		 "mod" 		  }, built_in */
/*	{ "PI",			 "pi" 		  }, built_in */
    {"POWER", "pow($*)"},
/*	{ "RADIANS",		 "radians"	  }, built_in */
    {"%0RAND", "random()"},	/* 0 parameters */
    {"%1RAND", "(setseed($1) * .0 + random())"},	/* 1 parameters */
/*	{ "ROUND",		 "round"	  }, built_in */
/*	{ "SIGN",		 "sign"		  }, built_in */
/*	{ "SIN",		 "sin"		  }, built_in */
/*	{ "SQRT",		 "sqrt"		  }, built_in */
/*	{ "TAN",		 "tan"		  }, built_in */
    {"TRUNCATE", "trunc($*)"},

    {"CURRENT_DATE", "current_date"},
    {"CURRENT_TIME", "current_time"},
    {"CURRENT_TIMESTAMP", "current_timestamp"},
    {"LOCALTIME", "localtime"},
    {"LOCALTIMESTAMP", "localtimestamp"},
    {"CURRENT_USER", "cast(current_user as text)"},
    {"SESSION_USER", "cast(session_user as text)"},
    {"CURDATE", "current_date"},
    {"CURTIME", "current_time"},
    {"DAYNAME", "to_char($1, 'Day')"},
    {"DAYOFMONTH", "cast(extract(day from $1) as integer)"},
    {"DAYOFWEEK", "(cast(extract(dow from $1) as integer) + 1)"},
    {"DAYOFYEAR", "cast(extract(doy from $1) as integer)"},
    {"HOUR", "cast(extract(hour from $1) as integer)"},
    {"MINUTE", "cast(extract(minute from $1) as integer)"},
    {"MONTH", "cast(extract(month from $1) as integer)"},
    {"MONTHNAME", " to_char($1, 'Month')"},
/*	{ "NOW",		 "now"		  }, built_in */
    {"QUARTER", "cast(extract(quarter from $1) as integer)"},
    {"SECOND", "cast(extract(second from $1) as integer)"},
    {"WEEK", "cast(extract(week from $1) as integer)"},
    {"YEAR", "cast(extract(year from $1) as integer)"},

/*	{ "DATABASE",	 "database"   }, */
    {"IFNULL", "coalesce($*)"},
    {"USER", "cast(current_user as text)"},
    {0, 0}
};

static const char *mapFunction(const char *func, int param_count);
static int conv_from_octal(const UCHAR * s);
static SQLLEN pg_bin2hex(UCHAR * src, UCHAR * dst, SQLLEN length);

/*---------
 *			A Guide for date/time/timestamp conversions
 *
 *			field_type		fCType				Output
 *			----------		------				----------
 *			PG_TYPE_DATE	SQL_C_DEFAULT		SQL_C_DATE
 *			PG_TYPE_DATE	SQL_C_DATE			SQL_C_DATE
 *			PG_TYPE_DATE	SQL_C_TIMESTAMP		SQL_C_TIMESTAMP		(time = 0 (midnight))
 *			PG_TYPE_TIME	SQL_C_DEFAULT		SQL_C_TIME
 *			PG_TYPE_TIME	SQL_C_TIME			SQL_C_TIME
 *			PG_TYPE_TIME	SQL_C_TIMESTAMP		SQL_C_TIMESTAMP		(date = current date)
 *			PG_TYPE_ABSTIME SQL_C_DEFAULT		SQL_C_TIMESTAMP
 *			PG_TYPE_ABSTIME SQL_C_DATE			SQL_C_DATE			(time is truncated)
 *			PG_TYPE_ABSTIME SQL_C_TIME			SQL_C_TIME			(date is truncated)
 *			PG_TYPE_ABSTIME SQL_C_TIMESTAMP		SQL_C_TIMESTAMP
 *---------
 */


/*
 *	Macros for unsigned long handling.
 */
#ifdef	WIN32
#define	ATOI32U	atol
#elif	defined(HAVE_STRTOUL)
#define	ATOI32U(val)	strtoul(val, NULL, 10)
#else				/* HAVE_STRTOUL */
#define	ATOI32U	atol
#endif				/* WIN32 */

/*
 *	Macros for BIGINT handling.
 */
#ifdef	ODBCINT64
#ifdef	WIN32
#define	ATOI64	_atoi64
#define	ATOI64U	_atoi64
#define	FORMATI64	"%I64d"
#define	FORMATI64U	"%I64u"
#elif	(SIZEOF_LONG == 8)
#define	ATOI64(val)	strtol(val, NULL, 10)
#define	ATOI64U(val)	strtoul(val, NULL, 10)
#define	FORMATI64	"%ld"
#define	FORMATI64U	"%lu"
#else
#define	FORMATI64	"%lld"
#define	FORMATI64U	"%llu"
#if	defined(HAVE_STRTOLL)
#define	ATOI64(val)	strtoll(val, NULL, 10)
#define	ATOI64U(val)	strtoull(val, NULL, 10)
#else
static ODBCINT64 ATOI64(const char *val)
{
    ODBCINT64 ll;
    sscanf(val, "%lld", &ll);
    return ll;
}
static unsigned ODBCINT64 ATOI64U(const char *val)
{
    unsigned ODBCINT64 ll;
    sscanf(val, "%llu", &ll);
    return ll;
}
#endif				/* HAVE_STRTOLL */
#endif				/* WIN32 */
#endif				/* ODBCINT64 */

/*
 *	TIMESTAMP <-----> SIMPLE_TIME
 *		precision support since 7.2.
 *		time zone support is unavailable(the stuff is unreliable)
 */
// FIXME: Anyone calling this function is probably doing something undesirable
static BOOL
stime2timestamp(const SIMPLE_TIME * st, char *str, BOOL bZone,
		BOOL precision)
{
    char precstr[16], zonestr[16];
    int i;

    precstr[0] = '\0';
    if (st->infinity > 0)
    {
	strcpy(str, INFINITY_STRING);
	return TRUE;
    } else if (st->infinity < 0)
    {
	strcpy(str, MINFINITY_STRING);
	return TRUE;
    }
    if (precision && st->fr)
    {
	sprintf(precstr, ".%09d", st->fr);
	for (i = 9; i > 0; i--)
	{
	    if (precstr[i] != '0')
		break;
	    precstr[i] = '\0';
	}
    }
    zonestr[0] = '\0';
#ifdef	TIMEZONE_GLOBAL
    if (bZone && tzname[0] && tzname[0][0] && st->y >= 1970)
    {
	long zoneint;
	struct tm tm;
	time_t time0;

	zoneint = TIMEZONE_GLOBAL;
	if (daylight && st->y >= 1900)
	{
	    tm.tm_year = st->y - 1900;
	    tm.tm_mon = st->m - 1;
	    tm.tm_mday = st->d;
	    tm.tm_hour = st->hh;
	    tm.tm_min = st->mm;
	    tm.tm_sec = st->ss;
	    tm.tm_isdst = -1;
	    time0 = mktime(&tm);
	    if (time0 >= 0 && tm.tm_isdst > 0)
		zoneint -= 3600;
	}
	if (zoneint > 0)
	    sprintf(zonestr, "-%02d", (int) zoneint / 3600);
	else
	    sprintf(zonestr, "+%02d", -(int) zoneint / 3600);
    }
#endif				/* TIMEZONE_GLOBAL */
    if (st->y < 0)
	sprintf(str, "%.4d-%.2d-%.2d %.2d:%.2d:%.2d%s%s BC", -st->y,
		st->m, st->d, st->hh, st->mm, st->ss, precstr, zonestr);
    else
	sprintf(str, "%.4d-%.2d-%.2d %.2d:%.2d:%.2d%s%s", st->y, st->m,
		st->d, st->hh, st->mm, st->ss, precstr, zonestr);
    return TRUE;
}

/*	This is called by SQLFetch() */
int
copy_and_convert_field_bindinfo(StatementClass * stmt, OID field_type,
				void *value, int col)
{
    ARDFields *opts = SC_get_ARDF(stmt);
    BindInfoClass *bic = &(opts->bindings[col]);
    SQLULEN offset = opts->row_offset_ptr ? *opts->row_offset_ptr : 0;

    SC_set_current_col(stmt, -1);
    return copy_and_convert_field(stmt, field_type, value,
				  bic->returntype,
				  (PTR) (bic->buffer + offset),
				  bic->buflen, LENADDR_SHIFT(bic->used,
							     offset),
				  LENADDR_SHIFT(bic->indicator,
						offset));
}

static double get_double_value(const char *str)
{
    if (stricmp(str, NAN_STRING) == 0)
	return NAN;
    else if (stricmp(str, INFINITY_STRING) == 0)
	return INFINITY;
    else if (stricmp(str, MINFINITY_STRING) == 0)
	return -INFINITY;
    return atof(str);
}

/*	This is called by SQLGetData() */
int
copy_and_convert_field(StatementClass * stmt, OID field_type,
		       void *valuei, SQLSMALLINT fCType, PTR rgbValue,
		       SQLLEN cbValueMax, SQLLEN * pcbValue,
		       SQLLEN * pIndicator)
{
    CSTR func = "copy_and_convert_field";
    const char *value = (const char *)valuei;
    ARDFields *opts = SC_get_ARDF(stmt);
    GetDataInfo *gdata = SC_get_GDTI(stmt);
    SQLLEN len = 0, copy_len = 0, needbuflen = 0;
    SIMPLE_TIME std_time;
    time_t stmt_t = SC_get_time(stmt);
    struct tm *tim;
    SQLLEN pcbValueOffset, rgbValueOffset;
    char *rgbValueBindRow = NULL;
    SQLLEN *pcbValueBindRow = NULL, *pIndicatorBindRow = NULL;
    const char *ptr;
    SQLSETPOSIROW bind_row = stmt->bind_row;
    int bind_size = opts->bind_size;
    int result = COPY_OK;
    ConnectionClass *conn = SC_get_conn(stmt);
    BOOL changed, true_is_minus1 = FALSE;
    BOOL text_handling, localize_needed;
    const char *neut_str = value;
    char midtemp[2][32];
    int mtemp_cnt = 0;
    GetDataClass *pgdc;
#ifdef	UNICODE_SUPPORT
    BOOL wconverted = FALSE;
#endif				/* UNICODE_SUPPORT */
#ifdef	WIN_UNICODE_SUPPORT
    SQLWCHAR *allocbuf = NULL;
    ssize_t wstrlen;
#endif				/* WIN_UNICODE_SUPPORT */
#ifdef HAVE_LOCALE_H
    char *saved_locale;
#endif				/* HAVE_LOCALE_H */

    if (stmt->current_col >= 0)
    {
	if (stmt->current_col >= opts->allocated)
	{
	    return SQL_ERROR;
	}
	if (gdata->allocated != opts->allocated)
	    extend_getdata_info(gdata, opts->allocated, TRUE);
	pgdc = &gdata->gdata[stmt->current_col];
	if (pgdc->data_left == -2)
	    pgdc->data_left = (cbValueMax > 0) ? 0 : -1;	/* This seems to be *
								 * needed by ADO ? */
	if (pgdc->data_left == 0)
	{
	    if (pgdc->ttlbuf != NULL)
	    {
		free(pgdc->ttlbuf);
		pgdc->ttlbuf = NULL;
		pgdc->ttlbuflen = 0;
	    }
	    pgdc->data_left = -2;	/* needed by ADO ? */
	    return COPY_NO_DATA_FOUND;
	}
    }
	/*---------
	 *	rgbValueOffset is *ONLY* for character and binary data.
	 *	pcbValueOffset is for computing any pcbValue location
	 *---------
	 */

    if (bind_size > 0)
	pcbValueOffset = rgbValueOffset = (bind_size * bind_row);
    else
    {
	pcbValueOffset = bind_row * sizeof(SQLLEN);
	rgbValueOffset = bind_row * cbValueMax;
    }
    /*
     *      The following is applicable in case bind_size > 0
     *      or the fCType is of variable length. 
     */
    if (rgbValue)
	rgbValueBindRow = (char *) rgbValue + rgbValueOffset;
    if (pcbValue)
	pcbValueBindRow = LENADDR_SHIFT(pcbValue, pcbValueOffset);
    if (pIndicator)
    {
	pIndicatorBindRow = LENADDR_SHIFT(pIndicator, pcbValueOffset);
	*pIndicatorBindRow = 0;
    }

    memset(&std_time, 0, sizeof(SIMPLE_TIME));

    /* Initialize current date */
#ifdef	HAVE_GMTIME_R
    struct tm tm;
    tim = gmtime_r(&stmt_t, &tm);
#else
    tim = gmtime(&stmt_t);
#endif				/* HAVE_GMTIME_R */
    std_time.m = tim->tm_mon + 1;
    std_time.d = tim->tm_mday;
    std_time.y = tim->tm_year + 1900;

    mylog
	("copy_and_convert: field_type = %d, fctype = %d, value = '%s', cbValueMax=%d\n",
	 field_type, fCType, (value == NULL) ? "<NULL>" : value,
	 cbValueMax);

    if (!value)
    {
	mylog("null_cvt_date_string=%d\n",
	      conn->connInfo.cvt_null_date_string);
	/* a speicial handling for FOXPRO NULL -> NULL_STRING */
	if (conn->connInfo.cvt_null_date_string > 0 &&
	    PG_TYPE_DATE == field_type &&
	    (SQL_C_CHAR == fCType ||
	     SQL_C_TYPE_DATE == fCType || SQL_C_DEFAULT == fCType))
	{
	    if (pcbValueBindRow)
		*pcbValueBindRow = 0;
	    switch (fCType)
	    {
	    case SQL_C_CHAR:
		if (rgbValueBindRow && cbValueMax > 0)
		    rgbValueBindRow = '\0';
		else
		    result = COPY_RESULT_TRUNCATED;
		break;
	    case SQL_C_TYPE_DATE:
	    case SQL_C_DEFAULT:
		if (rgbValueBindRow
		    && cbValueMax >= sizeof(DATE_STRUCT))
		{
		    memset(rgbValueBindRow, 0, cbValueMax);
		    if (pcbValueBindRow)
			*pcbValueBindRow = sizeof(DATE_STRUCT);
		} else
		    result = COPY_RESULT_TRUNCATED;
		break;
#ifdef	UNICODE_SUPPORT
	    case SQL_C_WCHAR:
		if (rgbValueBindRow && cbValueMax >= WCLEN)
		    memset(rgbValueBindRow, 0, WCLEN);
		else
		    result = COPY_RESULT_TRUNCATED;
		break;
#endif				/* UNICODE_SUPPORT */
	    }
	    return result;
	}
	/*
	 * handle a null just by returning SQL_NULL_DATA in pcbValue, and
	 * doing nothing to the buffer.
	 */
	else if (pIndicator)
	{
	    *pIndicatorBindRow = SQL_NULL_DATA;
	    return COPY_OK;
	} else
	{
	    SC_set_error(stmt, STMT_RETURN_NULL_WITHOUT_INDICATOR,
			 "StrLen_or_IndPtr was a null pointer and NULL data was retrieved",
			 func);
	    return SQL_ERROR;
	}
    }

    if (stmt->hdbc->DataSourceToDriver != NULL)
    {
	size_t length = strlen(value);

	stmt->hdbc->DataSourceToDriver(stmt->hdbc->translation_option,
				       SQL_CHAR, valuei,
				       (SDWORD) length, valuei,
				       (SDWORD) length, NULL, NULL, 0,
				       NULL);
    }

    /*
     * First convert any specific postgres types into more useable data.
     *
     * NOTE: Conversions from PG char/varchar of a date/time/timestamp value
     * to SQL_C_DATE,SQL_C_TIME, SQL_C_TIMESTAMP not supported
     */
    switch (field_type)
    {
    /*
     * $$$ need to add parsing for date/time/timestamp strings in
     * PG_TYPE_CHAR,VARCHAR $$$
     */
    case VX_TYPE_DATETIME:
    {
	long long secs;
	int usecs;
	sscanf(value, "[%lld,%d]", &secs, &usecs);

        // January 1, 1900, 00:00:00. Note: outside the range of 32-bit time_t.
        long long sql_epoch = -2208988800LL; 
        int seconds_per_day = 60*60*24;
        if (secs >= sql_epoch && secs < sql_epoch + seconds_per_day)
        {
            // The value is a time of day for the SQL Epoch, aka a SQL time
            // value.  Fudge it to the Unix Epoch, so gmtime() can deal
            // with it on 32-bit systems.  If it was a DateTime, it was going
            // to be wrong anyway.
            secs += -sql_epoch;
        }

	// FIXME: This loses precision on 32-bit systems.
	time_t secs_time_t = (time_t)secs;

	struct tm *ptm;
#ifdef	HAVE_GMTIME_R
	struct tm tm;
	if ((ptm = gmtime_r(&secs_time_t, &tm)) != NULL)
#else
	if ((ptm = gmtime(&secs_time_t)) != NULL)
#endif				/* HAVE_GMTIME_R */
	{
	    std_time.y = ptm->tm_year + 1900;
	    std_time.m = ptm->tm_mon + 1;
	    std_time.d = ptm->tm_mday;
	    std_time.hh = ptm->tm_hour;
	    std_time.mm = ptm->tm_min;
	    std_time.ss = ptm->tm_sec;
            // The server provides us with millionths of a second, but ODBC
            // uses billionths
            std_time.fr = usecs * 1000;
	}
	break;
    }
    case PG_TYPE_DATE:
	sscanf(value, "%4d-%2d-%2d", &std_time.y, &std_time.m,
	       &std_time.d);
	break;

    case PG_TYPE_TIME:
	sscanf(value, "%2d:%2d:%2d", &std_time.hh, &std_time.mm,
	       &std_time.ss);
	break;

    case PG_TYPE_BOOL:
	{			/* change T/F to 1/0 */
	    char *s;

	    s = midtemp[mtemp_cnt];
	    switch (((char *) value)[0])
	    {
	    case 'f':
	    case 'F':
	    case 'n':
	    case 'N':
	    case '0':
		strcpy(s, "0");
		break;
	    default:
		if (true_is_minus1)
		    strcpy(s, "-1");
		else
		    strcpy(s, "1");
	    }
	    neut_str = midtemp[mtemp_cnt];
	    mtemp_cnt++;
	}
	break;

	/* This is for internal use by SQLStatistics() */
    case PG_TYPE_INT2VECTOR:
	if (SQL_C_DEFAULT == fCType)
	{
	    int i, nval, maxc;
	    const char *vp;
	    /* this is an array of eight integers */
	    short *short_array = (short *) rgbValueBindRow, shortv;

	    maxc = 0;
	    if (NULL != short_array)
		maxc = (int) cbValueMax / sizeof(short);
	    vp = value;
	    nval = 0;
	    mylog("index=(");
	    for (i = 0;; i++)
	    {
		if (sscanf(vp, "%hi", &shortv) != 1)
		    break;
		mylog(" %hi", shortv);
		if (0 == shortv && PG_VERSION_LT(conn, 7.2))
		    break;
		nval++;
		if (nval < maxc)
		    short_array[i + 1] = shortv;

		/* skip the current token */
		while ((*vp != '\0') && (!isspace((UCHAR) * vp)))
		    vp++;
		/* and skip the space to the next token */
		while ((*vp != '\0') && (isspace((UCHAR) * vp)))
		    vp++;
		if (*vp == '\0')
		    break;
	    }
	    mylog(") nval = %i\n", nval);
	    if (maxc > 0)
		short_array[0] = nval;

	    /* There is no corresponding fCType for this. */
	    len = (nval + 1) * sizeof(short);
	    if (pcbValue)
		*pcbValueBindRow = len;

	    if (len <= cbValueMax)
		return COPY_OK;	/* dont go any further or the data will be
				 * trashed */
	    else
		return COPY_RESULT_TRUNCATED;
	}
	break;

	/*
	 * This is a large object OID, which is used to store
	 * LONGVARBINARY objects.
	 */
    case PG_TYPE_LO_UNDEFINED:

	SC_set_error(stmt, STMT_EXEC_ERROR, "Large objects are not supported.",
		     func);
	return SQL_ERROR;

    default:
	break;

    }

    /* Change default into something useable */
    if (fCType == SQL_C_DEFAULT)
    {
	fCType = pgtype_to_ctype(stmt, field_type);
	if (fCType == SQL_C_WCHAR && CC_default_is_c(conn))
	    fCType = SQL_C_CHAR;

	mylog("copy_and_convert, SQL_C_DEFAULT: fCType = %d\n", fCType);
    }

    text_handling = localize_needed = FALSE;
    switch (fCType)
    {
    case INTERNAL_ASIS_TYPE:
#ifdef	UNICODE_SUPPORT
    case SQL_C_WCHAR:
#endif				/* UNICODE_SUPPORT */
    case SQL_C_CHAR:
	text_handling = TRUE;
	break;
    case SQL_C_BINARY:
	switch (field_type)
	{
	case PG_TYPE_UNKNOWN:
	case PG_TYPE_BPCHAR:
	case PG_TYPE_VARCHAR:
	case PG_TYPE_TEXT:
	case PG_TYPE_BPCHARARRAY:
	case PG_TYPE_VARCHARARRAY:
	case PG_TYPE_TEXTARRAY:
	    text_handling = TRUE;
	    break;
	}
	break;
    }
    if (text_handling)
    {
#ifdef	WIN_UNICODE_SUPPORT
	if (SQL_C_CHAR == fCType || SQL_C_BINARY == fCType)
	    localize_needed = TRUE;
#endif				/* WIN_UNICODE_SUPPORT */
    }

    if (text_handling)
    {
	/* Special character formatting as required */

	/*
	 * These really should return error if cbValueMax is not big
	 * enough.
	 */
	switch (field_type)
	{
	case PG_TYPE_DATE:
	    len = 10;
	    if (cbValueMax > len)
		sprintf(rgbValueBindRow, "%.4d-%.2d-%.2d", std_time.y,
			std_time.m, std_time.d);
	    break;

	case PG_TYPE_TIME:
	    len = 8;
	    if (cbValueMax > len)
		sprintf(rgbValueBindRow, "%.2d:%.2d:%.2d", std_time.hh,
			std_time.mm, std_time.ss);
	    break;

	case VX_TYPE_DATETIME:
	    len = 19;
	    if (cbValueMax > len)
		sprintf(rgbValueBindRow,
			"%.4d-%.2d-%.2d %.2d:%.2d:%.2d", std_time.y,
			std_time.m, std_time.d, std_time.hh,
			std_time.mm, std_time.ss);
	    break;

	case PG_TYPE_BOOL:
	    len = strlen(neut_str);
	    if (cbValueMax > len)
	    {
		strcpy(rgbValueBindRow, neut_str);
		mylog("PG_TYPE_BOOL: rgbValueBindRow = '%s'\n",
		      rgbValueBindRow);
	    }
	    break;

	    /*
	     * Currently, data is SILENTLY TRUNCATED for BYTEA and
	     * character data types if there is not enough room in
	     * cbValueMax because the driver can't handle multiple
	     * calls to SQLGetData for these, yet.  Most likely, the
	     * buffer passed in will be big enough to handle the
	     * maximum limit of postgres, anyway.
	     *
	     * LongVarBinary types are handled correctly above, observing
	     * truncation and all that stuff since there is
	     * essentially no limit on the large object used to store
	     * those.
	     */
	case PG_TYPE_BYTEA:	/* convert binary data to hex strings
				 * (i.e, 255 = "FF") */

	default:
	    if (stmt->current_col < 0)
	    {
		pgdc = &(gdata->fdata);
		pgdc->data_left = -1;
	    } else
		pgdc = &gdata->gdata[stmt->current_col];
#ifdef	UNICODE_SUPPORT
	    if (fCType == SQL_C_WCHAR)
		wconverted = TRUE;
#endif				/* UNICODE_SUPPORT */
	    if (pgdc->data_left < 0)
	    {
		BOOL lf_conv = conn->connInfo.lf_conversion;
#ifdef	UNICODE_SUPPORT
		if (fCType == SQL_C_WCHAR)
		{
		    len =
			utf8_to_ucs2_lf(neut_str, SQL_NTS, lf_conv,
					NULL, 0);
		    len *= WCLEN;
		    changed = TRUE;
		} else
#endif				/* UNICODE_SUPPORT */
		if (PG_TYPE_BYTEA == field_type)
		{
		    len = convert_from_pgbinary((const UCHAR *)neut_str, NULL, 0);
		    len *= 2;
		    changed = TRUE;
		} else
#ifdef	WIN_UNICODE_SUPPORT
		if (localize_needed)
		{
		    wstrlen =
			utf8_to_ucs2_lf(neut_str, SQL_NTS, lf_conv,
					NULL, 0);
		    allocbuf =
			(SQLWCHAR *) malloc(WCLEN * (wstrlen + 1));
		    wstrlen =
			utf8_to_ucs2_lf(neut_str, SQL_NTS, lf_conv,
					allocbuf, wstrlen + 1);
		    len =
			WideCharToMultiByte(CP_ACP, 0,
					    (LPCWSTR) allocbuf,
					    (int) wstrlen, NULL, 0,
					    NULL, NULL);
		    changed = TRUE;
		} else
#endif				/* WIN_UNICODE_SUPPORT */
		    /* convert linefeeds to carriage-return/linefeed */
		    len =
			convert_linefeeds(neut_str, NULL, 0, lf_conv,
					  &changed);
		if (cbValueMax == 0)	/* just returns length
					 * info */
		{
		    result = COPY_RESULT_TRUNCATED;
#ifdef	WIN_UNICODE_SUPPORT
		    if (allocbuf)
			free(allocbuf);
#endif				/* WIN_UNICODE_SUPPORT */
		    break;
		}
		if (!pgdc->ttlbuf)
		    pgdc->ttlbuflen = 0;
		needbuflen = len;
		switch (fCType)
		{
#ifdef	UNICODE_SUPPORT
		case SQL_C_WCHAR:
		    needbuflen += WCLEN;
		    break;
#endif				/* UNICODE_SUPPORT */
		case SQL_C_BINARY:
		    break;
		default:
		    needbuflen++;
		}
		if (changed || needbuflen > cbValueMax)
		{
		    if (needbuflen > (SQLLEN) pgdc->ttlbuflen)
		    {
			pgdc->ttlbuf = (char *)
			    realloc(pgdc->ttlbuf, needbuflen);
			pgdc->ttlbuflen = needbuflen;
		    }
#ifdef	UNICODE_SUPPORT
		    if (fCType == SQL_C_WCHAR)
		    {
			utf8_to_ucs2_lf(neut_str, SQL_NTS, lf_conv,
					(SQLWCHAR *) pgdc->ttlbuf,
					len / WCLEN);
		    } else
#endif				/* UNICODE_SUPPORT */
		    if (PG_TYPE_BYTEA == field_type)
		    {
			len =
			    convert_from_pgbinary((UCHAR *)neut_str,
						  (UCHAR *)pgdc->ttlbuf,
						  pgdc->ttlbuflen);
			pg_bin2hex((UCHAR *)pgdc->ttlbuf,
				   (UCHAR *)pgdc->ttlbuf, len);
			len *= 2;
		    } else
#ifdef	WIN_UNICODE_SUPPORT
		    if (localize_needed)
		    {
			len =
			    WideCharToMultiByte(CP_ACP, 0, allocbuf,
						(int) wstrlen,
						pgdc->ttlbuf,
						(int) pgdc->ttlbuflen,
						NULL, NULL);
			free(allocbuf);
			allocbuf = NULL;
		    } else
#endif				/* WIN_UNICODE_SUPPORT */
			convert_linefeeds(neut_str, pgdc->ttlbuf,
					  pgdc->ttlbuflen, lf_conv,
					  &changed);
		    ptr = pgdc->ttlbuf;
		    pgdc->ttlbufused = len;
		} else
		{
		    if (pgdc->ttlbuf)
		    {
			free(pgdc->ttlbuf);
			pgdc->ttlbuf = NULL;
		    }
		    ptr = neut_str;
		}
	    } else
	    {
		ptr = pgdc->ttlbuf;
		len = pgdc->ttlbufused;
	    }

	    mylog("DEFAULT: len = %d, ptr = '%.*s'\n", len, len, ptr);

	    if (stmt->current_col >= 0)
	    {
		if (pgdc->data_left > 0)
		{
		    ptr += len - pgdc->data_left;
		    len = pgdc->data_left;
		    needbuflen =
			len + (pgdc->ttlbuflen - pgdc->ttlbufused);
		} else
		    pgdc->data_left = len;
	    }

	    if (cbValueMax > 0)
	    {
		BOOL already_copied = FALSE;

		if (fCType == SQL_C_BINARY)
		    copy_len = (len > cbValueMax) ? cbValueMax : len;
		else
		    copy_len =
			(len >= cbValueMax) ? (cbValueMax - 1) : len;
#ifdef	UNICODE_SUPPORT
		if (fCType == SQL_C_WCHAR)
		{
		    copy_len /= WCLEN;
		    copy_len *= WCLEN;
		}
#endif				/* UNICODE_SUPPORT */
#ifdef HAVE_LOCALE_H
		switch (field_type)
		{
		case PG_TYPE_FLOAT4:
		case PG_TYPE_FLOAT8:
		case PG_TYPE_NUMERIC:
		    {
			struct lconv *lc;
			char *new_string;
			int i, j;

			new_string = (char *)malloc(cbValueMax);
			lc = localeconv();
			for (i = 0, j = 0; ptr[i]; i++)
			    if (ptr[i] == '.')
			    {
				strncpy(&new_string[j],
					lc->decimal_point,
					strlen(lc->decimal_point));
				j += strlen(lc->decimal_point);
			    } else
				new_string[j++] = ptr[i];
			new_string[j] = '\0';
			strncpy_null(rgbValueBindRow, new_string,
				     copy_len + 1);
			free(new_string);
			already_copied = TRUE;
			break;
		    }
		}
#endif				/* HAVE_LOCALE_H */
		if (!already_copied)
		{
		    /* Copy the data */
		    memcpy(rgbValueBindRow, ptr, copy_len);
		    /* Add null terminator */
#ifdef	UNICODE_SUPPORT
		    if (fCType == SQL_C_WCHAR)
		    {
			if (copy_len + WCLEN <= cbValueMax)
			    memset(rgbValueBindRow + copy_len, 0,
				   WCLEN);
		    } else
#endif				/* UNICODE_SUPPORT */
		    if (copy_len < cbValueMax)
			rgbValueBindRow[copy_len] = '\0';
		}
		/* Adjust data_left for next time */
		if (stmt->current_col >= 0)
		    pgdc->data_left -= copy_len;
	    }

	    /*
	     * Finally, check for truncation so that proper status can
	     * be returned
	     */
	    if (cbValueMax > 0 && needbuflen > cbValueMax)
		result = COPY_RESULT_TRUNCATED;
	    else
	    {
		if (pgdc->ttlbuf != NULL)
		{
		    free(pgdc->ttlbuf);
		    pgdc->ttlbuf = NULL;
		}
	    }


	    if (SQL_C_WCHAR == fCType)
		mylog
		    ("    SQL_C_WCHAR, default: len = %d, cbValueMax = %d, rgbValueBindRow = '%s'\n",
		     len, cbValueMax, rgbValueBindRow);
	    else if (SQL_C_BINARY == fCType)
		mylog
		    ("    SQL_C_BINARY, default: len = %d, cbValueMax = %d, rgbValueBindRow = '%.*s'\n",
		     len, cbValueMax, copy_len, rgbValueBindRow);
	    else
		mylog
		    ("    SQL_C_CHAR, default: len = %d, cbValueMax = %d, rgbValueBindRow = '%s'\n",
		     len, cbValueMax, rgbValueBindRow);
	    break;
	}
#ifdef	UNICODE_SUPPORT
	if (SQL_C_WCHAR == fCType && !wconverted)
	{
	    char *str = strdup(rgbValueBindRow);
	    SQLLEN ucount =
		utf8_to_ucs2(str, len, (SQLWCHAR *) rgbValueBindRow,
			     cbValueMax / WCLEN);
	    if (cbValueMax < WCLEN * ucount)
		result = COPY_RESULT_TRUNCATED;
	    len = ucount * WCLEN;
	    free(str);
	}
#endif				/* UNICODE_SUPPORT */

    } else
    {
	/*
	 * for SQL_C_CHAR, it's probably ok to leave currency symbols in.
	 * But to convert to numeric types, it is necessary to get rid of
	 * those.
	 */
	if (field_type == PG_TYPE_MONEY)
	{
	    if (convert_money
		(neut_str, midtemp[mtemp_cnt], sizeof(midtemp[0])))
	    {
		neut_str = midtemp[mtemp_cnt];
		mtemp_cnt++;
	    } else
	    {
		qlog("couldn't convert money type to %d\n", fCType);
		return COPY_UNSUPPORTED_TYPE;
	    }
	}

	switch (fCType)
	{
	case SQL_C_DATE:
	case SQL_C_TYPE_DATE:	/* 91 */
	    len = 6;
	    {
		DATE_STRUCT *ds;

		if (bind_size > 0)
		    ds = (DATE_STRUCT *) rgbValueBindRow;
		else
		    ds = (DATE_STRUCT *) rgbValue + bind_row;
		ds->year = std_time.y;
		ds->month = std_time.m;
		ds->day = std_time.d;
	    }
	    break;

	case SQL_C_TIME:
	case SQL_C_TYPE_TIME:	/* 92 */
	    len = 6;
	    {
		TIME_STRUCT *ts;

		if (bind_size > 0)
		    ts = (TIME_STRUCT *) rgbValueBindRow;
		else
		    ts = (TIME_STRUCT *) rgbValue + bind_row;
		ts->hour = std_time.hh;
		ts->minute = std_time.mm;
		ts->second = std_time.ss;
	    }
	    break;

	case SQL_C_TIMESTAMP:
	case SQL_C_TYPE_TIMESTAMP:	/* 93 */
	    len = 16;
	    {
		TIMESTAMP_STRUCT *ts;

		if (bind_size > 0)
		    ts = (TIMESTAMP_STRUCT *) rgbValueBindRow;
		else
		    ts = (TIMESTAMP_STRUCT *) rgbValue + bind_row;
		ts->year = std_time.y;
		ts->month = std_time.m;
		ts->day = std_time.d;
		ts->hour = std_time.hh;
		ts->minute = std_time.mm;
		ts->second = std_time.ss;
		ts->fraction = std_time.fr;
	    }
	    break;

	case SQL_C_BIT:
	    len = 1;
	    if (bind_size > 0)
		*((UCHAR *) rgbValueBindRow) = atoi(neut_str);
	    else
		*((UCHAR *) rgbValue + bind_row) = atoi(neut_str);

	    /*
	     * mylog("SQL_C_BIT: bind_row = %d val = %d, cb = %d,
	     * rgb=%d\n", bind_row, atoi(neut_str), cbValueMax,
	     * *((UCHAR *)rgbValue));
	     */
	    break;

	case SQL_C_STINYINT:
	case SQL_C_TINYINT:
	    len = 1;
	    if (bind_size > 0)
		*((SCHAR *) rgbValueBindRow) = atoi(neut_str);
	    else
		*((SCHAR *) rgbValue + bind_row) = atoi(neut_str);
	    break;

	case SQL_C_UTINYINT:
	    len = 1;
	    if (bind_size > 0)
		*((UCHAR *) rgbValueBindRow) = atoi(neut_str);
	    else
		*((UCHAR *) rgbValue + bind_row) = atoi(neut_str);
	    break;

	case SQL_C_FLOAT:
#ifdef HAVE_LOCALE_H
	    saved_locale = strdup(setlocale(LC_ALL, NULL));
	    setlocale(LC_ALL, "C");
#endif				/* HAVE_LOCALE_H */
	    len = 4;
	    if (bind_size > 0)
		*((SFLOAT *) rgbValueBindRow) =
		    (float) get_double_value(neut_str);
	    else
		*((SFLOAT *) rgbValue + bind_row) =
		    (float) get_double_value(neut_str);
#ifdef HAVE_LOCALE_H
	    setlocale(LC_ALL, saved_locale);
	    free(saved_locale);
#endif				/* HAVE_LOCALE_H */
	    break;

	case SQL_C_DOUBLE:
#ifdef HAVE_LOCALE_H
	    saved_locale = strdup(setlocale(LC_ALL, NULL));
	    setlocale(LC_ALL, "C");
#endif				/* HAVE_LOCALE_H */
	    len = 8;
	    if (bind_size > 0)
		*((SDOUBLE *) rgbValueBindRow) =
		    get_double_value(neut_str);
	    else
		*((SDOUBLE *) rgbValue + bind_row) =
		    get_double_value(neut_str);
#ifdef HAVE_LOCALE_H
	    setlocale(LC_ALL, saved_locale);
	    free(saved_locale);
#endif				/* HAVE_LOCALE_H */
	    break;

	case SQL_C_NUMERIC:
#ifdef HAVE_LOCALE_H
	    /* strcpy(saved_locale, setlocale(LC_ALL, NULL));
	       setlocale(LC_ALL, "C"); not needed currently */
#endif				/* HAVE_LOCALE_H */
	    {
		SQL_NUMERIC_STRUCT *ns;
		int i, nlen, bit, hval, tv, dig, sta, olen;
		char calv[SQL_MAX_NUMERIC_LEN * 3];
		const char *wv;
		BOOL dot_exist;

		len = sizeof(SQL_NUMERIC_STRUCT);
		if (bind_size > 0)
		    ns = (SQL_NUMERIC_STRUCT *) rgbValueBindRow;
		else
		    ns = (SQL_NUMERIC_STRUCT *) rgbValue + bind_row;
		for (wv = neut_str; *wv && isspace(*wv); wv++)
		    ;
		ns->sign = 1;
		if (*wv == '-')
		{
		    ns->sign = 0;
		    wv++;
		} else if (*wv == '+')
		    wv++;
		while (*wv == '0')
		    wv++;
		ns->precision = 0;
		ns->scale = 0;
		for (nlen = 0, dot_exist = FALSE;; wv++)
		{
		    if (*wv == '.')
		    {
			if (dot_exist)
			    break;
			dot_exist = TRUE;
		    } else if (!isdigit(*wv))
			break;
		    else
		    {
			if (dot_exist)
			    ns->scale++;
			ns->precision++;
			calv[nlen++] = *wv;
		    }
		}
		memset(ns->val, 0, sizeof(ns->val));
		for (hval = 0, bit = 1L, sta = 0, olen = 0; sta < nlen;)
		{
		    for (dig = 0, i = sta; i < nlen; i++)
		    {
			tv = dig * 10 + calv[i] - '0';
			dig = tv % 2;
			calv[i] = tv / 2 + '0';
			if (i == sta && tv < 2)
			    sta++;
		    }
		    if (dig > 0)
			hval |= bit;
		    bit <<= 1;
		    if (bit >= (1L << 8))
		    {
			ns->val[olen++] = hval;
			hval = 0;
			bit = 1L;
			if (olen >= SQL_MAX_NUMERIC_LEN - 1)
			{
			    ns->scale = sta - ns->precision;
			    break;
			}
		    }
		}
		if (hval && olen < SQL_MAX_NUMERIC_LEN - 1)
		    ns->val[olen++] = hval;
	    }
#ifdef HAVE_LOCALE_H
	    /* setlocale(LC_ALL, saved_locale); */
#endif				/* HAVE_LOCALE_H */
	    break;

	case SQL_C_SSHORT:
	case SQL_C_SHORT:
	    len = 2;
	    if (bind_size > 0)
		*((SQLSMALLINT *) rgbValueBindRow) = atoi(neut_str);
	    else
		*((SQLSMALLINT *) rgbValue + bind_row) = atoi(neut_str);
	    break;

	case SQL_C_USHORT:
	    len = 2;
	    if (bind_size > 0)
		*((SQLUSMALLINT *) rgbValueBindRow) = atoi(neut_str);
	    else
		*((SQLUSMALLINT *) rgbValue + bind_row) =
		    atoi(neut_str);
	    break;

	case SQL_C_SLONG:
	case SQL_C_LONG:
	    len = 4;
	    if (bind_size > 0)
		*((SQLINTEGER *) rgbValueBindRow) = atol(neut_str);
	    else
		*((SQLINTEGER *) rgbValue + bind_row) = atol(neut_str);
	    break;

	case SQL_C_ULONG:
	    len = 4;
	    if (bind_size > 0)
		*((SQLUINTEGER *) rgbValueBindRow) = ATOI32U(neut_str);
	    else
		*((SQLUINTEGER *) rgbValue + bind_row) =
		    ATOI32U(neut_str);
	    break;

	case SQL_C_SBIGINT:
	    len = 8;
	    if (bind_size > 0)
		*((SQLBIGINT *) rgbValueBindRow) = ATOI64(neut_str);
	    else
		*((SQLBIGINT *) rgbValue + bind_row) = ATOI64(neut_str);
	    break;

	case SQL_C_UBIGINT:
	    len = 8;
	    if (bind_size > 0)
		*((SQLUBIGINT *) rgbValueBindRow) = ATOI64U(neut_str);
	    else
		*((SQLUBIGINT *) rgbValue + bind_row) =
		    ATOI64U(neut_str);
	    break;

	case SQL_C_BINARY:
	    if (PG_TYPE_UNKNOWN == field_type ||
		PG_TYPE_TEXT == field_type ||
		PG_TYPE_VARCHAR == field_type ||
		PG_TYPE_BPCHAR == field_type ||
		PG_TYPE_TEXTARRAY == field_type ||
		PG_TYPE_VARCHARARRAY == field_type ||
		PG_TYPE_BPCHARARRAY == field_type)
	    {
		ssize_t len = SQL_NULL_DATA;

		if (neut_str)
		    len = strlen(neut_str);
		if (pcbValue)
		    *pcbValueBindRow = len;
		if (len > 0 && cbValueMax > 0)
		{
		    memcpy(rgbValueBindRow, neut_str,
			   len < cbValueMax ? len : cbValueMax);
		    if (cbValueMax >= len + 1)
			rgbValueBindRow[len] = '\0';
		}
		if (cbValueMax >= len)
		    return COPY_OK;
		else
		    return COPY_RESULT_TRUNCATED;
	    }
	    /* The following is for SQL_C_VARBOOKMARK */
	    else if (PG_TYPE_INT4 == field_type)
	    {
		UInt4 ival = ATOI32U(neut_str);

		inolog("SQL_C_VARBOOKMARK value=%d\n", ival);
		if (pcbValue)
		    *pcbValueBindRow = sizeof(ival);
		if (cbValueMax >= sizeof(ival))
		{
		    memcpy(rgbValueBindRow, &ival, sizeof(ival));
		    return COPY_OK;
		} else
		    return COPY_RESULT_TRUNCATED;
	    } else if (PG_TYPE_BYTEA != field_type)
	    {
		mylog("couldn't convert the type %d to SQL_C_BINARY\n",
		      field_type);
		qlog("couldn't convert the type %d to SQL_C_BINARY\n",
		     field_type);
		return COPY_UNSUPPORTED_TYPE;
	    }
	    /* truncate if necessary */
	    /* convert octal escapes to bytes */

	    if (stmt->current_col < 0)
	    {
		pgdc = &(gdata->fdata);
		pgdc->data_left = -1;
	    } else
		pgdc = &gdata->gdata[stmt->current_col];
	    if (!pgdc->ttlbuf)
		pgdc->ttlbuflen = 0;
	    if (pgdc->data_left < 0)
	    {
		if (cbValueMax <= 0)
		{
		    len = convert_from_pgbinary((const UCHAR *)neut_str, NULL, 0);
		    result = COPY_RESULT_TRUNCATED;
		    break;
		}
		if (len =
		    strlen(neut_str), len >= (int) pgdc->ttlbuflen)
		{
		    pgdc->ttlbuf = (char *)realloc(pgdc->ttlbuf, len + 1);
		    pgdc->ttlbuflen = len + 1;
		}
		len =
		    convert_from_pgbinary((const UCHAR *)neut_str,
					  (UCHAR *)pgdc->ttlbuf,
					  pgdc->ttlbuflen);
		pgdc->ttlbufused = len;
	    } else
		len = pgdc->ttlbufused;
	    ptr = pgdc->ttlbuf;

	    if (stmt->current_col >= 0)
	    {
		/*
		 * Second (or more) call to SQLGetData so move the
		 * pointer
		 */
		if (pgdc->data_left > 0)
		{
		    ptr += len - pgdc->data_left;
		    len = pgdc->data_left;
		}

		/* First call to SQLGetData so initialize data_left */
		else
		    pgdc->data_left = len;

	    }

	    if (cbValueMax > 0)
	    {
		copy_len = (len > cbValueMax) ? cbValueMax : len;

		/* Copy the data */
		memcpy(rgbValueBindRow, ptr, copy_len);

		/* Adjust data_left for next time */
		if (stmt->current_col >= 0)
		    pgdc->data_left -= copy_len;
	    }

	    /*
	     * Finally, check for truncation so that proper status can
	     * be returned
	     */
	    if (len > cbValueMax)
		result = COPY_RESULT_TRUNCATED;
	    else if (pgdc->ttlbuf)
	    {
		free(pgdc->ttlbuf);
		pgdc->ttlbuf = NULL;
	    }
	    mylog("SQL_C_BINARY: len = %d, copy_len = %d\n", len,
		  copy_len);
	    break;

	default:
	    qlog("conversion to the type %d isn't supported\n", fCType);
	    return COPY_UNSUPPORTED_TYPE;
	}
    }

    /* store the length of what was copied, if there's a place for it */
    if (pcbValue)
	*pcbValueBindRow = len;

    if (result == COPY_OK && stmt->current_col >= 0)
	gdata->gdata[stmt->current_col].data_left = 0;
    return result;

}


/*--------------------------------------------------------------------
 *	Functions/Macros to get rid of query size limit.
 *
 *	I always used the follwoing macros to convert from
 *	old_statement to new_statement.  Please improve it
 *	if you have a better way.	Hiroshi 2001/05/22
 *--------------------------------------------------------------------
 */

#define	FLGP_PREPARE_DUMMY_CURSOR	1L
#define	FLGP_CURSOR_CHECK_OK	(1L << 1)
#define	FLGP_SELECT_INTO		(1L << 2)
#define	FLGP_SELECT_FOR_UPDATE	(1L << 3)
#define	FLGP_BUILDING_PREPARE_STATEMENT	(1L << 4)
#define	FLGP_MULTIPLE_STATEMENT	(1L << 5)
typedef struct _QueryParse {
    const char *statement;
    int statement_type;
    size_t opos;
    Int4 from_pos;		/* PG comm length restriction */
    Int4 where_pos;		/* PG comm length restriction */
    ssize_t stmt_len;
    char in_literal, in_identifier, in_escape, in_dollar_quote;
    const char *dollar_tag;
    ssize_t taglen;
    char token_save[64];
    int token_len;
    BOOL prev_token_end;
    BOOL proc_no_param;
    size_t declare_pos;
    UInt4 flags;
    encoded_str encstr;
} QueryParse;

static ssize_t
QP_initialize(QueryParse * q, const StatementClass * stmt)
{
    q->statement =
	stmt->execute_statement ? stmt->execute_statement : stmt->
	statement;
    q->statement_type = stmt->statement_type;
    q->opos = 0;
    q->from_pos = -1;
    q->where_pos = -1;
    q->stmt_len = (q->statement) ? strlen(q->statement) : -1;
    q->in_literal = q->in_identifier = q->in_escape =
	q->in_dollar_quote = FALSE;
    q->dollar_tag = NULL;
    q->taglen = -1;
    q->token_save[0] = '\0';
    q->token_len = 0;
    q->prev_token_end = TRUE;
    q->proc_no_param = TRUE;
    q->declare_pos = 0;
    q->flags = 0;
    make_encoded_str(&q->encstr, SC_get_conn(stmt), q->statement);

    return q->stmt_len;
}

#define	FLGB_PRE_EXECUTING	1L
#define	FLGB_BUILDING_PREPARE_STATEMENT	(1L << 1)
#define	FLGB_BUILDING_BIND_REQUEST	(1L << 2)
#define	FLGB_EXECUTE_PREPARED		(1L << 3)

#define	FLGB_INACCURATE_RESULT	(1L << 4)
#define	FLGB_CREATE_KEYSET	(1L << 5)
#define	FLGB_KEYSET_DRIVEN	(1L << 6)
#define	FLGB_CONVERT_LF		(1L << 7)
#define	FLGB_DISCARD_OUTPUT	(1L << 8)
#define	FLGB_BINARY_AS_POSSIBLE	(1L << 9)
#define	FLGB_LITERAL_EXTENSION	(1L << 10)
typedef struct _QueryBuild {
    char *query_statement;
    size_t str_size_limit;
    size_t str_alsize;
    size_t npos;
    SQLLEN current_row;
    Int2 param_number;
    Int2 dollar_number;
    Int2 num_io_params;
    Int2 num_output_params;
    Int2 num_discard_params;
    Int2 proc_return;
    APDFields *apdopts;
    IPDFields *ipdopts;
    size_t load_stmt_len;
    UInt4 flags;
    int ccsc;
    int errornumber;
    const char *errormsg;

    ConnectionClass *conn;	/* mainly needed for LO handling */
    StatementClass *stmt;	/* needed to set error info in ENLARGE_.. */
} QueryBuild;

#define INIT_MIN_ALLOC	4096
static ssize_t
QB_initialize(QueryBuild * qb, size_t size, StatementClass * stmt,
	      ConnectionClass * conn)
{
    size_t newsize = 0;

    qb->flags = 0;
    qb->load_stmt_len = 0;
    qb->stmt = stmt;
    qb->apdopts = NULL;
    qb->ipdopts = NULL;
    qb->proc_return = 0;
    qb->num_io_params = 0;
    qb->num_output_params = 0;
    qb->num_discard_params = 0;
    if (conn)
	qb->conn = conn;
    else if (stmt)
    {
	Int2 dummy;

	qb->apdopts = SC_get_APDF(stmt);
	qb->ipdopts = SC_get_IPDF(stmt);
	qb->conn = SC_get_conn(stmt);
	if (stmt->pre_executing)
	    qb->flags |= FLGB_PRE_EXECUTING;
	if (stmt->discard_output_params)
	    qb->flags |= FLGB_DISCARD_OUTPUT;
	qb->num_io_params =
	    CountParameters(stmt, NULL, &dummy, &qb->num_output_params);
	qb->proc_return = stmt->proc_return;
	if (0 != (qb->flags & FLGB_DISCARD_OUTPUT))
	    qb->num_discard_params = qb->num_output_params;
	if (qb->num_discard_params < qb->proc_return)
	    qb->num_discard_params = qb->proc_return;
    } else
    {
	qb->conn = NULL;
	return -1;
    }
    if (qb->conn->connInfo.lf_conversion)
	qb->flags |= FLGB_CONVERT_LF;
    qb->ccsc = qb->conn->ccsc;
    if (CC_get_escape(qb->conn) && PG_VERSION_GE(qb->conn, 8.1))
	qb->flags |= FLGB_LITERAL_EXTENSION;

    if (stmt)
	qb->str_size_limit = stmt->stmt_size_limit;
    else
	qb->str_size_limit = -1;
    if (qb->str_size_limit > 0)
    {
	if (size > qb->str_size_limit)
	    return -1;
	newsize = qb->str_size_limit;
    }
    else
    {
	newsize = INIT_MIN_ALLOC;
	while (newsize <= size)
	    newsize *= 2;
    }
    if ((qb->query_statement = (char *)malloc(newsize)) == NULL)
    {
	qb->str_alsize = 0;
	return -1;
    }
    qb->query_statement[0] = '\0';
    qb->str_alsize = newsize;
    qb->npos = 0;
    qb->current_row =
	stmt->exec_current_row < 0 ? 0 : stmt->exec_current_row;
    qb->param_number = -1;
    qb->dollar_number = 0;
    qb->errornumber = 0;
    qb->errormsg = NULL;

    return newsize;
}

static int
QB_initialize_copy(QueryBuild * qb_to, const QueryBuild * qb_from,
		   UInt4 size)
{
    memcpy(qb_to, qb_from, sizeof(QueryBuild));

    if (qb_to->str_size_limit > 0)
    {
	if (size > qb_to->str_size_limit)
	    return -1;
    }
    if ((qb_to->query_statement = (char *)malloc(size)) == NULL)
    {
	qb_to->str_alsize = 0;
	return -1;
    }
    qb_to->query_statement[0] = '\0';
    qb_to->str_alsize = size;
    qb_to->npos = 0;

    return size;
}

static void
QB_replace_SC_error(StatementClass * stmt, const QueryBuild * qb,
		    const char *func)
{
    int number;

    if (0 == qb->errornumber)
	return;
    if ((number = SC_get_errornumber(stmt)) > 0)
	return;
    if (number < 0 && qb->errornumber < 0)
	return;
    SC_set_error(stmt, qb->errornumber, qb->errormsg, func);
}

static void QB_Destructor(QueryBuild * qb)
{
    if (qb->query_statement)
    {
	free(qb->query_statement);
	qb->query_statement = NULL;
	qb->str_alsize = 0;
    }
}

/*
 * New macros (Aceto)
 *--------------------
 */

#define F_OldChar(qp) \
qp->statement[qp->opos]

#define F_OldPtr(qp) \
(qp->statement + qp->opos)

#define F_OldNext(qp) \
(++qp->opos)

#define F_OldPrior(qp) \
(--qp->opos)

#define F_OldPos(qp) \
qp->opos

#define F_ExtractOldTo(qp, buf, ch, maxsize) \
do { \
	size_t	c = 0; \
	while (qp->statement[qp->opos] != '\0' && qp->statement[qp->opos] != ch) \
	{ \
		if (c >= maxsize) \
			break; \
		buf[c++] = qp->statement[qp->opos++]; \
	} \
	if (qp->statement[qp->opos] == '\0') \
		return SQL_ERROR; \
	buf[c] = '\0'; \
} while (0)

#define F_NewChar(qb) \
qb->query_statement[qb->npos]

#define F_NewPtr(qb) \
(qb->query_statement + qb->npos)

#define F_NewNext(qb) \
(++qb->npos)

#define F_NewPos(qb) \
(qb->npos)


static int convert_escape(QueryParse * qp, QueryBuild * qb);
static int
processParameters(QueryParse * qp, QueryBuild * qb,
		  size_t * output_count, SQLLEN param_pos[][2]);
static size_t
convert_to_pgbinary(const UCHAR * in, char *out, size_t len,
		    QueryBuild * qb);

static ssize_t enlarge_query_statement(QueryBuild * qb, size_t newsize)
{
    size_t newalsize = INIT_MIN_ALLOC;
    CSTR func = "enlarge_statement";

    if (qb->str_size_limit > 0 && qb->str_size_limit < (int) newsize)
    {
	free(qb->query_statement);
	qb->query_statement = NULL;
	qb->str_alsize = 0;
	if (qb->stmt)
	{

	    SC_set_error(qb->stmt, STMT_EXEC_ERROR,
			 "Query buffer overflow in copy_statement_with_parameters",
			 func);
	} else
	{
	    qb->errormsg =
		"Query buffer overflow in copy_statement_with_parameters";
	    qb->errornumber = STMT_EXEC_ERROR;
	}
	return -1;
    }
    while (newalsize <= newsize)
	newalsize *= 2;
    if (!
	(qb->query_statement = (char *)realloc(qb->query_statement, newalsize)))
    {
	qb->str_alsize = 0;
	if (qb->stmt)
	{
	    SC_set_error(qb->stmt, STMT_EXEC_ERROR,
			 "Query buffer allocate error in copy_statement_with_parameters",
			 func);
	} else
	{
	    qb->errormsg =
		"Query buffer allocate error in copy_statement_with_parameters";
	    qb->errornumber = STMT_EXEC_ERROR;
	}
	return 0;
    }
    qb->str_alsize = newalsize;
    return newalsize;
}

/*----------
 *	Enlarge stmt_with_params if necessary.
 *----------
 */
#define ENLARGE_NEWSTATEMENT(qb, newpos) \
	if (newpos >= qb->str_alsize) \
	{ \
		if (enlarge_query_statement(qb, newpos) <= 0) \
			return SQL_ERROR; \
	}

/*----------
 *	Terminate the stmt_with_params string with NULL.
 *----------
 */
#define CVT_TERMINATE(qb) \
do { \
	qb->query_statement[qb->npos] = '\0'; \
} while (0)

/*----------
 *	Append a data.
 *----------
 */
#define CVT_APPEND_DATA(qb, s, len) \
do { \
	size_t	newpos = qb->npos + len; \
	ENLARGE_NEWSTATEMENT(qb, newpos) \
	memcpy(&qb->query_statement[qb->npos], s, len); \
	qb->npos = newpos; \
	qb->query_statement[newpos] = '\0'; \
} while (0)

/*----------
 *	Append a string.
 *----------
 */
#define CVT_APPEND_STR(qb, s) \
do { \
	size_t	len = strlen(s); \
	CVT_APPEND_DATA(qb, s, len); \
} while (0)

/*----------
 *	Append a char.
 *----------
 */
#define CVT_APPEND_CHAR(qb, c) \
do { \
	ENLARGE_NEWSTATEMENT(qb, qb->npos + 1); \
	qb->query_statement[qb->npos++] = c; \
} while (0)

/*----------
 *	Append a binary data.
 *	Newly reqeuired size may be overestimated currently.
 *----------
 */
#define CVT_APPEND_BINARY(qb, buf, used) \
do { \
	size_t	newlimit = qb->npos + 5 * used; \
	ENLARGE_NEWSTATEMENT(qb, newlimit); \
	qb->npos += convert_to_pgbinary((const UCHAR *)buf, &qb->query_statement[qb->npos], used, qb); \
} while (0)

/*----------
 *
 *----------
 */
#define CVT_SPECIAL_CHARS(qb, buf, used) \
do { \
	size_t cnvlen = convert_special_chars(buf, NULL, used, qb->flags, qb->ccsc, CC_get_escape(qb->conn)); \
	size_t	newlimit = qb->npos + cnvlen; \
\
	ENLARGE_NEWSTATEMENT(qb, newlimit); \
	convert_special_chars(buf, &qb->query_statement[qb->npos], used, qb->flags, qb->ccsc, CC_get_escape(qb->conn)); \
	qb->npos += cnvlen; \
} while (0)

#ifdef NOT_USED
#define CVT_TEXT_FIELD(qb, buf, used) \
do { \
	char	escape_ch = CC_get_escape(qb->conn); \
	int flags = ((0 != qb->flags & FLGB_CONVERT_LF) ? CONVERT_CRLF_TO_LF : 0) | ((0 != qb->flags & FLGB_BUILDING_BIND_REQUEST) ? 0 : DOUBLE_LITERAL_QUOTE | (escape_ch ? DOUBLE_LITERAL_IN_ESCAPE : 0)); \
	int cnvlen = (flags & (DOUBLE_LITERAL_QUOTE | DOUBLE_LITERAL_IN_ESCAPE)) != 0 ? used * 2 : used; \
	if (used > 0 && qb->npos + cnvlen >= qb->str_alsize) \
	{ \
		cnvlen = convert_text_field(buf, NULL, used, qb->ccsc, escape_ch, &flags); \
		size_t	newlimit = qb->npos + cnvlen; \
\
		ENLARGE_NEWSTATEMENT(qb, newlimit); \
	} \
	cnvlen = convert_text_field(buf, &qb->query_statement[qb->npos], used, qb->ccsc, escape_ch, &flags); \
	qb->npos += cnvlen; \
} while (0)
#endif				/* NOT_USED */

/*----------
 *	Check if the statement is
 *	SELECT ... INTO table FROM .....
 *	This isn't really a strict check but ...
 *----------
 */
static BOOL into_table_from(const char *stmt)
{
    if (strnicmp(stmt, "into", 4))
	return FALSE;
    stmt += 4;
    if (!isspace((UCHAR) * stmt))
	return FALSE;
    while (isspace((UCHAR) * (++stmt)));
    switch (*stmt)
    {
    case '\0':
    case ',':
    case LITERAL_QUOTE:
	return FALSE;
    case IDENTIFIER_QUOTE:	/* double quoted table name ? */
	do
	{
	    do
		while (*(++stmt) != IDENTIFIER_QUOTE && *stmt);
	    while (*stmt && *(++stmt) == IDENTIFIER_QUOTE);
	    while (*stmt && !isspace((UCHAR) * stmt)
		   && *stmt != IDENTIFIER_QUOTE)
		stmt++;
	}
	while (*stmt == IDENTIFIER_QUOTE);
	break;
    default:
	while (!isspace((UCHAR) * (++stmt)));
	break;
    }
    if (!*stmt)
	return FALSE;
    while (isspace((UCHAR) * (++stmt)));
    if (strnicmp(stmt, "from", 4))
	return FALSE;
    return isspace((UCHAR) stmt[4]);
}

/*----------
 *	Check if the statement is
 *	SELECT ... FOR UPDATE .....
 *	This isn't really a strict check but ...
 *----------
 */
static BOOL table_for_update(const char *stmt, int *endpos)
{
    const char *wstmt = stmt;

    while (isspace((UCHAR) * (++wstmt)));
    if (!*wstmt)
	return FALSE;
    if (strnicmp(wstmt, "update", 6))
	return FALSE;
    wstmt += 6;
    *endpos = wstmt - stmt;
    return !wstmt[0] || isspace((UCHAR) wstmt[0]);
}

/*----------
 *	Check if the statement has OUTER JOIN
 *	This isn't really a strict check but ...
 *----------
 */
static BOOL
check_join(StatementClass * stmt, const char *curptr, size_t curpos)
{
    const char *wstmt;
    ssize_t stapos, endpos, tokenwd;
    const int backstep = 4;
    BOOL outerj = TRUE;

    for (endpos = curpos, wstmt = curptr;
	 endpos >= 0 && isspace((UCHAR) * wstmt); endpos--, wstmt--)
	;
    if (endpos < 0)
	return FALSE;
    for (endpos -= backstep, wstmt -= backstep;
	 endpos >= 0 && isspace((UCHAR) * wstmt); endpos--, wstmt--)
	;
    if (endpos < 0)
	return FALSE;
    for (stapos = endpos; stapos >= 0 && !isspace((UCHAR) * wstmt);
	 stapos--, wstmt--)
	;
    if (stapos < 0)
	return FALSE;
    wstmt++;
    switch (tokenwd = endpos - stapos)
    {
    case 4:
	if (strnicmp(wstmt, "FULL", tokenwd) == 0 ||
	    strnicmp(wstmt, "LEFT", tokenwd) == 0)
	    break;
	return FALSE;
    case 5:
	if (strnicmp(wstmt, "OUTER", tokenwd) == 0 ||
	    strnicmp(wstmt, "RIGHT", tokenwd) == 0)
	    break;
	if (strnicmp(wstmt, "INNER", tokenwd) == 0 ||
	    strnicmp(wstmt, "CROSS", tokenwd) == 0)
	{
	    outerj = FALSE;
	    break;
	}
	return FALSE;
    default:
	return FALSE;
    }
    if (stmt)
    {
	if (outerj)
	    SC_set_outer_join(stmt);
	else
	    SC_set_inner_join(stmt);
    }
    return TRUE;
}

/*----------
 *	Check if the statement is
 *	INSERT INTO ... () VALUES ()
 *	This isn't really a strict check but ...
 *----------
 */
static BOOL insert_without_target(const char *stmt, int *endpos)
{
    const char *wstmt = stmt;

    while (isspace((UCHAR) * (++wstmt)));
    if (!*wstmt)
	return FALSE;
    if (strnicmp(wstmt, "VALUES", 6))
	return FALSE;
    wstmt += 6;
    if (!wstmt[0] || !isspace((UCHAR) wstmt[0]))
	return FALSE;
    while (isspace((UCHAR) * (++wstmt)));
    if (*wstmt != '(' || *(++wstmt) != ')')
	return FALSE;
    wstmt++;
    *endpos = wstmt - stmt;
    return !wstmt[0] || isspace((UCHAR) wstmt[0]) || ';' == wstmt[0];
}

#define		my_strchr(conn, s1,c1) pg_mbschr(conn->ccsc, s1,c1)

static void remove_declare_cursor(QueryBuild * qb, QueryParse * qp)
{
    memmove(qb->query_statement, qb->query_statement + qp->declare_pos,
	    qb->npos - qp->declare_pos);
    qb->npos -= qp->declare_pos;
    qp->declare_pos = 0;
}

Int4 findTag(const char *tag, char dollar_quote, int ccsc)
{
    Int4 taglen = 0;
    encoded_str encstr;
    char tchar;
    const char *sptr;

    encoded_str_constr(&encstr, ccsc, tag + 1);
    for (sptr = tag + 1; *sptr; sptr++)
    {
	tchar = encoded_nextchar(&encstr);
	if (ENCODE_STATUS(encstr) != 0)
	    continue;
	if (dollar_quote == tchar)
	{
	    taglen = sptr - tag + 1;
	    break;
	}
	if (isspace(tchar))
	    break;
    }
    return taglen;
}

static int inner_process_tokens(QueryParse * qp, QueryBuild * qb)
{
    CSTR func = "inner_process_tokens";
    BOOL lf_conv = ((qb->flags & FLGB_CONVERT_LF) != 0);
    const char *bestitem = NULL;

    RETCODE retval;
    char oldchar;
    StatementClass *stmt = qb->stmt;
    char literal_quote = LITERAL_QUOTE, dollar_quote =
	DOLLAR_QUOTE, escape_in_literal = '\0';

    if (stmt && stmt->ntab > 0)
	bestitem = GET_NAME(stmt->ti[0]->bestitem);
    if (qp->from_pos == (Int4) qp->opos)
    {
	CVT_APPEND_STR(qb, ", \"ctid");
	if (bestitem)
	{
	    CVT_APPEND_STR(qb, "\", \"");
	    CVT_APPEND_STR(qb, bestitem);
	}
	CVT_APPEND_STR(qb, "\" ");
    } else if (qp->where_pos == (Int4) qp->opos)
    {
	qb->load_stmt_len = qb->npos;
	if (0 != (qb->flags & FLGB_KEYSET_DRIVEN))
	{
	    CVT_APPEND_STR(qb, "where ctid = '(0,0)';select \"ctid");
	    if (bestitem)
	    {
		CVT_APPEND_STR(qb, "\", \"");
		CVT_APPEND_STR(qb, bestitem);
	    }
	    CVT_APPEND_STR(qb, "\" from ");
	    CVT_APPEND_DATA(qb, qp->statement + qp->from_pos + 5,
			    qp->where_pos - qp->from_pos - 5);
	}
    }
    oldchar = encoded_byte_check(&qp->encstr, qp->opos);
    if (ENCODE_STATUS(qp->encstr) != 0)
    {
	CVT_APPEND_CHAR(qb, oldchar);
	return SQL_SUCCESS;
    }

    /*
     * From here we are guaranteed to handle a 1-byte character.
     */
    if (qp->in_escape)		/* escape check */
    {
	qp->in_escape = FALSE;
	CVT_APPEND_CHAR(qb, oldchar);
	return SQL_SUCCESS;
    } else if (qp->in_dollar_quote)	/* dollar quote check */
    {
	if (oldchar == dollar_quote)
	{
	    if (strncmp(F_OldPtr(qp), qp->dollar_tag, qp->taglen) == 0)
	    {
		CVT_APPEND_DATA(qb, F_OldPtr(qp), qp->taglen);
		qp->opos += (qp->taglen - 1);
		qp->in_dollar_quote = FALSE;
		qp->in_literal = FALSE;
		qp->dollar_tag = NULL;
		qp->taglen = -1;
		return SQL_SUCCESS;
	    }
	}
	CVT_APPEND_CHAR(qb, oldchar);
	return SQL_SUCCESS;
    } else if (qp->in_literal)	/* quote check */
    {
	if (oldchar == escape_in_literal)
	    qp->in_escape = TRUE;
	else if (oldchar == literal_quote)
	    qp->in_literal = FALSE;
	CVT_APPEND_CHAR(qb, oldchar);
	return SQL_SUCCESS;
    } else if (qp->in_identifier)	/* double quote check */
    {
	if (oldchar == IDENTIFIER_QUOTE)
	    qp->in_identifier = FALSE;
	CVT_APPEND_CHAR(qb, oldchar);
	return SQL_SUCCESS;
    }

    /*
     * From here we are guranteed to be in neither a literal_escape,
     * a literal_quote nor an idetifier_quote.
     */
    /* Squeeze carriage-return/linefeed pairs to linefeed only */
    else if (lf_conv &&
	     PG_CARRIAGE_RETURN == oldchar &&
	     qp->opos + 1 < qp->stmt_len &&
	     PG_LINEFEED == qp->statement[qp->opos + 1])
	return SQL_SUCCESS;

    /*
     * Handle literals (date, time, timestamp) and ODBC scalar
     * functions
     */
    else if (oldchar == '{')
    {
	if (SQL_ERROR == convert_escape(qp, qb))
	{
	    if (0 == qb->errornumber)
	    {
		qb->errornumber = STMT_EXEC_ERROR;
		qb->errormsg = "ODBC escape convert error";
	    }
	    mylog("%s convert_escape error\n", func);
	    return SQL_ERROR;
	}
	if (isalnum((UCHAR) F_OldPtr(qp)[1]))
	    CVT_APPEND_CHAR(qb, ' ');
	return SQL_SUCCESS;
    }
    /* End of an escape sequence */
    else if (oldchar == '}')
    {
	if (qp->statement_type == STMT_TYPE_PROCCALL)
	{
	    if (qp->proc_no_param)
		CVT_APPEND_STR(qb, "()");
	} else if (!isspace(F_OldPtr(qp)[1]))
	    CVT_APPEND_CHAR(qb, ' ');
	return SQL_SUCCESS;
    } else if (oldchar == '@' &&
	       strnicmp(F_OldPtr(qp), "@@identity", 10) == 0)
    {
	ConnectionClass *conn = SC_get_conn(stmt);
	BOOL converted = FALSE;
	COL_INFO *coli;

	if (PG_VERSION_GE(conn, 8.1))
	{
	    CVT_APPEND_STR(qb, "lastval()");
	    converted = TRUE;
	} else if (NAME_IS_VALID(conn->tableIns))
	{
	    TABLE_INFO ti, *pti = &ti;

	    memset(&ti, 0, sizeof(ti));
	    NAME_TO_NAME(ti.schema_name, conn->schemaIns);
	    NAME_TO_NAME(ti.table_name, conn->tableIns);
	    getCOLIfromTI(func, conn, NULL, 0, &pti);
	    coli = ti.col_info;
	    NULL_THE_NAME(ti.schema_name);
	    NULL_THE_NAME(ti.table_name);
	    if (NULL != coli)
	    {
		int i, num_fields = QR_NumResultCols(coli->result);

		for (i = 0; i < num_fields; i++)
		{
		    if (*
			((char *)
			 QR_get_value_backend_text(coli->result, i,
						   COLUMNS_AUTO_INCREMENT))
			== '1')
		    {
			CVT_APPEND_STR(qb, "curr");
			CVT_APPEND_STR(qb,
				       (char *)
				       QR_get_value_backend_text(coli->
								 result,
								 i,
								 COLUMNS_COLUMN_DEF)
				       + 4);
			converted = TRUE;
			break;
		    }
		}
	    }
	}
	if (!converted)
	    CVT_APPEND_STR(qb, "NULL");
	qp->opos += 10;
	return SQL_SUCCESS;
    }

    /*
     * Can you have parameter markers inside of quotes?  I dont think
     * so. All the queries I've seen expect the driver to put quotes
     * if needed.
     */
    else if (oldchar != '?')
    {
	if (oldchar == dollar_quote)
	{
	    qp->taglen =
		findTag(F_OldPtr(qp), dollar_quote, qp->encstr.ccsc);
	    if (qp->taglen > 0)
	    {
		qp->in_literal = TRUE;
		qp->in_dollar_quote = TRUE;
		qp->dollar_tag = F_OldPtr(qp);
		CVT_APPEND_DATA(qb, F_OldPtr(qp), qp->taglen);
		qp->opos += (qp->taglen - 1);
		return SQL_SUCCESS;
	    }
	} else if (oldchar == literal_quote)
	{
	    if (!qp->in_identifier)
	    {
		qp->in_literal = TRUE;
		escape_in_literal = CC_get_escape(qb->conn);
		if (!escape_in_literal)
		{
		    if (LITERAL_EXT == F_OldPtr(qp)[-1])
			escape_in_literal = ESCAPE_IN_LITERAL;
		}
	    }
	} else if (oldchar == IDENTIFIER_QUOTE)
	{
	    if (!qp->in_literal)
		qp->in_identifier = TRUE;
	} else if (oldchar == ';')
	{
	    /*
	     * can't parse multiple statement using protocol V3.
	     * reset the dollar number here in case it is divided
	     * to parse.
	     */
	    qb->dollar_number = 0;
	    if (0 != (qp->flags & FLGP_CURSOR_CHECK_OK))
	    {
		const char *vp = &(qp->statement[qp->opos + 1]);

		while (*vp && isspace(*vp))
		    vp++;
		if (*vp)	/* multiple statement */
		{
		    qp->flags |= FLGP_MULTIPLE_STATEMENT;
		    qp->flags &= ~FLGP_CURSOR_CHECK_OK;
		    qb->flags &= ~FLGB_KEYSET_DRIVEN;
		    remove_declare_cursor(qb, qp);
		}
	    }
	} else
	{
	    if (isspace((UCHAR) oldchar))
	    {
		if (!qp->prev_token_end)
		{
		    qp->prev_token_end = TRUE;
		    qp->token_save[qp->token_len] = '\0';
		    if (qp->token_len == 4)
		    {
			if (0 != (qp->flags & FLGP_CURSOR_CHECK_OK) &&
			    into_table_from(&qp->
					    statement[qp->opos -
						      qp->token_len]))
			{
			    qp->flags |= FLGP_SELECT_INTO;
			    qp->flags &= ~FLGP_CURSOR_CHECK_OK;
			    qb->flags &= ~FLGB_KEYSET_DRIVEN;
			    qp->statement_type = STMT_TYPE_CREATE;
			    remove_declare_cursor(qb, qp);
			} else if (stricmp(qp->token_save, "join") == 0)
			{
			    if (stmt)
				check_join(stmt, F_OldPtr(qp),
					   F_OldPos(qp));
			}
		    } else if (qp->token_len == 3)
		    {
			int endpos;

			if (0 != (qp->flags & FLGP_CURSOR_CHECK_OK) &&
			    strnicmp(qp->token_save, "for", 3) == 0 &&
			    table_for_update(F_OldPtr(qp), &endpos))
			{
			    qp->flags |= FLGP_SELECT_FOR_UPDATE;
			    qp->flags &= ~FLGP_CURSOR_CHECK_OK;
			    if (qp->flags & FLGP_PREPARE_DUMMY_CURSOR)
			    {
				qb->npos -= 4;
				qp->opos += endpos;
			    } else
			    {
				remove_declare_cursor(qb, qp);
			    }
			}
		    } else if (qp->token_len == 2)
		    {
			int endpos;

			if (STMT_TYPE_INSERT == qp->statement_type &&
			    strnicmp(qp->token_save, "()", 2) == 0 &&
			    insert_without_target(F_OldPtr(qp),
						  &endpos))
			{
			    qb->npos -= 2;
			    CVT_APPEND_STR(qb, "DEFAULT VALUES");
			    qp->opos += endpos;
			    return SQL_SUCCESS;
			}
		    }
		}
	    } else if (qp->prev_token_end)
	    {
		qp->prev_token_end = FALSE;
		qp->token_save[0] = oldchar;
		qp->token_len = 1;
	    } else if (qp->token_len + 1 < sizeof(qp->token_save))
		qp->token_save[qp->token_len++] = oldchar;
	}
	CVT_APPEND_CHAR(qb, oldchar);
	return SQL_SUCCESS;
    }

    /*
     * Its a '?' parameter alright
     */
    if (0 == qb->errornumber)
    {
	qb->errornumber = STMT_EXEC_ERROR;
	qb->errormsg = "SQL bound parameters not yet supported";
    }
    mylog("%s convert_escape error\n", func);
    return SQL_ERROR;
}

static BOOL
ResolveNumericParam(const SQL_NUMERIC_STRUCT * ns, char *chrform)
{
    static const int prec[] =
	{ 1, 3, 5, 8, 10, 13, 15, 17, 20, 22, 25, 27, 29, 32, 34, 37,
	39
    };
    Int4 i, j, k, ival, vlen, len, newlen;
    UCHAR calv[40];
    const UCHAR *val = (const UCHAR *) ns->val;
    BOOL next_figure;

    inolog("C_NUMERIC [prec=%d scale=%d]", ns->precision, ns->scale);
    if (0 == ns->precision)
    {
	strcpy(chrform, "0");
	return TRUE;
    } else if (ns->precision < prec[sizeof(Int4)])
    {
	for (i = 0, ival = 0;
	     i < sizeof(Int4) && prec[i] <= ns->precision; i++)
	{
	    inolog("(%d)", val[i]);
	    ival += (val[i] << (8 * i));	/* ns->val is little endian */
	}
	inolog(" ival=%d,%d", ival,
	       (val[3] << 24) | (val[2] << 16) | (val[1] << 8) |
	       val[0]);
	if (0 == ns->scale)
	{
	    if (0 == ns->sign)
		ival *= -1;
	    sprintf(chrform, "%d", ival);
	} else if (ns->scale > 0)
	{
	    Int4 i, div, o1val, o2val;

	    for (i = 0, div = 1; i < ns->scale; i++)
		div *= 10;
	    o1val = ival / div;
	    o2val = ival % div;
	    if (0 == ns->sign)
		o1val *= -1;
	    sprintf(chrform, "%d.%0.*d", o1val, ns->scale, o2val);
	}
	inolog(" convval=%s\n", chrform);
	return TRUE;
    }

    for (i = 0; i < SQL_MAX_NUMERIC_LEN && prec[i] <= ns->precision;
	 i++)
	;
    vlen = i;
    len = 0;
    memset(calv, 0, sizeof(calv));
    inolog(" len1=%d", vlen);
    for (i = vlen - 1; i >= 0; i--)
    {
	for (j = len - 1; j >= 0; j--)
	{
	    if (!calv[j])
		continue;
	    ival = (((Int4) calv[j]) << 8);
	    calv[j] = (ival % 10);
	    ival /= 10;
	    calv[j + 1] += (ival % 10);
	    ival /= 10;
	    calv[j + 2] += (ival % 10);
	    ival /= 10;
	    calv[j + 3] += ival;
	    for (k = j;; k++)
	    {
		next_figure = FALSE;
		if (calv[k] > 0)
		{
		    if (k >= len)
			len = k + 1;
		    while (calv[k] > 9)
		    {
			calv[k + 1]++;
			calv[k] -= 10;
			next_figure = TRUE;
		    }
		}
		if (k >= j + 3 && !next_figure)
		    break;
	    }
	}
	ival = val[i];
	if (!ival)
	    continue;
	calv[0] += (ival % 10);
	ival /= 10;
	calv[1] += (ival % 10);
	ival /= 10;
	calv[2] += ival;
	for (j = 0;; j++)
	{
	    next_figure = FALSE;
	    if (calv[j] > 0)
	    {
		if (j >= len)
		    len = j + 1;
		while (calv[j] > 9)
		{
		    calv[j + 1]++;
		    calv[j] -= 10;
		    next_figure = TRUE;
		}
	    }
	    if (j >= 2 && !next_figure)
		break;
	}
    }
    inolog(" len2=%d", len);
    newlen = 0;
    if (0 == ns->sign)
	chrform[newlen++] = '-';
    if (i = len - 1, i < ns->scale)
	i = ns->scale;
    for (; i >= ns->scale; i--)
	chrform[newlen++] = calv[i] + '0';
    if (ns->scale > 0)
    {
	chrform[newlen++] = '.';
	for (; i >= 0; i--)
	    chrform[newlen++] = calv[i] + '0';
    }
    if (0 == len)
	chrform[newlen++] = '0';
    chrform[newlen] = '\0';
    inolog(" convval(2) len=%d %s\n", newlen, chrform);
    return TRUE;
}


static const char *mapFunction(const char *func, int param_count)
{
    int i;

    for (i = 0; mapFuncs[i][0]; i++)
    {
	if (mapFuncs[i][0][0] == '%')
	{
	    if (mapFuncs[i][0][1] - '0' == param_count &&
		!stricmp(mapFuncs[i][0] + 2, func))
		return mapFuncs[i][1];
	} else if (!stricmp(mapFuncs[i][0], func))
	    return mapFuncs[i][1];
    }

    return NULL;
}

/*
 * processParameters()
 * Process function parameters and work with embedded escapes sequences.
 */
static int
processParameters(QueryParse * qp, QueryBuild * qb,
		  size_t * output_count, SQLLEN param_pos[][2])
{
    CSTR func = "processParameters";
    int retval, innerParenthesis, param_count;
    BOOL stop;

    /* begin with outer '(' */
    innerParenthesis = 0;
    param_count = 0;
    stop = FALSE;
    for (; F_OldPos(qp) < qp->stmt_len; F_OldNext(qp))
    {
	retval = inner_process_tokens(qp, qb);
	if (retval == SQL_ERROR)
	    return retval;
	if (ENCODE_STATUS(qp->encstr) != 0)
	    continue;
	if (qp->in_identifier || qp->in_literal || qp->in_escape)
	    continue;

	switch (F_OldChar(qp))
	{
	case ',':
	    if (1 == innerParenthesis)
	    {
		param_pos[param_count][1] = F_NewPos(qb) - 2;
		param_count++;
		param_pos[param_count][0] = F_NewPos(qb);
		param_pos[param_count][1] = -1;
	    }
	    break;
	case '(':
	    if (0 == innerParenthesis)
	    {
		param_pos[param_count][0] = F_NewPos(qb);
		param_pos[param_count][1] = -1;
	    }
	    innerParenthesis++;
	    break;

	case ')':
	    innerParenthesis--;
	    if (0 == innerParenthesis)
	    {
		param_pos[param_count][1] = F_NewPos(qb) - 2;
		param_count++;
		param_pos[param_count][0] =
		    param_pos[param_count][1] = -1;
	    }
	    if (output_count)
		*output_count = F_NewPos(qb);
	    break;

	case '}':
	    stop = (0 == innerParenthesis);
	    break;

	}
	if (stop)		/* returns with the last } position */
	    break;
    }
    if (param_pos[param_count][0] >= 0)
    {
	mylog("%s closing ) not found %d\n", func, innerParenthesis);
	qb->errornumber = STMT_EXEC_ERROR;
	qb->errormsg = "processParameters closing ) not found";
	return SQL_ERROR;
    } else if (1 == param_count)	/* the 1 parameter is really valid ? */
    {
	BOOL param_exist = FALSE;
	SQLLEN i;

	for (i = param_pos[0][0]; i <= param_pos[0][1]; i++)
	{
	    if (!isspace(qb->query_statement[i]))
	    {
		param_exist = TRUE;
		break;
	    }
	}
	if (!param_exist)
	{
	    param_pos[0][0] = param_pos[0][1] = -1;
	}
    }

    return SQL_SUCCESS;
}

/*
 * convert_escape()
 * This function doesn't return a pointer to static memory any longer !
 */
static int convert_escape(QueryParse * qp, QueryBuild * qb)
{
    CSTR func = "convert_escape";
    RETCODE retval = SQL_SUCCESS;
    char buf[1024], buf_small[128], key[65];
    UCHAR ucv;
    UInt4 prtlen;

    if (F_OldChar(qp) == '{')	/* skip the first { */
	F_OldNext(qp);
    /* Separate off the key, skipping leading and trailing whitespace */
    while ((ucv = F_OldChar(qp)) != '\0' && isspace(ucv))
	F_OldNext(qp);
    /*
     * procedure calls
     */
    if (qp->statement_type == STMT_TYPE_PROCCALL)
    {
	int lit_call_len = 4;
	ConnectionClass *conn = qb->conn;

	/* '?=' to accept return values exists ? */
	if (F_OldChar(qp) == '?')
	{
	    qb->param_number++;
	    qb->proc_return = 1;
	    if (qb->stmt)
		qb->stmt->proc_return = 1;
	    while (isspace((UCHAR) qp->statement[++qp->opos]));
	    if (F_OldChar(qp) != '=')
	    {
		F_OldPrior(qp);
		return SQL_SUCCESS;
	    }
	    while (isspace((UCHAR) qp->statement[++qp->opos]));
	}
	if (strnicmp(F_OldPtr(qp), "call", lit_call_len) ||
	    !isspace((UCHAR) F_OldPtr(qp)[lit_call_len]))
	{
	    F_OldPrior(qp);
	    return SQL_SUCCESS;
	}
	qp->opos += lit_call_len;
	if (qb->num_io_params > 1 ||
	    (0 == qb->proc_return && PG_VERSION_GE(qb->conn, 7.3)))
	    CVT_APPEND_STR(qb, "SELECT * FROM");
	else
	    CVT_APPEND_STR(qb, "SELECT");
	if (my_strchr(conn, (const UCHAR *)F_OldPtr(qp), '('))
	    qp->proc_no_param = FALSE;
	return SQL_SUCCESS;
    }

    sscanf(F_OldPtr(qp), "%32s", key);
    while ((ucv = F_OldChar(qp)) != '\0' && (!isspace(ucv)))
	F_OldNext(qp);
    while ((ucv = F_OldChar(qp)) != '\0' && isspace(ucv))
	F_OldNext(qp);

    /* Avoid the concatenation of the function name with the previous word. Aceto */

    if (F_NewPos(qb) > 0 && isalnum((UCHAR) F_NewPtr(qb)[-1]))
	CVT_APPEND_CHAR(qb, ' ');

    if (stricmp(key, "d") == 0)
    {
	/* Literal; return the escape part adding type cast */
	F_ExtractOldTo(qp, buf_small, '}', sizeof(buf_small));
	if (PG_VERSION_LT(qb->conn, 7.3))
	    prtlen = snprintf(buf, sizeof(buf), "%s ", buf_small);
	else
	    prtlen = snprintf(buf, sizeof(buf), "%s::date ", buf_small);
	CVT_APPEND_DATA(qb, buf, prtlen);
    } else if (stricmp(key, "t") == 0)
    {
	/* Literal; return the escape part adding type cast */
	F_ExtractOldTo(qp, buf_small, '}', sizeof(buf_small));
	prtlen = snprintf(buf, sizeof(buf), "%s::time", buf_small);
	CVT_APPEND_DATA(qb, buf, prtlen);
    } else if (stricmp(key, "ts") == 0)
    {
	/* Literal; return the escape part adding type cast */
	F_ExtractOldTo(qp, buf_small, '}', sizeof(buf_small));
	if (PG_VERSION_LT(qb->conn, 7.1))
	    prtlen =
		snprintf(buf, sizeof(buf), "%s::datetime", buf_small);
	else
	    prtlen =
		snprintf(buf, sizeof(buf), "%s::timestamp", buf_small);
	CVT_APPEND_DATA(qb, buf, prtlen);
    } else if (stricmp(key, "oj") == 0)	/* {oj syntax support for 7.1 * servers */
    {
	if (qb->stmt)
	    SC_set_outer_join(qb->stmt);
	F_OldPrior(qp);
	return SQL_SUCCESS;	/* Continue at inner_process_tokens loop */
    } else if (stricmp(key, "fn") == 0)
    {
	QueryBuild nqb;
	const char *mapExpr;
	int i, param_count;
	SQLLEN from, to;
	size_t param_consumed;
	SQLLEN param_pos[16][2];
	BOOL cvt_func = FALSE;

	/* Separate off the func name, skipping leading and trailing whitespace */
	i = 0;
	while ((ucv = F_OldChar(qp)) != '\0' && ucv != '(' &&
	       (!isspace(ucv)))
	{
	    if (i < sizeof(key) - 1)
		key[i++] = ucv;
	    F_OldNext(qp);
	}
	key[i] = '\0';
	while ((ucv = F_OldChar(qp)) != '\0' && isspace(ucv))
	    F_OldNext(qp);

	/*
	 * We expect left parenthesis here, else return fn body as-is
	 * since it is one of those "function constants".
	 */
	if (F_OldChar(qp) != '(')
	{
	    CVT_APPEND_STR(qb, key);
	    return SQL_SUCCESS;
	}

	/*
	 * Process parameter list and inner escape
	 * sequences
	 * Aceto 2002-01-29
	 */

	QB_initialize_copy(&nqb, qb, 1024);
	if (retval =
	    processParameters(qp, &nqb, &param_consumed, param_pos),
	    retval == SQL_ERROR)
	{
	    qb->errornumber = nqb.errornumber;
	    qb->errormsg = nqb.errormsg;
	    QB_Destructor(&nqb);
	    return retval;
	}

	for (param_count = 0;; param_count++)
	{
	    if (param_pos[param_count][0] < 0)
		break;
	}
	if (param_count == 1 && param_pos[0][1] < param_pos[0][0])
	    param_count = 0;

	mapExpr = NULL;
	if (stricmp(key, "convert") == 0)
	    cvt_func = TRUE;
	else
	    mapExpr = mapFunction(key, param_count);
	if (cvt_func)
	{
	    if (2 == param_count)
	    {
		BOOL add_cast = FALSE, add_quote = FALSE;
		const char *pptr;

		from = param_pos[0][0];
		to = param_pos[0][1];
		for (pptr = nqb.query_statement + from;
		     *pptr && isspace(*pptr); pptr++)
		    ;
		if (LITERAL_QUOTE == *pptr)
		    ;
		else if ('-' == *pptr)
		    add_quote = TRUE;
		else if (isdigit(*pptr))
		    add_quote = TRUE;
		else
		    add_cast = TRUE;
		if (add_quote)
		    CVT_APPEND_CHAR(qb, LITERAL_QUOTE);
		else if (add_cast)
		    CVT_APPEND_CHAR(qb, '(');
		CVT_APPEND_DATA(qb, nqb.query_statement + from,
				to - from + 1);
		if (add_quote)
		    CVT_APPEND_CHAR(qb, LITERAL_QUOTE);
		else if (add_cast)
		{
		    const char *cast_form = NULL;

		    CVT_APPEND_CHAR(qb, ')');
		    from = param_pos[1][0];
		    to = param_pos[1][1];
		    if (to < from + 9)
		    {
			char num[10];
			memcpy(num, nqb.query_statement + from,
			       to - from + 1);
			num[to - from + 1] = '\0';
			mylog("%d-%d num=%s SQL_BIT=%d\n", to, from,
			      num, SQL_BIT);
			switch (atoi(num))
			{
			case SQL_BIT:
			    cast_form = "boolean";
			    break;
			case SQL_INTEGER:
			    cast_form = "int4";
			    break;
			}
		    }
		    if (NULL != cast_form)
		    {
			CVT_APPEND_STR(qb, "::");
			CVT_APPEND_STR(qb, cast_form);
		    }
		}
	    } else
	    {
		qb->errornumber = STMT_EXEC_ERROR;
		qb->errormsg = "convert param count must be 2";
		retval = SQL_ERROR;
	    }
	} else if (mapExpr == NULL)
	{
	    CVT_APPEND_STR(qb, key);
	    CVT_APPEND_DATA(qb, nqb.query_statement, nqb.npos);
	} else
	{
	    const char *mapptr;
	    SQLLEN paramlen;
	    int pidx;

	    for (prtlen = 0, mapptr = mapExpr; *mapptr; mapptr++)
	    {
		if (*mapptr != '$')
		{
		    CVT_APPEND_CHAR(qb, *mapptr);
		    continue;
		}
		mapptr++;
		if (*mapptr == '*')
		{
		    from = 1;
		    to = param_consumed - 2;
		} else if (isdigit(*mapptr))
		{
		    pidx = *mapptr - '0' - 1;
		    if (pidx < 0 || param_pos[pidx][0] < 0)
		    {
			qb->errornumber = STMT_EXEC_ERROR;
			qb->errormsg = "param not found";
			qlog("%s %dth param not found for the expression %s\n", pidx + 1, mapExpr);
			retval = SQL_ERROR;
			break;
		    }
		    from = param_pos[pidx][0];
		    to = param_pos[pidx][1];
		} else
		{
		    qb->errornumber = STMT_EXEC_ERROR;
		    qb->errormsg = "internal expression error";
		    qlog("%s internal expression error %s\n", func,
			 mapExpr);
		    retval = SQL_ERROR;
		    break;
		}
		paramlen = to - from + 1;
		if (paramlen > 0)
		    CVT_APPEND_DATA(qb, nqb.query_statement + from,
				    paramlen);
	    }
	}
	if (0 == qb->errornumber)
	{
	    qb->errornumber = nqb.errornumber;
	    qb->errormsg = nqb.errormsg;
	}
	if (SQL_ERROR != retval)
	{
	    qb->param_number = nqb.param_number;
	    qb->flags = nqb.flags;
	}
	QB_Destructor(&nqb);
    } else
    {
	/* Bogus key, leave untranslated */
	return SQL_ERROR;
    }

    return retval;
}

BOOL convert_money(const char *s, char *sout, size_t soutmax)
{
    size_t i = 0, out = 0;

    for (i = 0; s[i]; i++)
    {
	if (s[i] == '$' || s[i] == ',' || s[i] == ')')
	    ;			/* skip these characters */
	else
	{
	    if (out + 1 >= soutmax)
		return FALSE;	/* sout is too short */
	    if (s[i] == '(')
		sout[out++] = '-';
	    else
		sout[out++] = s[i];
	}
    }
    sout[out] = '\0';
    return TRUE;
}


/*
 *	This function parses a character string for date/time info and fills in SIMPLE_TIME
 *	It does not zero out SIMPLE_TIME in case it is desired to initialize it with a value
 */
char parse_datetime(const char *buf, SIMPLE_TIME * st)
{
    int y, m, d, hh, mm, ss;
    int nf;

    y = m = d = hh = mm = ss = 0;
    st->fr = 0;
    st->infinity = 0;

    /* escape sequence ? */
    if (buf[0] == '{')
    {
	while (*(++buf) && *buf != LITERAL_QUOTE);
	if (!(*buf))
	    return FALSE;
	buf++;
    }
    if (buf[4] == '-')		/* year first */
	nf = sscanf(buf, "%4d-%2d-%2d %2d:%2d:%2d", &y, &m, &d, &hh,
		    &mm, &ss);
    else
	nf = sscanf(buf, "%2d-%2d-%4d %2d:%2d:%2d", &m, &d, &y, &hh,
		    &mm, &ss);

    if (nf == 5 || nf == 6)
    {
	st->y = y;
	st->m = m;
	st->d = d;
	st->hh = hh;
	st->mm = mm;
	st->ss = ss;

	return TRUE;
    }

    if (buf[4] == '-')		/* year first */
	nf = sscanf(buf, "%4d-%2d-%2d", &y, &m, &d);
    else
	nf = sscanf(buf, "%2d-%2d-%4d", &m, &d, &y);

    if (nf == 3)
    {
	st->y = y;
	st->m = m;
	st->d = d;

	return TRUE;
    }

    nf = sscanf(buf, "%2d:%2d:%2d", &hh, &mm, &ss);
    if (nf == 2 || nf == 3)
    {
	st->hh = hh;
	st->mm = mm;
	st->ss = ss;

	return TRUE;
    }

    return FALSE;
}


/*	Change linefeed to carriage-return/linefeed */
size_t
convert_linefeeds(const char *si, char *dst, size_t max, BOOL convlf,
		  BOOL * changed)
{
    size_t i = 0, out = 0;

    if (max == 0)
	max = 0xffffffff;
    *changed = FALSE;
    for (i = 0; si[i] && out < max - 1; i++)
    {
	if (convlf && si[i] == '\n')
	{
	    /* Only add the carriage-return if needed */
	    if (i > 0 && PG_CARRIAGE_RETURN == si[i - 1])
	    {
		if (dst)
		    dst[out++] = si[i];
		else
		    out++;
		continue;
	    }
	    *changed = TRUE;

	    if (dst)
	    {
		dst[out++] = PG_CARRIAGE_RETURN;
		dst[out++] = '\n';
	    } else
		out += 2;
	} else
	{
	    if (dst)
		dst[out++] = si[i];
	    else
		out++;
	}
    }
    if (dst)
	dst[out] = '\0';
    return out;
}


/*
 *	Change carriage-return/linefeed to just linefeed
 *	Plus, escape any special characters.
 */
size_t
convert_special_chars(const char *si, char *dst, SQLLEN used,
		      UInt4 flags, int ccsc, int escape_in_literal)
{
    size_t i = 0, out = 0, max;
    char *p = NULL, literal_quote = LITERAL_QUOTE, tchar;
    encoded_str encstr;
    BOOL convlf = (0 != (flags & FLGB_CONVERT_LF)),
	double_special = (0 == (flags & FLGB_BUILDING_BIND_REQUEST));

    if (used == SQL_NTS)
	max = strlen(si);
    else
	max = used;
    if (dst)
    {
	p = dst;
	p[0] = '\0';
    }
    encoded_str_constr(&encstr, ccsc, si);

    for (i = 0; i < max && si[i]; i++)
    {
	tchar = encoded_nextchar(&encstr);
	if (ENCODE_STATUS(encstr) != 0)
	{
	    if (p)
		p[out] = tchar;
	    out++;
	    continue;
	}
	if (convlf &&		/* CR/LF -> LF */
	    PG_CARRIAGE_RETURN == tchar && PG_LINEFEED == si[i + 1])
	    continue;
	else if (double_special &&	/* double special chars ? */
		 (tchar == literal_quote || tchar == escape_in_literal))
	{
	    if (p)
		p[out++] = tchar;
	    else
		out++;
	}
	if (p)
	    p[out++] = tchar;
	else
	    out++;
    }
    if (p)
	p[out] = '\0';
    return out;
}

#ifdef NOT_USED
#define	CVT_CRLF_TO_LF			1L
#define	DOUBLE_LITERAL_QUOTE		(1L << 1)
#define	DOUBLE_ESCAPE_IN_LITERAL	(1L << 2)
static int
convert_text_field(const char *si, char *dst, int used, int ccsc,
		   int escape_in_literal, UInt4 * flags)
{
    size_t i = 0, out = 0, max;
    UInt4 iflags = *flags;
    char *p = NULL, literal_quote = LITERAL_QUOTE, tchar;
    encoded_str encstr;
    BOOL convlf = (0 != (iflags & CVT_CRLF_TO_LF)),
	double_literal_quote = (0 != (iflags & DOUBLE_LITERAL_QUOTE)),
	double_escape_in_literal =
	(0 != (iflags & DOUBLE_ESCAPE_IN_LITERAL));

    if (SQL_NTS == used)
	max = strlen(si);
    else
	max = used;
    if (0 == iflags)
    {
	if (dst)
	    strncpy_null(dst, si, max + 1);
	else
	    return max;
    }
    if (dst)
    {
	p = dst;
	p[0] = '\0';
    }
    encoded_str_constr(&encstr, ccsc, si);

    *flags = 0;
    for (i = 0; i < max && si[i]; i++)
    {
	tchar = encoded_nextchar(&encstr);
	if (ENCODE_STATUS(encstr) != 0)
	{
	    if (p)
		p[out] = tchar;
	    out++;
	    continue;
	}
	if (convlf &&		/* CR/LF -> LF */
	    PG_CARRIAGE_RETURN == tchar && PG_LINEFEED == si[i + 1])
	{
	    *flags |= CVT_CRLF_TO_LF;
	    continue;
	} else if (double_literal_quote &&	/* double literal quote ? */
		   tchar == literal_quote)
	{
	    if (p)
		p[out] = tchar;
	    out++;
	    *flags |= DOUBLE_LITERAL_QUOTE;
	} else if (double_escape_in_literal &&	/* double escape ? */
		   tchar == escape_in_literal)
	{
	    if (p)
		p[out] = tchar;
	    out++;
	    *flags |= DOUBLE_ESCAPE_IN_LITERAL;
	}
	if (p)
	    p[out] = tchar;
	out++;
    }
    if (p)
	p[out] = '\0';
    return out;
}
#endif				/* NOT_USED */


/*	!!! Need to implement this function !!!  */
int
convert_pgbinary_to_char(const char *value, char *rgbValue,
			 ssize_t cbValueMax)
{
    mylog("convert_pgbinary_to_char: value = '%s'\n", value);

    strncpy_null(rgbValue, value, cbValueMax);
    return 0;
}


static int conv_from_octal(const UCHAR * s)
{
    ssize_t i;
    int y = 0;

    for (i = 1; i <= 3; i++)
	y += (s[i] - '0') << (3 * (3 - i));

    return y;
}


/*	convert octal escapes to bytes */
size_t
convert_from_pgbinary(const UCHAR * value, UCHAR * rgbValue,
		      SQLLEN cbValueMax)
{
    size_t i, ilen = strlen((const char *)value);
    size_t o = 0;


    for (i = 0; i < ilen;)
    {
	if (value[i] == BYTEA_ESCAPE_CHAR)
	{
	    if (value[i + 1] == BYTEA_ESCAPE_CHAR)
	    {
		if (rgbValue)
		    rgbValue[o] = value[i];
		i += 2;
	    } else
	    {
		if (rgbValue)
		    rgbValue[o] = conv_from_octal(&value[i]);
		i += 4;
	    }
	} else
	{
	    if (rgbValue)
		rgbValue[o] = value[i];
	    i++;
	}
		/** if (rgbValue)
			mylog("convert_from_pgbinary: i=%d, rgbValue[%d] = %d, %c\n", i, o, rgbValue[o], rgbValue[o]); ***/
	o++;
    }

    if (rgbValue)
	rgbValue[o] = '\0';	/* extra protection */

    mylog("convert_from_pgbinary: in=%d, out = %d\n", ilen, o);

    return o;
}


static UInt2 conv_to_octal(UCHAR val, char *octal, char escape_ch)
{
    int i, pos = 0, len;

    if (escape_ch)
	octal[pos++] = escape_ch;
    octal[pos] = BYTEA_ESCAPE_CHAR;
    len = 4 + pos;
    octal[len] = '\0';

    for (i = len - 1; i > pos; i--)
    {
	octal[i] = (val & 7) + '0';
	val >>= 3;
    }

    return (UInt2) len;
}


static char *conv_to_octal2(UCHAR val, char *octal)
{
    int i;

    octal[0] = BYTEA_ESCAPE_CHAR;
    octal[4] = '\0';

    for (i = 3; i > 0; i--)
    {
	octal[i] = (val & 7) + '0';
	val >>= 3;
    }

    return octal;
}


/*	convert non-ascii bytes to octal escape sequences */
static size_t
convert_to_pgbinary(const UCHAR * in, char *out, size_t len,
		    QueryBuild * qb)
{
    CSTR func = "convert_to_pgbinary";
    UCHAR inc;
    size_t i, o = 0;
    char escape_in_literal = CC_get_escape(qb->conn);
    BOOL esc_double = (0 == (qb->flags & FLGB_BUILDING_BIND_REQUEST)
		       && 0 != escape_in_literal);

    for (i = 0; i < len; i++)
    {
	inc = in[i];
	mylog("%s: in[%d] = %d, %c\n", func, i, inc, inc);
	if (inc < 128 && (isalnum(inc) || inc == ' '))
	    out[o++] = inc;
	else
	{
	    if (esc_double)
	    {
		o += conv_to_octal(inc, &out[o], escape_in_literal);
	    } else
	    {
		conv_to_octal2(inc, &out[o]);
		o += 4;
	    }
	}
    }

    mylog("%s: returning %d, out='%.*s'\n", func, o, o, out);

    return o;
}


static const char *hextbl = "0123456789ABCDEF";
static SQLLEN pg_bin2hex(UCHAR * src, UCHAR * dst, SQLLEN length)
{
    UCHAR chr, *src_wk, *dst_wk;
    BOOL backwards;
    int i;

    backwards = FALSE;
    if (dst < src)
    {
	if (dst + length > src + 1)
	    return -1;
    } else if (dst < src + length)
	backwards = TRUE;
    if (backwards)
    {
	for (i = 0, src_wk = src + length - 1, dst_wk =
	     dst + 2 * length - 1; i < length; i++, src_wk--)
	{
	    chr = *src_wk;
	    *dst_wk-- = hextbl[chr % 16];
	    *dst_wk-- = hextbl[chr >> 4];
	}
    } else
    {
	for (i = 0, src_wk = src, dst_wk = dst; i < length;
	     i++, src_wk++)
	{
	    chr = *src_wk;
	    *dst_wk++ = hextbl[chr >> 4];
	    *dst_wk++ = hextbl[chr % 16];
	}
    }
    dst[2 * length] = '\0';
    return length;
}

SQLLEN pg_hex2bin(const UCHAR * src, UCHAR * dst, SQLLEN length)
{
    UCHAR chr;
    const UCHAR *src_wk;
    UCHAR *dst_wk;
    SQLLEN i;
    int val;
    BOOL HByte = TRUE;

    for (i = 0, src_wk = src, dst_wk = dst; i < length; i++, src_wk++)
    {
	chr = *src_wk;
	if (!chr)
	    break;
	if (chr >= 'a' && chr <= 'f')
	    val = chr - 'a' + 10;
	else if (chr >= 'A' && chr <= 'F')
	    val = chr - 'A' + 10;
	else
	    val = chr - '0';
	if (HByte)
	    *dst_wk = (val << 4);
	else
	{
	    *dst_wk += val;
	    dst_wk++;
	}
	HByte = !HByte;
    }
    *dst_wk = '\0';
    return length;
}

