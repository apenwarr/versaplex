/* File:			convert.h
 *
 * Description:		See "convert.c"
 *
 * Comments:		See "notice.txt" for copyright and license information.
 *
 */

#ifndef __CONVERT_H__
#define __CONVERT_H__

#include "psqlodbc.h"

#ifdef	__cplusplus
extern "C" {
#endif
/* copy_and_convert results */
#define COPY_OK							0
#define COPY_UNSUPPORTED_TYPE					1
#define COPY_UNSUPPORTED_CONVERSION				2
#define COPY_RESULT_TRUNCATED					3
#define COPY_GENERAL_ERROR						4
#define COPY_NO_DATA_FOUND						5

typedef struct
{
	int		infinity;
	int			m;
	int			d;
	int			y;
	int			hh;
	int			mm;
	int			ss;
	int			fr;
} SIMPLE_TIME;

int		copy_and_convert_field_bindinfo(StatementClass *stmt, OID field_type, void *value, int col);
int	copy_and_convert_field(StatementClass *stmt, OID field_type,
			void *value, SQLSMALLINT fCType, PTR rgbValue,
			SQLLEN cbValueMax, SQLLEN *pcbValue, SQLLEN *pIndicator);

BOOL		convert_money(const char *s, char *sout, size_t soutmax);
char		parse_datetime(const char *buf, SIMPLE_TIME *st);
size_t		convert_linefeeds(const char *s, char *dst, size_t max, BOOL convlf, BOOL *changed);
size_t		convert_special_chars(const char *si, char *dst, SQLLEN used, UInt4 flags,int ccsc, int escape_ch);

int		convert_pgbinary_to_char(const char *value, char *rgbValue, ssize_t cbValueMax);
size_t		convert_from_pgbinary(const UCHAR *value, UCHAR *rgbValue, SQLLEN cbValueMax);
SQLLEN		pg_hex2bin(const UCHAR *in, UCHAR *out, SQLLEN len);
Int4		findTag(const char *str, char dollar_quote, int ccsc);

#ifdef	__cplusplus
}
#endif
#endif
