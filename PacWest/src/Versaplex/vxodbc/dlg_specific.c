/*-------
 * Module:	dlg_specific.c
 *
 * Description:	This module contains any specific code for handling
 *		dialog boxes such as driver/datasource options.  Both the
 *		ConfigDSN() and the SQLDriverConnect() functions use
 *		functions in this module.  If you were to add a new option
 *		to any dialog box, you would most likely only have to change
 *		things in here rather than in 2 separate places as before.
 *
 * Classes:	none
 *
 * API functions: none
 *
 * Comments:	See "notice.txt" for copyright and license information.
 *-------
 */
/* Multibyte support	Eiji Tokuya 2001-03-15 */

#include <ctype.h>
#include "dlg_specific.h"

#include "convert.h"

#include "multibyte.h"
#include "pgapifunc.h"

extern GLOBAL_VALUES globals;

static void encode(const UCHAR * in, UCHAR * out);

UInt4 getExtraOptions(const ConnInfo * ci)
{
    UInt4 flag = 0;
    return flag;
}

CSTR hex_format = "%x";
CSTR dec_format = "%u";
CSTR octal_format = "%o";
static UInt4 replaceExtraOptions(ConnInfo * ci, UInt4 flag,
				 BOOL overwrite)
{
    return getExtraOptions(ci);
}

BOOL setExtraOptions(ConnInfo * ci, const char *optstr,
		     const char *format)
{
    UInt4 flag = 0;

    if (!format)
    {
	if ('0' == *optstr)
	{
	    switch (optstr[1])
	    {
	    case '\0':
		format = dec_format;
		break;
	    case 'x':
	    case 'X':
		optstr += 2;
		format = hex_format;
		break;
	    default:
		format = octal_format;
		break;
	    }
	} else
	    format = dec_format;
    }

    if (sscanf(optstr, format, &flag) < 1)
	return FALSE;
    replaceExtraOptions(ci, flag, TRUE);
    return TRUE;
}

UInt4 add_removeExtraOptions(ConnInfo * ci, UInt4 aflag, UInt4 dflag)
{
    return getExtraOptions(ci);
}

void makeConnectString(char *connect_string, const ConnInfo * ci, UWORD len)
{
    char got_dsn = (ci->dsn[0] != '\0');
    char encoded_conn_settings[LARGE_REGISTRY_LEN];
    ssize_t hlen, nlen, olen;
    /*BOOL          abbrev = (len <= 400); */
    BOOL abbrev = (len < 1024) || 0 < ci->force_abbrev_connstr;

    inolog("force_abbrev=%d abbrev=%d\n", ci->force_abbrev_connstr,
	   abbrev);
    /* fundamental info */
    nlen = MAX_CONNECT_STRING;
    olen =
	snprintf(connect_string, nlen,
		 "%s=%s;DATABASE=%s;SERVER=%s;PORT=%s;UID=%s;PWD=%s",
		 got_dsn ? "DSN" : "DRIVER",
		 got_dsn ? ci->dsn : ci->drivername, ci->database,
		 ci->server, ci->port, ci->username, ci->password);
    if (olen < 0 || olen >= nlen)
    {
	connect_string[0] = '\0';
	return;
    }

    encode(ci->conn_settings, encoded_conn_settings);

    /* extra info */
    hlen = strlen(connect_string);
    nlen = MAX_CONNECT_STRING - hlen;
    inolog("hlen=%d", hlen);
    if (!abbrev)
    {
	char protocol_and[16];

	if (ci->rollback_on_error >= 0)
	    snprintf(protocol_and, sizeof(protocol_and), "%s-%d",
		     ci->protocol, ci->rollback_on_error);
	else
	    strncpy(protocol_and, ci->protocol, sizeof(protocol_and));
	olen = snprintf(&connect_string[hlen], nlen, ";"
			INI_READONLY "=%s;", ci->onlyread
	    );
    }
    /* Abbreviation is needed ? */
    if (abbrev || olen >= nlen || olen < 0)
    {
	hlen = strlen(connect_string);
	nlen = MAX_CONNECT_STRING - hlen;
	olen = snprintf(&connect_string[hlen], nlen, ";");
	if (olen < nlen
	    && (PROTOCOL_74(ci) || ci->rollback_on_error >= 0))
	{
	    hlen = strlen(connect_string);
	    nlen = MAX_CONNECT_STRING - hlen;
	}
    }
    if (olen < 0 || olen >= nlen)	/* failed */
	connect_string[0] = '\0';
}

BOOL
copyAttributes(ConnInfo * ci, const char *attribute, const char *value)
{
    CSTR func = "copyAttributes";
    BOOL found = TRUE;

    if (stricmp(attribute, "DSN") == 0)
	strcpy(ci->dsn, value);

    else if (stricmp(attribute, "driver") == 0)
	strcpy(ci->drivername, value);

    else if (stricmp(attribute, INI_DATABASE) == 0)
	strcpy(ci->database, value);

    else if (stricmp(attribute, INI_SERVER) == 0
	     || stricmp(attribute, SPEC_SERVER) == 0)
	strcpy(ci->server, value);

    else if (stricmp(attribute, INI_USER) == 0
	     || stricmp(attribute, INI_UID) == 0)
	strcpy(ci->username, value);

    else if (stricmp(attribute, INI_PASSWORD) == 0
	     || stricmp(attribute, "pwd") == 0)
	strcpy(ci->password, value);

    else if (stricmp(attribute, INI_PORT) == 0)
	strcpy(ci->port, value);

    else if (stricmp(attribute, INI_READONLY) == 0)
	strcpy(ci->onlyread, value);
    else
	found = FALSE;

    mylog
	("%s: DSN='%s',server='%s',dbase='%s',user='%s',passwd='%s',port='%s',onlyread='%s',protocol='%s',conn_settings='%s',disallow_premature=%d)\n",
	 func, ci->dsn, ci->server, ci->database, ci->username,
	 ci->password ? "xxxxx" : "", ci->port, ci->onlyread,
	 ci->protocol, ci->conn_settings, ci->disallow_premature);

    return found;
}

BOOL copyCommonAttributes(ConnInfo * ci, const char *attribute,
			  const char *value)
{
    BOOL found = FALSE;
    return found;
}


void getDSNdefaults(ConnInfo * ci)
{
    mylog("calling getDSNdefaults\n");

    if (ci->port[0] == '\0')
	strcpy(ci->port, DEFAULT_PORT);

    if (ci->onlyread[0] == '\0')
	sprintf(ci->onlyread, "%d", globals.onlyread);

    if (ci->protocol[0] == '\0')
	strcpy(ci->protocol, DEFAULT_PROTOCOL);

    if (ci->fake_oid_index[0] == '\0')
	sprintf(ci->fake_oid_index, "%d", DEFAULT_FAKEOIDINDEX);

    if (ci->show_oid_column[0] == '\0')
	sprintf(ci->show_oid_column, "%d", DEFAULT_SHOWOIDCOLUMN);

    if (ci->show_system_tables[0] == '\0')
	sprintf(ci->show_system_tables, "%d", DEFAULT_SHOWSYSTEMTABLES);

    if (ci->row_versioning[0] == '\0')
	sprintf(ci->row_versioning, "%d", DEFAULT_ROWVERSIONING);

    if (ci->disallow_premature < 0)
	ci->disallow_premature = DEFAULT_DISALLOWPREMATURE;
    if (ci->allow_keyset < 0)
	ci->allow_keyset = DEFAULT_UPDATABLECURSORS;
    if (ci->lf_conversion < 0)
	ci->lf_conversion = DEFAULT_LFCONVERSION;
    if (ci->true_is_minus1 < 0)
	ci->true_is_minus1 = DEFAULT_TRUEISMINUS1;
    if (ci->int8_as < -100)
	ci->int8_as = DEFAULT_INT8AS;
    if (ci->bytea_as_longvarbinary < 0)
	ci->bytea_as_longvarbinary = DEFAULT_BYTEAASLONGVARBINARY;
    if (ci->use_server_side_prepare < 0)
	ci->use_server_side_prepare = DEFAULT_USESERVERSIDEPREPARE;
    if (ci->lower_case_identifier < 0)
	ci->lower_case_identifier = DEFAULT_LOWERCASEIDENTIFIER;
    if (ci->sslmode[0] == '\0')
	strcpy(ci->sslmode, DEFAULT_SSLMODE);
    if (ci->force_abbrev_connstr < 0)
	ci->force_abbrev_connstr = 0;
    if (ci->fake_mss < 0)
	ci->fake_mss = 0;
    if (ci->bde_environment < 0)
	ci->bde_environment = 0;
    if (ci->cvt_null_date_string < 0)
	ci->cvt_null_date_string = 0;
}

int
getDriverNameFromDSN(const char *dsn, char *driver_name, int namelen)
{
    return SQLGetPrivateProfileString(ODBC_DATASOURCES, dsn, "",
				      driver_name, namelen, ODBC_INI);
}

void getDSNinfo(ConnInfo * ci, char overwrite)
{
    CSTR func = "getDSNinfo";
    char *DSN = ci->dsn;

    /*
     *	If a driver keyword was present, then dont use a DSN and return.
     *	If DSN is null and no driver, then use the default datasource.
     */
    mylog("%s: DSN=%s overwrite=%d\n", func, DSN, overwrite);
    if (DSN[0] == '\0')
    {
	if (ci->drivername[0] != '\0')
	    return;
	else
	    strncpy_null(DSN, INI_DSN, sizeof(ci->dsn));
    }

    /* brute-force chop off trailing blanks... */
    while (*(DSN + strlen(DSN) - 1) == ' ')
	*(DSN + strlen(DSN) - 1) = '\0';

    if (ci->drivername[0] == '\0' || overwrite)
    {
	getDriverNameFromDSN(DSN, ci->drivername,
			     sizeof(ci->drivername));
	if (ci->drivername[0] && stricmp(ci->drivername, DBMS_NAME))
	    getCommonDefaults(ci->drivername, ODBCINST_INI, ci);
    }

    /* Proceed with getting info for the given DSN. */

    if (ci->server[0] == '\0' || overwrite)
	SQLGetPrivateProfileString(DSN, INI_SERVER, "", ci->server,
				   sizeof(ci->server), ODBC_INI);

    if (ci->database[0] == '\0' || overwrite)
	SQLGetPrivateProfileString(DSN, INI_DATABASE, "", ci->database,
				   sizeof(ci->database), ODBC_INI);

    if (ci->username[0] == '\0' || overwrite)
	SQLGetPrivateProfileString(DSN, INI_USER, "", ci->username,
				   sizeof(ci->username), ODBC_INI);

    if (ci->password[0] == '\0' || overwrite)
	SQLGetPrivateProfileString(DSN, INI_PASSWORD, "", ci->password,
				   sizeof(ci->password), ODBC_INI);

    if (ci->port[0] == '\0' || overwrite)
	SQLGetPrivateProfileString(DSN, INI_PORT, "", ci->port,
				   sizeof(ci->port), ODBC_INI);

    if (ci->onlyread[0] == '\0' || overwrite)
	SQLGetPrivateProfileString(DSN, INI_READONLY, "", ci->onlyread,
				   sizeof(ci->onlyread), ODBC_INI);

    /* Allow override of odbcinst.ini parameters here */
    getCommonDefaults(DSN, ODBC_INI, ci);

    qlog("DSN info: DSN='%s',server='%s',port='%s',dbase='%s',user='%s',passwd='%s'\n", DSN, ci->server, ci->port, ci->database, ci->username, ci->password ? "xxxxx" : "");
    qlog("          onlyread='%s',protocol='%s',showoid='%s',fakeoidindex='%s',showsystable='%s'\n", ci->onlyread, ci->protocol, ci->show_oid_column, ci->fake_oid_index, ci->show_system_tables);

    if (get_qlog())
    {
	UCHAR *enc = check_client_encoding(ci->conn_settings);

	qlog("          conn_settings='%s',conn_encoding='%s'\n",
	     ci->conn_settings, enc ? enc : (UCHAR*)"(null)");
	if (enc)
	    free(enc);
	qlog("          translation_dll='%s',translation_option='%s'\n",
	     ci->translation_dll, ci->translation_option);
    }
}

/*
 * This function writes any global parameters (that can be manipulated)
 * to the ODBCINST.INI portion of the registry
 */
int writeDriverCommoninfo(const char *fileName, const char *sectionName,
		      const GLOBAL_VALUES * comval)
{
    char tmp[128];
    int errc = 0;

    if (ODBCINST_INI == fileName && NULL == sectionName)
	sectionName = DBMS_NAME;

    if (stricmp(ODBCINST_INI, fileName) == 0)
	return errc;

    /*
     * Never update the onlyread from this module.
     */
    if (stricmp(ODBCINST_INI, fileName) == 0)
    {
	sprintf(tmp, "%d", comval->onlyread);
	SQLWritePrivateProfileString(sectionName, INI_READONLY, tmp,
				     fileName);
    }

    return errc;
}

/*	This is for datasource based options only */
void writeDSNinfo(const ConnInfo * ci)
{
    const char *DSN = ci->dsn;

    SQLWritePrivateProfileString(DSN,
				 INI_DATABASE, ci->database, ODBC_INI);

    SQLWritePrivateProfileString(DSN, INI_SERVER, ci->server, ODBC_INI);

    SQLWritePrivateProfileString(DSN, INI_PORT, ci->port, ODBC_INI);

    SQLWritePrivateProfileString(DSN, INI_USER, ci->username, ODBC_INI);
    SQLWritePrivateProfileString(DSN, INI_UID, ci->username, ODBC_INI);

    SQLWritePrivateProfileString(DSN,
				 INI_PASSWORD, ci->password, ODBC_INI);

    SQLWritePrivateProfileString(DSN,
				 INI_READONLY, ci->onlyread, ODBC_INI);
}


/*
 *	This function reads the ODBCINST.INI portion of
 *	the registry and gets any driver defaults.
 */
void
getCommonDefaults(const char *section, const char *filename,
		  ConnInfo * ci)
{
    char temp[256];
    GLOBAL_VALUES *comval;
    BOOL inst_position = (stricmp(filename, ODBCINST_INI) == 0);

    if (ci)
	comval = &(ci->drivers);
    else
	comval = &globals;
    if (!ci)
	logs_on_off(0, 0, 0);

    /* Dont allow override of an override! */
    if (inst_position)
    {
	/* Default state for future DSN's Readonly attribute */
	SQLGetPrivateProfileString(section, INI_READONLY, "",
				   temp, sizeof(temp), filename);
	if (temp[0])
	    comval->onlyread = atoi(temp);
	else
	    comval->onlyread = DEFAULT_READONLY;
    }
}

static void encode(const UCHAR * in, UCHAR * out)
{
    size_t i, ilen = strlen(in), o = 0;
    UCHAR inc;

    for (i = 0; i < ilen; i++)
    {
	inc = in[i];
	if (inc == '+')
	{
	    sprintf(&out[o], "%%2B");
	    o += 3;
	} else if (isspace(inc))
	    out[o++] = '+';
	else if (!isalnum(inc))
	{
	    sprintf(&out[o], "%%%02x", inc);
	    o += 3;
	} else
	    out[o++] = inc;
    }
    out[o++] = '\0';
}

