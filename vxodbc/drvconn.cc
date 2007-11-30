/*
 * Description:	This module contains only routines related to
 *		implementing SQLDriverConnect.
 */

#include "psqlodbc.h"

#include <stdio.h>
#include <stdlib.h>

#include "connection.h"

#ifndef WIN32
#include <sys/types.h>
#include <sys/socket.h>
#define NEAR
#else
#include <winsock2.h>
#endif

#include <string.h>

#ifdef WIN32
#include <windowsx.h>
#include "resource.h"
#endif
#include "pgapifunc.h"

#include "dlg_specific.h"

#define	NULL_IF_NULL(a) (a ? a : "(NULL)")

static char *hide_password(const char *str)
{
    char *outstr, *pwdp;

    if (!str)
	return NULL;
    outstr = strdup(str);
    if (pwdp = strstr(outstr, "PWD="), !pwdp)
	pwdp = strstr(outstr, "pwd=");
    if (pwdp)
    {
	char *p;

	for (p = pwdp + 4; *p && *p != ';'; p++)
	    *p = 'x';
    }
    return outstr;
}

/* prototypes */
void dconn_get_connect_attributes(const SQLCHAR FAR * connect_string,
				  ConnInfo * ci);
static void dconn_get_common_attributes(const SQLCHAR FAR *
					connect_string, ConnInfo * ci);

#ifdef WIN32
#ifdef __cplusplus
extern "C" {
#endif
    BOOL CALLBACK dconn_FDriverConnectProc(HWND hdlg, UINT wMsg,
					   WPARAM wParam, LPARAM lParam);
    RETCODE dconn_DoDialog(HWND hwnd, ConnInfo* ci);
#ifdef __cplusplus
}
#endif

extern HINSTANCE NEAR s_hModule;	/* Saved module handle. */
#endif


RETCODE SQL_API
PGAPI_DriverConnect(HDBC hdbc,
		    HWND hwnd,
		    const SQLCHAR FAR * szConnStrIn,
		    SQLSMALLINT cbConnStrIn,
		    SQLCHAR FAR * szConnStrOut,
		    SQLSMALLINT cbConnStrOutMax,
		    SQLSMALLINT FAR * pcbConnStrOut,
		    SQLUSMALLINT fDriverCompletion)
{
    CSTR func = "PGAPI_DriverConnect";
    ConnectionClass *conn = (ConnectionClass *) hdbc;

    CC_log_error(func, "SQLDriverConnect is not yet supported", conn);
    return SQL_ERROR;
}

#ifdef WIN32
RETCODE dconn_DoDialog(HWND hwnd, ConnInfo * ci)
{
    LRESULT dialog_result;

    mylog("dconn_DoDialog: ci = %p\n", ci);

    if (hwnd)
    {
	dialog_result =
	    DialogBoxParam(s_hModule, MAKEINTRESOURCE(DLG_CONFIG), hwnd,
			   dconn_FDriverConnectProc, (LPARAM) ci);
	if (!dialog_result || (dialog_result == -1))
	    return SQL_NO_DATA_FOUND;
	else
	    return SQL_SUCCESS;
    }

    return SQL_ERROR;
}


BOOL CALLBACK dconn_FDriverConnectProc(HWND hdlg, UINT wMsg,
				       WPARAM wParam, LPARAM lParam)
{
    ConnInfo *ci;

    switch (wMsg)
    {
    case WM_INITDIALOG:
	ci = (ConnInfo *) lParam;

	/* Change the caption for the setup dialog */
	SetWindowText(hdlg, "Versabanq PLEXUS Connection");

	/* Hide the DSN and description fields */
	ShowWindow(GetDlgItem(hdlg, IDC_DSNAMETEXT), SW_HIDE);
	ShowWindow(GetDlgItem(hdlg, IDC_DSNAME), SW_HIDE);
	ShowWindow(GetDlgItem(hdlg, IDC_TEST), SW_HIDE);
	if ('\0' != ci->server[0])
	    EnableWindow(GetDlgItem(hdlg, IDC_SERVER), FALSE);
	if ('\0' != ci->port[0])
	    EnableWindow(GetDlgItem(hdlg, IDC_PORT), FALSE);

	SetWindowLongPtr(hdlg, DWLP_USER, lParam); /* Save the ConnInfo for
						    * the "OK" */
	SetDlgStuff(hdlg, ci);

	if (ci->database[0] == '\0')
	    ;			/* default focus */
	else if (ci->server[0] == '\0')
	    SetFocus(GetDlgItem(hdlg, IDC_SERVER));
	else if (ci->port[0] == '\0')
	    SetFocus(GetDlgItem(hdlg, IDC_PORT));
	else if (ci->username[0] == '\0')
	    SetFocus(GetDlgItem(hdlg, IDC_USER));
	else if (ci->focus_password)
	    SetFocus(GetDlgItem(hdlg, IDC_PASSWORD));
	break;

    case WM_COMMAND:
	switch (GET_WM_COMMAND_ID(wParam, lParam))
	{
	case IDOK:
	    ci = (ConnInfo *) GetWindowLongPtr(hdlg, DWLP_USER);

	    GetDlgStuff(hdlg, ci);

	case IDCANCEL:
	    EndDialog(hdlg, GET_WM_COMMAND_ID(wParam, lParam) == IDOK);
	    return TRUE;
	}
    }

    return FALSE;
}
#endif				/* WIN32 */


typedef BOOL (*copyfunc) (ConnInfo *, const char *attribute,
			  const char *value);
static void dconn_get_attributes(copyfunc func,
				 const SQLCHAR FAR * connect_string,
				 ConnInfo * ci)
{
    char *our_connect_string;
    const char *pair, *attribute, *value;
    char *equals;
    char *strtok_arg;
#ifdef	HAVE_STRTOK_R
    char *last;
#endif				/* HAVE_STRTOK_R */

    our_connect_string = strdup((const char *)connect_string);
    strtok_arg = our_connect_string;

#ifdef	FORCE_PASSWORD_DISPLAY
    mylog("our_connect_string = '%s'\n", our_connect_string);
#else
    if (get_mylog())
    {
	char *hide_str = hide_password(our_connect_string);

	mylog("our_connect_string = '%s'\n", hide_str);
	free(hide_str);
    }
#endif				/* FORCE_PASSWORD_DISPLAY */

    while (1)
    {
#ifdef	HAVE_STRTOK_R
	pair = strtok_r(strtok_arg, ";", &last);
#else
	pair = strtok(strtok_arg, ";");
#endif				/* HAVE_STRTOK_R */
	if (strtok_arg)
	    strtok_arg = 0;
	if (!pair)
	    break;

	equals = strchr(pair, '=');
	if (!equals)
	    continue;

	*equals = '\0';
	attribute = pair;	/* ex. DSN */
	value = equals + 1;	/* ex. 'CEO co1' */

#ifndef	FORCE_PASSWORD_DISPLAY
	if (stricmp(attribute, INI_PASSWORD) == 0 ||
	    stricmp(attribute, "pwd") == 0)
	    mylog("attribute = '%s', value = 'xxxxx'\n", attribute);
	else
#endif				/* FORCE_PASSWORD_DISPLAY */
	    mylog("attribute = '%s', value = '%s'\n", attribute, value);

	if (!attribute || !value)
	    continue;

	/* Copy the appropriate value to the conninfo  */
	(*func) (ci, attribute, value);

    }

    free(our_connect_string);
}

void dconn_get_connect_attributes(const SQLCHAR FAR * connect_string,
				  ConnInfo * ci)
{

    CC_conninfo_init(ci);
    dconn_get_attributes(copyAttributes, connect_string, ci);
}

static void dconn_get_common_attributes(const SQLCHAR FAR * connect_string,
					ConnInfo * ci)
{
    dconn_get_attributes(copyCommonAttributes, connect_string, ci);
}
