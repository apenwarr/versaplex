/*
 * Description:	This module contains only routines related to
 *		implementing SQLDriverConnect.
 */

#include "psqlodbc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

#include "wvdbusconn.h"

#include "dlg_specific.h"
#include "wvssl_necessities.h"

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
    ConnInfo *ci;

#ifdef WIN32
    RETCODE dialog_result;
#endif
    BOOL paramRequired, didUI = FALSE;
    RETCODE result;
    char *connStrIn = NULL;
    char connStrOut[MAX_CONNECT_STRING];
    int retval;
    char salt[5];
    char password_required = AUTH_REQ_OK;
    ssize_t len = 0;
    SQLSMALLINT lenStrout;


    mylog("%s: entering...\n", func);
    init_wvssl();

    if (!conn)
    {
	CC_log_error(func, "", NULL);
	return SQL_INVALID_HANDLE;
    }

    connStrIn = make_string(szConnStrIn, cbConnStrIn, NULL, 0);

#ifdef	FORCE_PASSWORD_DISPLAY
    mylog
	("**** PGAPI_DriverConnect: fDriverCompletion=%d, connStrIn='%s'\n",
	 fDriverCompletion, connStrIn);
    qlog("conn=%p, PGAPI_DriverConnect( in)='%s', fDriverCompletion=%d\n", conn, connStrIn, fDriverCompletion);
#else
    if (get_qlog() || get_mylog())
    {
	char *hide_str = hide_password(connStrIn);

	mylog
	    ("**** PGAPI_DriverConnect: fDriverCompletion=%d, connStrIn='%s'\n",
	     fDriverCompletion, NULL_IF_NULL(hide_str));
	qlog("conn=%p, PGAPI_DriverConnect( in)='%s', fDriverCompletion=%d\n", conn, NULL_IF_NULL(hide_str), fDriverCompletion);
	if (hide_str)
	    free(hide_str);
    }
#endif				/* FORCE_PASSWORD_DISPLAY */

    ci = &(conn->connInfo);

    /* Parse the connect string and fill in conninfo for this hdbc. */
    dconn_get_connect_attributes((const UCHAR *)connStrIn, ci);

    bool dbus_provided = ci->dbus_moniker != NULL && ci->dbus_moniker[0] != '\0';

    /*
     * If the ConnInfo in the hdbc is missing anything, this function will
     * fill them in from the registry (assuming of course there is a DSN
     * given -- if not, it does nothing!)
     */
    getDSNinfo(ci, CONN_DONT_OVERWRITE);
    dconn_get_common_attributes((const UCHAR *)connStrIn, ci);
    logs_on_off(1, TRUE, TRUE);
    if (connStrIn)
    {
	free(connStrIn);
	connStrIn = NULL;
    }

    /* Fill in any default parameters if they are not there. */
    getDSNdefaults(ci);
    CC_initialize_pg_version(conn);
    memset(salt, 0, sizeof(salt));

    ci->focus_password = password_required;

    inolog("DriverCompletion=%d\n", fDriverCompletion);
    switch (fDriverCompletion)
    {
#ifdef WIN32
    case SQL_DRIVER_PROMPT:
	dialog_result = dconn_DoDialog(hwnd, ci);
	didUI = TRUE;
	if (dialog_result != SQL_SUCCESS)
	    return dialog_result;
	break;

    case SQL_DRIVER_COMPLETE_REQUIRED:

	/* Fall through */

    case SQL_DRIVER_COMPLETE:

	paramRequired = password_required;
	/* Password is not a required parameter. */
	if (ci->database[0] == '\0')
	    paramRequired = TRUE;
	else if (!dbus_provided && ci->port[0] == '\0')
	    paramRequired = TRUE;
#ifdef	WIN32
	else if (!dbus_provided && ci->server[0] == '\0')
	    paramRequired = TRUE;
#endif				/* WIN32 */
	if (paramRequired)
	{
	    dialog_result = dconn_DoDialog(hwnd, ci);
	    didUI = TRUE;
	    if (dialog_result != SQL_SUCCESS)
		return dialog_result;
	}
	break;
#else
    case SQL_DRIVER_PROMPT:
    case SQL_DRIVER_COMPLETE:
    case SQL_DRIVER_COMPLETE_REQUIRED:
#endif
    case SQL_DRIVER_NOPROMPT:
	break;
    }

    /*
     * Password is not a required parameter unless authentication asks for
     * it. For now, I think it's better to just let the application ask
     * over and over until a password is entered (the user can always hit
     * Cancel to get out)
     */
    paramRequired = FALSE;
    WvString missingoptions = "Connection string lacks options: ";
    if (ci->database[0] == '\0')
    {
	paramRequired = TRUE;
	missingoptions.append("'database' ");
    }
    else if (!dbus_provided && ci->port[0] == '\0')
    {
	paramRequired = TRUE;
	missingoptions.append("'port' ");
    }
    else if (!dbus_provided && ci->server[0] == '\0')
    {
	paramRequired = TRUE;
	missingoptions.append("'server' ");
    }
    if (!dbus_provided)
    {
        missingoptions.append("'dbus connection' ");
    }
    if (paramRequired)
    {
	if (didUI)
	    return SQL_NO_DATA_FOUND;
	CC_set_error(conn, CONN_OPENDB_ERROR,
		     missingoptions, func);
	return SQL_ERROR;
    }

    if (!dbus_provided)
    {
        // If we weren't provided with a pre-made DBus moniker, use the
        // provided server and port.  If we weren't provided with those
        // either, we'll have already returned an error above.
        WvString moniker("dbus:tcp:host=%s,port=%s", ci->server, ci->port);
        mylog("Moniker=%s\n", moniker.cstr());
        if (moniker.len() < sizeof(ci->dbus_moniker))
            strncpy(ci->dbus_moniker, moniker.cstr(), sizeof(ci->dbus_moniker));
        else
        {
            CC_set_error(conn, CONN_OPENDB_ERROR, 
                "The DBus connection moniker was too long.", func);
            return SQL_ERROR;
        }
    }

    mylog("PGAPI_DriverConnect making DBus connection to %s\n", 
        ci->dbus_moniker);
    mylog("dbus:session is '%s'\n", getenv("DBUS_SESSION_BUS_ADDRESS"));
    conn->dbus = new WvDBusConn(ci->dbus_moniker);
    
    WvDBusMsg reply = conn->dbus->send_and_wait
	(WvDBusMsg("vx.versaplexd", "/db", "vx.db", "Test"),
	 15000);
    
    if (!conn->dbus->isok())
    {
        CC_set_error(conn, CONN_OPENDB_ERROR, WvString(
            "Could not open DBus connection to %s: %s (%s).", 
	    ci->dbus_moniker,
            conn->dbus->errstr(), conn->dbus->geterr()).cstr(), 
            func);
        return SQL_ERROR;
    }
    
    if (reply.iserror())
    {
	WvDBusMsg::Iter i(reply);
	WvString errstr = i.getnext();
        CC_set_error(conn, CONN_OPENDB_ERROR, WvString(
            "DBus connected, but test failed: %s.  Is versaplexd running?", 
	    errstr).cstr(),
            func);
        return SQL_ERROR;
    }

    /*
     * Create the Output Connection String
     */
    result = SQL_SUCCESS;

    lenStrout = cbConnStrOutMax;
    if (conn->ms_jet && lenStrout > 255)
	lenStrout = 255;
    makeConnectString(connStrOut, ci, lenStrout);
    len = strlen(connStrOut);

    if (szConnStrOut)
    {
	/*
	 * Return the completed string to the caller. The correct method
	 * is to only construct the connect string if a dialog was put up,
	 * otherwise, it should just copy the connection input string to
	 * the output. However, it seems ok to just always construct an
	 * output string.  There are possible bad side effects on working
	 * applications (Access) by implementing the correct behavior,
	 * anyway.
	 */
	/*strncpy_null(szConnStrOut, connStrOut, cbConnStrOutMax); */
	strncpy((char *)szConnStrOut, connStrOut, cbConnStrOutMax);

	if (len >= cbConnStrOutMax)
	{
	    int clen;

	    for (clen = cbConnStrOutMax - 1;
		 clen >= 0 && szConnStrOut[clen] != ';'; clen--)
		szConnStrOut[clen] = '\0';
	    result = SQL_SUCCESS_WITH_INFO;
	    CC_set_error(conn, CONN_TRUNCATED,
			 "The buffer was too small for the ConnStrOut.",
			 func);
	}
    }

    if (pcbConnStrOut)
	*pcbConnStrOut = (SQLSMALLINT) len;

#ifdef	FORCE_PASSWORD_DISPLAY
    if (cbConnStrOutMax > 0)
    {
	mylog("szConnStrOut = '%s' len=%d,%d\n",
	      NULL_IF_NULL(szConnStrOut), len, cbConnStrOutMax);
	qlog("conn=%p, PGAPI_DriverConnect(out)='%s'\n", conn,
	     NULL_IF_NULL(szConnStrOut));
    }
#else
    if (get_qlog() || get_mylog())
    {
	char *hide_str = NULL;

	if (cbConnStrOutMax > 0)
	    hide_str = hide_password((const char *)szConnStrOut);
	mylog("szConnStrOut = '%s' len=%d,%d\n", NULL_IF_NULL(hide_str),
	      len, cbConnStrOutMax);
	qlog("conn=%p, PGAPI_DriverConnect(out)='%s'\n", conn,
	     NULL_IF_NULL(hide_str));
	if (hide_str)
	    free(hide_str);
    }
#endif				/* FORCE_PASSWORD_DISPLAY */

    if (connStrIn)
	free(connStrIn);
    mylog("PGAPI_DriverConnect: returning %d\n", result);
    return result;
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
	SetWindowText(hdlg, "Versaplex Connection");

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

	equals = strchr((char *)pair, '=');
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
