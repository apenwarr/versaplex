#ifdef	WIN32
/*-------
 * Module:			dlg_wingui.c
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

#include "dlg_specific.h"
#include "win_setup.h"

#include "convert.h"

#include "multibyte.h"
#include "pgapifunc.h"

extern GLOBAL_VALUES globals;

extern HINSTANCE NEAR s_hModule;

void SetDlgStuff(HWND hdlg, const ConnInfo * ci)
{
    /*
     * If driver attribute NOT present, then set the datasource name
     */
    SetDlgItemText(hdlg, IDC_DSNAME, ci->dsn);
    SetDlgItemText(hdlg, IDC_DATABASE, ci->database);
    SetDlgItemText(hdlg, IDC_SERVER, ci->server);
    SetDlgItemText(hdlg, IDC_USER, ci->username);
    SetDlgItemText(hdlg, IDC_PASSWORD, ci->password);
    SetDlgItemText(hdlg, IDC_PORT, ci->port);
}


void GetDlgStuff(HWND hdlg, ConnInfo * ci)
{
    GetDlgItemText(hdlg, IDC_DATABASE, ci->database,
		   sizeof(ci->database));
    GetDlgItemText(hdlg, IDC_SERVER, ci->server, sizeof(ci->server));
    GetDlgItemText(hdlg, IDC_USER, ci->username, sizeof(ci->username));
    GetDlgItemText(hdlg, IDC_PASSWORD, ci->password,
		   sizeof(ci->password));
    GetDlgItemText(hdlg, IDC_PORT, ci->port, sizeof(ci->port));
}

#endif				/* WIN32 */
