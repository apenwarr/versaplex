#include "uniconfroot.h"
#include "wvx509mgr.h"
#include "wvbase64.h"
#include "wvlinkerhack.h"
#include <wincrypt.h>

/* regwriter.cc
 * Only compiled on Windows, not necessary on Linux.  Takes a file called
 * vxodbc.ini (see the vxodbc.ini.tmpl in this directory), and transcribes it,
 * encrypted, into the Windows registry for VxODBC use.  Also checks to make
 * sure the certificates contained therein (cert = your SSL certificate,
 * privrsa = your private RSA key, dbuscert = certificate you're expecting the
 * dbus server VxODBC is connecting to to have) are valid and match.  Takes
 * no command line parameters, as I was lazy and didn't add any customization
 * features.
 *
 * If this program returns 0, then you're all good and set up to use VxODBC
 * and SSH.  Otherwise, something went wrong; consult the output and modify
 * your vxodbc.ini file accordingly.
 */

WV_LINK_TO(UniGenHack);

WvString wvprotectdata(WvStringParm data, WvStringParm description)
{
	DATA_BLOB encrypt_me, return_me;
	encrypt_me.cbData = data.len() + 1;
	encrypt_me.pbData = new BYTE[encrypt_me.cbData];
	/* OK, we assume sizeof(char) = sizeof(BYTE), but that should never
	* change
	*/
	memcpy(encrypt_me.pbData, data.cstr(), encrypt_me.cbData); //+ null ptr!
	WvString ret = WvString::null;
	WvDynBuf inbuf, outbuf;
	WvBase64Encoder encoder64;

	const unsigned int desclen = description.len() + 1;
	wchar_t *desc = new wchar_t[desclen];
	mbstowcs(desc, description.cstr(), desclen);
    
	if (!CryptProtectData(&encrypt_me, desc, NULL, NULL, NULL, 0, &return_me))
	{
		/* FIXME:  Any error reporting useful here?  GetLastError? */
		goto out;
	}

	inbuf.put(return_me.pbData, return_me.cbData);
	if (!encoder64.encodebufstr(inbuf, ret, false, true)) {
		ret = WvString::null;
	}

	    LocalFree(return_me.pbData);
out:
	delete [] desc;
	delete [] encrypt_me.pbData;
	return ret;
}

int main()
{
    int ret = 0;
    UniConfRoot conf("temp");

    if (!conf["win"].mount(
	"registry:HKEY_CURRENT_USER/Software/Versabanq/VxODBC"))
    {
	printf("FAILED TO MOUNT WINDOWS REGISTRY\n");
	return 1;
    }

    if (!conf["ini"].mount(
	"readonly:ini:vxodbc.ini"))
    {
	printf("FAILED TO MOUNT INI REGISTRY\n");
	return 2;
    }

    if (!conf.isok())
	return 255;

    conf["win/cert"].setme(WvString::null);
    conf["win/privrsa"].setme(WvString::null);
    if (conf["ini/cert"].exists() && conf["ini/privrsa"].exists())
    {
	printf("Found your certificate and RSA key... testing...\n");
	WvX509Mgr mycert;
	WvString cert = *conf["ini/cert"];
	WvString privrsa = *conf["ini/privrsa"];
	mycert.decode(WvX509::CertPEM, cert);
	mycert.decode(WvRSAKey::RsaPEM, privrsa);

	if (mycert.isok())
	{
	    printf("Your RSA key and certificate match; writing to registry\n");
	    conf["win/cert"].setme(wvprotectdata(cert, "My SSL certificate"));
	    conf["win/privrsa"].setme(wvprotectdata(privrsa,
					"My private RSA key"));
	}
	else
	{
	    printf("Error matching your RSA key and certificate; not writing\n");
	    ret = 101;
	}
    }
    else
    {
	printf("No certificate or RSA key found for me!\n");
	ret = 101;
    }

    conf["win/dbuscert"].setme(WvString::null);
    if (conf["ini/dbuscert"].exists())
    {
	printf("Found DBus certificate to connect to... testing...\n");
	WvX509 mydbuscert;
	WvString dbuscert = *conf["ini/dbuscert"];
	mydbuscert.decode(WvX509::CertPEM, dbuscert);

	if (mydbuscert.isok())
	{
	    printf("Writing expected DBus certificate to registry...\n");
	    conf["win/dbuscert"].setme(wvprotectdata(dbuscert,
				"Expected DBus certificate to connect to"));
	}
	else
	{
	    printf("Error loading your DBus certificate; not writing\n");
	    ret = 101;
	}
    }
    else
    {
	printf("No DBus certificate to expect found!\n");
	ret = 101;
    }
 
    return ret;
}
