#include "wvssl_necessities.h"
#include "wvmoniker.h"
#include "wvsslstream.h"
#include "wvstrutils.h"
#include "uniconfroot.h";
#include "wvlinkerhack.h"

#include <wincrypt.h>
#include "wvbase64.h"

WV_LINK_TO(UniGenHack);

WvX509Mgr *clicert = NULL;

/* So like... what the fsck?  Why, if we're overloading the SSL creation
 * moniker anyway, don't we pass in a suitable function for the callback, and
 * instead use the global assignment?  Well, good question.
 *
 * There is also an sslcert moniker, which need our callback, but takes a
 * certificate already encoded on the command line.  It's possible, however
 * unlikely, that someone would want to use that method (for, oh, say, testing).
 *
 * I don't want to overload sslcert... that thing is hairy.  So, I only
 * overload SSL so that it actually uses *a* certificate, and with the global
 * assignment to global_vcb, either method will get the callback!  Yay!
 */
static IWvStream *create_ssl(WvStringParm s, IObject *obj)
{
    return new WvSSLStream(IWvStream::create(s, obj), clicert, 0, false);
}

static WvMoniker<IWvStream> ssl_override("ssl", create_ssl, true);

static char *dbus_cert = NULL;

#ifdef _WIN32
static WvString wvunprotectdata(WvStringParm data)
{
    /* Data is base64 encoded, must first decrypt it */
    WvBase64Decoder decoder64;
    WvDynBuf databuf;

    decoder64.flushstrbuf(data, databuf, true);
    DATA_BLOB decrypt_me, return_me;
    decrypt_me.cbData = databuf.used();
    decrypt_me.pbData = new BYTE[decrypt_me.cbData];
    databuf.copy(decrypt_me.pbData, 0, decrypt_me.cbData);
    WvString ret = WvString::null;

    char *strdata;
    if (!CryptUnprotectData(&decrypt_me, NULL, NULL, NULL, NULL, 0, &return_me))
    {
	goto out;
    }

    /* Since, clearly, all went well, we should have a null-terminated
     * char * array stored in return_me.pbData */
    strdata = new char[return_me.cbData];
    memcpy(strdata, return_me.pbData, return_me.cbData);
    ret = WvString(strdata).unique();

    delete [] strdata;
    LocalFree(return_me.pbData);
out:
    delete [] decrypt_me.pbData;
    return ret;
}
#else
    #define wvunprotectdata
#endif

static bool verify_server(WvX509 *, WvSSLStream *s)
{
    if (dbus_cert)
    {
	WvString pcert = s->getattr("peercert");
	if (!strcmp(dbus_cert, trim_string(pcert.edit())))
	    return true;
    }

    return false;
}

static bool inited = false;

void init_wvssl()
{
    if (inited)
	return;

    UniConfRoot conf;
#ifndef _WIN32
    #warning On Linux, testing SSL requires a vxodbc.ini file.  Check template.
    conf.mount("ini:vxodbc.ini");
#else
    conf.mount("registry:HKEY_CURRENT_USER/Software/Versabanq/VxODBC");
#endif

    if (conf.isok() && conf["cert"].exists() && conf["privrsa"].exists())
    {
	clicert = new WvX509Mgr;
	clicert->decode(WvX509::CertPEM, wvunprotectdata(*conf["cert"]));
	clicert->decode(WvRSAKey::RsaPEM, wvunprotectdata(*conf["privrsa"]));

	if (!clicert->isok())
	    WVRELEASE(clicert);
    }

    if (conf.isok() && conf["dbuscert"].exists())
    {
	WvString dcert = *conf["dbuscert"];
	dbus_cert = new char[dcert.len() + 1];
	dbus_cert = trim_string(strcpy(dbus_cert, dcert.cstr()));
	WvSSLStream::global_vcb = verify_server;
    }

    inited = true;
}

void cleanup_wvssl()
{
    WVRELEASE(clicert);
    WvSSLStream::global_vcb = NULL;
    delete [] dbus_cert;
    inited = false;
}
