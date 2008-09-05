#ifndef WVSSL_NECESSITIES_H
#define WVSSL_NECESSITIES_H

#include "wvx509mgr.h"

extern WvX509Mgr *clicert;

void init_wvssl();

/* You don't have to call this unless you're a stupid unit test checking
 * for valgrind errors.
 */
void cleanup_wvssl();

#endif
