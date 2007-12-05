/*
 * Description:	This module contains routines related to
 *		reading and storing the field information from a query.
 */

#include "pgtypes.h"
#include "columninfo.h"

#include "connection.h"
#include <stdlib.h>
#include <string.h>
#include "pgapifunc.h"

ColumnInfoClass *CI_Constructor()
{
    ColumnInfoClass *rv;

    rv = (ColumnInfoClass *) malloc(sizeof(ColumnInfoClass));

    if (rv)
    {
	rv->num_fields = 0;
	rv->coli_array = NULL;
    }

    return rv;
}


void CI_Destructor(ColumnInfoClass * self)
{
    CI_free_memory(self);

    free(self);
}


void CI_free_memory(ColumnInfoClass * self)
{
    register Int2 lf;
    int num_fields = self->num_fields;

    /* Safe to call even if null */
    self->num_fields = 0;
    if (self->coli_array)
    {
	for (lf = 0; lf < num_fields; lf++)
	{
	    if (self->coli_array[lf].name)
	    {
		free(self->coli_array[lf].name);
		self->coli_array[lf].name = NULL;
	    }
	}
	free(self->coli_array);
	self->coli_array = NULL;
    }
}


#ifdef __cplusplus
typedef ColumnInfoClass_::srvr_info  srvr_info;
#endif

void
CI_set_num_fields(ColumnInfoClass * self, int new_num_fields,
		  BOOL allocrelatt)
{
    CI_free_memory(self);	/* always safe to call */

    self->num_fields = new_num_fields;

    self->coli_array = (srvr_info *)
	calloc(sizeof(srvr_info), self->num_fields);
}


void
CI_set_field_info(ColumnInfoClass * self, int field_num, 
		  const char *new_name,
		  OID new_adtid, Int2 new_adtsize, Int4 new_atttypmod,
		  OID new_relid, OID new_attid)
{
    /* check bounds */
    if ((field_num < 0) || (field_num >= self->num_fields))
	return;

    /* store the info */
    self->coli_array[field_num].name = strdup(new_name);
    self->coli_array[field_num].adtid = new_adtid;
    self->coli_array[field_num].adtsize = new_adtsize;
    self->coli_array[field_num].atttypmod = new_atttypmod;

    self->coli_array[field_num].display_size = 0;
    self->coli_array[field_num].relid = new_relid;
    self->coli_array[field_num].attid = new_attid;
}
