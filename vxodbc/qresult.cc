/*
 * Description:	This module contains functions related to
 *		managing result information (i.e, fetching rows
 *		from the backend, managing the tuple cache, etc.)
 *		and retrieving it.	Depending on the situation, a
 *		QResultClass will hold either data from the backend
 *		or a manually built result.
 */
#include "qresult.h"
#include "statement.h"

#include "misc.h"
#include <stdio.h>
#include <string.h>
#include <limits.h>

/*
 *	Used for building a Manual Result only
 *	All info functions call this function to create the manual result set.
 */
void QR_set_num_fields(QResultClass * self, int new_num_fields)
{
    BOOL allocrelatt = FALSE;

    if (!self)
	return;
    mylog("in QR_set_num_fields\n");

    CI_set_num_fields(self->fields, new_num_fields, allocrelatt);

    mylog("exit QR_set_num_fields\n");
}


void QR_set_position(QResultClass * self, SQLLEN pos)
{
    self->tupleField =
	self->backend_tuples +
	((QR_get_rowstart_in_cache(self) + pos) * self->num_fields);
}


void QR_set_cache_size(QResultClass * self, SQLLEN cache_size)
{
    self->cache_size = cache_size;
}


void QR_set_rowset_size(QResultClass * self, Int4 rowset_size)
{
    self->rowset_size_include_ommitted = rowset_size;
}

void QR_set_cursor(QResultClass * self, const char *name)
{
    ConnectionClass *conn = QR_get_conn(self);

    if (self->cursor_name)
    {
	free(self->cursor_name);
	if (conn)
	{
	    CONNLOCK_ACQUIRE(conn);
	    conn->ncursors--;
	    CONNLOCK_RELEASE(conn);
	}
	self->cursTuple = -1;
	self->pstatus = 0;
    }
    if (name)
    {
	self->cursor_name = strdup(name);
	if (conn)
	{
	    CONNLOCK_ACQUIRE(conn);
	    conn->ncursors++;
	    CONNLOCK_RELEASE(conn);
	}
    } else
    {
	self->cursor_name = NULL;
	QR_set_no_cursor(self);
    }
}


void QR_set_num_cached_rows(QResultClass * self, SQLLEN num_rows)
{
    self->num_cached_rows = num_rows;
    if (QR_synchronize_keys(self))
	self->num_cached_keys = self->num_cached_rows;
}

void QR_set_rowstart_in_cache(QResultClass * self, SQLLEN start)
{
    mylog("qrflags:%x\n", (int)self->flags);
    if (QR_synchronize_keys(self))
	self->key_base = start;
    self->base = start;
}

void QR_inc_rowstart_in_cache(QResultClass * self, SQLLEN base_inc)
{
    if (!QR_has_valid_base(self))
	mylog
	    ("QR_inc_rowstart_in_cache called while the cache is not ready\n");
    self->base += base_inc;
    if (QR_synchronize_keys(self))
	self->key_base = self->base;
}


/*
 * CLASS QResult
 */
QResultClass *QR_Constructor()
{
    QResultClass *rv;

    mylog("in QR_Constructor\n");
    rv = (QResultClass *) malloc(sizeof(QResultClass));

    if (rv != NULL)
    {
	rv->rstatus = PORES_EMPTY_QUERY;
	rv->pstatus = 0;

	/* construct the column info */
	if (!(rv->fields = CI_Constructor()))
	{
	    free(rv);
	    return NULL;
	}
	rv->backend_tuples = NULL;
	rv->sqlstate[0] = '\0';
	rv->message = NULL;
	rv->command = NULL;
	rv->notice = NULL;
	rv->conn = NULL;
	rv->next = NULL;
	rv->pstatus = 0;
	rv->count_backend_allocated = 0;
	rv->count_keyset_allocated = 0;
	rv->num_total_read = 0;
	rv->num_cached_rows = 0;
	rv->num_cached_keys = 0;
	rv->fetch_number = 0;
	rv->flags = 0;
	QR_set_rowstart_in_cache(rv, -1);
	rv->key_base = -1;
	rv->recent_processed_row_count = -1;
	rv->cursTuple = -1;
	rv->move_offset = 0;
	rv->num_fields = 0;
	rv->num_key_fields = PG_NUM_NORMAL_KEYS;	/* CTID + OID */
	rv->tupleField = NULL;
	rv->cursor_name = NULL;
	rv->aborted = FALSE;

	rv->cache_size = 0;
	rv->rowset_size_include_ommitted = 1;
	rv->move_direction = 0;
	rv->keyset = NULL;
	rv->reload_count = 0;
	rv->rb_alloc = 0;
	rv->rb_count = 0;
	rv->rollback = NULL;
	rv->ad_alloc = 0;
	rv->ad_count = 0;
	rv->added_keyset = NULL;
	rv->added_tuples = NULL;
	rv->up_alloc = 0;
	rv->up_count = 0;
	rv->updated = NULL;
	rv->updated_keyset = NULL;
	rv->updated_tuples = NULL;
	rv->dl_alloc = 0;
	rv->dl_count = 0;
	rv->deleted = NULL;
	rv->deleted_keyset = NULL;
    }

    mylog("exit QR_Constructor\n");
    return rv;
}


// VX_CLEANUP: There's a decent chance this is useless
void QR_close_result(QResultClass * self, BOOL destroy)
{
    ConnectionClass *conn;

    if (!self)
	return;
    mylog("QResult: in QR_close_result\n");

    /*
     * If conn is defined, then we may have used "backend_tuples", so in
     * case we need to, free it up.  Also, close the cursor.
     */
    QR_free_memory(self);	/* safe to call anyway */

    /* Should have been freed in the close() but just in case... */
    QR_set_cursor(self, NULL);

    /* Free up column info */
    if (destroy && self->fields)
    {
	CI_Destructor(self->fields);
	self->fields = NULL;
    }

    /* Free command info (this is from strdup()) */
    if (self->command)
    {
	free(self->command);
	self->command = NULL;
    }

    /* Free message info (this is from strdup()) */
    if (self->message)
    {
	free(self->message);
	self->message = NULL;
    }

    /* Free notice info (this is from strdup()) */
    if (self->notice)
    {
	free(self->notice);
	self->notice = NULL;
    }
    /* Destruct the result object in the chain */
    QR_Destructor(self->next);
    self->next = NULL;

    mylog("QResult: exit close_result\n");
    if (destroy)
    {
	free(self);
    }
}

void QR_Destructor(QResultClass * self)
{
    mylog("QResult: enter DESTRUCTOR (%p)\n", self);
    if (!self)
	return;
    QR_close_result(self, TRUE);

    mylog("QResult: exit DESTRUCTOR\n");
}


void QR_set_command(QResultClass * self, const char *msg)
{
    if (self->command)
	free(self->command);

    self->command = msg ? strdup(msg) : NULL;
}


void QR_set_message(QResultClass * self, const char *msg)
{
    if (self->message)
	free(self->message);

    self->message = msg ? strdup(msg) : NULL;
}

void QR_add_message(QResultClass * self, const char *msg)
{
    char *message = self->message;
    size_t alsize, pos;

    if (!msg || !msg[0])
	return;
    if (message)
    {
	pos = strlen(message) + 1;
	alsize = pos + strlen(msg) + 1;
    } else
    {
	pos = 0;
	alsize = strlen(msg) + 1;
    }
    message = (char *)realloc(message, alsize);
    if (pos > 0)
	message[pos - 1] = ';';
    strcpy(message + pos, msg);
    self->message = message;
}


void QR_set_notice(QResultClass * self, const char *msg)
{
    if (self->notice)
	free(self->notice);

    self->notice = msg ? strdup(msg) : NULL;
}

void QR_add_notice(QResultClass * self, const char *msg)
{
    char *message = self->notice;
    size_t alsize, pos;

    if (!msg || !msg[0])
	return;
    if (message)
    {
	pos = strlen(message) + 1;
	alsize = pos + strlen(msg) + 1;
    } else
    {
	pos = 0;
	alsize = strlen(msg) + 1;
    }
    message = (char *)realloc(message, alsize);
    if (pos > 0)
	message[pos - 1] = ';';
    strcpy(message + pos, msg);
    self->notice = message;
}


TupleField *QR_AddNew(QResultClass * self)
{
    size_t alloc;
    UInt4 num_fields;

    if (!self)
	return NULL;
    inolog("QR_AddNew %dth row(%d fields) alloc=%d\n",
	   self->num_cached_rows, QR_NumResultCols(self),
	   self->count_backend_allocated);
    if (num_fields = QR_NumResultCols(self), !num_fields)
	return NULL;
    if (self->num_fields <= 0)
    {
	self->num_fields = num_fields;
	QR_set_reached_eof(self);
    }
    alloc = self->count_backend_allocated;
    if (!self->backend_tuples)
    {
	self->num_cached_rows = 0;
	alloc = TUPLE_MALLOC_INC;
	self->backend_tuples = (TupleField *)
	    malloc(alloc * sizeof(TupleField) * num_fields);
    } else if (self->num_cached_rows >= self->count_backend_allocated)
    {
	alloc = self->count_backend_allocated * 2;
	self->backend_tuples = (TupleField *)
	    realloc(self->backend_tuples,
		    alloc * sizeof(TupleField) * num_fields);
    }
    self->count_backend_allocated = alloc;

    if (self->backend_tuples)
    {
	memset(self->backend_tuples +
	       num_fields * self->num_cached_rows, 0,
	       num_fields * sizeof(TupleField));
	self->num_cached_rows++;
	self->ad_count++;
    }
    return self->backend_tuples + num_fields * (self->num_cached_rows -
						1);
}

void QR_free_memory(QResultClass * self)
{
    SQLLEN num_backend_rows = self->num_cached_rows;
    int num_fields = self->num_fields;

    mylog("QResult: free memory in, fcount=%d\n", num_backend_rows);

    if (self->backend_tuples)
    {
	ClearCachedRows(self->backend_tuples, num_fields,
			num_backend_rows);
	free(self->backend_tuples);
	self->count_backend_allocated = 0;
	self->backend_tuples = NULL;
    }
    if (self->keyset)
    {
	ConnectionClass *conn = QR_get_conn(self);

	free(self->keyset);
	self->keyset = NULL;
	self->count_keyset_allocated = 0;
	// VX_CLEANUP: It might be smart to find whatever allocates this, and
	// kill it too.
	// VX_CLEANUP: Some of the remainder here might also be useless.
	self->reload_count = 0;
    }
    if (self->rollback)
    {
	free(self->rollback);
	self->rb_alloc = 0;
	self->rb_count = 0;
	self->rollback = NULL;
    }
    if (self->deleted)
    {
	free(self->deleted);
	self->deleted = NULL;
    }
    if (self->deleted_keyset)
    {
	free(self->deleted_keyset);
	self->deleted_keyset = NULL;
    }
    self->dl_alloc = 0;
    self->dl_count = 0;
    /* clear added info */
    if (self->added_keyset)
    {
	free(self->added_keyset);
	self->added_keyset = NULL;
    }
    if (self->added_tuples)
    {
	ClearCachedRows(self->added_tuples, num_fields, self->ad_count);
	free(self->added_tuples);
	self->added_tuples = NULL;
    }
    self->ad_alloc = 0;
    self->ad_count = 0;
    /* clear updated info */
    if (self->updated)
    {
	free(self->updated);
	self->updated = NULL;
    }
    if (self->updated_keyset)
    {
	free(self->updated_keyset);
	self->updated_keyset = NULL;
    }
    if (self->updated_tuples)
    {
	ClearCachedRows(self->updated_tuples, num_fields,
			self->up_count);
	free(self->updated_tuples);
	self->updated_tuples = NULL;
    }
    self->up_alloc = 0;
    self->up_count = 0;

    self->num_total_read = 0;
    self->num_cached_rows = 0;
    self->num_cached_keys = 0;
    self->cursTuple = -1;
    self->pstatus = 0;

    mylog("QResult: free memory out\n");
}


static SQLLEN enlargeKeyCache(QResultClass * self, SQLLEN add_size,
			      const char *message)
{
    size_t alloc, alloc_req;
    Int4 num_fields = self->num_fields;
    BOOL curs = (NULL != QR_get_cursor(self));

    if (add_size <= 0)
	return self->count_keyset_allocated;
    alloc = self->count_backend_allocated;
    if (num_fields > 0
	&& ((alloc_req = (Int4) self->num_cached_rows + add_size) >
	    alloc || !self->backend_tuples))
    {
	if (1 > alloc)
	{
	    if (curs)
		alloc = alloc_req;
	    else
		alloc =
		    (alloc_req >
		     TUPLE_MALLOC_INC ? alloc_req : TUPLE_MALLOC_INC);
	} else
	{
	    do
	    {
		alloc *= 2;
	    }
	    while (alloc < alloc_req);
	}
	self->count_backend_allocated = 0;
	QR_REALLOC_return_with_error(self->backend_tuples, TupleField,
				     num_fields * sizeof(TupleField) *
				     alloc, self, message, -1);
	self->count_backend_allocated = alloc;
    }
    alloc = self->count_keyset_allocated;
    if (QR_haskeyset(self)
	&& ((alloc_req = (Int4) self->num_cached_keys + add_size) >
	    alloc || !self->keyset))
    {
	if (1 > alloc)
	{
	    if (curs)
		alloc = alloc_req;
	    else
		alloc =
		    (alloc_req >
		     TUPLE_MALLOC_INC ? alloc_req : TUPLE_MALLOC_INC);
	} else
	{
	    do
	    {
		alloc *= 2;
	    }
	    while (alloc < alloc_req);
	}
	self->count_keyset_allocated = 0;
	QR_REALLOC_return_with_error(self->keyset, KeySet,
				     sizeof(KeySet) * alloc, self,
				     message, -1);
	self->count_keyset_allocated = alloc;
    }
    return alloc;
}

