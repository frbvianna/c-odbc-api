#ifndef __PGAPI__H__
#define __PGAPI__H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>

void list_drivers_sources(
    SQLHENV env
);

void connect_to_db(
    SQLHDBC dbc,
    SQLCHAR *in_conn_str
);

void free_handles(
    SQLHENV env,
    SQLHDBC dbc,
    SQLHSTMT stmt
);

void extract_error(
    char *func,
    SQLHANDLE handle,
    SQLSMALLINT type
);

#endif  //!__PGAPI__H__