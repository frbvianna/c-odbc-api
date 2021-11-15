#include <stdio.h>
#include <stdlib.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include "pgapi/pgapi.h"

int main(void) {

    SQLRETURN ret;

    SQLHENV env = NULL;
    SQLHDBC dbc = NULL;
    SQLHSTMT stmt = NULL;
    
    /**
     * Allocate environment handle
     **/
    printf("=== allocating env handle\n");
    ret = SQLAllocHandle(SQL_HANDLE_ENV,
                         SQL_NULL_HANDLE,
                         &env);
    if ( !SQL_SUCCEEDED(ret) ) {
        printf(env == SQL_NULL_HENV ?
              "env handle not allocated\n" :
              "env handle allocated\n");
    }

    /**
     * Set attributes for environment handle
     **/
    printf("=== setting env attribute: ODBC version 3.80\n");
    ret = SQLSetEnvAttr(env,
                        SQL_ATTR_ODBC_VERSION,
                        (void*) SQL_OV_ODBC3_80,
                        0);
    if (ret != SQL_SUCCESS) {
        exit(EXIT_FAILURE);
    }
    
    //list_drivers_sources(env);

    /**
     * Allocate db connection handle
     **/
    printf("=== allocating dbc handle\n");
    ret = SQLAllocHandle(SQL_HANDLE_DBC, 
                         env,
                         &dbc);
    if ( !SQL_SUCCEEDED(ret) ) { 
        extract_error("SQLAllocHandle for dbc", env, SQL_HANDLE_ENV);
        exit(EXIT_FAILURE);
    }
    

    SQLCHAR in_conn_str[] = "DSN=pgsql-dvdrental;UID=postgres;PWD=*****";

    connect_to_db(dbc, in_conn_str);

    /**
     * Allocate statement handle
     **/
    printf("=== allocating stmt handle\n");
    ret = SQLAllocHandle(SQL_HANDLE_STMT,
                         dbc,
                         &stmt);
    if ( !SQL_SUCCEEDED(ret) ) { 
        extract_error("SQLAllocHandle for stmt", dbc, SQL_HANDLE_DBC);
        exit(EXIT_FAILURE);
    }

    int row = 1;
    SQLSMALLINT num_columns;
    /**
     * Get tables/schemas information
     **/
    printf("=== fetching tables/schemas information\n");

    SQLTables(stmt, NULL, 0, NULL, 0, NULL, 0, "TABLE", SQL_NTS);
    SQLNumResultCols(stmt, &num_columns);
    
    while ( SQL_SUCCEEDED(ret = SQLFetch(stmt)) ) {
        printf("Row %d\n", row++);

        char buf[128];

        for (SQLULEN i = 1; i <= num_columns; i++) {
            SQLLEN indicator;

            ret = SQLGetData(stmt, 
                             i,
                             SQL_C_CHAR, 
                             buf, 
                             sizeof buf, 
                             &indicator);
            
            if (SQL_SUCCEEDED(ret)) {
                if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
                printf("\tColumn %lu : %s\n", i, buf);

            }
        }
    }

    /**
     * Get table data with SQLExecDirect and SQLGetData
     **/
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    printf("=== (1) fetching data on table \"public.actor\"\n");
    SQLExecDirect(stmt,
                  "select * from public.actor limit 5", 
                  SQL_NTS);
    SQLNumResultCols(stmt, &num_columns);

    row = 1;
    while ( SQL_SUCCEEDED(ret = SQLFetch(stmt)) ) {
        printf("Row %d:\n", row++);

        char buf[1024];
        SQLLEN indicator = 0;

        for (SQLULEN i = 1; i <= num_columns; ++i) {
            
            ret = SQLGetData(stmt, 
                             i, 
                             SQL_C_CHAR,
                             buf, 
                             sizeof buf, 
                             &indicator);

            if (SQL_SUCCEEDED(ret)) {
                if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
                printf("\tColumn %li: %s\n", i, buf);
            }
        }
    }

    /**
     * Get table data with SQLExecDirect, SQLBindCol and SQLFetch
     */
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    printf("=== (2) fetching data on table \"public.actor\"\n");
    SQLExecDirect(stmt,
                  "select * from public.actor limit 5", 
                  SQL_NTS);
    SQLNumResultCols(stmt, &num_columns);

    SQLCHAR buf[num_columns][256];
    SQLLEN indicator[num_columns];
    for (size_t i = 0; i < num_columns; ++i) {
        SQLBindCol(stmt,
                   i+1,
                   SQL_C_CHAR,
                   buf[i],
                   sizeof buf[i],
                   &indicator[i]);
    }

    row = 1;
    while ( SQL_SUCCEEDED(ret = SQLFetch(stmt)) ) {
        printf("Row %d:\n", row++);

        for (size_t i = 0; i < num_columns; ++i) {
            if (indicator[i] == SQL_NULL_DATA) {
                printf("\tColumn %li: NULL\n", i);
            } else {
                printf("\tColumn %li: %s\n", i, buf[i]);
            }
        }
    }

    /**
     * Get many rows of table data
     */
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    int fetch_size = 5; 
    SQLUSMALLINT row_count;
    SQLUSMALLINT row_status[fetch_size];

    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, SQL_BIND_BY_COLUMN, 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, &fetch_size, 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_STATUS_PTR, row_status, 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &row_count, 0);

    printf("=== scroll fetching data on table \"public.actor\"\n");
    SQLExecDirect(stmt,
                  "select * from public.actor limit 5", 
                  SQL_NTS);
    SQLNumResultCols(stmt, &num_columns);

    SQLCHAR buf[num_columns][fetch_size][256];
    SQLLEN indicator[num_columns];
    for (size_t i = 0; i < num_columns; ++i) {
        SQLBindCol(stmt,
                   i+1,
                   SQL_C_CHAR,
                   buf[i],
                   sizeof buf[i][0],
                   &indicator[i]);
    }

    row = 1;
    if ( SQL_SUCCEEDED(ret = 
                SQLFetchScroll(stmt,
                               SQL_FETCH_NEXT, 
                               0)) ) {    
        printf("Rows fetched: %d\n", row_count);    
        for (size_t j = 0; j < fetch_size; ++j) {
            printf("Row %d:\n", row++);
            printf("Row status: %d\n", row_status[j]);
            if (row_status[j] == SQL_ERROR) {
                printf("Row error\n");
            } else if (row_status[row] == SQL_SUCCESS ||
                row_status[row] == SQL_SUCCESS_WITH_INFO)
            {
                for (size_t i = 0; i < num_columns; ++i) {
                    if (indicator[i] == SQL_NULL_DATA) {
                        printf("\tColumn %li: NULL\n", i);
                    } else {
                        printf("\tColumn %li: %s\n", i, buf[i][j]);
                    }
                }
            }
        }
    }

    // do queries n stuff

    free_handles(env, dbc, stmt);

    return 0;
}
