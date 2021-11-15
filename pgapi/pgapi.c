#include "pgapi.h"

void list_drivers_sources(SQLHENV env) {

    SQLRETURN ret = 0;
    /**
     * List installed drivers
     **/
    char driver[256];
    char attr[256];
    SQLSMALLINT driver_ret = 0;
    SQLSMALLINT attr_ret = 0;
    SQLUSMALLINT direction;

    printf("=== listing installed drivers\n");
    direction = SQL_FETCH_FIRST;
    while ( SQL_SUCCEEDED(
                ret = SQLDrivers(env,
                        direction,
                        driver,
                        sizeof driver,
                        &driver_ret,
                        attr,
                        sizeof attr,
                        &attr_ret)) 
    ) {
        if ( ret == SQL_SUCCESS ) {
            printf("%s - %s\n", driver, attr);
        }
        else if ( ret == SQL_SUCCESS_WITH_INFO ) {
            printf("\tdata truncation\n");
        }
        else {
            extract_error("SQLDrivers", env, SQL_HANDLE_ENV);
        }
        direction = SQL_FETCH_NEXT;
    }

    /**
     * List installed data sources
     **/
    char dsn[256];
    char desc[256];
    SQLSMALLINT dsn_len = 0;
    SQLSMALLINT desc_len = 0;

    printf("=== listing installed data sources\n");
    direction = SQL_FETCH_FIRST;
    while ( SQL_SUCCEEDED(
                ret = SQLDataSources(env,
                        direction,
                        dsn,
                        sizeof dsn,
                        &dsn_len,
                        desc,
                        sizeof desc,
                        &desc_len))
    ) {
        if ( ret == SQL_SUCCESS ) {
            printf("%s - %s\n", dsn, desc);
        }
        else if ( ret == SQL_SUCCESS_WITH_INFO ) {
            printf("\tdata truncation\n");
        }
        else {
            extract_error("SQLDrivers", env, SQL_HANDLE_ENV);
        }
        direction = SQL_FETCH_NEXT;
    }
}

void connect_to_db(SQLHDBC dbc, SQLCHAR *in_conn_str) {
    
    SQLRETURN ret = 0;
    /**
     * Connect to a Driver or Data Source
     **/     
    SQLCHAR out_conn_str[256];
    SQLSMALLINT out_conn_str_len = 0;

    printf("=== connecting to driver\n");
    ret = SQLDriverConnect(dbc,
                      NULL,
                      in_conn_str,
                      SQL_NTS,
                      out_conn_str,
                      sizeof out_conn_str,
                      &out_conn_str_len,
                      SQL_DRIVER_COMPLETE);
                      
    if ( SQL_SUCCEEDED(ret) ) {
        printf("=== connected\n");
        printf("return connection string: %s\n", out_conn_str);
        if (ret == SQL_SUCCESS_WITH_INFO) {
            extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
        }
        /*
         * Information about the driver
         */
        SQLCHAR dbms_name[256], dbms_version[256];
        SQLUINTEGER getdata_support;
        SQLUSMALLINT max_concur_act;
        SQLSMALLINT string_len;

        SQLGetInfo(dbc,
                   SQL_DBMS_NAME,
                   (SQLPOINTER)dbms_name,
                   sizeof dbms_name,
                   NULL);

        SQLGetInfo(dbc,
                   SQL_DBMS_VER,
                   (SQLPOINTER)dbms_version,
                   sizeof dbms_version,
                   NULL);

        SQLGetInfo(dbc,
                   SQL_GETDATA_EXTENSIONS,
                   (SQLPOINTER)&getdata_support,
                   0, 0);
        
        SQLGetInfo(dbc,
                   SQL_MAX_CONCURRENT_ACTIVITIES,
                   &max_concur_act, 
                   0, 0);

        printf("DBMS Name: %s\n", dbms_name);
        printf("DBMS Version: %s\n", dbms_version);
        if (max_concur_act == 0) {
            printf("SQL_MAX_CONCURRENT_ACTIVITIES - no limit or undefined\n");
        } else {
            printf("SQL_MAX_CONCURRENT_ACTIVITIES = %u\n", max_concur_act);
        }
        if (getdata_support & SQL_GD_ANY_ORDER) {
            printf("SQLGetData - columns can be retrieved in any order\n");
        } else {
            printf("SQLGetData - columns must be retrieved in order\n");
        }
        if (getdata_support & SQL_GD_ANY_COLUMN) {
            printf("SQLGetData - can retrieve columns before last bound one\n\n");
        } else {
            printf("SQLGetData - columns must be retrieved after last bound one\n\n");
        }
    } 
    else {
        fprintf(stderr, "failed to connect to database");
        extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
    }
}

void free_handles(SQLHENV env, SQLHDBC dbc, SQLHSTMT stmt) {
    
    SQLRETURN ret = 0;
    /**
     * Free handles in opposite direction
     **/
    printf("=== freeing stmt handle\n");
    ret = SQLFreeHandle(SQL_HANDLE_STMT,
                        stmt);

    SQLDisconnect(dbc);

    printf("=== freeing dbc handle\n");
    ret = SQLFreeHandle(SQL_HANDLE_DBC,
                        dbc);

    printf("=== freeing env handle\n");
    ret = SQLFreeHandle(SQL_HANDLE_ENV,
                        env);
}

void extract_error(char *func,
                   SQLHANDLE handle,
                   SQLSMALLINT type) {

    SQLINTEGER  i = 0;
    SQLINTEGER  native = 0;
    SQLCHAR     state[7];
    SQLCHAR     text[256];
    SQLSMALLINT text_len = 0;
    SQLRETURN   ret = 0;

    do {
        ret = SQLGetDiagRec(type,
                            handle,
                            ++i,
                            state,
                            &native,
                            text,
                            sizeof text,
                            &text_len);
        
        if ( !SQL_SUCCEEDED(ret) ) {
            fprintf(stderr, 
            "\n"
            "driver reported the following diagnostics whilst running "
            "%s:\n",
            func);
            printf("%s:%i:%i:%s\n\n", state, i, native, text);
        }
    } while ( ret == SQL_SUCCESS );    
}