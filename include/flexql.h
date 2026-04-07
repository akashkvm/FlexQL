/* ============================================================
 * flexql.h  —  Public API for the FlexQL database driver
 *
 * This is the ONLY header the user of the library includes.
 * The internal FlexQL struct is intentionally kept opaque:
 * its definition lives in flexql_api.cpp, not here.
 * ============================================================ */

#ifndef FLEXQL_H
#define FLEXQL_H

#ifdef __cplusplus
extern "C" {
#endif

/* ---- Error codes ---- */
#define FLEXQL_OK     0   /* Operation succeeded          */
#define FLEXQL_ERROR  1   /* Operation failed             */

/* ---- Opaque database handle ----
 * The struct is defined in flexql_api.cpp.
 * Users only ever hold a pointer to it (FlexQL *db).
 * This hides all internal fields from the caller. */
typedef struct FlexQL FlexQL;

/* ============================================================
 * flexql_open
 *
 * Establish a TCP connection to the FlexQL server and
 * allocate a new database handle.
 *
 * Parameters:
 *   host   — hostname or IP string, e.g. "127.0.0.1"
 *   port   — port number the server is listening on
 *   db     — OUT: pointer to a newly allocated FlexQL handle
 *
 * Returns FLEXQL_OK on success, FLEXQL_ERROR on failure.
 * ============================================================ */
int flexql_open(const char *host, int port, FlexQL **db);

/* ============================================================
 * flexql_close
 *
 * Close the network connection and free all memory associated
 * with the handle.  After this call *db is invalid.
 *
 * Returns FLEXQL_OK on success, FLEXQL_ERROR if db is NULL.
 * ============================================================ */
int flexql_close(FlexQL *db);

/* ============================================================
 * flexql_exec
 *
 * Send an SQL statement to the server for execution.
 * For SELECT queries, callback is invoked once per result row.
 *
 * Parameters:
 *   db       — open database handle
 *   sql      — null-terminated SQL string
 *   callback — function called for each result row, or NULL
 *   arg      — user data forwarded as the first callback arg
 *   errmsg   — OUT: malloc'd error string on failure (free with flexql_free)
 *
 * Callback signature:
 *   int cb(void *arg, int col_count, char **values, char **col_names)
 *   Return 0 to continue, 1 to abort.
 *
 * Returns FLEXQL_OK on success, FLEXQL_ERROR on failure.
 * ============================================================ */
int flexql_exec(
    FlexQL      *db,
    const char  *sql,
    int        (*callback)(void *, int, char **, char **),
    void        *arg,
    char       **errmsg
);

/* ============================================================
 * flexql_free
 *
 * Free memory allocated by the FlexQL API (e.g. errmsg strings).
 * Safe to call with NULL.
 * ============================================================ */
void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif

#endif /* FLEXQL_H */
