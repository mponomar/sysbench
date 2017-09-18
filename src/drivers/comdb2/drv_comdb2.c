#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#ifdef HAVE_STRING_H
# include <string.h>
#endif
#ifdef HAVE_STRINGS_H
# include <strings.h>
#endif

#include "sb_options.h"
#include "db_driver.h"

#include <cdb2api.h>

static db_error_t comdb2_drv_query(struct db_conn *sb_conn, const char *query, size_t len, struct db_result *rs);

static sb_arg_t comdb2_drv_args[] = {
    SB_OPT_END
};

static drv_caps_t comdb2_drv_caps =
{
  .multi_rows_insert = 0,
  .prepared_statements = 1,
  .auto_increment = 0,
  .needs_commit = 0,
  .serial = 0,
  .unsigned_int = 0
};


/* initialize driver */
static int comdb2_drv_init(void)
{
  return 0;
}

/* thread-local driver initialization */
static int comdb2_drv_thread_init(int thread_id)
{
  (void)thread_id;  
  return 0;
}

/* describe database capabilities */
static int comdb2_drv_describe(drv_caps_t *caps)
{
  *caps = comdb2_drv_caps;
  return 0;
}

/* connect to database */
static int comdb2_drv_connect(struct db_conn *sb_conn)
{
  int rc;
  cdb2_hndl_tp *db;
  rc = cdb2_open(&db, "mikedb", "local", 0);
  sb_conn->ptr = db;
  return rc == 0 ? 0 : DB_ERROR_FATAL;
}

/* reconnect with the same parameters */
static int comdb2_drv_reconnect(struct db_conn *sb_conn)
{
  cdb2_hndl_tp *db;
  db = (cdb2_hndl_tp*) sb_conn->ptr;
  if (db)
      cdb2_close(db);
  int rc = cdb2_open(&db, "mikedb", "local", 0);
  sb_conn->ptr = db;
  return rc == 0 ?  0 : DB_ERROR_FATAL;
}

/* disconnect from database */
static int comdb2_drv_disconnect(struct db_conn *sb_conn)
{
  cdb2_hndl_tp *db;
  db = (cdb2_hndl_tp*) sb_conn->ptr;
  if (db)
      cdb2_close(db);
  return 0;
}

/* prepare statement */
static int comdb2_drv_prepare(struct db_stmt *stmt, const char *query, size_t len)
{
  (void) len; 
  stmt->query = strdup(query);
  return 0;
}

#define DO_NUMERIC_PARAM(var, cdb2type, field) \
              values[i].type = cdb2type;                              \
              if (params[i].is_null != NULL && *params[i].is_null)  { \
                  values[i].addr= NULL;                               \
                  values[i].len = 0;                                  \
              }                                                       \
              else {                                                  \
                  memcpy(&var, params[i].buffer, sizeof(var));        \
                  values[i].value.field = var;                        \
                  values[i].addr = &values[i].value.field;            \
                  values[i].len = sizeof(var);                        \
              }

struct comdb2_bound_values {
    cdb2_coltype type;
    int len;
    union {
        int64_t ival;
        char *strval;
        double rval;
        // TODO: datetime/interval types
    } value;
    int isnull;
    void *addr;
};


/* temporary kludge */
__thread struct comdb2_bound_values *values;
__thread int nvalues;

/* bind params for prepared statements */
static int comdb2_drv_bind_param(struct db_stmt *stmt, db_bind_t *params, size_t len)
{
  if (values) {
      for (int i = 0; i < nvalues; i++) {
          if (values[i].addr && values[i].addr == CDB2_CSTRING) {
              free(values[i].addr);
          }
      }
      free(values);
  }
  values = calloc(len, sizeof(db_bind_t));
  nvalues = len;

  cdb2_hndl_tp *db = (cdb2_hndl_tp*) stmt->connection->ptr;
  cdb2_clearbindings(db);
  stmt->bound_param = params;
  stmt->bound_param_len = len;

  for (size_t i = 0; i < len; i++) {
      int8_t tiny;
      int16_t small;
      int32_t reg;
      int64_t big;
      float  f;
      double d;

      switch (params[i].type) {
          case DB_TYPE_TINYINT:
              DO_NUMERIC_PARAM(tiny, CDB2_INTEGER, ival);
              break;
          case DB_TYPE_SMALLINT:
              DO_NUMERIC_PARAM(small, CDB2_INTEGER, ival);
              break;
          case DB_TYPE_INT:
              DO_NUMERIC_PARAM(reg, CDB2_INTEGER, ival);
              break;
          case DB_TYPE_BIGINT:
              DO_NUMERIC_PARAM(big, CDB2_INTEGER, ival);
              break;
          case DB_TYPE_FLOAT:
              DO_NUMERIC_PARAM(f, CDB2_REAL, rval);
              break;
          case DB_TYPE_DOUBLE:
              DO_NUMERIC_PARAM(d, CDB2_REAL, rval);
              break;

          case DB_TYPE_CHAR:
          case DB_TYPE_VARCHAR:
              if (params[i].is_null != NULL && *params[i].is_null)
                  values[i].addr = NULL;
              else {
                  values[i].value.strval = malloc(*params[i].data_len);
                  memcpy(values[i].value.strval, params[i].buffer, *params[i].data_len);
                  values[i].addr = values[i].value.strval;
                  values[i].len = *params[i].data_len;
                  values[i].type = CDB2_CSTRING;
              }
              break;

          default:
              log_text(LOG_FATAL, "Unknown type\n");
              return DB_ERROR_FATAL;
      }

      int rc = cdb2_bind_index(db, i+1, values[i].type, values[i].addr, values[i].len);
      if (rc) {
          log_text(LOG_FATAL, "bind arg %d rc %d %s\n", (int) i, rc, cdb2_errstr(db));
          return DB_ERROR_FATAL;
      }
  }
  return 0;
}

/* bind results for prepared statement */
static int comdb2_drv_bind_result(struct db_stmt *stmt, db_bind_t *params, size_t len)
{
  return 0;
}

/* execute prepared statement */
static db_error_t comdb2_drv_execute(struct db_stmt *stmt, struct db_result *rs)
{
  // rebind
  int bindrc = comdb2_drv_bind_param(stmt, stmt->bound_param, stmt->bound_param_len);
  if (bindrc) {
      log_text(LOG_FATAL, "can't rebind params\n");
      return DB_ERROR_FATAL;
  }
  db_error_t rc = comdb2_drv_query(stmt->connection, stmt->query, strlen(stmt->query), rs);
  return rc;
}

/* fetch row for prepared statement */
static int comdb2_drv_fetch(struct db_result *rs)
{
  return 0;
}

/* fetch row for queries */
static int comdb2_drv_fetch_row(struct db_result *rs, struct db_row *row)
{
  return 0;
}

/* execute non-prepared statement */
static db_error_t comdb2_drv_query(struct db_conn *sb_conn, const char *query, size_t len, struct db_result *rs)
{
  rs->ptr = sb_conn->ptr;
  cdb2_hndl_tp *db = (cdb2_hndl_tp*) sb_conn->ptr;

  int rc = cdb2_run_statement(db, query);

  sb_conn->sql_errno = 0;
  sb_conn->sql_state = NULL;
  sb_conn->sql_errmsg = NULL;

  log_text(LOG_DEBUG, "%s(sb_conn=%p, query=%.*s, len=%d, rs=%p) rc=%d err=%s", __func__, sb_conn, (len > 100) ? 100 : (int) len, query, (int) len, rs, rc, cdb2_errstr(db));
  if (rc) {
      sb_conn->error = 0;
      sb_conn->sql_errno = rc;
      sb_conn->sql_errmsg = cdb2_errstr(db);
      log_text(LOG_ALERT, "%s(sb_conn=%p, query=%.*s, len=%d, rs=%p) rc=%d err=%s", __func__, sb_conn, (len > 100) ? 100 : (int) len, query, (int) len, rs, rc, cdb2_errstr(db));
      if (rc == CDB2ERR_DUPLICATE)
          return 0;  // pretend this didn't happen
      else 
          return DB_ERROR_FATAL;
  }
  return 0;
}

/* free result set */
static int comdb2_drv_free_results(struct db_result *rs)
{
  return 0;
}

/* close prepared statement */
static int comdb2_drv_close(struct db_stmt *stmt)
{
  stmt->bound_param = NULL;
  return 0;
}

/* thread-local driver deinitialization */
static int comdb2_drv_thread_done(int thread_id)
{
  return 0;
}

/* uninitialize driver */
static int comdb2_drv_done(void)
{
  return 0;
}


static db_driver_t comdb2_driver =
{
  .sname = "comdb2",
  .lname = "Comdb2 driver",
  .args = comdb2_drv_args,
  .ops = {
    .init = comdb2_drv_init,
    .disconnect = comdb2_drv_disconnect,
    .reconnect = comdb2_drv_reconnect,
    .connect = comdb2_drv_connect,
    .thread_init = comdb2_drv_thread_init,
    .prepare = comdb2_drv_prepare,
    .describe = comdb2_drv_describe,
    .bind_param = comdb2_drv_bind_param,
    .bind_result = comdb2_drv_bind_result,
    .execute = comdb2_drv_execute,
    .fetch = comdb2_drv_fetch,
    .fetch_row = comdb2_drv_fetch_row,
    .free_results = comdb2_drv_free_results,
    .close = comdb2_drv_close,
    .query = comdb2_drv_query,
    .thread_done = comdb2_drv_thread_done,
    .done = comdb2_drv_done
  }
};

int register_driver_comdb2(sb_list_t *drivers) {
    SB_LIST_ADD_TAIL(&comdb2_driver.listitem, drivers);
    return 0;
}
