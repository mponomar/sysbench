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

#define DEBUG(format, ...) do { if (db_globals.debug) log_text(LOG_DEBUG, format, __VA_ARGS__); } while (0)

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
  DEBUG("%s()", __func__);
  return 0;
}

/* thread-local driver initialization */
static int comdb2_drv_thread_init(int thread_id)
{
  DEBUG("%s(thread_id=%d)", __func__, thread_id);
  return 0;
}

/* describe database capabilities */
static int comdb2_drv_describe(drv_caps_t *caps)
{
  DEBUG("%s(caps=%p)", __func__, caps);
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
  DEBUG("%s(sb_conn=%p) rc %d", __func__, sb_conn, rc);
  return 0;
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
  DEBUG("%s(sb_conn=%p) rc %d", __func__, sb_conn, rc);
  return 0;
}

/* disconnect from database */
static int comdb2_drv_disconnect(struct db_conn *sb_conn)
{
  cdb2_hndl_tp *db;
  db = (cdb2_hndl_tp*) sb_conn->ptr;
  if (db)
      cdb2_close(db);
  DEBUG("%s(sb_conn=%p)", __func__, sb_conn);
  return 0;
}

/* prepare statement */
static int comdb2_drv_prepare(struct db_stmt *stmt, const char *query, size_t len)
{
  DEBUG("%s(stmt=%p, query=%.*s, len=%d)", __func__, stmt, (int) len, query, (int) len);
  return 0;
}

/* bind params for prepared statements */
static int comdb2_drv_bind_param(struct db_stmt *stmt, db_bind_t *params, size_t len)
{
  DEBUG("%s(stmt=%p, params=%p, le=%d)", __func__, stmt, params, (int) len);
  return 0;
}

/* bind results for prepared statement */
static int comdb2_drv_bind_result(struct db_stmt *stmt, db_bind_t *params, size_t len)
{
  DEBUG("%s(stmt=%p, params=%p, len=%d)", __func__, stmt, params, (int) len);
  return 0;
}

/* execute prepared statement */
static db_error_t comdb2_drv_execute(struct db_stmt *stmt, struct db_result *rs)
{
  DEBUG("%s(stmt=%p, rs=%p)", __func__, stmt, rs);
  return 0;
}

/* fetch row for prepared statement */
static int comdb2_drv_fetch(struct db_result *rs)
{
  DEBUG("%s(rs=%p)", __func__, rs);
  return 0;
}

/* fetch row for queries */
static int comdb2_drv_fetch_row(struct db_result *rs, struct db_row *row)
{
  DEBUG("%s(rs=%p, row=%p)", __func__, rs, row);
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

  DEBUG("%s(sb_conn=%p, query=%.*s, len=%d, rs=%p) rc %d", __func__, sb_conn, (int) len, query, (int) len, rs, rc);
  if (rc) {
      sb_conn->error = 0;
      sb_conn->sql_errno = rc;
      sb_conn->sql_errno = cdb2_errstr(db);
      return DB_ERROR_FATAL;
  }
  return 0;
}

/* free result set */
static int comdb2_drv_free_results(struct db_result *rs)
{
  DEBUG("%s(rs=%p)", __func__, rs);
  return 0;
}

/* close prepared statement */
static int comdb2_drv_close(struct db_stmt *sb_conn)
{
  DEBUG("%s(sb_conn=%p)", __func__, sb_conn);
  return 0;
}

/* thread-local driver deinitialization */
static int comdb2_drv_thread_done(int thread_id)
{
  DEBUG("%s(thread_id=%d)", __func__, thread_id);
  return 0;
}

/* uninitialize driver */
static int comdb2_drv_done(void)
{
  DEBUG("%s()", __func__);
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
