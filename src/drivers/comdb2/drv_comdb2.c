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

int comdb2_drv_init(void);


static sb_arg_t comdb2_drv_args[] = {
    SB_OPT_END
};

static drv_caps_t comdb2_drv_caps =
{
  .multi_rows_insert = 1,
  .prepared_statements = 0,
  .auto_increment = 0,
  .needs_commit = 0,
  .serial = 0,
  .unsigned_int = 0
};


static int comdb2_drv_describe(drv_caps_t *caps)
{
  *caps = comdb2_drv_caps;

  return 0;
}

int comdb2_drv_init(void) 
{
    return 0;
}

typedef struct
{
    cdb2_hndl_tp *db;
} db_comdb2_conn_t;

static int comdb2_drv_connect(db_comdb2_conn_t *db_comdb2_con) 
{
    int rc;
    rc = cdb2_open(&db_comdb2_con->db, "mikedb", "local", 0);
    if (rc)
        return 0;
}

static db_driver_t comdb2_driver =
{
  .sname = "comdb2",
  .lname = "Comdb2 driver",
  .args = comdb2_drv_args,
  .ops = {
    .init = comdb2_drv_init,
    .thread_init = NULL,
    .describe = comdb2_drv_describe,
    .connect = comdb2_drv_connect,
    .disconnect = NULL,
    .reconnect = NULL,
    .prepare = NULL,
    .bind_param = NULL,
    .bind_result = NULL,
    .execute = NULL,
    .fetch = NULL,
    .fetch_row = NULL,
    .free_results = NULL,
    .close = NULL,
    .query = NULL,
    .thread_done = NULL,
    .done = NULL
  }
};

int register_driver_comdb2(sb_list_t *drivers) {
    SB_LIST_ADD_TAIL(&comdb2_driver.listitem, drivers);
    return 0;
}
