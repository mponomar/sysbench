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

static sb_arg_t comdb2_drv_args[] = {
    SB_OPT_END
};


static db_driver_t comdb2_driver =
{
  .sname = "comdb2",
  .lname = "Comdb2 driver",
  .args = comdb2_drv_args,
  .ops = {
    .init = NULL,
    .thread_init = NULL,
    .describe = NULL,
    .connect = NULL,
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
