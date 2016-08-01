//
// Created by Valentino on 27/07/16.
// Minimal version of shared.h required by rc_pingpong.h
//

#ifndef FI_TEST_REDUCED_H
#define FI_TEST_REDUCED_H

#include <inttypes.h>

#include "rdma/fabric.h"

extern char default_port[8];

enum ft_comp_method {
  FT_COMP_SPIN = 0,
  FT_COMP_SREAD,
  FT_COMP_WAITSET,
  FT_COMP_WAIT_FD
};

enum {
  FT_OPT_ACTIVE = 1 << 0,
  FT_OPT_ITER = 1 << 1,
  FT_OPT_SIZE = 1 << 2,
  FT_OPT_RX_CQ = 1 << 3,
  FT_OPT_TX_CQ = 1 << 4,
  FT_OPT_RX_CNTR = 1 << 5,
  FT_OPT_TX_CNTR = 1 << 6,
  FT_OPT_VERIFY_DATA = 1 << 7,
  FT_OPT_ALIGN = 1 << 8,
};

struct ft_opts {
  int iterations;
  int warmup_iterations;
  int transfer_size;
  int window_size;
  char *src_port;
  char *dst_port;
  char *src_addr;
  char *dst_addr;
  char *av_name;
  int sizes_enabled;
  int options;
  enum ft_comp_method comp_method;
  int machr;
  int argc;
  char **argv;
};

extern struct fi_info *fi, *hints;

void ft_parseinfo(int op, char *optarg, struct fi_info *hints);
void ft_parse_addr_opts(int op, char *optarg, struct ft_opts *opts);
void ft_usage(char *name, char *desc);

#define ADDR_OPTS "b:p:s:a:"
#define INFO_OPTS "n:f:"


int ft_read_addr_opts(
    char **node,
    char **service,
    struct fi_info *hints,
    uint64_t *flags,
    struct ft_opts *opts);

#define FT_PRINTERR(call, retv) \
  do { fprintf(stderr, call "(): %s:%d, ret=%d (%s)\n", __FILE__, __LINE__, \
      (int) retv, fi_strerror((int) -retv)); } while (0)

#define FT_LOG(level, fmt, ...) \
  do { fprintf(stderr, "[%s] fabtests:%s:%d: " fmt "\n", level, __FILE__, \
      __LINE__, ##__VA_ARGS__); } while (0)

#define FT_ERR(fmt, ...) FT_LOG("error", fmt, ##__VA_ARGS__)

#define FT_EQ_ERR(eq, entry, buf, len) \
  FT_ERR("eq_readerr: %s", fi_eq_strerror(eq, entry.prov_errno, \
        entry.err_data, buf, len))




void eq_readerr(struct fid_eq *eq, const char *eq_str);

#define FT_PROCESS_QUEUE_ERR(readerr, rd, queue, fn, str)  \
  do {              \
    if (rd == -FI_EAVAIL) {        \
      readerr(queue, fn " " str);    \
    } else {          \
      FT_PRINTERR(fn, rd);      \
    }            \
  } while (0)

#define FT_PROCESS_EQ_ERR(rd, eq, fn, str) \
  FT_PROCESS_QUEUE_ERR(eq_readerr, rd, eq, fn, str)

#define FT_PRINT_OPTS_USAGE(opt, desc) fprintf(stderr, " %-20s %s\n", opt, desc)

#endif //FI_TEST_REDUCED_H
