//
// Created by Valentino on 29/07/16.
//

#include <iostream>
#include <cstdint>
#include <unistd.h>
// #include <getopt.h>
#include "common/utils.hpp"
#include "rdma/fi_errno.h"
#include "rdma/fi_endpoint.h"
#include "rdma/fi_cm.h"

struct pingpong_context {
  struct fi_info *info;
  struct fid_fabric *fabric;
  struct fid_domain *dom;
  struct fid_mr *mr;
  struct fid_pep *pep;
  struct fid_ep *ep;
  struct fid_eq *eq;
  struct fid_cq *cq;
  int size;
  int rx_depth;
  int use_event;
  int routs;
  void *buf;
};

// Dependencies from redeuced.h

// Required by
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

#define INIT_OPTS (struct ft_opts) \
  {  .options = FT_OPT_RX_CQ | FT_OPT_TX_CQ, \
    .iterations = 1000, \
    .warmup_iterations = 10, \
    .transfer_size = 1024, \
    .window_size = 64, \
    .sizes_enabled = 1, \
    .argc = argc, .argv = argv \
  }

enum {
  PINGPONG_RECV_WCID = 1,
  PINGPONG_SEND_WCID = 2,
};

// Error handling

#define FT_LOG(level, fmt, ...) \
  do { fprintf(stderr, "[%s] fabtests:%s:%d: " fmt "\n", level, __FILE__, \
      __LINE__, ##__VA_ARGS__); } while (0)

#define FT_ERR(fmt, ...) FT_LOG("error", fmt, ##__VA_ARGS__)

#define FT_EQ_ERR(eq, entry, buf, len) \
  FT_ERR("eq_readerr: %s", fi_eq_strerror(eq, entry.prov_errno, \
        entry.err_data, buf, len))

#define FT_PRINTERR(call, retv) \
  do { fprintf(stderr, call "(): %s:%d, ret=%d (%s)\n", __FILE__, __LINE__, \
      (int) retv, fi_strerror((int) -retv)); } while (0)

void eq_readerr(struct fid_eq *eq, const char *eq_str) {
  struct fi_eq_err_entry eq_err;
  int rd;

  rd = fi_eq_readerr(eq, &eq_err, 0);
  if (rd!=sizeof(eq_err)) {
    FT_PRINTERR("fi_eq_readerr", rd);
  } else {
    FT_EQ_ERR(eq, eq_err, NULL, 0);
  }
}

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

// End Error Handling

char default_port[8] = "9228";
struct ft_opts opts;
struct fi_info *hints, *fi;
static int page_size;

static struct pingpong_context *pp_init_ctx(
    struct fi_info *info,
    int size,
    int rx_depth,
    int use_event) {
  struct pingpong_context *ctx;
  int rc = 0;

  // TODO(pico) : replace calloc
  ctx = static_cast<pingpong_context *>(calloc(1, sizeof *ctx));
  if (!ctx)
    return NULL;

  ctx->info = info;
  ctx->size = size;
  ctx->rx_depth = rx_depth;
  ctx->use_event = use_event;
  ctx->routs = 0;

  if (posix_memalign(&(ctx->buf), page_size, size)) {
    fprintf(stderr, "Couldn't allocate work buf.\n");
    goto err1;
  }

  /* FIXME memset(ctx->buf, 0, size); */
  memset(ctx->buf, 0x7b, size);

  /* Open the fabric */
  rc = fi_fabric(info->fabric_attr, &ctx->fabric, NULL);
  if (rc) {
    FT_PRINTERR("fi_fabric", rc);
    goto err2;
  }

  return ctx;

  err2:
  free(ctx->buf);
  err1:
  free(ctx);
  return NULL;
}

static int pp_eq_create(struct pingpong_context *ctx) {
  struct fi_eq_attr cm_attr;
  int rc;

  memset(&cm_attr, 0, sizeof cm_attr);
  cm_attr.wait_obj = FI_WAIT_FD;

  rc = fi_eq_open(ctx->fabric, &cm_attr, &ctx->eq, NULL);
  if (rc)
    FT_PRINTERR("fi_eq_open", rc);

  return rc;
}

static int pp_listen_ctx(struct pingpong_context *ctx) {
  int rc = 0;

  rc = fi_passive_ep(ctx->fabric, ctx->info, &ctx->pep, NULL);
  if (rc) {
    fprintf(stderr, "Unable to open listener endpoint\n");
    return 1;
  }

  /* Create listener EQ */
  rc = pp_eq_create(ctx);
  if (rc) {
    fprintf(stderr, "Unable to allocate listener resources\n");
    return 1;
  }

  rc = fi_pep_bind(ctx->pep, &ctx->eq->fid, 0);
  if (rc) {
    FT_PRINTERR("fi_pep_bind", rc);
    return 1;
  }

  rc = fi_listen(ctx->pep);
  if (rc) {
    FT_PRINTERR("fi_listen", rc);
    return 1;
  }

  printf("Listening for incoming connections...\n");
  return 0;
}

static int pp_post_recv(struct pingpong_context *ctx, int n) {
  int rc = 0;
  int i;

  for (i = 0; i < n; ++i) {
    rc = fi_recv(
        ctx->ep,
        ctx->buf,
        ctx->size,
        fi_mr_desc(ctx->mr),
        0,
        (void *) (uintptr_t) PINGPONG_RECV_WCID);
    if (rc) {
      FT_PRINTERR("fi_recv", rc);
      break;
    }
  }

  return i;
}

static int pp_cq_create(struct pingpong_context *ctx) {
  struct fi_cq_attr cq_attr;
  int rc = 0;

  memset(&cq_attr, 0, sizeof cq_attr);
  cq_attr.format = FI_CQ_FORMAT_CONTEXT;
  if (ctx->use_event)
    cq_attr.wait_obj = FI_WAIT_FD;
  else
    cq_attr.wait_obj = FI_WAIT_UNSPEC;
  cq_attr.size = ctx->rx_depth + 1;

  rc = fi_cq_open(ctx->dom, &cq_attr, &ctx->cq, NULL);
  if (rc) {
    FT_PRINTERR("fi_cq_open", rc);
    return 1;
  }

  return 0;
}

static int pp_accept_ctx(struct pingpong_context *ctx) {
  struct fi_eq_cm_entry entry;
  uint32_t event;
  int rc = 0;
  ssize_t rd;

  rd = fi_eq_sread(ctx->eq, &event, &entry, sizeof entry, -1, 0);
  if (rd!=sizeof entry) {
    FT_PROCESS_EQ_ERR(rd, ctx->eq, "fi_eq_sread", "listen");
    return 1;
  }

  if (event!=FI_CONNREQ) {
    fprintf(stderr, "Unexpected CM event %d\n", event);
    return 1;
  }

  rc = fi_domain(ctx->fabric, entry.info, &ctx->dom, NULL);
  if (rc) {
    FT_PRINTERR("fi_fdomain", rc);
    return 1;
  }

  rc = fi_mr_reg(ctx->dom, ctx->buf, ctx->size,
                 FI_SEND | FI_RECV, 0, 0, 0, &ctx->mr,
                 NULL);
  if (rc) {
    FT_PRINTERR("fi_mr_reg", rc);
    return 1;
  }

  rc = fi_endpoint(ctx->dom, entry.info, &ctx->ep, NULL);
  if (rc) {
    FT_PRINTERR("fi_endpoint", rc);
    return 1;
  }
  fi_freeinfo(entry.info);

  /* Create event queue */
  if (pp_cq_create(ctx)) {
    fprintf(stderr, "Unable to create event queue\n");
    return 1;
  }

  rc = fi_ep_bind(ctx->ep, &ctx->cq->fid, FI_SEND | FI_RECV);
  if (rc) {
    FT_PRINTERR("fi_ep_bind", rc);
    return 1;
  }

  rc = fi_ep_bind(ctx->ep, &ctx->eq->fid, 0);
  if (rc) {
    FT_PRINTERR("fi_ep_bind", rc);
    return 1;
  }

  rc = fi_enable(ctx->ep);
  if (rc) {
    FT_PRINTERR("fi_enable", rc);
    return EXIT_FAILURE;
  }

  ctx->routs = pp_post_recv(ctx, ctx->rx_depth);
  if (ctx->routs < ctx->rx_depth) {
    FT_ERR("Couldn't post receive (%d)", ctx->routs);
    return 1;
  }

  rc = fi_accept(ctx->ep, NULL, 0);
  if (rc) {
    FT_PRINTERR("fi_accept", rc);
    return 1;
  }

  rd = fi_eq_sread(ctx->eq, &event, &entry, sizeof entry, -1, 0);
  if (rd!=sizeof entry) {
    FT_PROCESS_EQ_ERR(rd, ctx->eq, "fi_eq_sread", "accept");
    return 1;
  }

  if (event!=FI_CONNECTED) {
    fprintf(stderr, "Unexpected CM event %d\n", event);
    return 1;
  }
  printf("Connection accepted\n");

  return 0;
}

#define FT_CLOSE(DESC, STR)         \
  do {            \
    if (DESC) {       \
      if (fi_close(&DESC->fid)) { \
        fprintf(stderr, STR); \
        return 1;   \
      }       \
    }         \
  } while (0)

int pp_close_ctx(struct pingpong_context *ctx) {
  FT_CLOSE(ctx->pep, "Couldn't destroy listener EP\n");
  FT_CLOSE(ctx->ep, "Couldn't destroy EP\n");
  FT_CLOSE(ctx->eq, "Couldn't destroy EQ\n");
  FT_CLOSE(ctx->cq, "Couldn't destroy CQ\n");
  FT_CLOSE(ctx->mr, "Couldn't destroy MR\n");
  FT_CLOSE(ctx->dom, "Couldn't deallocate Domain\n");
  FT_CLOSE(ctx->fabric, "Couldn't close fabric\n");

  if (ctx->buf)
    free(ctx->buf);
  free(ctx);
  return 0;
}

int main(int argc, char **argv) {
  uint64_t flags = 0;
  char *service = nullptr;
  char *node = nullptr;
  struct pingpong_context *ctx;

  unsigned long size = 4096;
  int rx_depth_default = 500;
  int rx_depth;
  int use_event = 0; // not sure if required
  int rcnt, scnt = 0;
  int ret, rc = 0;

  char *ptr; // getopt parsing
  char *dst_addr = nullptr;
  char *dst_port = nullptr;

  opts = INIT_OPTS;

  hints = fi_allocinfo();
  if (!hints)
    return 1;

  while (1) {
    int c;
    c = getopt(argc, argv, "p:");
    if (c==-1)
      break;

    switch (c) {
    default:
      break;
    }
  }

  if (optind==argc - 1) { // Last element
    opts.dst_addr = argv[optind];
  } else if (optind < argc) {
    // usage(argv[0]);
    return 1;
  }

  page_size = sysconf(_SC_PAGE_SIZE);

  hints->ep_attr->type = FI_EP_MSG;
  hints->caps = FI_MSG;
  hints->mode = FI_LOCAL_MR;

  // inlined call ft_read_addr_opts for the server
  if (!opts.src_port) {
    opts.src_port = default_port;
  }
  node = opts.src_addr;
  service = opts.src_port;
  flags = FI_SOURCE;
  // end ft_read_addr_opts

  rc = fi_getinfo(fi_version(), node, service, flags, hints, &fi);
  if (rc) {
    FT_PRINTERR("fi_getinfo", rc);
    return -rc;
  }
  fi_freeinfo(hints);

  if (rx_depth) { // TODO(pico): set by command line option

  } else {
    rx_depth = (rx_depth_default > fi->rx_attr->size) ? fi->rx_attr->size
                                                      : rx_depth_default;
  }

  ctx = pp_init_ctx(fi, size, rx_depth, use_event);
  if (!ctx) {
    rc = 1;
    goto err1;
  }

  pp_listen_ctx(ctx);
  pp_accept_ctx(ctx);

  while (1) {
    struct fi_cq_entry wc;
    struct fi_cq_err_entry cq_err;
    size_t rd;

    if (use_event) {
      rd = fi_cq_sread(ctx->cq, &wc, 1, NULL, -1);
    } else {
      do {
        rd = fi_cq_read(ctx->cq, &wc, 1);
      } while (rd==-FI_EAGAIN);
    }

    if (rd < 0) {
      fi_cq_readerr(ctx->cq, &cq_err, 0);
      fprintf(
          stderr,
          "cq fi_cq_readerr() %s (%d)\n",
          fi_cq_strerror(ctx->cq, cq_err.err, cq_err.err_data, NULL, 0),
          cq_err.err);
      rc = rd;
      goto err3;
    }

    assert(reinterpret_cast<long int>(wc.op_context) == PINGPONG_RECV_WCID && "Unexpected cq entry context");

    if(!(std::cout << static_cast<char*>(ctx -> buf))) {
      goto err3;
    }

    if (--ctx->routs <= 1) {
      ctx->routs += pp_post_recv(ctx, ctx->rx_depth - ctx->routs);
      if (ctx->routs < ctx->rx_depth) {
        fprintf(stderr, "Couldn't post receive (%d)\n", ctx->routs);
        rc = 1;
        goto err3;
      }
    }
  }

  return 0;
  err3:
  fi_shutdown(ctx->ep, 0);
  err2:
  ret = pp_close_ctx(ctx);
  if (!rc)
    rc = ret;
  err1:
  fi_freeinfo(fi);
  return rc;
}