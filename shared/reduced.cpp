//
// Created by Valentino on 27/07/16.
//

#include "reduced.hpp"

#include <netdb.h> // EAI_MEMORY
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <rdma/fi_errno.h>
#include "rdma/fi_eq.h"

char default_port[8] = "9228";
struct fi_info *fi, *hints;
struct ft_opts opts;

void ft_parseinfo(int op, char *optarg, struct fi_info *hints) {
  switch (op) {
  case 'n':
    if (!hints->domain_attr) {
      hints->domain_attr = static_cast<fi_domain_attr*>(malloc(
          sizeof *(hints->domain_attr)));
      if (!hints->domain_attr) {
        perror("malloc");
        exit(EXIT_FAILURE);
      }
    }
    hints->domain_attr->name = strdup(optarg);
    break;
  case 'f':
    if (!hints->fabric_attr) {
      hints->fabric_attr = static_cast<fi_fabric_attr*>(malloc(
          sizeof *(hints->fabric_attr)));
      if (!hints->fabric_attr) {
        perror("malloc");
        exit(EXIT_FAILURE);
      }
    }
    hints->fabric_attr->prov_name = strdup(optarg);
    break;
  default:
    /* let getopt handle unknown opts*/
    break;

  }
}

void ft_parse_addr_opts(int op, char *optarg, struct ft_opts *opts) {
  switch (op) {
  case 's':
    opts->src_addr = optarg;
    break;
  case 'b':
    opts->src_port = optarg;
    break;
  case 'p':
    opts->dst_port = optarg;
    break;
  default:
    /* let getopt handle unknown opts*/
    break;
  }
}

void ft_usage(char *name, char *desc) {
  fprintf(stderr, "Usage:\n");
  fprintf(stderr, "  %s [OPTIONS]\t\tstart server\n", name);
  fprintf(stderr, "  %s [OPTIONS] <host>\tconnect to server\n", name);

  if (desc)
    fprintf(stderr, "\n%s\n", desc);

  fprintf(stderr, "\nOptions:\n");
  FT_PRINT_OPTS_USAGE("-n <domain>", "domain name");
  FT_PRINT_OPTS_USAGE("-b <src_port>", "non default source port number");
  FT_PRINT_OPTS_USAGE("-p <dst_port>", "non default destination port number");
  FT_PRINT_OPTS_USAGE(
      "-f <provider>",
      "specific provider name eg sockets, verbs");
  FT_PRINT_OPTS_USAGE("-s <address>", "source address");
  FT_PRINT_OPTS_USAGE("-a <address vector name>", "name of address vector");
  FT_PRINT_OPTS_USAGE("-h", "display this help output");

  return;
}

static int dupaddr(
    void **dst_addr,
    size_t *dst_addrlen,
    void *src_addr,
    size_t src_addrlen) {
  *dst_addr = malloc(src_addrlen);
  if (!*dst_addr) {
    FT_ERR("address allocation failed");
    return EAI_MEMORY;
  }
  *dst_addrlen = src_addrlen;
  memcpy(*dst_addr, src_addr, src_addrlen);
  return 0;
}

static int getaddr(
    char *node,
    char *service,
    struct fi_info *hints,
    uint64_t flags) {
  int ret;
  struct fi_info *fi;

  if (!node && !service) {
    if (flags & FI_SOURCE) {
      hints->src_addr = NULL;
      hints->src_addrlen = 0;
    } else {
      hints->dest_addr = NULL;
      hints->dest_addrlen = 0;
    }
    return 0;
  }

  ret = fi_getinfo(FT_FIVERSION, node, service, flags, hints, &fi);
  if (ret) {
    FT_PRINTERR("fi_getinfo", ret);
    return ret;
  }
  hints->addr_format = fi->addr_format;

  if (flags & FI_SOURCE) {
    ret = dupaddr(
        &hints->src_addr,
        &hints->src_addrlen,
        fi->src_addr,
        fi->src_addrlen);
  } else {
    ret = dupaddr(
        &hints->dest_addr,
        &hints->dest_addrlen,
        fi->dest_addr,
        fi->dest_addrlen);
  }

  fi_freeinfo(fi);
  return ret;
}

int ft_getsrcaddr(char *node, char *service, struct fi_info *hints) {
  return getaddr(node, service, hints, FI_SOURCE);
}

int ft_read_addr_opts(
    char **node,
    char **service,
    struct fi_info *hints,
    uint64_t *flags,
    struct ft_opts *opts) {
  int ret;

  if (opts->dst_addr) {
    if (!opts->dst_port)
      opts->dst_port = default_port;

    ret = ft_getsrcaddr(opts->src_addr, opts->src_port, hints);
    if (ret)
      return ret;
    *node = opts->dst_addr;
    *service = opts->dst_port;
  } else {
    if (!opts->src_port)
      opts->src_port = default_port;

    *node = opts->src_addr;
    *service = opts->src_port;
    *flags = FI_SOURCE;
  }

  return 0;
}

void eq_readerr(struct fid_eq *eq, const char *eq_str) {
  struct fi_eq_err_entry eq_err;
  int rd;

  rd = fi_eq_readerr(eq, &eq_err, 0);
  if (rd != sizeof(eq_err)) {
    FT_PRINTERR("fi_eq_readerr", rd);
  } else {
    FT_EQ_ERR(eq, eq_err, NULL, 0);
  }
}
