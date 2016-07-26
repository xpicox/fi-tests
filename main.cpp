#include "rdma/fabric.h"
#include <iostream>
#include <unistd.h> // gethostname

int main(int argc, const char* argv[]) {
  char hostname[1024];
  gethostname(hostname, sizeof(hostname));
  std::string host { hostname };
  std::cout << "Hostname: " << host << "\nSize: " << host.size() << "\n";

  const std::unique_ptr<fi_info, decltype(&fi_freeinfo)> hints_p(
      fi_allocinfo(),
      fi_freeinfo);
  hints_p->ep_attr->type = FI_EP_MSG;
  std::cout << "Hints:\n" << fi_tostr(hints_p.get(), FI_TYPE_INFO);
  fi_info* info { nullptr };
  std::cout << info << "\n";
  fi_getinfo(
      fi_version(),
      hostname,
      nullptr,
      0 /*FI_PROV_ATTR_ONLY*/,
      hints_p.get(),
      &info);
  std::cout << info << "\nAccess domains:\n";
  for (fi_info* begin = info, *end = nullptr; begin != end; begin =
      begin->next) {
    std::cout << fi_tostr(begin, FI_TYPE_INFO) << "\n";
  }
  fi_freeinfo(info);  // Should free all the substructures
  return 0;
}
