//
// Created by Valentino on 26/07/16.
//

#ifndef FI_TEST_UTILS_HPP
#define FI_TEST_UTILS_HPP
#include <iostream>
#include "rdma/fabric.h"

std::ostream &operator<<(std::ostream &os, const fi_info& info) {
  os << fi_tostr(&info, FI_TYPE_INFO);
  return os;
}

#endif //FI_TEST_UTILS_HPP
