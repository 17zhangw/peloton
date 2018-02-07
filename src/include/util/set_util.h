//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// set_util.h
//
// Identification: src/include/util/set_util.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <set>

namespace peloton {

class SetUtil {
 public:
  template <typename T>
  static bool isDisjoint(const std::set<T> &setA, const std::set<T> &setB) {
    bool disjoint = true;
    if (setA.empty() || setB.empty()) return disjoint;

    typename std::set<T>::const_iterator setA_it = setA.begin();
    typename std::set<T>::const_iterator setB_it = setB.begin();
    while (setA_it != setA.end() && setB_it != setB.end() && disjoint) {
      if (*setA_it == *setB_it) {
        disjoint = false;
      } else if (*setA_it < *setB_it) {
        setA_it++;
      } else {
        setB_it++;
      }
    }

    return disjoint;
  }
};

}  // namespace peloton
