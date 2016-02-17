//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// BWTree.h
//
// Identification: src/backend/index/BWTree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
template <typename KeyType, typename ValueType, class KeyComparator>
class BWTree {
// TODO: disable default/copy constructor

public:
  typedef oid_t PID;
  typedef void* ptr_t;

  // reference: https://gist.github.com/jeetsukumaran/307264
  class Iterator;


private:
  class PageTable {
  private:
    std::vector<std::atomic<void*>> table;
    std::atomic<PID> nextPid{0};
  public:
    PageTable(size_t capacity);
    ~PageTable();

    bool update(PID pid, ptr_t old_val, ptr_t new_val);
    PID insert(ptr_t val);
    ptr_t getVal(PID pid);
  };

  class Node {
  friend class BWTree;

  private:
  const PageTable& page_table;
  PID pid;

  public:
    Node(PID pid, const PageTable& page_table);
    virtual ~Node(){}

    virtual Node lookup(KeyType k) = 0;
  };
};

}  // End index namespace
}  // End peloton namespace
