//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwtree.cpp
//
// Identification: src/backend/index/bwtree.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/bwtree.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator>

BWTree<KeyType, ValueType, KeyComparator>::BWTree(KeyComparator kcp):
  key_comp(kcp),
  node_table() {

  // Create a root node
  InnerNode *root = new InnerNode();
  PID pid = node_table.InsertNode(root);
  assert(pid == 0);

  // Create a leaf node
  LeafNode *leaf = new LeafNode();
  pid = node_table.InsertNode(leaf);
  // Insert the leaf node as the children of the root
  root->children.emplace_back(std::make_pair(std::numeric_limits<KeyType>::max(), pid));
}
// Add your function definitions here
template <typename KeyType, typename ValueType, class KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::Node BWTree<KeyType, ValueType, KeyComparator>::Node::lookup(KeyType k) { }

}  // End index namespace
}  // End peloton namespace
