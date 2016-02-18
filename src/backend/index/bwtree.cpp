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
  Node *root = new InnerNode();
  PID pid = node_table.InsertNode(root);
  assert(pid == 0);

  // Create a leaf node
  Node *leaf = new LeafNode();
  pid = node_table.InsertNode(leaf);
  // Insert the leaf node as the children of the root
  root->children.emplace_back(std::make_pair(std::numeric_limits<KeyType>::max(), pid));
}

template <typename KeyType, typename ValueType, class KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::NodeTable::NodeTable(size_t capacity = NODE_TABLE_DFT_CAPACITY) :
  table(capacity, nullptr) {}


template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::NodeTable::UpdateNode(
    Node *old_node, Node *new_node) {
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::PID
BWTree<KeyType, ValueType, KeyComparator>::NodeTable::InsertNode(Node *node) {
  return INVALID_PID;
}

template <typename KeyType, typename ValueType, class KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::Node::Node(const NodeTable &node_table_) :
  node_table(node_table_),
  pid(INVALID_PID) {}

template <typename KeyType, typename ValueType, class KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::InnerNode::InnerNode(const NodeTable &node_table_) :
  Node(node_table_) {}

template <typename KeyType, typename ValueType, class KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::LeafNode::LeafNode(const NodeTable &node_table_) :
  Node(node_table_) {}

template <typename KeyType, typename ValueType, class KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::DeleteDelta::DeleteDelta(const NodeTable &node_table_) :
  Node(node_table_) {}

template <typename KeyType, typename ValueType, class KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::InsertDelta::InsertDelta(const NodeTable &node_table_) :
  Node(node_table_) {}

// Add your function definitions here
template <typename KeyType, typename ValueType, class KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::Node*
BWTree<KeyType, ValueType, KeyComparator>::Node::lookup(KeyType k) { }

// Add your function definitions here
template <typename KeyType, typename ValueType, class KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::Node*
BWTree<KeyType, ValueType, KeyComparator>::InnerNode::lookup(KeyType k) { }
}  // End index namespace
}  // End peloton namespace
