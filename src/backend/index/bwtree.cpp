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
#include "backend/index/index_key.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::BWTree(KeyComparator kcp):
  key_comp(kcp),
  node_table(NODE_TABLE_DFT_CAPACITY) {

  // Create a root node
  InnerNode*root = new InnerNode(node_table);
  PID pid = node_table.InsertNode(static_cast<Node *>(root));
  assert(pid == 0);

  // Create a leaf node
  LeafNode *leaf = new LeafNode(node_table);
  pid = node_table.InsertNode(static_cast<Node *>(leaf));
  // Insert the leaf node as the children of the root
  root->children.emplace_back(std::make_pair(std::numeric_limits<KeyType>::max(), pid));
}

template <typename KeyType, typename ValueType, class KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::NodeTable::NodeTable(size_t capacity = NODE_TABLE_DFT_CAPACITY) :
  table(capacity)
{
  for (auto &item : table) {
    item.store(nullptr);
  }
}


template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::NodeTable::UpdateNode(Node *old_node, Node *new_node)
{
  auto &item = table[old_node->pid];
  return item.compare_exchange_weak(old_node, new_node);
}

template <typename KeyType, typename ValueType, class KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::PID
BWTree<KeyType, ValueType, KeyComparator>::NodeTable::InsertNode(Node *node) {
  PID new_pid = next_pid++;
  if (new_pid >= table.capacity()) {
    LOG_ERROR("BWTree mapping table is full, can't insert new node");
    return INVALID_PID;
  }

  node->pid = new_pid;
  table[new_pid].store(node);

  return new_pid;
}

template <typename KeyType, typename ValueType, class KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::Node*
BWTree<KeyType, ValueType, KeyComparator>::NodeTable::GetNode(PID pid) {
  assert(pid < table.capacity());

  return table[pid].load();
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
  Node(node_table_), prev(INVALID_PID), next(INVALID_PID) {}

template <typename KeyType, typename ValueType, class KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::DeleteDelta::DeleteDelta(const NodeTable &node_table_) :
  Node(node_table_) {}

template <typename KeyType, typename ValueType, class KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::InsertDelta::InsertDelta(const NodeTable &node_table_) :
  Node(node_table_) {}

// Add your function definitions here
template <typename KeyType, typename ValueType, class KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::Node*
BWTree<KeyType, ValueType, KeyComparator>::LeafNode::lookup(__attribute__((unused))  KeyType k) {
    return nullptr;
  }

// Add your function definitions here
template <typename KeyType, typename ValueType, class KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::Node*
BWTree<KeyType, ValueType, KeyComparator>::InnerNode::lookup(__attribute__((unused))  KeyType k) {
    return nullptr;
  }



  // Explicit template instantiation
  template class BWTree<IntsKey<1>, ItemPointer, IntsComparator<1>>;
  template class BWTree<IntsKey<2>, ItemPointer, IntsComparator<2>>;
  template class BWTree<IntsKey<3>, ItemPointer, IntsComparator<3>>;
  template class BWTree<IntsKey<4>, ItemPointer, IntsComparator<4>>;

  template class BWTree<GenericKey<4>, ItemPointer, GenericComparator<4>>;
  template class BWTree<GenericKey<8>, ItemPointer, GenericComparator<8>>;
  template class BWTree<GenericKey<12>, ItemPointer, GenericComparator<12>>;
  template class BWTree<GenericKey<16>, ItemPointer, GenericComparator<16>>;
  template class BWTree<GenericKey<24>, ItemPointer, GenericComparator<24>>;
  template class BWTree<GenericKey<32>, ItemPointer, GenericComparator<32>>;
  template class BWTree<GenericKey<48>, ItemPointer, GenericComparator<48>>;
  template class BWTree<GenericKey<64>, ItemPointer, GenericComparator<64>>;
  template class BWTree<GenericKey<96>, ItemPointer, GenericComparator<96>>;
  template class BWTree<GenericKey<128>, ItemPointer, GenericComparator<128>>;
  template class BWTree<GenericKey<256>, ItemPointer, GenericComparator<256>>;
  template class BWTree<GenericKey<512>, ItemPointer, GenericComparator<512>>;

  template class BWTree<TupleKey, ItemPointer, TupleKeyComparator>;
}  // End index namespace
}  // End peloton namespace
