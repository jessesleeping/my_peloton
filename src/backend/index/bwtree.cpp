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

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTree(KeyComparator kcp):
  key_comp(kcp),
  node_table(NODE_TABLE_DFT_CAPACITY) {

  // Create a root node
  InnerNode*root = new InnerNode(*this);
  PID pid = node_table.InsertNode(static_cast<Node *>(root));
  assert(pid == 0);

  // Create a leaf node
  LeafNode *leaf = new LeafNode(*this);
  pid = node_table.InsertNode(static_cast<Node *>(leaf));
  // Insert the leaf node as the children of the root
  root->children.emplace_back(std::make_pair(std::numeric_limits<KeyType>::max(), pid));
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::NodeTable::NodeTable(size_t capacity = NODE_TABLE_DFT_CAPACITY) :
  table(capacity)
{
  for (auto &item : table) {
    item.store(nullptr);
  }
}


template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::NodeTable::UpdateNode(Node *old_node, Node *new_node)
{
  auto &item = table[old_node->pid];
  return item.compare_exchange_weak(old_node, new_node);
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::PID
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::NodeTable::InsertNode(Node *node) {
  PID new_pid = next_pid++;
  if (new_pid >= table.capacity()) {
    LOG_ERROR("BWTree mapping table is full, can't insert new node");
    return INVALID_PID;
  }

  node->pid = new_pid;
  table[new_pid].store(node);

  return new_pid;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Node*
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::NodeTable::GetNode(PID pid) {
  assert(pid < table.capacity());

  return table[pid].load();
}

/**
 * NODE LOOKUP FUNCTIONS
 */

//template <typename KeyType, typename ValueType, class KeyComparator>
//void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::InnerNode::Scan(
//  __attribute__((unused)) const KeyType& lowerBound,
//  __attribute__((unused)) bool equality,
//  __attribute__((unused)) BufferResult &scanRes,
//  __attribute__((unused)) Node *&nodeRes)
//{}
//
//
//template <typename KeyType, typename ValueType, class KeyComparator>
//void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::LeafNode::Scan(
//  const KeyType& lowerBound, bool equality, BufferResult &scanRes, Node *&nodeRes) {
//
//  nodeRes = this;
//  if(items.empty()){
//    return;
//  }
//  size_t b = 0, e = items.size() - 1;
//  while(b < e){
//    size_t m = b + (e - b) / 2;
//    auto &k = items[m].first;
//    if(Node::bwTree.IsKeyEqual(lowerBound, k)){
//      // find
//      break;
//    }else if(Node::bwTree.key_comp(k, lowerBound)){
//      // key < lowerBound
//      b = ++m;
//    }else{
//      // key > lowerBound
//      e = m;
//    }
//  }
//  assert(b == e);
//  // b == e
//  auto &k = items[b].first;
//  if(Node::bwTree.key_comp(k, lowerBound)){
//    assert(e == items.size() - 1);
//    return;
//  }
//
//  // debug
//  if(b > 0){
//    assert(Node::bwTree.key_comp(items[b - 1].first, lowerBound));
//  }
//
//  for(; b < items.size(); b++){
//    auto &k = items[b].first;
//    auto &v = items[b].second;
//    if(equality && !Node::bwTree.IsKeyEqual(k, lowerBound)){
//      break;
//    }
//    assert(Node::bwTree.key_comp(lowerBound, k) ||
//           Node::bwTree.IsKeyEqual(lowerBound, k));
//
//    scanRes.insert(std::make_pair(k,v));
//  }
//
//}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::InnerNode::Search(
  __attribute__((unused)) KeyType target,
  __attribute__((unused)) bool upwards)
{
  return nullptr;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::LeafNode::Search(
  __attribute__((unused)) KeyType target,
  __attribute__((unused)) bool upwards)
{
  return nullptr;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DeleteDelta::Search(
  __attribute__((unused)) KeyType target,
  __attribute__((unused)) bool upwards)
{
  return nullptr;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::InsertDelta::Search(
  __attribute__((unused)) KeyType target,
  __attribute__((unused)) bool upwards)
{
  return nullptr;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker>::PID
BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker>::LeafNode::Buffer(__attribute__((unused)) BufferResult &result)
{ return INVALID_PID; }

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker>::PID
BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker>::DeleteDelta::Buffer(__attribute__((unused)) BufferResult &result)
{ return INVALID_PID; }

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::PID
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::InsertDelta::Buffer(__attribute__((unused)) BufferResult &result)
{ return INVALID_PID; }




// Explicit template instantiation
template class BWTree<IntsKey<1>, ItemPointer, IntsComparator<1>,
  IntsEqualityChecker<1>>;
template class BWTree<IntsKey<2>, ItemPointer, IntsComparator<2>,
  IntsEqualityChecker<2>>;
template class BWTree<IntsKey<3>, ItemPointer, IntsComparator<3>,
  IntsEqualityChecker<3>>;
template class BWTree<IntsKey<4>, ItemPointer, IntsComparator<4>,
  IntsEqualityChecker<4>>;

template class BWTree<GenericKey<4>, ItemPointer, GenericComparator<4>,
  GenericEqualityChecker<4>>;
template class BWTree<GenericKey<8>, ItemPointer, GenericComparator<8>,
  GenericEqualityChecker<8>>;
template class BWTree<GenericKey<12>, ItemPointer, GenericComparator<12>,
  GenericEqualityChecker<12>>;
template class BWTree<GenericKey<16>, ItemPointer, GenericComparator<16>,
  GenericEqualityChecker<16>>;
template class BWTree<GenericKey<24>, ItemPointer, GenericComparator<24>,
  GenericEqualityChecker<24>>;
template class BWTree<GenericKey<32>, ItemPointer, GenericComparator<32>,
  GenericEqualityChecker<32>>;
template class BWTree<GenericKey<48>, ItemPointer, GenericComparator<48>,
  GenericEqualityChecker<48>>;
template class BWTree<GenericKey<64>, ItemPointer, GenericComparator<64>,
  GenericEqualityChecker<64>>;
template class BWTree<GenericKey<96>, ItemPointer, GenericComparator<96>,
  GenericEqualityChecker<96>>;
template class BWTree<GenericKey<128>, ItemPointer, GenericComparator<128>,
  GenericEqualityChecker<128>>;
template class BWTree<GenericKey<256>, ItemPointer, GenericComparator<256>,
  GenericEqualityChecker<256>>;
template class BWTree<GenericKey<512>, ItemPointer, GenericComparator<512>,
  GenericEqualityChecker<512>>;

template class BWTree<TupleKey, ItemPointer, TupleKeyComparator,
  TupleKeyEqualityChecker>;
}  // End index namespace
}  // End peloton namespace
