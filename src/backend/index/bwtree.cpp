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

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::BWTree(KeyComparator kcp, KeyEqualityChecker keq):
  key_comp(kcp),
  key_equals(keq),
  val_equals(ValueEqualityChecker()),
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

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner::Scanner(KeyType k, bool fw, bool eq, const BWTree &bwTree_):
  buffer_result(),
  iterators(),
  next_pid(),
  equal(eq),
  forward(fw),
  has_next(),
  key(k),
  bwTree(bwTree_)
{
  DataNode *node = bwTree.node_table.GetNode(0)->Search(key, forward);
  next_pid = node->Buffer(buffer_result, forward);
  iterators = buffer_result.equal_range(key);
  has_next = (iterators.first != buffer_result.end());
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
const KeyType &BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner::GetKey()
{
  return key;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
const ValueType &BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner::GetValue()
{
  return nullptr;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner::Next()
{
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::NodeTable::NodeTable(size_t capacity = NODE_TABLE_DFT_CAPACITY) :
  table(capacity)
{
  for (auto &item : table) {
    item.store(nullptr);
  }
}


template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::NodeTable::UpdateNode(Node *old_node, Node *new_node)
{
  assert(old_node);
  assert(new_node);
  assert(old_node->GetPID() == new_node->GetPID());
  auto &item = table[old_node->pid];

  // set new node 's pid ??
  return item.compare_exchange_weak(old_node, new_node);
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::PID
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::NodeTable::InsertNode(Node *node) {
  PID new_pid = next_pid++;
  if (new_pid >= table.capacity()) {
    LOG_ERROR("BWTree mapping table is full, can't insert new node");
    return INVALID_PID;
  }

  node->pid = new_pid;
  table[new_pid].store(node);

  return new_pid;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Node*
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::NodeTable::GetNode(PID pid) const
{
  assert(pid < table.capacity());

  return table[pid].load();
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InnerNode::Search(
  KeyType target,
  bool forward)
{
  PID next_pid = INVALID_PID;
  long left = 0, right = this->children.size()-1;

  if (forward) {
    while (left<right) {

      if (left+1 == right) {
        if (Node::bwTree.key_comp(target, this->children[left].first)) {
          next_pid = this->children[left].second;
        } else {
          assert(Node::bwTree.key_comp(target, this->children[right].first));
          next_pid = this->children[right].second;
        }
        break;
      }

      long mid = left + (right - left) / 2;
      auto &mid_key = this->children[mid].first;
      if (Node::bwTree.key_equals(mid_key, target)) {
        next_pid = this->children[mid + 1].second;
        assert(mid+1<children.size());
        break;
      }

      if (Node::bwTree.key_comp(target, mid_key)) {
        right = mid;
      } else {
        left = mid+1;
      }
    }
  } else {
    return nullptr;
  }

  assert(next_pid != INVALID_PID);
  Node *next_node = Node::bwTree.node_table.GetNode(next_pid);
  return next_node->Search(target, forward);
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::LeafNode::Search(
  KeyType target,
  bool forward)
{

  /*
  if(forward){

    // empty
    // max < target
    // go to next
    if(items.empty() || Node::bwTree.key_comp(items.back().first, target)){
      if(next == INVALID_PID){
        return nullptr;
      }else{
        return Node::bwTree.node_table.GetNode(next)->Search(target, forward);
      }
    }
  }

  else{
    // empty
    // target < min
    // go to prev
    if(items.empty() || Node::bwTree.key_comp(target, items.front().first)){
      if(prev == INVALID_PID){
        return nullptr;
      }else{
        return Node::bwTree.node_table.GetNode(prev)->Search(target, forward);
      }
    }
  }
  */
  return this;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DeleteDelta::Search(
  KeyType target,
  bool forward)
{
  if (Node::bwTree.key_equals(target, this->info.first)) {
    return this;
  }
  return this->next->Search(target, forward);
};

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InsertDelta::Search(
  KeyType target,
  bool forward)
{
  if (Node::bwTree.key_equals(target, this->info.first)) {
    return this;
  }
  return this->next->Search(target, forward);
}

//==-----------------------------
////////// BUFFER FUNCTION
//==-----------------------------
template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::PID
BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::LeafNode::Buffer(BufferResult &result, bool upwards) {
  for(auto& item : items){
    // result.emplace(item);
    result.insert(item);
  }
  return upwards ? this->next : this->prev;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::PID
BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::DeleteDelta::Buffer(BufferResult &result, bool upwards) {
  // buffer succeed firstly
  auto resPid = next->Buffer(result, upwards);

  // apply delete
  auto searchRes = result.equal_range(info.first);
  for(auto itr = searchRes.first; itr != searchRes.second; itr++){
    if(Node::bwTree.val_equals(info.second, itr->second)){
      // erase free the memory of itr???
      result.erase(itr);
      break;
    }
  }

  return resPid;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::PID
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InsertDelta::Buffer(BufferResult &result, bool upwards) {
  auto resPid = next->Buffer(result, upwards);

  // apply insert
  result.insert(info);

  return resPid;
}


class ItemPointerEqualChecker {
public:
  inline bool operator() (const ItemPointer &pointer1, const ItemPointer &pointer2) const {
    return (pointer1.block == pointer2.block &&
            pointer1.offset == pointer2.offset);
  }
};


//==-------------------------------------------
/////////// BwTree Implementation /////////////
//==-------------------------------------------
template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DeleteKV(KeyType k, ValueType v)
{
  // First locate the data node to delete the key
  auto root = node_table.GetNode(0);
  auto dnode = root->Search(k, true);
  PID pid = dnode->GetPID();
  // Construct a delete delta
  DeleteDelta *delta = new DeleteDelta(*this, k, v);
  auto old_node = node_table.GetNode(pid);
  // CAS into the mapping table
  bool success = node_table.UpdateNode(old_node, static_cast<Node *>delta);

  return success;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::InsertKV(KeyType k,
                                                                                                    ValueType v) {
  auto dt_node = node_table.GetNode(0)->Search(k, true);
  assert(dt_node);
  auto old_node = node_table.GetNode(dt_node->GetPID());
  auto ins_node = new InsertDelta(*this, k, v, (DataNode *)old_node);
  assert(dt_node->GetPID() == old_node->GetPID());
  ins_node->SetPID(old_node->GetPID());
  return node_table.UpdateNode(old_node, (Node *)ins_node);
}

// Explicit template instantiation
template class BWTree<IntsKey<1>, ItemPointer, IntsComparator<1>,
  IntsEqualityChecker<1>, ItemPointerEqualChecker>;
template class BWTree<IntsKey<2>, ItemPointer, IntsComparator<2>,
  IntsEqualityChecker<2>, ItemPointerEqualChecker>;
template class BWTree<IntsKey<3>, ItemPointer, IntsComparator<3>,
  IntsEqualityChecker<3>, ItemPointerEqualChecker>;
template class BWTree<IntsKey<4>, ItemPointer, IntsComparator<4>,
  IntsEqualityChecker<4>, ItemPointerEqualChecker>;

template class BWTree<GenericKey<4>, ItemPointer, GenericComparator<4>,
  GenericEqualityChecker<4>, ItemPointerEqualChecker>;
template class BWTree<GenericKey<8>, ItemPointer, GenericComparator<8>,
  GenericEqualityChecker<8>, ItemPointerEqualChecker>;
template class BWTree<GenericKey<12>, ItemPointer, GenericComparator<12>,
  GenericEqualityChecker<12>, ItemPointerEqualChecker>;
template class BWTree<GenericKey<16>, ItemPointer, GenericComparator<16>,
  GenericEqualityChecker<16>, ItemPointerEqualChecker>;
template class BWTree<GenericKey<24>, ItemPointer, GenericComparator<24>,
  GenericEqualityChecker<24>, ItemPointerEqualChecker>;
template class BWTree<GenericKey<32>, ItemPointer, GenericComparator<32>,
  GenericEqualityChecker<32>, ItemPointerEqualChecker>;
template class BWTree<GenericKey<48>, ItemPointer, GenericComparator<48>,
  GenericEqualityChecker<48>, ItemPointerEqualChecker>;
template class BWTree<GenericKey<64>, ItemPointer, GenericComparator<64>,
  GenericEqualityChecker<64>, ItemPointerEqualChecker>;
template class BWTree<GenericKey<96>, ItemPointer, GenericComparator<96>,
  GenericEqualityChecker<96>, ItemPointerEqualChecker>;
template class BWTree<GenericKey<128>, ItemPointer, GenericComparator<128>,
  GenericEqualityChecker<128>, ItemPointerEqualChecker>;
template class BWTree<GenericKey<256>, ItemPointer, GenericComparator<256>,
  GenericEqualityChecker<256>, ItemPointerEqualChecker>;
template class BWTree<GenericKey<512>, ItemPointer, GenericComparator<512>,
  GenericEqualityChecker<512>, ItemPointerEqualChecker>;

template class BWTree<TupleKey, ItemPointer, TupleKeyComparator,
  TupleKeyEqualityChecker, ItemPointerEqualChecker>;
}  // End index namespace
}  // End peloton namespace
