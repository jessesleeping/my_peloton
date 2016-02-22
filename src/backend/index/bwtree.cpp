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

//==----------------------------------
///////// SCANNER FUNCTIONS
//==----------------------------------

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner::Scanner(KeyType k, bool fw, bool eq, const BWTree &bwTree_, KeyComparator kcmp):
  buffer_result(kcmp),
  iterator_cur(),
  iterator_end(),
  next_pid(INVALID_PID),
  equal(eq),
  forward(fw),
  key(k),
  bwTree(bwTree_)
{
  iterator_cur = buffer_result.end();
  iterator_end = buffer_result.end();
  DataNode *data_node = bwTree.node_table.GetNode(0)->Search(key, forward);
  next_pid = data_node->Buffer(buffer_result, forward);
  auto iterators = buffer_result.equal_range(key);
  iterator_cur = iterators.first;
  iterator_end = equal ? iterators.second : buffer_result.end();
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner::Scanner(const BWTree &bwTree_, KeyComparator kcmp):
  buffer_result(kcmp),
  iterator_cur(),
  iterator_end(),
  next_pid(INVALID_PID),
  equal(false),
  forward(true),
  key(),
  bwTree(bwTree_)
{
  iterator_cur = buffer_result.end();
  iterator_end = buffer_result.end();
  DataNode *data_node = bwTree.node_table.GetNode(0)->GetLeftMostdescendant();
  next_pid = data_node->Buffer(buffer_result, forward);
  iterator_cur = buffer_result.begin();
  iterator_end = buffer_result.end();
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
std::pair<KeyType, ValueType> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner::GetNext()
{
  std::pair<KeyType, ValueType> scan_res = *iterator_cur;
  // Use ++ may cause problem when we are using backward direction
  if (++iterator_cur == iterator_end && iterator_end == buffer_result.end() && next_pid != INVALID_PID) {
    // make new buffer
    DataNode *data_node = dynamic_cast<DataNode*>(bwTree.node_table.GetNode(next_pid)); // ugly assumption
    assert(data_node != NULL);
    next_pid = data_node->Buffer(buffer_result, forward);
    if (equal) {
      auto iterators = buffer_result.equal_range(key);
      iterator_cur = iterators.first;
      iterator_end = iterators.second;
    } else {
      iterator_cur = buffer_result.begin();
      iterator_end = buffer_result.end();
    }
  }
  return scan_res;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner::HasNext()
{
  // In case some valid datanode returns empty buffer. Otherwise we can simply return (cur != end)
  if ((equal || next_pid == INVALID_PID) && iterator_cur == iterator_end) {
    return false;
  }
  return true;
}


//==----------------------------------
///////// MAPPING TABLE FUNCTIONS
//==----------------------------------

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::NodeTable::NodeTable(size_t capacity = NODE_TABLE_DFT_CAPACITY) :
  table(capacity)
{
  for (auto &item : table) {
    item.store(nullptr);
  }
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::NodeTable::UpdateNode(Node * old_node, Node *new_node)
{
  assert(old_node);
  assert(new_node);
  assert(old_node->GetPID() == new_node->GetPID());
  return table[old_node->pid].compare_exchange_strong(old_node, new_node);
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
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InsertDelta::hasKV(
  const KeyType &t_k, const ValueType &t_v) {
  if(Node::bwTree.key_equals(t_k, info.first) &&
    Node::bwTree.val_equals(t_v, info.second)){
    return true;
  }

  return next->hasKV(t_k, t_v);

}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DeleteDelta::hasKV(
  const KeyType &t_k, const ValueType &t_v) {
  if(Node::bwTree.key_equals(t_k, info.first) &&
     Node::bwTree.val_equals(t_v, info.second)){
    return false;
  }

  return next->hasKV(t_k, t_v);

}

//==----------------------------------
///////// NODE FUNCTIONS
//==----------------------------------
template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::LeafNode::hasKV(
  const KeyType &t_k, const ValueType &t_v) {

  if(items.empty()){
    return false;
  }
  size_t b = 0, e = items.size() - 1;
  size_t m = 0;
  while(b < e){
    m = b + (e - b) / 2;
    auto &k = items[m].first;
    if(Node::bwTree.key_equals(k, t_k)){
      break;
    }else if(Node::bwTree.key_comp(k, t_k)){
      // k < t_k
      b = ++m;
    }else{
      // k > t_k
      e = --m;
    }
  }


  for(auto i = m; i < items.size(); i++){
    auto &k = items[i].first;
    if(!Node::bwTree.key_equals(k, t_k)){
      break;
    }
    auto &v = items[i].second;
    if(Node::bwTree.val_equals(v, t_v)){
      return true;
    }
  }

  for(auto i = m; i >= 0; i--){
    auto &k = items[i].first;
    if(!Node::bwTree.key_equals(k, t_k)){
      break;
    }
    auto &v = items[i].second;
    if(Node::bwTree.val_equals(v, t_v)){
      return true;
    }
  }

  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InnerNode::GetLeftMostdescendant() {
  return this->bwTree.node_table.GetNode(this->children.begin()->second)->GetLeftMostdescendant();
}

// TODO: There must be a simple clean way to implement this
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
    while (left < right) {
      // Return the DataNode that contains the key which is just less than or equal to target
      if (left+1 == right) {
        // Look at left and right, if right.begin <= target, return right (left < right.begin <= target)
        // Notice that right.begin is children[left].first
        if (!Node::bwTree.key_comp(target, this->children[left].first)) {
          next_pid = this->children[right].second;
        } else {
          // if right.begin > target, should return left, left.begin <= target < right
          next_pid = this->children[left].second;
        }
        break;
      }

      long mid = left + (right-left) / 2;
      auto &mid_key = this->children[mid].first;
      if (Node::bwTree.key_equals(mid_key, target)) {
        next_pid = this->children[mid+1].second;
        break;
      }

      if (Node::bwTree.key_comp(target, mid_key)) {
        right = mid;
      } else {
        left = mid+1;
      }
    }
  }
  if(next_pid == INVALID_PID){
    assert(this->children.size() == 1);
    assert(Node::bwTree.key_comp(this->children.front().first, target));
    next_pid = this->children.front().second;
  }
  Node *next_node = Node::bwTree.node_table.GetNode(next_pid);
  return next_node->Search(target, forward);
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode::GetLeftMostdescendant() {
  return this;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::LeafNode::Search(
  __attribute__((unused)) KeyType target,
  __attribute__((unused)) bool forward)
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
////////// BUFFER FUNCTIONS
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
  for(auto itr = searchRes.first; itr != searchRes.second; ){
    if(Node::bwTree.val_equals(info.second, itr->second)){
      // erase free the memory of itr???
      itr = result.erase(itr);
    }else{
      itr++;
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
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DeleteKV(const KeyType &k, const ValueType &v)
{
  // First locate the data node to delete the key
  auto root = node_table.GetNode(0);
  auto dnode = root->Search(k, true);
  PID pid = dnode->GetPID();
  // Construct a delete delta
  for(;;) {
    Node* old_node = node_table.GetNode(pid);
    assert(old_node == node_table.GetNode(pid));
    DeleteDelta *delta = new DeleteDelta(*this, k, v, static_cast<DataNode *>(old_node));
    // CAS into the mapping table
    bool success = node_table.UpdateNode(old_node, static_cast<Node *>(delta));
    if(!success){
      printf("delete fail\n");
      delete delta;
    }else{
      return true;
    }
  }
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::InsertKV(const KeyType &k,
                                                                                                    const ValueType &v) {
  auto dt_node = node_table.GetNode(0)->Search(k, true);
  /*
  if(dt_node->hasKV(k, v)){
    //printf("dup kv\n");
    return true;
  }*/
  assert(dt_node);
  for(;;) {
    auto old_node = node_table.GetNode(dt_node->GetPID());
    auto ins_node = new InsertDelta(*this, k, v, (DataNode *) old_node);
    assert(dt_node->GetPID() == old_node->GetPID());
    bool res = node_table.UpdateNode(old_node, (Node *) ins_node);
    if(!res){
      delete ins_node;
    }else{
      return true;
    }
  }
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
std::unique_ptr<typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scan(const KeyType &key,
                                                                                          bool forward, bool equality)
{
  Scanner *scannerp = new Scanner(key, forward, equality, *this, key_comp);
  return std::unique_ptr<Scanner>(scannerp);
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
std::unique_ptr<typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::ScanFromBegin(){
  Scanner *scannerp = new Scanner(*this, key_comp);
  return std::unique_ptr<Scanner>(scannerp);
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
