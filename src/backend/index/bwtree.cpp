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
  root->children.insert(std::make_pair(MIN_KEY, pid));
}

//==----------------------------------
///////// SCANNER FUNCTIONS
//==----------------------------------
template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner::Scanner(KeyType k, bool fw, bool eq, BWTree &bwTree_, KeyComparator kcmp):
  buffer_result(kcmp, bwTree_.MIN_KEY),
  iterator_cur(),
  iterator_end(),
  next_pid(INVALID_PID),
  equal(eq),
  forward(fw),
  key(k),
  bwTree(bwTree_)
{
  PathState path_state;
  // TODO: support backward scan
  assert(forward == true);

  // TODO: Assume that root is always in PID 0
  Node *root = bwTree.node_table.GetNode(0);

  // Initialize path_state
  path_state.begin_key = bwTree.MIN_KEY;
  path_state.pid_path.push_back(0);
  path_state.node_path.push_back(root);

  iterator_cur = buffer_result.buffer.end();
  iterator_end = buffer_result.buffer.end();

  DataNode *data_node = root->Search(key, forward, path_state);
  data_node->Buffer(buffer_result);
  next_pid = (forward) ? buffer_result.next_pid : buffer_result.prev_pid;

  // Check if root needs consolidate
  if (root->GetDepth() > BWTree::DELTA_CHAIN_LIMIT) {
    StructNode *struct_node = dynamic_cast<StructNode *>(root);
    assert(struct_node != nullptr);
    // Special consolidatation
    bwTree.Consolidate<StructNode>(struct_node, path_state);
  }

  auto iterators = buffer_result.buffer.equal_range(key);
  iterator_cur = iterators.first;
  iterator_end = equal ? iterators.second : buffer_result.buffer.end();
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner::Scanner(BWTree &bwTree_, KeyComparator kcmp):
  buffer_result(kcmp, bwTree.MIN_KEY),
  iterator_cur(),
  iterator_end(),
  next_pid(INVALID_PID),
  equal(false),
  forward(true),
  key(bwTree.MIN_KEY),
  bwTree(bwTree_)
{
  PathState path_state;

  // Initialize path_state
  Node *root = bwTree.node_table.GetNode(0);
  path_state.begin_key = bwTree.MIN_KEY;
  path_state.pid_path.push_back(0);
  path_state.node_path.push_back(root);

  iterator_cur = buffer_result.buffer.end();
  iterator_end = buffer_result.buffer.end();
  DataNode *data_node = root->Search(bwTree.MIN_KEY, forward, path_state);
  data_node->Buffer(buffer_result);
  next_pid = (forward) ? buffer_result.next_pid : buffer_result.prev_pid;

  // Check if root need consolidate
  if (root->GetDepth() > BWTree::DELTA_CHAIN_LIMIT) {
    StructNode *struct_node = dynamic_cast<StructNode *>(root);
    assert(struct_node != nullptr);
    // Special consolidatation
    bwTree.Consolidate<StructNode>(struct_node, path_state);
  }

  iterator_cur = buffer_result.buffer.begin();
  iterator_end = buffer_result.buffer.end();
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
std::pair<KeyType, ValueType> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scanner::GetNext()
{
  std::pair<KeyType, ValueType> scan_res = *iterator_cur;
  // Use ++ may cause problem when we are using backward direction
  if (++iterator_cur == iterator_end && iterator_end == buffer_result.buffer.end() && next_pid != INVALID_PID) {
    // make new buffer
    DataNode *data_node = dynamic_cast<DataNode*>(bwTree.node_table.GetNode(next_pid)); // ugly assumption
    assert(data_node != NULL);
    data_node->Buffer(buffer_result);
    next_pid = (forward) ? buffer_result.next_pid : buffer_result.prev_pid;

    if (equal) {
      auto iterators = buffer_result.buffer.equal_range(key);
      iterator_cur = iterators.first;
      iterator_end = iterators.second;
    } else {
      iterator_cur = buffer_result.buffer.begin();
      iterator_end = buffer_result.buffer.end();
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

//template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
//typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
//BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InnerNode::GetLeftMostDescendant() {
//  // TODO: fix it by using min key
//  //return this->bwTree.node_table.GetNode(this->children.begin()->second)->GetLeftMostDescendant();
//  return nullptr;
//}
//
//template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
//typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
//BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode::GetLeftMostDescendant() {
//  return this;
//}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
  BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InnerNode::Search(KeyType target,
                                                                                                         bool forwards,
                                                                                                         PathState &path_state){
  //return nullptr;
  // TODO: direction

  assert(!children.empty());
  auto res = children.upper_bound(target);
  auto next = res--;

  Node *child = Node::bwTree.node_table.GetNode(res->second);

//  path_state.node_path.push_back(Node::bwTree.node_table.GetNode(this->GetPID()));
  path_state.node_path.push_back(child);
//  path_state.pid_path.push_back(this->GetPID());
  path_state.pid_path.push_back(child->Node::GetPID());

  auto old_bk = path_state.begin_key;
  auto old_ek = path_state.end_key;

  //path_state.begin_key = res->first;
  // bk < first ? first : bk
  // get max
  path_state.begin_key = Node::bwTree.key_comp(path_state.begin_key, res->first) ? res->first : path_state.begin_key;
  if(next == children.end()){
    path_state.end_key = res->first;
    path_state.open = true;
  }else{
    path_state.open = false;
  }

  DataNode *dt = child->Search(target, forwards, path_state);

  // check consolidate
  if(child->GetDepth() > BWTree::DELTA_CHAIN_LIMIT){
    // TODO: consolidate
    DataNode *data_node = dynamic_cast<DataNode*>(child);
    StructNode *struct_node = dynamic_cast<StructNode*>(child);
    assert((data_node != nullptr && struct_node == nullptr) || (data_node == nullptr && struct_node != nullptr));
    if (data_node != nullptr) {
      Node::bwTree.Consolidate<DataNode>(data_node, path_state);
    } else {
      Node::bwTree.Consolidate<StructNode>(struct_node, path_state);
    }
  }

  path_state.pid_path.pop_back();
  path_state.node_path.pop_back();
  path_state.begin_key = old_bk;
  path_state.end_key = old_ek;
  return dt;
};

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InnerInsertDelta::Search(__attribute__((unused)) KeyType target, __attribute__((unused)) bool forwards, __attribute__((unused)) PathState &path_state){
  auto old_bk = path_state.begin_key;
  auto old_ek = path_state.end_key;
  DataNode *res = nullptr;
  if(Node::bwTree.key_comp(target, end_k) &&
    (Node::bwTree.key_equals(target, begin_k) || Node::bwTree.key_comp(begin_k, target))){
    // begin_k <= target < end_k
    auto child = Node::bwTree.node_table.GetNode(sep_pid);

    path_state.node_path.push_back(child);
    path_state.pid_path.push_back(child->Node::GetPID());


    path_state.begin_key = begin_k;
    path_state.end_key = end_k;

    res = child->Search(target, forwards, path_state);

    if(child->GetDepth() > BWTree::DELTA_CHAIN_LIMIT){
      // TODO: consolidate
    }


    path_state.node_path.pop_back();
    path_state.pid_path.pop_back();
  }else {
    // else branch
    if(!Node::bwTree.key_comp(target, end_k)){
      // end_k <= target
      path_state.begin_key = Node::bwTree.key_comp(path_state.begin_key, end_k) ? end_k : path_state.begin_key;
    }else{
      // target < begin_k
      // do nothing
    }

    res = next->Search(target, forwards, path_state);


  }

  path_state.begin_key = old_bk;
  path_state.end_key = old_ek;

  return res;
};

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InnerDeleteDelta::Search(__attribute__((unused)) KeyType target, __attribute__((unused)) bool forwards, __attribute__((unused)) PathState &path_state){
  assert(0);
  return nullptr;
};

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::LeafNode::Search(__attribute__((unused)) KeyType target, __attribute__((unused)) bool forwards, __attribute__((unused)) PathState &path_state){
  return this;
};

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InsertDelta::Search(__attribute__((unused)) KeyType target, __attribute__((unused)) bool forwards, __attribute__((unused)) PathState &path_state){
  if(Node::bwTree.key_equals(target, info.first)){
    return this;
  }

  return next->Search(target, forwards, path_state);
};

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DeleteDelta::Search(__attribute__((unused)) KeyType target, __attribute__((unused)) bool forwards, __attribute__((unused)) PathState &path_state){
  if(Node::bwTree.key_equals(target, info.first)){
    return this;
  }

  return next->Search(target, forwards, path_state);
};

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::StructSplitDelta::Search(__attribute__((unused)) KeyType target, __attribute__((unused)) bool forwards, __attribute__((unused)) PathState &path_state)
{
  auto old_bk = path_state.begin_key;
  auto old_ek = path_state.end_key;
  DataNode *res;
  if (Node::bwTree.key_comp(path_state.begin_key, split_key)) {
    // try and go
    // begin_key < split_key < end_key
    // sep: [path_state.begin_k, split_key), pid
    assert(path_state.node_path.size() >= 2);
    auto path_size = path_state.node_path.size();
    Node::bwTree.InstallSeparator((StructNode *) path_state.node_path[path_size - 2],
                                  path_state.begin_key,
                                  split_key,
                                  split_pid);
    auto sibling = Node::bwTree.node_table.GetNode(split_pid);

    res = sibling->Search(target, forwards, path_state);
    if (sibling->Node::GetDepth() > DELTA_CHAIN_LIMIT) {
      StructNode *node = dynamic_cast<StructNode*>(sibling);
      assert(node != nullptr);
      Node::bwTree.Consolidate<StructNode>(node, path_state);
    }

  } else {
    path_state.begin_key = split_key;
    res = next->Search(target, forwards, path_state);
  }

  path_state.end_key = old_ek;
  path_state.begin_key = old_bk;
  return res;
};

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataNode *
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataSplitDelta::Search(__attribute__((unused)) KeyType target, __attribute__((unused)) bool forwards, __attribute__((unused)) PathState &path_state) {
  auto old_bk = path_state.begin_key;
  auto old_ek = path_state.end_key;
  DataNode *res;
  if (Node::bwTree.key_comp(path_state.begin_key, split_key)) {
    // try and go
    // begin_key < split_key < end_key
    // sep: [path_state.begin_k, split_key), pid
    assert(path_state.node_path.size() >= 2);
    auto path_size = path_state.node_path.size();
    Node::bwTree.InstallSeparator((StructNode *) path_state.node_path[path_size - 2],
                                  path_state.begin_key,
                                  split_key,
                                  split_pid);
    auto sibling = Node::bwTree.node_table.GetNode(split_pid);

    res = sibling->Search(target, forwards, path_state);
    if (sibling->Node::GetDepth() > DELTA_CHAIN_LIMIT) {
      DataNode *node = dynamic_cast<DataNode*>(sibling);
      assert(node != nullptr);
      // TODO: check if the path_state key range is OK
      Node::bwTree.Consolidate<DataNode>(node, path_state);
    }

  } else {
    path_state.begin_key = split_key;
    res = next->Search(target, forwards, path_state);
  }

  path_state.end_key = old_ek;
  path_state.begin_key = old_bk;
  return res;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
template <typename NodeType>
void BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::Consolidate(NodeType *node, PathState &state) {
  typename NodeType::SplitDeltaType *split_delta = nullptr;
  NodeType *new_node = nullptr;
  typename NodeType::BaseNodeType *new_base = nullptr;
  typename NodeType::BaseNodeType *new_base_from_split = nullptr;

  assert(node->Node::GetDepth() > DELTA_CHAIN_LIMIT);

  StructNode *struct_node = nullptr;
  PID left_pid = INVALID_PID;

  // Get the buffer
  BufferResult<NodeType> buffer_result(this->key_comp, state.begin_key);
  node->Buffer(buffer_result);

  // Check if we observe incomplete SMO
  switch (buffer_result.smo_type) {
    case NONE:
      break;
    case SPLIT:
      assert(buffer_result.smo_node != nullptr);
      split_delta = dynamic_cast<typename NodeType::SplitDeltaType *>(buffer_result.smo_node);
      assert(split_delta != nullptr);
      // try to finish the split
      assert(state.node_path.size() >= 2);
      struct_node = dynamic_cast<StructNode *>(state.node_path[state.node_path.size()-2]);
      assert(struct_node != nullptr);
      if (!InstallSeparator(struct_node, state.begin_key, split_delta->split_key, split_delta->pid)) {
        // Complete SMO failed
        return;
      }
      break;
    case MERGE:
    // TODO: implement it
      assert(0);
      break;
    default:
      throw Exception("Invalid SMO type\n");
  }


  // Complete SMO success
  // Check if need split/merge
  if (buffer_result.buffer.size() > MAX_PAGE_SIZE) {
    // Do split
    // Handle root consolidate
    if (node->Node::GetPID() == 0) {
      // TODO: do root split
      assert(0);
    }

    new_base = new typename NodeType::BaseNodeType(*this);
    new_base->SetPID(node->GetPID());
    new_base_from_split = new typename NodeType::BaseNodeType(*this);

    auto itr = buffer_result.buffer.begin();
    int i = 0;
    size_t half_size = buffer_result.buffer.size()/2;

    // new node take the left half
    for (; i < half_size; ++i, ++itr) {
      new_base_from_split->GetContent().insert(*itr);
    }

    // get split position
    auto split_itr = itr;

    // old node keep the right half
    new_base->GetContent().insert(itr, buffer_result.buffer.end());

    // set the new node and install it
    new_base_from_split->SetBrothers(buffer_result.prev_pid, node->Node::GetPID());
    left_pid = node_table.InsertNode(new_base_from_split);
    assert(left_pid != INVALID_PID);

    // set the old node
    new_base->SetBrothers(left_pid, buffer_result.next_pid);

    // create a DataSplitDelta for the old node
    split_delta = new typename NodeType::SplitDeltaType(*this, new_base, split_itr->first, left_pid);
    new_node = split_delta;
  }
// else if (buffer_result.buffer.size() < MIN_PAGE_SIZE) {
//    // TODO: Implement Merge
// }
  else {
    // Normal consolidate
    new_base = new typename NodeType::BaseNodeType(*this);
    new_base->SetPID(node->Node::GetPID());
    new_base->SetBrothers(buffer_result.prev_pid, buffer_result.next_pid);
    new_base->GetContent() = buffer_result.buffer;
    new_node = new_base;
  }

  // Install the consolidated node/chain
  if (node_table.UpdateNode(node, new_node)) {
    // install success
    if (new_base_from_split != nullptr) {
      // Try to install delta
      assert(state.node_path.size() >= 2);
      struct_node = dynamic_cast<StructNode *>(state.node_path[state.node_path.size()-2]);
      assert(struct_node != nullptr);
      InstallSeparator(struct_node, buffer_result.key_lower_bound, split_delta->split_key, split_delta->pid);
    }
    // TODO: GC the old node
  } else {
    // install failed
    // TODO: GC the new_node_from_split if not null
    // TODO: free the new_node, potentially a chain
    // TODO: memory leak here
  }
}

//==-----------------------------
////////// BUFFER FUNCTIONS
//==-----------------------------
template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::InnerNode::Buffer(BufferResult<StructNode> &result) {
    auto itr = children.begin();
    // Find the first one that is not less than key range's lower bound
    for (; itr != children.end(); ++itr) {
      // TODO: according to our design, the content in a splited node is always consistent, because it has been cosolidated before being splited
      if (!Node::bwTree.key_comp(itr->first, result.key_lower_bound)) {
        break;
      }
    }
    assert(itr != children.end());
    // insert to result buffer
    result.buffer.insert(itr, children.end());

    // set next and prev
//  // TODO: check if we need to handle merge/split here
    result.next_pid = INVALID_PID;
    result.prev_pid = this->left_pid;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::InnerInsertDelta::Buffer(BufferResult<StructNode> &result) {
  next->Buffer(result);
  // apply insert
  result.buffer.emplace(begin_k, sep_pid);
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::InnerDeleteDelta::Buffer( __attribute__((unused)) BufferResult<StructNode> &result) {
  assert(0);
  return;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::StructSplitDelta::Buffer(BufferResult<StructNode> &result) {
  // see if we observe an incomplete split
  if (Node::bwTree.key_comp(result.key_lower_bound, split_key)) {
    // key_range.first < split_key
    assert(result.smo_type == NONE); // We can only have one SMO in the chain
    result.smo_type = SPLIT;
    result.smo_node = this;
    // rearrnage key range for the following .Buffer
    result.key_lower_bound = split_key;
    // We do not buffer the PID pointed by this split delta
  }
  next->Buffer(result);
  // Set the prev pid
  result.prev_pid = this->pid;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::LeafNode::Buffer(BufferResult<DataNode> &result) {
  /*
  for(auto& item : items){
    // result.emplace(item);
    result.insert(item);
  }
   */
  auto itr = items.begin();
  // Find the first one that is not less than key range's lower bound
  for (; itr != items.end(); ++itr) {
    // TODO: according to our design, the content in a splited node is always consistent, because it has been cosolidated before being splited
    if (!Node::bwTree.key_comp(itr->first, result.key_lower_bound)) {
      break;
    }
  }
  assert(itr != items.end());
  // insert to result buffer
  result.buffer.insert(itr, items.end());

  // set next and prev
//  // TODO: check if we need to handle merge/split here
  result.next_pid = this->next;
  result.prev_pid = this->prev;
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::DeleteDelta::Buffer(BufferResult<DataNode> &result) {
  // buffer succeed firstly
  next->Buffer(result);

  // apply delete
  auto searchRes = result.buffer.equal_range(info.first);
  for(auto itr = searchRes.first; itr != searchRes.second; ){
    if(Node::bwTree.val_equals(info.second, itr->second)){
      // erase free the memory of itr???
      itr = result.buffer.erase(itr);
    }else{
      itr++;
    }
  }
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InsertDelta::Buffer(BufferResult<DataNode> &result) {
  next->Buffer(result);
  // apply insert
  result.buffer.insert(info);
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DataSplitDelta::Buffer(BufferResult<DataNode> &result) {
  // see if we observe an incomplete split
  if (Node::bwTree.key_comp(result.key_lower_bound, split_key)) {
    // key_range.first < split_key
    assert(result.smo_type == NONE); // We can only have one SMO in the chain
    result.smo_type = SPLIT;
    result.smo_node = this;
    // rearrnage key range for the following .Buffer
    result.key_lower_bound = split_key;
    // We do not buffer the PID pointed by this split delta
  }

  next->Buffer(result);
  // Set the prev pid
  result.prev_pid = this->pid;
};

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
  // TODO: implement it
  assert(0);
  // First locate the data node to delete the key

  // TODO: new search
  // auto root = node_table.GetNode(0);
  //auto dnode = root->Search(k, true);
  Node* dnode = nullptr;
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
      // Check if we need consolidate
      if (delta->Node::GetDepth() > BWTree::DELTA_CHAIN_LIMIT) {
//        BufferResult buffer_result(key_comp);
//        delta->Buffer(buffer_result);
//        ConsolidateDataNode(delta, buffer_result);
      }
      return true;
    }
  }
}

template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator,  KeyEqualityChecker, ValueEqualityChecker>::InsertKV(const KeyType &k,
                                                                                                    const ValueType &v)
{
  // TODO: new search
  assert(0);
  //auto dt_node = node_table.GetNode(0)->Search(k, true);
  Node* dt_node = nullptr;
  /*
  if(dt_node->hasKV(k, v)){
    //printf("dup kv\n");
    return true;
  }*/
  assert(dt_node);
  for(;;) {
    auto old_node = node_table.GetNode(dt_node->GetPID());
    auto delta = new InsertDelta(*this, k, v, (DataNode *) old_node);
    assert(dt_node->GetPID() == old_node->GetPID());
    bool res = node_table.UpdateNode(old_node, (Node *) delta);
    if(!res){
      delete delta;
    }else{
      // Check if we need consolidate
      if (delta->Node::GetDepth() > BWTree::DELTA_CHAIN_LIMIT) {
//        BufferResult buffer_result(key_comp);
//        delta->Buffer(buffer_result);
//        ConsolidateDataNode(delta, buffer_result);
      }
      return true;
    }
  }
}


template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::SplitRoot(InnerNode *root)
{
  // TODO: Ensure that the root given in the parameter
  if (root->children.size() < SPLIT_LIMIT)
    return;
  if (node_table.GetNode(0) != root)
    return;
  // First determine the separate key of the root. Find the middle key
  auto itr = root->children.begin();

  for (int middle = 0; middle < root->children.size() / 2; itr++, middle++)
    ;

  assert(itr != root->children.end());
  KeyType split_key = itr->first;

  // Make a new node with the splited data
  InnerNode *node1 = new InnerNode(*this);
  InnerNode *node2 = new InnerNode(*this);
  // node1 has the range [root.begin, itr)
  // node2 has the range [itr, root.end), node2 is now the old root
  node1->children = RangeType(root->children.begin(), itr, key_comp);
  node2->children = RangeType(itr, root->children.end(), key_comp);
  node2->left_pid = root->left_pid;
  node1->left_pid = root->left_pid;
  // Store the new nodes into node_table
  PID pid1 = node_table.InsertNode(node1);
  PID pid2 = node_table.InsertNode(node2);
  // Add a split delta to the old root
  StructSplitDelta *splitDelta = new StructSplitDelta(*this, node2, split_key, pid1);
  bool success = node_table.UpdateNode(node2, splitDelta);
  if (!success) {
    // TODO: GC
    return;
  }
  // Create a new root
  InnerNode *new_root = new InnerNode(*this);
  new_root->children[MIN_KEY] = pid2;
  new_root->Node::SetPID(0);
  // Install the new root
  success = node_table.UpdateNode(root, new_root);
  if (!success) {
    // TODO: GC
    return;
  }
  // Add the separator delta
  InstallSeparator(new_root, MIN_KEY, split_key, pid1);
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
