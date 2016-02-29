//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// btree_index.cpp
//
// Identification: src/backend/index/btree_index.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/index/bwtree_index.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      container(KeyComparator(metadata), KeyEqualityChecker(metadata)),
      equals(metadata),
      comparator(metadata) {
  // Add your implementation here
  // Set min key of bwtree
  auto schema = metadata->GetKeySchema();
  auto col_count = schema->GetColumnCount();
  std::unique_ptr<storage::Tuple> min_key_tuple(new storage::Tuple(metadata->GetKeySchema(), true));

  for (oid_t column_itr = 0; column_itr < col_count; column_itr++) {
    auto value_type = schema->GetType(column_itr);
    min_key_tuple->SetValue(column_itr, Value::GetMinValue(value_type), GetPool());
  }

  KeyType min_key;
  min_key.SetFromKey(min_key_tuple.get());

  container.SetMinKey(min_key);
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::~BWTreeIndex() {
  // Add your implementation here
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::InsertEntry(
    __attribute__((unused)) const storage::Tuple *key, __attribute__((unused)) const ItemPointer location) {
  // Add your implementation here
  KeyType index_key;
  index_key.SetFromKey(key);
  return container.InsertKV(index_key, location);
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DeleteEntry(
    const storage::Tuple *key, const ItemPointer location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  container.DeleteKV(index_key, (ValueType)location);
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const std::vector<Value> &values,
    const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types,
    const ScanDirectionType& scan_direction) {
  std::vector<ItemPointer> result;
  KeyType index_key;

  // Check if we have leading (leftmost) column equality
  // refer : http://www.postgresql.org/docs/8.2/static/indexes-multicolumn.html
  oid_t leading_column_id = 0;
  auto key_column_ids_itr = std::find(
    key_column_ids.begin(), key_column_ids.end(), leading_column_id);

  // SPECIAL CASE : leading column id is one of the key column ids
  // and is involved in a equality constraint
  bool special_case = false;
  if (key_column_ids_itr != key_column_ids.end()) {
    auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
    if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQUAL) {
      special_case = true;
    }
  }

  LOG_TRACE("Special case : %d ", special_case);

  std::unique_ptr<storage::Tuple> start_key;
  bool all_constraints_are_equal = false;
  bool from_begin = true;

  // If it is a special case, we can figure out the range to scan in the index
  if (special_case == true) {

    start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));
    index_key.SetFromKey(start_key.get());
    from_begin = false;

    // Construct the lower bound key tuple
    all_constraints_are_equal =
      ConstructLowerBoundTuple(start_key.get(), values, key_column_ids, expr_types);
    LOG_TRACE("All constraints are equal : %d ", all_constraints_are_equal);

  }
  switch(scan_direction){
    case SCAN_DIRECTION_TYPE_FORWARD:
    case SCAN_DIRECTION_TYPE_BACKWARD: {
      // TODO: implement backward scanner

      auto scanner_itr = (from_begin) ? container.ScanFromBegin():container.Scan(index_key, true, false);
      // Scan the index entries in forward direction
//      for (auto scan_itr = scan_begin_itr; scan_itr != container.end(); scan_itr++) {
      while (scanner_itr.get()->HasNext()) {
        auto kv_pair = scanner_itr.get()->GetNext();
        auto scan_current_key = kv_pair.first;
        auto tuple = scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

        // Compare the current key in the scan with "values" based on "expression types"
        // For instance, "5" EXPR_GREATER_THAN "2" is true
        if (Compare(tuple, key_column_ids, expr_types, values) == true) {
          ItemPointer location = kv_pair.second;
          result.push_back(location);
        }
        else {
          // We can stop scanning if we know that all constraints are equal
          if(all_constraints_are_equal == true) {
            break;
          }
        }
      }

    }
      break;

    case SCAN_DIRECTION_TYPE_INVALID:
    default:
      throw Exception("Invalid scan direction \n");
      break;
  }


  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys() {
  std::vector<ItemPointer> result;
  auto scanner_ptr = container.ScanFromBegin();
  while (scanner_ptr.get()->HasNext()) {
    result.push_back(scanner_ptr.get()->GetNext().second);
  }

  return result;
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(const storage::Tuple *key) {
  std::vector<ItemPointer> result;
  KeyType index_key;
  index_key.SetFromKey(key);

  auto scanner_ptr = container.Scan(index_key, true, true);
  while (scanner_ptr.get()->HasNext()) {
    result.push_back(scanner_ptr.get()->GetNext().second);
  }
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
std::string
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::GetTypeName() const {
  return "BWTree";
}

// Explicit template instantiation
template class BWTreeIndex<IntsKey<1>, ItemPointer, IntsComparator<1>,
IntsEqualityChecker<1>>;
template class BWTreeIndex<IntsKey<2>, ItemPointer, IntsComparator<2>,
IntsEqualityChecker<2>>;
template class BWTreeIndex<IntsKey<3>, ItemPointer, IntsComparator<3>,
IntsEqualityChecker<3>>;
template class BWTreeIndex<IntsKey<4>, ItemPointer, IntsComparator<4>,
IntsEqualityChecker<4>>;

template class BWTreeIndex<GenericKey<4>, ItemPointer, GenericComparator<4>,
GenericEqualityChecker<4>>;
template class BWTreeIndex<GenericKey<8>, ItemPointer, GenericComparator<8>,
GenericEqualityChecker<8>>;
template class BWTreeIndex<GenericKey<12>, ItemPointer, GenericComparator<12>,
GenericEqualityChecker<12>>;
template class BWTreeIndex<GenericKey<16>, ItemPointer, GenericComparator<16>,
GenericEqualityChecker<16>>;
template class BWTreeIndex<GenericKey<24>, ItemPointer, GenericComparator<24>,
GenericEqualityChecker<24>>;
template class BWTreeIndex<GenericKey<32>, ItemPointer, GenericComparator<32>,
GenericEqualityChecker<32>>;
template class BWTreeIndex<GenericKey<48>, ItemPointer, GenericComparator<48>,
GenericEqualityChecker<48>>;
template class BWTreeIndex<GenericKey<64>, ItemPointer, GenericComparator<64>,
GenericEqualityChecker<64>>;
template class BWTreeIndex<GenericKey<96>, ItemPointer, GenericComparator<96>,
GenericEqualityChecker<96>>;
template class BWTreeIndex<GenericKey<128>, ItemPointer, GenericComparator<128>,
GenericEqualityChecker<128>>;
template class BWTreeIndex<GenericKey<256>, ItemPointer, GenericComparator<256>,
GenericEqualityChecker<256>>;
template class BWTreeIndex<GenericKey<512>, ItemPointer, GenericComparator<512>,
GenericEqualityChecker<512>>;

template class BWTreeIndex<TupleKey, ItemPointer, TupleKeyComparator,
TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
