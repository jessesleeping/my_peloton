//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction.h
//
// Identification: src/backend/concurrency/transaction.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"
#include "backend/common/exception.h"
#include "backend/common/printable.h"
#include "backend/concurrency/transaction_manager.h"

#include <atomic>
#include <cassert>
#include <vector>
#include <map>

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//

class Transaction : public Printable {
  friend class TransactionManager;

  Transaction(Transaction const &) = delete;

 public:
  Transaction()
      : txn_id(INVALID_TXN_ID),
        cid(INVALID_CID),
        last_cid(INVALID_CID),
        ref_count(BASE_REF_COUNT),
        waiting_to_commit(false),
        next(nullptr) {}

  Transaction(txn_id_t txn_id, cid_t last_cid)
      : txn_id(txn_id),
        cid(INVALID_CID),
        last_cid(last_cid),
        ref_count(BASE_REF_COUNT),
        waiting_to_commit(false),
        next(nullptr) {}

  ~Transaction() {
    if (next != nullptr) {
      next->DecrementRefCount();
    }
  }

  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  inline txn_id_t GetTransactionId() const { return txn_id; }

  inline cid_t GetCommitId() const { return cid; }

  inline cid_t GetLastCommitId() const { return last_cid; }

  // record inserted tuple
  void RecordInsert(ItemPointer location);

  // record deleted tuple
  void RecordDelete(ItemPointer location);

  const std::map<oid_t, std::vector<oid_t>> &GetInsertedTuples();

  const std::map<oid_t, std::vector<oid_t>> &GetDeletedTuples();

  // reset inserted tuples and deleted tuples
  // used by recovery (logging)
  void ResetState(void);

  // maintain reference counts for transactions
  inline void IncrementRefCount();

  inline void DecrementRefCount();

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Set result and status
  inline void SetResult(Result result);

  // Get result and status
  inline Result GetResult() const;

 protected:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // transaction id
  txn_id_t txn_id;

  // commit id
  cid_t cid;

  // last visible commit id
  cid_t last_cid;

  // references
  std::atomic<size_t> ref_count;

  // waiting for commit ?
  std::atomic<bool> waiting_to_commit;

  // cid context
  Transaction *next __attribute__((aligned(16)));

  // inserted tuples
  std::map<oid_t, std::vector<oid_t>> inserted_tuples;

  // deleted tuples
  std::map<oid_t, std::vector<oid_t>> deleted_tuples;

  // synch helpers
  std::mutex txn_mutex;

  // result of the transaction
  Result result_ = peloton::RESULT_SUCCESS;
};

inline void Transaction::IncrementRefCount() { ++ref_count; }

inline void Transaction::DecrementRefCount() {
  // DROP transaction when ref count reaches 0
  // this returns the value immediately preceding the assignment
  if (ref_count.fetch_sub(1) == 1) {
    delete this;
  }
}

inline void Transaction::SetResult(Result result) { result_ = result; }

inline Result Transaction::GetResult() const { return result_; }

}  // End concurrency namespace
}  // End peloton namespace
