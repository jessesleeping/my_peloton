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
#include <vector>
#include <atomic>
#include <map>
#include "backend/common/types.h"
// #include "backend/common/platform.h"


namespace peloton {
  namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
    template <typename KeyType, typename ValueType, class KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
    class BWTree {
// TODO: disable default/copy constructor
// TODO: Add a equal_range() method to BWTree for index's use. equal_range() should behave
// similar like stx_btree (return a iterator to the sorted buffer);

    /** BWTREE CLASS **/
    class DataNode;
    class InnerNode;
    friend class InnerNode;
    /** BWTREE CLASS **/
    public:
      typedef oid_t PID;
      typedef std::multimap<KeyType, ValueType, KeyComparator> BufferResult;
      const static PID INVALID_PID = std::numeric_limits<PID>::max();
      const static size_t NODE_TABLE_DFT_CAPACITY = 1<<16;
      // reference: https://gist.github.com/jeetsukumaran/307264
      class Iterator;

    public:
      BWTree(KeyComparator kcp, KeyEqualityChecker kec);
      void InsertKV(KeyType k, ValueType v);
      void DeleteKV(KeyType k, ValueType v);
      BWTree() = delete;

    public:
      class Scanner {
      private:
        BufferResult buffer_result;
        BufferResult::iterator curriter;
        PID next_pid;
        bool equal;
        KeyType key;
      public:
        Scanner() = delete;
        Scanner(const Scanner& scanner) = delete;
        Scanner(KeyType k, bool eq) :key(k), equal(eq) {}
        const KeyType &GetKey();
        const ValueType &GetValue();
        bool Next();
      };
    private:
      // Class for the node mapping table: maps a PID to a BWTree node.
      class Node;

      class NodeTable {
      private:
        std::vector<std::atomic<Node *>> table;
        std::atomic<PID> next_pid{0};
      public:
        NodeTable(size_t capacity);
        NodeTable() = delete;
        // ~NodeTable();

        /**
         * @brief Compare and swap an old node with new node at PID.
         * @param old_node Expected old node to be updated.
         * @param new_node New node to replace old_node.
         * @return true when the CAS operation success, i.e. the old_node is still the value for PID.
         *         false otherwise.
         */
        bool UpdateNode(Node *old_node, Node *new_node);

        /**
         * @brief Insert a node into the mapping table, allocate a new PID for the node.
         * @param node Node to be inserted.
         * @return The allocated pid of the newly inserted node. The side affect of this function is set node's
         *   pid the allocated pid.
         */
        PID InsertNode(Node *node);

        /**
         * @brief Get a node by its pid.
         */
        Node *GetNode(PID pid) const;
      };

      /** @brief Class for BWTree node, only provides common interface */
      class Node {
        friend class BWTree;
        friend class iterator;

      protected:
        const BWTree& bwTree;
      private:
        //const NodeTable& node_table;
        PID pid;

      public:
        Node() = delete;
        Node(const BWTree &bwTree_)  : bwTree(bwTree_), pid(INVALID_PID) {};

        void SetPID(PID pid) {this->pid = pid;};
        virtual ~Node(){}

        /**
         * @brief Search a key in the bwtree, if upwards is true, return the first DataNode that has the key that is
         * larger than target.
         * @param target Key to find
         * @param upwards Search direction
         * @return The first DataNode that contains the key
         */
        virtual DataNode *Search(KeyType target, bool upwards) = 0;

//        /**
//         * @brief Scan the bwtree given a lower_bound on the key. Notice that this function will only
//         * @param lower_bound The lower bound of the key to be scanned from
//         * @param equality True if only scan key equals lower_bound
//         * @param scan_res The scan results will be in scan_res when return
//         * @param next The next PID of the BWTree node to be scanned
//         */
//        virtual void ScanUp(
//            const KeyType& lower_bound,
//            bool equality,
//            ScanResult &scan_res,
//            PID &next) = 0;
      };

      /** @brief Class for BWTree inner node */
      class InnerNode : protected Node {
        friend class BWTree;
      public:
        InnerNode(const BWTree &bwTree_) : Node(bwTree_), right_pid(INVALID_PID) {};
        DataNode *Search(KeyType target, bool upwards);
      private:
        PID right_pid;
        std::vector<std::pair<KeyType, PID> > children;
      };

      class DataNode : protected Node {
        friend class BWTree;
      public:
        DataNode(const BWTree &bwTree_) : Node(bwTree_){};
      private:
        virtual PID Buffer(BufferResult &result, bool upwards) = 0;
        virtual DataNode *Search(KeyType target, bool upwards) = 0;
      };

      /** @brief Class for BWTree leaf node  */
      class LeafNode : protected DataNode {
        friend class BWTree;
      public:
        LeafNode(const BWTree &bwTree_) : DataNode(bwTree_), prev(INVALID_PID), next(INVALID_PID), items() {};
        PID Buffer(BufferResult &result, bool upwards);
        DataNode *Search(KeyType target, bool upwards);
      private:
        PID prev;
        PID next;
        std::vector<std::pair<KeyType, ValueType> > items;
      };

      /** @brief Class for BWTree Insert Delta node */
      class InsertDelta : protected DataNode {
      public:
        InsertDelta(const BWTree &bwTree_, const KeyType &k, const ValueType &v): DataNode(bwTree_), next(nullptr),
                                                                                  info(std::make_pair(k,v)) {};
        PID Buffer(BufferResult &result, bool upwards);
        DataNode *Search(KeyType target, bool upwards);
      private:
        DataNode *next;
        std::pair<KeyType, ValueType> info;
      };

      /** @brief Class for Delete Delta node */
      class DeleteDelta : protected DataNode {
      public:
        DeleteDelta(const BWTree &bwTree_, const KeyType &k, const ValueType &v): DataNode(bwTree_), next(nullptr),
                                                                                  info(std::make_pair(k,v)) {};
        PID Buffer(BufferResult &result, bool upwards);
        DataNode *Search(KeyType target, bool upwards);
      private:
        DataNode *next;
        std::pair<KeyType, ValueType> info;
      };

    private:
      /** DATA FIELD **/
      KeyComparator key_comp;
      KeyEqualityChecker key_equals;
      ValueEqualityChecker val_equals;
      NodeTable node_table;
    };

  }  // End index namespace
}  // End peloton namespace
