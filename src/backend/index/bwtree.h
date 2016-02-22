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
#include <memory>
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
    private:
      class DataNode;
      class InnerNode;
      class Node;

    public:
      class Scanner;

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
      BWTree() = delete;

      /** @brief Insert a key/val pair from the bwtree */
      bool InsertKV(const KeyType &k, const ValueType &v);
      /** @brief Delete a key/val pair from the bwtree */
      bool DeleteKV(const KeyType &k, const ValueType &v);
      /** @brief Scan the BwTree given a key and direction */
      std::unique_ptr<Scanner> Scan(const KeyType &key, bool forward, bool equality);
      std::unique_ptr<Scanner> ScanFromBegin();
    public:
      class Scanner {
      private:
        BufferResult buffer_result;
        typename BufferResult::iterator iterator_cur;
        typename BufferResult::iterator iterator_end;
        PID next_pid;
        bool equal;
        bool forward;
        KeyType key;
        const BWTree &bwTree;
      public:
        Scanner() = delete;
        Scanner(const Scanner& scanner) = delete;
        Scanner(KeyType k, bool fw, bool eq, const BWTree &bwTree_, KeyComparator kcmp);
        Scanner(const BWTree &bwTree_, KeyComparator kcmp);
        std::pair<KeyType, ValueType> GetNext();
        bool HasNext();
      private:
        void GetNextNode();
      };
    private:
      // Class for the node mapping table: maps a PID to a BWTree node.

      class NodeTable {
      private:
        std::vector<std::atomic<Node *>> table;
        std::atomic<PID> next_pid{0};
      public:
        NodeTable(size_t capacity);
        NodeTable() = delete;
        ~NodeTable(){

          for(auto& head : table){
            Node *h = head.load();
            if(h == nullptr){
              continue;
            }
            Node *next = h->GetNext();
            while(next){
              delete h;
              h = next;
              next = next->GetNext();
            }
            delete h;
          }
        }

        /**
         * @brief Compare and swap an old node with new node at PID.
         * @param old_node Expected old node to be updated.
         * @param new_node New node to replace old_node.
         * @return true when the CAS operation success, i.e. the old_node is still the value for PID.
         *         false otherwise.
         */
        bool UpdateNode(Node * old_node, Node *new_node);

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
        const BWTree &bwTree;
      private:
        //const NodeTable& node_table;
        PID pid;

      public:
        Node() = delete;
        Node(const BWTree &bwTree_)  : bwTree(bwTree_), pid(INVALID_PID) {};

        void SetPID(PID pid) {this->pid = pid;};
        PID GetPID() const{ return this->pid;};
        virtual ~Node(){}
        virtual Node *GetNext() const = 0;
        /**
         * @brief Search a key in the bwtree, if upwards is true, return the first DataNode that has the key that is
         * JUST larger than target. if upwards is false, find the DataNode that has the key which is JUST less than
         * the target.
         * @param target Key to find
         * @param upwards Search direction
         * @return The first DataNode that contains the key according to search direction
         */
        virtual DataNode *Search(KeyType target, bool upwards = true) = 0;
        virtual DataNode *GetLeftMostdescendant() = 0;
      };

      /** @brief Class for BWTree inner node */
      class InnerNode : protected Node {
        friend class BWTree;
      public:
        InnerNode(const BWTree &bwTree_) : Node(bwTree_), right_pid(INVALID_PID) {};
        DataNode *Search(KeyType target, bool upwards = true);
        DataNode *GetLeftMostdescendant();
        Node *GetNext() const {return nullptr;};
      private:
        PID right_pid;
        std::vector<std::pair<KeyType, PID> > children;
      };

      class DataNode : public Node {
        friend class BWTree;
      public:
        DataNode(const BWTree &bwTree_) : Node(bwTree_){};
      private:
        virtual PID Buffer(BufferResult &result, bool upwards = true) = 0;
        virtual DataNode *Search(KeyType target, bool upwards = true) = 0;
        DataNode *GetLeftMostdescendant();
        virtual bool hasKV(const KeyType &t_k, const ValueType &t_v) = 0;
      };

      /** @brief Class for BWTree leaf node  */
      class LeafNode : protected DataNode {
        friend class BWTree;
      public:
        LeafNode(const BWTree &bwTree_) : DataNode(bwTree_), prev(INVALID_PID), next(INVALID_PID), items() {};
        PID Buffer(BufferResult &result, bool upwards = true);
        DataNode *Search(KeyType target, bool upwards = true);
        bool hasKV(const KeyType &t_k, const ValueType &t_v);
        Node *GetNext() const {return nullptr;};
      private:
        PID prev;
        PID next;
        std::vector<std::pair<KeyType, ValueType> > items;
      };

      /** @brief Class for BWTree Insert Delta node */
      class InsertDelta : protected DataNode {
      public:
        InsertDelta(const BWTree &bwTree_, const KeyType &k, const ValueType &v, DataNode *next_): DataNode(bwTree_), next(next_),
                                                                                  info(std::make_pair(k,v)) { Node::SetPID(next_->GetPID());};
        PID Buffer(BufferResult &result, bool upwards = true);
        DataNode *Search(KeyType target, bool upwards = true);
        bool hasKV(const KeyType &t_k, const ValueType &t_v);
        Node *GetNext() const {return (Node *)next;};
      private:
        DataNode *next;
        std::pair<KeyType, ValueType> info;
      };

      /** @brief Class for Delete Delta node */
      class DeleteDelta : public DataNode {
      public:
        DeleteDelta(const BWTree &bwTree_, const KeyType &k, const ValueType &v, DataNode *next_): DataNode(bwTree_), next(next_),
                                                                                  info(std::make_pair(k,v)) { Node::SetPID(next_->GetPID()); };
        PID Buffer(BufferResult &result, bool upwards = true);
        DataNode *Search(KeyType target, bool upwards = true);
        bool hasKV(const KeyType &t_k, const ValueType &t_v);
        Node *GetNext() const {return (Node *)next;};
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
