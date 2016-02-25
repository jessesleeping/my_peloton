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
      class LeafNode;

    public:
      class Scanner;

    friend class InnerNode;
    /** BWTREE CLASS **/
    public:
      typedef oid_t PID;
      typedef std::multimap<KeyType, ValueType, KeyComparator> BufferResult;
      const static PID INVALID_PID = std::numeric_limits<PID>::max();
      const static size_t NODE_TABLE_DFT_CAPACITY = 1<<16;
      const static size_t DATA_DELTA_CHAIN_LIMIT = 5;
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
    private:
      /** @brief Consolidate a data node given the buffer */
      void ConsolidateDataNode(DataNode *node, const BufferResult &buffer);
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
        BWTree &bwTree;
      public:
        Scanner() = delete;
        Scanner(const Scanner& scanner) = delete;
        Scanner(KeyType k, bool fw, bool eq, BWTree &bwTree_, KeyComparator kcmp);
        Scanner(BWTree &bwTree_, KeyComparator kcmp);
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
        size_t depth;

      public:
        Node() = delete;
        Node(const BWTree &bwTree_)  : bwTree(bwTree_), pid(INVALID_PID), depth(0) {};

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
        virtual DataNode *Search(KeyType target, bool upwards) = 0;
        virtual DataNode *GetLeftMostDescendant() = 0;
        inline void SetDepth(size_t d) {depth = d;}
        inline size_t GetDepth() const {return depth;}
        /**
         * @brief Consolidate a node
         * @return The consolidated new node. The new node's memory is allocated from heap. You should
         *  free it if you no longer use it, for example when CAS failed in node table.
         */
//        virtual Node *Consolidate();
      };

      /** @brief Class for BWTree inner node */
      class InnerNode : protected Node {
        friend class BWTree;
      public:
        InnerNode(const BWTree &bwTree_) : Node(bwTree_), right_pid(INVALID_PID) {};
        DataNode *Search(KeyType target, bool upwards);
        DataNode *GetLeftMostDescendant();
        Node *GetNext() const {return nullptr;};
      private:
        PID right_pid;
        std::vector<std::pair<KeyType, PID> > children;
      };

      class DataNode : public Node {
        friend class BWTree;
      private:
        LeafNode *base_page;
      public:
        DataNode(const BWTree &bwTree_, LeafNode *bp) : Node(bwTree_), base_page(bp){};
        virtual DataNode *Search(KeyType target, bool upwards) = 0;
      private:
        virtual PID Buffer(BufferResult &result, bool upwards = true) = 0;
        DataNode *GetLeftMostDescendant();
        virtual bool hasKV(const KeyType &t_k, const ValueType &t_v) = 0;
      };

      /** @brief Class for BWTree leaf node  */
      class LeafNode : protected DataNode {
        friend class BWTree;
      public:
        LeafNode(const BWTree &bwTree_) : DataNode(bwTree_, this), prev(INVALID_PID), next(INVALID_PID), items() {};
        PID Buffer(BufferResult &result, bool upwards = true);
        DataNode *Search(KeyType target, bool upwards);
        bool hasKV(const KeyType &t_k, const ValueType &t_v);
        Node *GetNext() const {return nullptr;};
      private:
        PID prev;
        PID next;
        std::vector<std::pair<KeyType, ValueType> > items;
      };

      /** @brief Class for BWTree Insert Delta node */
      class InsertDelta : public DataNode {
      public:
        InsertDelta(const BWTree &bwTree_, const KeyType &k, const ValueType &v, DataNode *next_): DataNode(bwTree_, next_->base_page), next(next_),
                                                                                  info(std::make_pair(k,v)) { Node::SetPID(next_->GetPID()); SetDepth(next_->GetDepth()+1);};
        PID Buffer(BufferResult &result, bool upwards = true);
        DataNode *Search(KeyType target, bool upwards);
        bool hasKV(const KeyType &t_k, const ValueType &t_v);
        Node *GetNext() const {return (Node *)next;};
      private:
        DataNode *next;
        std::pair<KeyType, ValueType> info;
      };

      /** @brief Class for Delete Delta node */
      class DeleteDelta : public DataNode {
      public:
        DeleteDelta(const BWTree &bwTree_, const KeyType &k, const ValueType &v, DataNode *next_): DataNode(bwTree_, next_->base_page), next(next_),
                                                                                  info(std::make_pair(k,v)) { Node::SetPID(next_->GetPID());SetDepth(next_->GetDepth()+1);};
        PID Buffer(BufferResult &result, bool upwards = true);
        DataNode *Search(KeyType target, bool upwards);
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
