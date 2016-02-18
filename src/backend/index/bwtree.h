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
#include "backend/common/types.h"
// #include "backend/common/platform.h"


namespace peloton {
  namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
    template <typename KeyType, typename ValueType, class KeyComparator>
    class BWTree {
// TODO: disable default/copy constructor
// TODO: Add a equal_range() method to BWTree for index's use. equal_range() should behave
// similar like stx_btree (return a iterator to the sorted buffer);

    public:
      typedef oid_t PID;
      const static PID INVALID_PID = std::numeric_limits<PID>::max();
      const static size_t NODE_TABLE_DFT_CAPACITY = 1<<16;
      // reference: https://gist.github.com/jeetsukumaran/307264
      class Iterator;

    public:
      BWTree(KeyComparator kcp);
      BWTree() = delete;


    private:
      // Class for the node mapping table: maps a PID to a BWTree node.
      class Node;

      class NodeTable {
      private:
        std::vector<std::atomic<Node *>> table;
        std::atomic<PID> next_pid;
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
        Node *GetNode(PID pid);
      };

      /** @brief Class for BWTree node, only provides common interface */
      class Node {
       friend class BWTree; // 我们为什么要这个

      private:
        const NodeTable& node_table;
        PID pid;

      public:
        Node() = delete;
        Node(const NodeTable& node_table);

        void SetPID(PID pid);
        virtual ~Node(){}
        virtual Node *lookup(KeyType k) = 0;
      };


      /** @brief Class for BWTree inner node */
      class InnerNode : protected Node {
        friend class BWTree;
      public:
        InnerNode(const NodeTable &node_table);
        Node *lookup(KeyType k);
      private:
        PID right_pid;
        std::vector<std::pair<KeyType, PID> > children;
      };

      /** @brief Class for BWTree leaf node  */
      class LeafNode : protected Node {
        friend class BWTree;
      public:
        LeafNode(const NodeTable &node_table);
        Node *lookup(KeyType k);
      private:
        std::vector<std::pair<KeyType, ValueType> > items;
        PID prev;
        PID next;
      };

      /** @brief Class for BWTree Insert Delta node */
      class InsertDelta : protected Node {
      public:
        InsertDelta(const NodeTable &node_table);
      private:
        Node *next;
        std::pair<KeyType, ValueType> info;
      };

      /** @brief Class for Delete Delta node */
      class DeleteDelta : protected Node {
      public:
        DeleteDelta(const NodeTable &node_table);
      private:
        Node *next;
        std::pair<KeyType, ValueType> info;
      };



      /** DATA FIELD **/
      KeyComparator key_comp;
      NodeTable node_table;
    };

  }  // End index namespace
}  // End peloton namespace
