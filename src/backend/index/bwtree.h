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
      class StructNode;

    public:
      class Scanner;

    friend class InnerNode;
    /** BWTREE CLASS **/
    public:
      typedef oid_t PID;
      typedef std::multimap<KeyType, ValueType, KeyComparator> BufferResult;
      typedef std::map<KeyType, PID, KeyComparator> InnerRange;
      const static PID INVALID_PID = std::numeric_limits<PID>::max();
      const static size_t NODE_TABLE_DFT_CAPACITY = 1<<16;
      const static size_t DELTA_CHAIN_LIMIT = 5;
      const static size_t SPLIT_LIMIT = 128;
      const static size_t MAX_PAGE_SIZE = 512;
      const static size_t MIN_PAGE_SIZE = 64;
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
      /**
       * @brief Special case split root procedure
       * @param root The consolidated root node to be splited
       */
      void SplitRoot(InnerNode *root);

    private:
      // Helper functions
      /**
       * @brief Install a separator delta to a structure node. This function will not retry install
       *        until success.
       * @param node        Node for which to install the separator, it should be the caller's ancestor
       * @param begin_key   The start of the splited range
       * @param end_key     The end of the splited range
       * @return true the install succeed, false otherwise.
       */
      bool InstallSeparator(StructNode *node, KeyType begin_key, KeyType end_key, PID new_pid) {
        InnerInsertDelta *iid = new InnerInsertDelta(*this, begin_key, end_key, new_pid, node);
        return node_table.UpdateNode(node, iid);
      }


    private:
      enum smo_t {NONE, SPLIT, MERGE};
      struct PathState {
        std::vector<PID> pid_path;
        std::vector<Node *> node_path;

        KeyType begin_key;
        KeyType end_key;
        bool open;
      };
      /** @brief Consolidate a data node given the buffer */
      void ConsolidateDataNode(DataNode *node, const BufferResult &buffer);
      /** @brief Consolidate a data node. If the size of the leaf page hit certain limits,
       *         we perform child split/merge and return corresponding smo_t.
       *  @return NONE if no SMO happen during consolidation. SPLIT/MERGE indicates that
       *          parent node should attempt to finish the split/merge operation
       */
      smo_t ConsolidateDataNodeWithSMO(DataNode *node, PathState &state);
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
        BWTree &bwTree;
      private:
        //const NodeTable& node_table;
        PID pid;
        size_t depth;

      public:
        Node() = delete;
        Node(BWTree &bwTree_)  : bwTree(bwTree_), pid(INVALID_PID), depth(0) {};
        virtual ~Node(){}

        /**
         * @brief Search a key in the bwtree, if upwards is true, return the first DataNode that has the key that is
         * JUST larger than target. if upwards is false, find the DataNode that has the key which is JUST less than
         * the target. Search will also finish incomplete SMO.
         * @param target Key to find
         * @param upwards Search direction
         * @return The first DataNode that contains the key according to search direction
         */
        virtual DataNode *Search(KeyType target, bool forwards, PathState &path_state) = 0;
        virtual bool Consolidate(smo_t &smo_result) = 0;
        
        virtual DataNode *GetLeftMostDescendant() = 0;
        virtual Node *GetNext() const = 0;
        // TODO: initialize depth in subclass
        inline void SetDepth(size_t d) {depth = d;}
        inline size_t GetDepth() const {return depth;}
        inline void SetPID(PID pid) {this->pid = pid;};
        inline PID GetPID() const{ return this->pid;};
        // Add inner node insert delta
      };

      /** @brief Class for BWTree structure node */
      class StructNode : public Node {
        friend class BWTree;
      public:
        StructNode(BWTree &bwTree_) : Node(bwTree_) {}
        virtual ~StructNode(){}
        virtual DataNode *Search(KeyType target, bool forwards, PathState &path_state) = 0;
        virtual DataNode *GetLeftMostDescendant() = 0;
        virtual Node *GetNext() const = 0;
        virtual bool Consolidate(smo_t &smo_result) = 0;
        // TODO: Structure node also need some kinds of .Buffer method for consolidation.
      };

      /** @brief Class for BWTree inner node */
      class InnerNode : public StructNode {
        friend class BWTree;
      public:
        InnerNode(BWTree &bwTree_) : StructNode(bwTree_), left_pid(INVALID_PID), children(bwTree_.key_comp) {};
        DataNode *Search(KeyType target, bool forwards, PathState &path_state);
        DataNode *GetLeftMostDescendant();
        Node *GetNext() const {return nullptr;};
        virtual bool Consolidate(__attribute__((unused)) smo_t &smo_result){assert(smo_result != smo_result); return true;};
      private:
        PID left_pid;
        //std::vector<std::pair<KeyType, PID> > children;
        InnerRange children;
      };

      /** @brief Class for spliting BWTree structure node */
      // TODO: implement it @jiexi
      class StructSplitDelta : public StructNode {
        friend class BWTree;
      public:
        // note: set depth and Node::pid
        StructSplitDelta(BWTree &bwTree_, const KeyType &split_key_, PID split_pid_, StructNode *next_)
          : StructNode(bwTree_), split_key(split_key_), split_pid(split_pid_), next(next_) {
          this->Node::SetDepth(next_->GetDepth()+1);
          this->Node::SetPID(next_->Node::GetPID());
        };
        virtual ~StructSplitDelta(){};
        virtual DataNode *Search(KeyType target, bool forwards, PathState &path_state);
        virtual DataNode *GetLeftMostDescendant() { return nullptr; };
        virtual Node *GetNext() const {return this->next;};
        virtual bool Consolidate(smo_t &smo_result);
      private:
        KeyType split_key;
        PID split_pid;
        StructNode *next;
      };

      /** @brief Class for BWTree structure separator node */
      // TODO: implement it
      class InnerInsertDelta : public StructNode {
        friend class BWTree;
      public:
        InnerInsertDelta(BWTree &bwTree_, const KeyType &begin_k_, const KeyType &end_k_, PID pid_, StructNode *next_)
          : StructNode(bwTree_), begin_k(begin_k_), end_k(end_k_), next(next_), sep_pid(pid_) {
          Node::SetPID(next_->GetPID());
          Node::SetDepth(next_->Node::GetDepth() + 1);
        };
        virtual ~InnerInsertDelta(){}
        virtual DataNode *GetLeftMostDescendant() {return nullptr;};
        virtual Node *GetNext() const {return this->next;};

        virtual bool Consolidate(smo_t &smo_result);
        virtual DataNode *Search(KeyType target, bool forwards, PathState &path_state);
      private:
        KeyType begin_k;
        KeyType end_k;
        StructNode *next;
        PID sep_pid;
      };

      /** @brief Class for BWTree structure separator node */
      // TODO: implement it
      class InnerDeleteDelta : public StructNode {
        friend class BWTree;
      public:
        InnerDeleteDelta(BWTree &bwTree_) : StructNode(bwTree_) {};
        virtual ~InnerDeleteDelta(){}
        virtual DataNode *Search(KeyType target, bool forwards, PathState &path_state);
        virtual DataNode *GetLeftMostDescendant() = 0;
        virtual Node *GetNext() const = 0;
        virtual bool Consolidate(smo_t &smo_result);
      };

      class DataNode : public Node {
        friend class BWTree;
      private:
        LeafNode *base_page;
      public:
        DataNode(BWTree &bwTree_, LeafNode *bp) : Node(bwTree_), base_page(bp){};
        virtual DataNode *Search(KeyType target, bool forwards, PathState &path_state) = 0;
        // TODO: Method Buffer need modification to handle all kinds of delta -- Jiexi
        virtual PID Buffer(BufferResult &result, bool upwards = true) = 0;
        virtual bool hasKV(const KeyType &t_k, const ValueType &t_v) = 0;
        virtual bool Consolidate(smo_t &smo_result) = 0;
      private:
        DataNode *GetLeftMostDescendant();
      };

      /** @brief Class for BWTree leaf node  */
      class LeafNode : public DataNode {
        friend class BWTree;
      public:
        LeafNode(BWTree &bwTree_) : DataNode(bwTree_, this), prev(INVALID_PID), next(INVALID_PID), items(bwTree_.key_comp) {};
        PID Buffer(BufferResult &result, bool upwards = true);
        DataNode *Search(KeyType target, bool forwards, PathState &path_state);
        bool hasKV(const KeyType &t_k, const ValueType &t_v);
        Node *GetNext() const {return nullptr;};
        virtual bool Consolidate(smo_t &smo_result);
      private:
        PID prev;
        PID next;
        std::multimap<KeyType, ValueType, KeyComparator> items;
      };

      /** @brief Class for BWTree Insert Delta node */
      class InsertDelta : public DataNode {
        friend class BWTree;
      public:
        InsertDelta(BWTree &bwTree_, const KeyType &k, const ValueType &v, DataNode *next_): DataNode(bwTree_, next_->base_page), next(next_),
                                                                                  info(std::make_pair(k,v)) {
          Node::SetPID(next_->GetPID());
          Node::SetDepth(next->Node::GetDepth()+1);};
        PID Buffer(BufferResult &result, bool upwards = true);
        DataNode *Search(KeyType target, bool forwards, PathState &path_state);
        bool hasKV(const KeyType &t_k, const ValueType &t_v);
        Node *GetNext() const {return (Node *)next;};
        virtual bool Consolidate(smo_t &smo_result);
      private:
        DataNode *next;
        std::pair<KeyType, ValueType> info;
      };

      /** @brief Class for Delete Delta node */
      class DeleteDelta : public DataNode {
        friend class BWTree;
      public:
        DeleteDelta(BWTree &bwTree_, const KeyType &k, const ValueType &v, DataNode *next_): DataNode(bwTree_, next_->base_page), next(next_),
                                                                                  info(std::make_pair(k,v)) { Node::SetPID(next_->GetPID());Node::SetDepth(next->Node::GetDepth()+1);};
        PID Buffer(BufferResult &result, bool upwards = true);
        DataNode *Search(KeyType target, bool forwards, PathState &path_state);
        bool hasKV(const KeyType &t_k, const ValueType &t_v);
        Node *GetNext() const {return (Node *)next;};
        virtual bool Consolidate(smo_t &smo_result);
      private:
        DataNode *next;
        std::pair<KeyType, ValueType> info;
      };

      /** @brief Class for spliting data node */
      // TODO: implement it
      class DataSplitDelta : public DataNode {
        friend class BWTree;
      public:
        DataSplitDelta(BWTree &bwTree_, DataNode *next_): DataNode(bwTree_, next_->base_page), next(next_) {Node::SetPID(next_->GetPID());Node::SetDepth(next->Node::GetDepth()+1);};
        ~DataSplitDelta() {}
        virtual DataNode *Search(KeyType target, bool forwards, PathState &path_state) = 0;
        virtual PID Buffer(BufferResult &result, bool upwards = true) = 0;
        virtual bool hasKV(const KeyType &t_k, const ValueType &t_v) = 0;
        virtual bool Consolidate(smo_t &smo_result);
      private:
        DataNode *next;
        // TODO: need more member
      };

    private:
      /** DATA FIELD **/
      KeyComparator key_comp;
      KeyEqualityChecker key_equals;
      ValueEqualityChecker val_equals;
      NodeTable node_table;
      KeyType MIN_KEY;

    };

  }  // End index namespace
}  // End peloton namespace
