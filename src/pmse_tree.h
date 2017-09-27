/*
 * Copyright 2014-2017, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef SRC_PMSE_TREE_H_
#define SRC_PMSE_TREE_H_

#include <libpmemobj.h>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/shared_mutex.hpp>

#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/db/index/index_descriptor.h"

using namespace nvml::obj;

namespace mongo {

const uint64_t TREE_ORDER = 7;  // number of elements in internal node
const int64_t BSON_MIN_SIZE = 5;

const uint64_t MIN_END = 1;
const uint64_t MAX_END = 2;

struct IndexKeyEntry_PM {
 public:
    static int64_t compareEntries(IndexKeyEntry& leftEntry, IndexKeyEntry_PM& rightEntry, const BSONObj& ordering);

    BSONObj getBSON();
    persistent_ptr<char> data;
    p<int64_t> loc;
};

struct PmseTreeNode {
    PmseTreeNode() : num_keys(0) {}

    explicit PmseTreeNode(bool node_leaf)
        : num_keys(0) {
        keys = make_persistent<IndexKeyEntry_PM[TREE_ORDER]>();
        if (node_leaf) {
            is_leaf = true;
        } else {
            for (uint64_t i = 0; i < TREE_ORDER; i++) {
                children_array[i] = nullptr;
            }
        }
        next = nullptr;
        previous = nullptr;
    }

    p<uint64_t> num_keys = 0;
    persistent_ptr<IndexKeyEntry_PM[TREE_ORDER]> keys;
    persistent_ptr<PmseTreeNode> children_array[TREE_ORDER + 1]; /* Exist only for internal nodes */
    persistent_ptr<PmseTreeNode> next = nullptr;
    persistent_ptr<PmseTreeNode> previous = nullptr;
    persistent_ptr<PmseTreeNode> parent = nullptr;
    p<bool> is_leaf = false;
    nvml::obj::shared_mutex _pmutex;
};

struct CursorObject {
    persistent_ptr<PmseTreeNode> node;
    uint64_t index;
};

class LocksPtr {
 public:
    LocksPtr(nvml::obj::shared_mutex *_ptr) : ptr(_ptr) {}
    nvml::obj::shared_mutex *ptr;
};

class PmseTree {
    friend class PmseCursor;

 public:
    Status insert(pool_base pop, IndexKeyEntry& entry,
                  const BSONObj& _ordering, bool dupsAllowed);
    bool remove(pool_base pop, IndexKeyEntry& entry,
                bool dupsAllowed, const BSONObj& _ordering, OperationContext* txn);

    uint64_t countElements();

    bool isEmpty();

 private:
    void unlockTree(std::list<LocksPtr>& locks);
    bool nodeIsSafeForOperation(persistent_ptr<PmseTreeNode> node, bool insert);
    uint64_t cut(uint64_t length);
    int64_t getNeighborIndex(persistent_ptr<PmseTreeNode> node);
    persistent_ptr<PmseTreeNode> coalesceNodes(
                    pool_base pop, persistent_ptr<PmseTreeNode> root,
                    persistent_ptr<PmseTreeNode> n,
                    persistent_ptr<PmseTreeNode> neighbor,
                    int64_t neighbor_index, IndexKeyEntry_PM k_prime);
    persistent_ptr<PmseTreeNode> redistributeNodes(
                    pool_base pop, persistent_ptr<PmseTreeNode> root,
                    persistent_ptr<PmseTreeNode> n,
                    persistent_ptr<PmseTreeNode> neighbor,
                    int64_t neighbor_index, int64_t k_prime_index,
                    IndexKeyEntry_PM k_prime);
    persistent_ptr<PmseTreeNode> makeTreeRoot(IndexKeyEntry& key);
    Status insertKeyIntoLeaf(persistent_ptr<PmseTreeNode> node, IndexKeyEntry& entry,
                             const BSONObj& _ordering);
    persistent_ptr<PmseTreeNode> locateLeafWithKeyPM(
                    persistent_ptr<PmseTreeNode> node, IndexKeyEntry& entry,
                    const BSONObj& _ordering, std::list<LocksPtr>& locks,
                    persistent_ptr<PmseTreeNode>& lockNode, bool insert);
    persistent_ptr<PmseTreeNode> splitFullNodeAndInsert(
                    pool_base pop, persistent_ptr<PmseTreeNode> node,
                    IndexKeyEntry& entry, const BSONObj& _ordering);
    persistent_ptr<PmseTreeNode> insertIntoNodeParent(
                    pool_base pop, persistent_ptr<PmseTreeNode> root,
                    persistent_ptr<PmseTreeNode> node, IndexKeyEntry_PM& new_key,
                    persistent_ptr<PmseTreeNode> new_leaf);
    persistent_ptr<PmseTreeNode> allocateNewRoot(
                    pool_base pop, persistent_ptr<PmseTreeNode> left,
                    IndexKeyEntry_PM& new_key, persistent_ptr<PmseTreeNode> right);
    uint64_t getLeftIndex(persistent_ptr<PmseTreeNode> parent,
                          persistent_ptr<PmseTreeNode> left);
    persistent_ptr<PmseTreeNode> insertKeyIntoNode(
                    pool_base pop, persistent_ptr<PmseTreeNode> root,
                    persistent_ptr<PmseTreeNode> parent, uint64_t left_index,
                    IndexKeyEntry_PM& new_key, persistent_ptr<PmseTreeNode> right);
    persistent_ptr<PmseTreeNode> insertToNodeAfterSplit(
                    pool_base pop, persistent_ptr<PmseTreeNode> root,
                    persistent_ptr<PmseTreeNode> old_node, uint64_t left_index,
                    IndexKeyEntry_PM& new_key, persistent_ptr<PmseTreeNode> right);
    persistent_ptr<PmseTreeNode> adjustRoot(persistent_ptr<PmseTreeNode> root);
    persistent_ptr<PmseTreeNode> deleteEntry(pool_base pop, IndexKeyEntry& key,
                                             persistent_ptr<PmseTreeNode> node,
                                             uint64_t index);
    persistent_ptr<PmseTreeNode> removeEntryFromNode(
                    IndexKeyEntry& key, persistent_ptr<PmseTreeNode> node,
                    uint64_t index);

    persistent_ptr<PmseTreeNode> _current;
    persistent_ptr<PmseTreeNode> _root;
    persistent_ptr<PmseTreeNode> _first;
    persistent_ptr<PmseTreeNode> _last;
    BSONObj _ordering;
};

}  // namespace mongo
#endif  // SRC_PMSE_TREE_H_
