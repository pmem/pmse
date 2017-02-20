/*
 * Copyright 2014-2016, Intel Corporation
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

#ifndef SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_TREE_H_
#define SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_TREE_H_

#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/db/index/index_descriptor.h"

#include <libpmemobj.h>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/pool.hpp>
#include "libpmemobj++/transaction.hpp"

using namespace nvml::obj;

namespace mongo {

const uint64_t TREE_ORDER = 3;     //number of elements in internal node
const int64_t BSON_MIN_SIZE = 5;

const uint64_t MAX_END = 2;
const uint64_t MIN_END = 1;

class BSONObj_PM {
public:
    BSONObj_PM() = default;

    BSONObj_PM(persistent_ptr<char> inputData) {
        data = inputData;
    }

    BSONObj getBSON() {
        char* data_ptr = data.get();
        return BSONObj(data_ptr);
    }
    persistent_ptr<char> data;
    uint64_t minMax = 0;
};

struct PmseTreeNode {
    PmseTreeNode() :
                    num_keys(0) {
    }

    PmseTreeNode(bool node_leaf) :
                    num_keys(0) {
        uint64_t i;
        keys = make_persistent<BSONObj_PM[TREE_ORDER]>();

        if (node_leaf) {
            values_array = make_persistent<RecordId[TREE_ORDER]>();
            is_leaf = true;
        } else {
            for (i = 0; i < TREE_ORDER; i++) {
                children_array[i] = nullptr;
            }
        }
        next = nullptr;
        previous = nullptr;
    }

    p<uint64_t> num_keys = 0;
    persistent_ptr<BSONObj_PM[TREE_ORDER]> keys;
    persistent_ptr<PmseTreeNode> children_array[TREE_ORDER + 1]; /* Exist only for internal nodes */

    persistent_ptr<RecordId[TREE_ORDER]> values_array; /* Exist only for leaf nodes */

    persistent_ptr<PmseTreeNode> next = nullptr;
    persistent_ptr<PmseTreeNode> previous = nullptr;
    persistent_ptr<PmseTreeNode> parent = nullptr;
    p<bool> is_leaf = false;
};

struct CursorObject {
    persistent_ptr<PmseTreeNode> node;
    uint64_t index;
};

class PmseTree {
    friend class PmseCursor;

public:
    Status insert(pool_base pop, BSONObj_PM& key, const RecordId& loc,
                  const BSONObj& _ordering, bool dupsAllowed);
    void remove(pool_base pop, BSONObj& key, const RecordId& loc,
                bool dupsAllowed, const BSONObj& _ordering, OperationContext* txn);

    Status dupKeyCheck(pool_base pop,BSONObj& key, const RecordId& loc);
    p<long long> _records = 0;
private:
    uint64_t cut(uint64_t length);
    void placeAfter(PMEMobjpool *pm_pool, BSONObj& key, const RecordId& loc);
    void placeBefore(PMEMobjpool *pm_pool, BSONObj& key, const RecordId& loc);
    int64_t getNeighborIndex(persistent_ptr<PmseTreeNode> node);
    persistent_ptr<PmseTreeNode> coalesceNodes(
                    pool_base pop, persistent_ptr<PmseTreeNode> root,
                    persistent_ptr<PmseTreeNode> n,
                    persistent_ptr<PmseTreeNode> neighbor,
                    int64_t neighbor_index, BSONObj_PM k_prime);

    persistent_ptr<PmseTreeNode> redistributeNodes(
                    pool_base pop, persistent_ptr<PmseTreeNode> root,
                    persistent_ptr<PmseTreeNode> n,
                    persistent_ptr<PmseTreeNode> neighbor,
                    int64_t neighbor_index, int64_t k_prime_index,
                    BSONObj_PM k_prime);

    persistent_ptr<PmseTreeNode> constructNewLeaf();
    persistent_ptr<PmseTreeNode> makeTreeRoot(BSONObj_PM& key,
                                              const RecordId& loc);
    Status insertKeyIntoLeaf(persistent_ptr<PmseTreeNode> node, BSONObj_PM& key,
                             const RecordId& loc, const BSONObj& _ordering);
    persistent_ptr<PmseTreeNode> locateLeafWithKey(
                    persistent_ptr<PmseTreeNode> node, BSONObj& key,
                    const BSONObj& _ordering);
    persistent_ptr<PmseTreeNode> locateLeafWithKeyPM(
                    persistent_ptr<PmseTreeNode> node, BSONObj_PM& key,
                    const BSONObj& _ordering);
    persistent_ptr<PmseTreeNode> splitFullNodeAndInsert(
                    pool_base pop, persistent_ptr<PmseTreeNode> node,
                    BSONObj_PM& key, const RecordId& loc,
                    const BSONObj& _ordering);
    persistent_ptr<PmseTreeNode> insertIntoNodeParent(
                    pool_base pop, persistent_ptr<PmseTreeNode> root,
                    persistent_ptr<PmseTreeNode> node, BSONObj_PM& new_key,
                    persistent_ptr<PmseTreeNode> new_leaf);
    persistent_ptr<PmseTreeNode> allocateNewRoot(
                    pool_base pop, persistent_ptr<PmseTreeNode> left,
                    BSONObj_PM& new_key, persistent_ptr<PmseTreeNode> right);
    uint64_t getLeftIndex(persistent_ptr<PmseTreeNode> parent,
                          persistent_ptr<PmseTreeNode> left);
    persistent_ptr<PmseTreeNode> insertKeyIntoNode(
                    pool_base pop, persistent_ptr<PmseTreeNode> root,
                    persistent_ptr<PmseTreeNode> parent, uint64_t left_index,
                    BSONObj_PM& new_key, persistent_ptr<PmseTreeNode> right);
    persistent_ptr<PmseTreeNode> insertToNodeAfterSplit(
                    pool_base pop, persistent_ptr<PmseTreeNode> root,
                    persistent_ptr<PmseTreeNode> old_node, uint64_t left_index,
                    BSONObj_PM& new_key, persistent_ptr<PmseTreeNode> right);
    persistent_ptr<PmseTreeNode> adjustRoot(persistent_ptr<PmseTreeNode> root);
    persistent_ptr<PmseTreeNode> deleteEntry(pool_base pop, BSONObj& key,
                                             persistent_ptr<PmseTreeNode> node,
                                             uint64_t index);

    persistent_ptr<PmseTreeNode> removeEntryFromNode(
                    BSONObj& key, persistent_ptr<PmseTreeNode> node,
                    uint64_t index);

    persistent_ptr<PmseTreeNode> current;
    persistent_ptr<PmseTreeNode> root;
    persistent_ptr<PmseTreeNode> first;
    persistent_ptr<PmseTreeNode> last;
    BSONObj _ordering;
};

}
#endif /* SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_TREE_H_ */
