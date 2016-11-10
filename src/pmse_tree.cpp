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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"
#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/util/log.h"
#include "mongo/stdx/memory.h"

#include "pmse_tree.h"

#include "errno.h"
#include "libpmemobj++/transaction.hpp"

namespace mongo {

void PmseTree::remove(pool_base pop, BSONObj& key,
                         const RecordId& loc, bool dupsAllowed) {
}
/*
 * Construct new leaf node and return pointer to it.
 */
inline persistent_ptr<PmseTreeNode> PmseTree::constructNewLeaf() {

    auto n = make_persistent<PmseTreeNode>(true);
    return n;
}

persistent_ptr<PmseTreeNode> PmseTree::makeTreeRoot(BSONObj_PM& key,
                                                  const RecordId& loc) {
    auto n = make_persistent<PmseTreeNode>(true);
    n->keys[0] = key;
    n->values_array[0] = loc;
    n->num_keys = n->num_keys + 1;
    n->next = nullptr;
    n->previous = nullptr;
    n->parent = nullptr;
    return n;
}

persistent_ptr<PmseTreeNode> PmseTree::locateLeafWithKey(
                persistent_ptr<PmseTreeNode> node, BSONObj_PM& key,
                const BSONObj& _ordering) {
    uint64_t i;
    int64_t cmp;
    bool wasEqual = false;
    persistent_ptr<PmseTreeNode> current = node;

    if (current == nullptr)
        return current;
    while (!current->is_leaf) {
        i = 0;
        while (i < current->num_keys) {

            cmp = key.getBSON().woCompare(current->keys[i].getBSON(), _ordering,
                            false);
            if (cmp > 0) {
                i++;
            } else {
                if (cmp == 0) {
                    if (wasEqual) {
                        break;
                    } else {
                        wasEqual = true;
                        i++;
                    }

                } else {
                    break;
                }
            }
        }
        current = current->children_array[i];
    }

    return current;
}

/*
 * Insert leaf into correct place.
 */
Status PmseTree::insertKeyIntoLeaf(persistent_ptr<PmseTreeNode> node,
                                      BSONObj_PM& key, const RecordId& loc,
                                      const BSONObj& _ordering) {
    uint64_t i, insertion_point;
    insertion_point = 0;

    while (insertion_point < node->num_keys
                    && key.getBSON().woCompare(
                                    node->keys[insertion_point].getBSON(),
                                    _ordering, false) > 0) {
        insertion_point++;

    }

    for (i = node->num_keys; i > insertion_point; i--) {
        node->keys[i] = node->keys[i - 1];
        node->values_array[i] = node->values_array[i - 1];
    }

    node->keys[insertion_point] = key;
    node->values_array[insertion_point] = loc;
    node->num_keys = node->num_keys + 1;

    return Status::OK();
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
uint64_t PmseTree::cut(uint64_t length) {
    if (length % 2 == 0)
        return length / 2;
    else
        return length / 2 + 1;
}

/*
 * Split node and insert value
 */
persistent_ptr<PmseTreeNode> PmseTree::splitFullNodeAndInsert(
                pool_base pop, persistent_ptr<PmseTreeNode> node, BSONObj_PM& key,
                const RecordId& loc, const BSONObj& _ordering) {
    persistent_ptr<PmseTreeNode> new_leaf;
    BSONObj_PM new_key;
    uint64_t insertion_index = 0;
    uint64_t i, j, split;
    persistent_ptr<PmseTreeNode> new_root;
    new_leaf = make_persistent<PmseTreeNode>(true);
    BSONObj_PM temp_keys_array[TREE_ORDER + 1];
    RecordId temp_values_array[TREE_ORDER + 1];

    while (insertion_index < (node->num_keys)
                    && key.getBSON().woCompare(
                                    node->keys[insertion_index].getBSON(),
                                    _ordering,
                                    false) > 0) {
        insertion_index++;

    }
    split = cut(TREE_ORDER);

    /*
     * Copy from existing to temp, leaving space for inserted one
     */
    for (i = 0, j = 0; i < node->num_keys; i++, j++) {
        if (j == insertion_index)
            j++;
        temp_keys_array[j] = node->keys[i];
        temp_values_array[j] = node->values_array[i];
    }

    /*
     * Fill free slot with inserted key
     */
    temp_keys_array[insertion_index] = key;
    temp_values_array[insertion_index] = loc;

    /*
     * Now copy from temp array to new and to old
     */
    node->num_keys = 0;
    for (i = 0; i < split; i++) {
        node->keys[i] = temp_keys_array[i];
        node->values_array[i] = temp_values_array[i];
        node->num_keys = node->num_keys + 1;
    }
    /*
     * Copy rest of keys to new node
     */
    for (i = split, j = 0; i < (TREE_ORDER + 1); i++, j++) {
        new_leaf->keys[j] = temp_keys_array[i];
        new_leaf->values_array[j] = temp_values_array[i];
        new_leaf->num_keys = new_leaf->num_keys + 1;
    }
    /*
     * Update pointers next, previous
     */
    new_leaf->next = node->next;
    node->next = new_leaf;
    new_leaf->previous = node;

    /*
     * Update parents
     */
    new_leaf->parent = node->parent;
    new_key = new_leaf->keys[0];
    new_root = insertIntoNodeParent(pop, root, node, new_key, new_leaf);
    last = new_leaf;

    return new_root;
}

uint64_t PmseTree::get_left_index(persistent_ptr<PmseTreeNode> parent,
                                     persistent_ptr<PmseTreeNode> left) {
    uint64_t left_index = 0;
    while (left_index <= parent->num_keys
                    && parent->children_array[left_index] != left) {
        left_index++;
    }
    return left_index;
}

/*
 * Insert key into internal node
 * Returns root.
 */
persistent_ptr<PmseTreeNode> PmseTree::insertKeyIntoNode(
                pool_base pop, persistent_ptr<PmseTreeNode> root,
                persistent_ptr<PmseTreeNode> n, uint64_t left_index,
                BSONObj_PM& new_key, persistent_ptr<PmseTreeNode> right) {
    uint64_t i;

    for (i = n->num_keys; i > left_index; i--) {
        n->children_array[i + 1] = n->children_array[i];
        n->keys[i] = n->keys[i - 1];
    }
    n->children_array[left_index + 1] = right;
    n->keys[left_index] = new_key;
    n->num_keys = n->num_keys + 1;
    return root;
}

persistent_ptr<PmseTreeNode> PmseTree::insertToNodeAfterSplit(
                pool_base pop, persistent_ptr<PmseTreeNode> root,
                persistent_ptr<PmseTreeNode> old_node, uint64_t left_index,
                BSONObj_PM& new_key, persistent_ptr<PmseTreeNode> right) {

    uint64_t i = 0, j, split;
    BSONObj_PM k_prime;
    persistent_ptr<PmseTreeNode> new_node;
    persistent_ptr<PmseTreeNode> child;
    persistent_ptr<PmseTreeNode> new_root;
    new_node = make_persistent<PmseTreeNode>(false);
    persistent_ptr<PmseTreeNode> temp_children_array[TREE_ORDER + 2];
    BSONObj_PM temp_keys_array[TREE_ORDER + 1];

    for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {

        if (j == left_index + 1)
            j++;
        temp_children_array[j] = old_node->children_array[i];
    }

    for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
        if (j == left_index)
            j++;
        temp_keys_array[j] = old_node->keys[i];
    }

    temp_children_array[left_index + 1] = right;
    temp_keys_array[left_index] = new_key;

    split = cut(TREE_ORDER + 1);
    old_node->num_keys = 0;
    for (i = 0; i < split - 1; i++) {
        old_node->children_array[i] = temp_children_array[i];
        old_node->keys[i] = temp_keys_array[i];
        old_node->num_keys = old_node->num_keys + 1;
    }
    old_node->children_array[i] = temp_children_array[i];
    k_prime = temp_keys_array[split - 1];

    for (++i, j = 0; i < (TREE_ORDER + 1); i++, j++) {
        new_node->children_array[j] = temp_children_array[i];
        new_node->keys[j] = temp_keys_array[i];
        new_node->num_keys = new_node->num_keys + 1;
    }
    new_node->children_array[j] = temp_children_array[i];
    new_node->parent = old_node->parent;
    for (i = 0; i <= new_node->num_keys; i++) {
        child = new_node->children_array[i];
        child->parent = new_node;
    }
    new_root = insertIntoNodeParent(pop, root, old_node, k_prime, new_node);
    return new_root;
}

/*
 * Inserts a new node into tree structure.
 * Returns root after tree modification.
 */
persistent_ptr<PmseTreeNode> PmseTree::insertIntoNodeParent(
                pool_base pop, persistent_ptr<PmseTreeNode> root,
                persistent_ptr<PmseTreeNode> left, BSONObj_PM& new_key,
                persistent_ptr<PmseTreeNode> right) {
    persistent_ptr<PmseTreeNode> parent;
    uint64_t left_index;
    parent = left->parent;
    /*
     * Check if parent exist. If not, create new one.
     */
    if (parent == nullptr)
        return allocateNewRoot(pop, left, new_key, right);

    /*
     * There is parent, insert key into it.
     */
    /*
     * Find place in parent where to insert new key
     */
    left_index = get_left_index(parent, left);
    /*
     * If there is slot for new key - just insert
     */
    if (parent->num_keys < TREE_ORDER) {
        return insertKeyIntoNode(pop, root, parent, left_index, new_key, right);
    }
    /*
     * There is no slot for new key - we need to split
     */
    return insertToNodeAfterSplit(pop, root, parent, left_index, new_key, right);
}
/*
 * Allocate node for new root and fill it with key.
 */
persistent_ptr<PmseTreeNode> PmseTree::allocateNewRoot(
                pool_base pop, persistent_ptr<PmseTreeNode> left,
                BSONObj_PM& new_key, persistent_ptr<PmseTreeNode> right) {
    persistent_ptr<PmseTreeNode> new_root;
    new_root = make_persistent<PmseTreeNode>(false);
    new_root->keys[0] = new_key;
    new_root->children_array[0] = left;
    new_root->children_array[1] = right;
    new_root->num_keys = new_root->num_keys + 1;
    new_root->parent = nullptr;
    left->parent = new_root;
    right->parent = new_root;
    return new_root;
}

persistent_ptr<PmseTreeNode> PmseTree::find(persistent_ptr<PmseTreeNode> root,
                                          BSONObj_PM& key,
                                          const BSONObj& _ordering) {
    persistent_ptr<PmseTreeNode> node;
    node = locateLeafWithKey(root, key, _ordering);
    return node;
}

Status PmseTree::insert(pool_base pop, BSONObj_PM& key,
                           const RecordId& loc, const BSONObj& _ordering,
                           bool dupsAllowed) {

    persistent_ptr<PmseTreeNode> node;
    Status status = Status::OK();

    if (!root)   //root not allocated yet
    {
        transaction::exec_tx(pop, [&] {
            root = makeTreeRoot(key,loc);
            first = root;
            last = root;
        });
        return Status::OK();
    }
    node = find(root, key, _ordering);

    /*
     * There is place for new value
     */
    if (node->num_keys < (TREE_ORDER)) {
        transaction::exec_tx(pop, [&] {
            status = insertKeyIntoLeaf(node,key,loc,_ordering);
        });
        return status;
    }

    /*
     * splitting
     */
    transaction::exec_tx(pop, [&] {
        root = splitFullNodeAndInsert(pop,node,key,loc,_ordering);
    });
    return Status::OK();
}

}
