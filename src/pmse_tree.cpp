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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "pmse_tree.h"
#include "pmse_sorted_data_interface.h"
#include "pmse_change.h"

#include <list>
#include <utility>

#include "mongo/platform/basic.h"
#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/util/log.h"
#include "mongo/stdx/memory.h"

#include "libpmemobj++/transaction.hpp"
#include "libpmemobj++/make_persistent_array.hpp"

namespace mongo {


int64_t IndexKeyEntry_PM::compareEntries(IndexKeyEntry& leftEntry,
                                         IndexKeyEntry_PM& rightEntry,
                                         const BSONObj& ordering) {
    int cmp;

    cmp = leftEntry.key.woCompare(rightEntry.getBSON(), ordering, false);
    if (cmp != 0)
        return cmp;
    // when entries keys are equal, compare RecordID
    if (leftEntry.loc.repr() < rightEntry.loc)
        return -1;
    else if (leftEntry.loc.repr() > rightEntry.loc)
        return 1;
    else
        return 0;
}

BSONObj IndexKeyEntry_PM::getBSON() {
    char* data_ptr = data.get();
    return BSONObj(data_ptr);
}

bool PmseTree::remove(pool_base pop, IndexKeyEntry& entry,
                      bool dupsAllowed, const BSONObj& ordering) {
    persistent_ptr<PmseTreeNode> node;
    uint64_t i;
    int64_t cmp;
    std::list<nvml::obj::shared_mutex*> locks;
    persistent_ptr<PmseTreeNode> lockNode;
    _ordering = ordering;
    // find node with key
    if (!_root)
        return false;
    node = locateLeafWithKeyPM(_root, entry, _ordering, locks, lockNode, false);

    for (i = 0; i < node->num_keys; i++) {
        cmp = IndexKeyEntry_PM::compareEntries(entry, node->keys[i], _ordering);
        if (cmp == 0) {
            break;
        }
    }
    // not found
    if (i == node->num_keys) {
        if (lockNode) {
               lockNode->_pmutex.unlock();
           }
        unlockTree(locks);
        return false;
    }
    _root = deleteEntry(pop, entry, node, i);
    if (lockNode) {
       lockNode->_pmutex.unlock();
    }
    unlockTree(locks);
    return true;
}

persistent_ptr<PmseTreeNode> PmseTree::deleteEntry(pool_base pop,
                                                   IndexKeyEntry& key,
                                                   persistent_ptr<PmseTreeNode> node,
                                                   uint64_t index) {
    uint64_t min_keys;
    int64_t neighbor_index;
    int64_t k_prime_index;
    IndexKeyEntry_PM k_prime;
    uint64_t capacity;
    persistent_ptr<PmseTreeNode> neighbor;

    // Remove key and pointer from node.
    node = removeEntryFromNode(key, node, index);

    if (node == _root) {
        return adjustRoot(_root);
    }
    /* Case:  deletion from a node below the root.
     * (Rest of function body.)
     */

    /* Determine minimum allowable size of node,
     * to be preserved after deletion.
     */
    min_keys = node->is_leaf ? cut(TREE_ORDER - 1) : cut(TREE_ORDER) - 1;
    /* Case:  node stays at or above minimum.
     * (The simple case.)
     */

    if (node->num_keys >= min_keys)
        return _root;

    /* Case:  node falls below minimum.
     * Either coalescence or redistribution
     * is needed.
     */

    /* Find the appropriate neighbor node with which
     * to coalesce.
     * Also find the key (k_prime) in the parent
     * between the pointer to node n and the pointer
     * to the neighbor.
     */
    neighbor_index = getNeighborIndex(node);
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    k_prime = node->parent->keys[k_prime_index];
    neighbor = neighbor_index == -1 ?
               node->parent->children_array[1] :
               node->parent->children_array[neighbor_index];

    capacity = node->is_leaf ? TREE_ORDER : TREE_ORDER - 1;

    /* Coalescence. */
    if (neighbor->num_keys + node->num_keys < capacity)
        return coalesceNodes(pop, _root, node, neighbor, neighbor_index, k_prime);
    else
        return redistributeNodes(pop, _root, node, neighbor, neighbor_index,
                                 k_prime_index, k_prime);
}

/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
persistent_ptr<PmseTreeNode> PmseTree::redistributeNodes(
                pool_base pop, persistent_ptr<PmseTreeNode> root,
                persistent_ptr<PmseTreeNode> n,
                persistent_ptr<PmseTreeNode> neighbor, int64_t neighbor_index,
                int64_t k_prime_index, IndexKeyEntry_PM k_prime) {
    uint64_t i;
    persistent_ptr<PmseTreeNode> tmp;
    /* Case: n has a neighbor to the left.
     * Pull the neighbor's last key-pointer pair over
     * from the neighbor's right end to n's left end.
     */
    if (neighbor_index != -1) {
        if (!n->is_leaf) {
            n->children_array[n->num_keys + 1] = n->children_array[n->num_keys];
            for (i = n->num_keys; i > 0; i--) {
                n->keys[i] = n->keys[i - 1];
                n->children_array[i] = n->children_array[i - 1];
            }
        } else {
            for (i = n->num_keys; i > 0; i--) {
                n->keys[i] = n->keys[i - 1];
            }
        }
        if (!n->is_leaf) {
            n->children_array[0] = neighbor->children_array[neighbor->num_keys];
            tmp = n->children_array[0];
            tmp->parent = n;
            neighbor->children_array[neighbor->num_keys] = nullptr;
            n->keys[0] = k_prime;
            n->parent->keys[k_prime_index].data =
                            neighbor->keys[neighbor->num_keys - 1].data;
            n->parent->keys[k_prime_index].loc = neighbor->keys[neighbor->num_keys - 1].loc;
        } else {
            n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
            persistent_ptr<char> obj;
                obj = pmemobj_tx_alloc(n->keys[0].getBSON().objsize(), 1);
                memcpy(static_cast<void*>(obj.get()),
                       n->keys[0].getBSON().objdata(),
                       n->keys[0].getBSON().objsize());
            if (n->parent->keys[k_prime_index].data) {
                pmemobj_tx_free(n->parent->keys[k_prime_index].data.raw());
            }
            n->parent->keys[k_prime_index].data = obj;
            n->parent->keys[k_prime_index].loc = n->keys[0].loc;
        }
    } else {
        /*
         * Case: n is the leftmost child.
         * Take a key-pointer pair from the neighbor to the right.
         * Move the neighbor's leftmost key-pointer pair
         * to n's rightmost position.
         */
        if (n->is_leaf) {
            n->keys[n->num_keys] = neighbor->keys[0];
            persistent_ptr<char> obj;
            obj = pmemobj_tx_alloc(neighbor->keys[1].getBSON().objsize(), 1);
            memcpy(static_cast<void*>(obj.get()),
                   neighbor->keys[1].getBSON().objdata(),
                   neighbor->keys[1].getBSON().objsize());

            if (n->parent->keys[k_prime_index].data) {
                pmemobj_tx_free(n->parent->keys[k_prime_index].data.raw());
            }

            n->parent->keys[k_prime_index].data = obj;
            n->parent->keys[k_prime_index].loc = neighbor->keys[1].loc;
        } else {
            n->keys[n->num_keys] = k_prime;
            n->children_array[n->num_keys + 1] = neighbor->children_array[0];
            tmp = n->children_array[n->num_keys + 1];
            tmp->parent = n;

            n->parent->keys[k_prime_index].data = neighbor->keys[0].data;
            n->parent->keys[k_prime_index].loc = neighbor->keys[0].loc;
        }
        if (!n->is_leaf) {
            for (i = 0; i < neighbor->num_keys - 1; i++) {
                neighbor->keys[i] = neighbor->keys[i + 1];
                neighbor->children_array[i] = neighbor->children_array[i + 1];
            }
            neighbor->children_array[i] = neighbor->children_array[i + 1];
        } else {
            for (i = 0; i < neighbor->num_keys - 1; i++) {
                neighbor->keys[i] = neighbor->keys[i + 1];
            }
        }
    }

    /*
     * n now has one more key and one more pointer;
     * the neighbor has one fewer of each.
     */
    n->num_keys++;
    neighbor->num_keys--;
    return root;
}

/*
 * Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
persistent_ptr<PmseTreeNode> PmseTree::coalesceNodes(
                pool_base pop, persistent_ptr<PmseTreeNode> root,
                persistent_ptr<PmseTreeNode> n,
                persistent_ptr<PmseTreeNode> neighbor, int64_t neighbor_index,
                IndexKeyEntry_PM k_prime) {
    uint64_t i, j, neighbor_insertion_index, n_end;
    persistent_ptr<PmseTreeNode> tmp;
    IndexKeyEntry k_prime_temp(k_prime.getBSON(), RecordId((k_prime).loc));
    // Swap neighbor with node if node is on the extreme left and neighbor is to its right.
    if (neighbor_index == -1) {
        std::swap(n, neighbor);
    }

    /*
     * Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that n and neighbor have swapped places
     * in the special case of n being a leftmost child.
     */
    neighbor_insertion_index = neighbor->num_keys;

    /*
     * Case:  nonleaf node.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */
    if (!n->is_leaf) {
        /*
         * Append k_prime.
         */
        neighbor->keys[neighbor_insertion_index].data = pmemobj_tx_alloc(k_prime.getBSON().objsize(), 1);
        memcpy(static_cast<void*>(neighbor->keys[neighbor_insertion_index].data.get()), k_prime.getBSON().objdata(),
               k_prime.getBSON().objsize());
        (neighbor->keys[neighbor_insertion_index]).loc = k_prime.loc;
        neighbor->num_keys++;
        n_end = n->num_keys;

        for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
            neighbor->keys[i] = n->keys[j];
            neighbor->children_array[i] = n->children_array[j];
            neighbor->num_keys++;
            n->num_keys--;
        }

        /*
         * The number of pointers is always
         * one more than the number of keys.
         */
        neighbor->children_array[i] = n->children_array[j];

         // All children must now point up to the same parent.
        for (i = 0; i < neighbor->num_keys + 1; i++) {
            tmp = neighbor->children_array[i];
            tmp->parent = neighbor;
        }
    } else {
        /* In a leaf, append the keys and pointers of
         * n to the neighbor.
         * Set the neighbor's last pointer to point to
         * what had been n's right neighbor.
         */
        for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++) {
            neighbor->keys[i] = n->keys[j];
            neighbor->num_keys++;
        }
        if (n->next) {
            n->next->previous = neighbor;
        }
        neighbor->next = n->next;
    }

    if (neighbor_index == -1) {
        for (i = 0; i < n->parent->num_keys; i++) {
            int cmp = IndexKeyEntry_PM::compareEntries(k_prime_temp, n->parent->keys[i], _ordering);
            if (cmp == 0) {
                break;
            }
        }
    } else {
       i = neighbor_index;
    }
    root = deleteEntry(pop, k_prime_temp, n->parent, i);
    delete_persistent<IndexKeyEntry_PM[TREE_ORDER]>(n->keys);
    delete_persistent<PmseTreeNode>(n);
    return root;
}

// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int64_t PmseTree::getNeighborIndex(persistent_ptr<PmseTreeNode> node) {
    /* Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.
     * If n is the leftmost child, this means
     * return -1.
     */
    for (uint64_t i = 0; i <= node->parent->num_keys; i++)
        if (node->parent->children_array[i] == node)
            return i - 1;

    // Error state.
    std::cout << "Error: Search for nonexistent pointer to node in parent.\n"
                    << std::endl;
    return 0;
}

persistent_ptr<PmseTreeNode> PmseTree::adjustRoot(
                persistent_ptr<PmseTreeNode> root) {
    persistent_ptr<PmseTreeNode> new_root;

    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */

    if (root->num_keys > 0)
        return root;
    /* Case: empty root.
     */

    // If it has a child, promote
    // the first (only) child
    // as the new root.
    if (!root->is_leaf) {
        new_root = root->children_array[0];
        new_root->parent = nullptr;
    } else {
        // If it is a leaf (has no children),
        // then the whole tree is empty.
        new_root = nullptr;
    }

    delete_persistent<IndexKeyEntry_PM[TREE_ORDER]>(root->keys);
    delete_persistent<PmseTreeNode>(root);
    return new_root;
}

persistent_ptr<PmseTreeNode> PmseTree::removeEntryFromNode(
                IndexKeyEntry& key, persistent_ptr<PmseTreeNode> node,
                uint64_t index) {
    uint64_t i = index, num_pointers;

    // Remove the key and shift other keys accordingly.
    IndexKeyEntry_PM entryPM;
    entryPM = (node->keys[i]);
    pmemobj_tx_free(entryPM.data.raw());

    for (++i; i < node->num_keys; i++) {
        node->keys[i - 1] = node->keys[i];
    }
    // Remove the pointer and shift other pointers accordingly.
    i = index;
    num_pointers = node->num_keys + 1;
    i++;
    for (++i; i < num_pointers; i++) {
        node->children_array[i - 1] = node->children_array[i];
    }

    // Set the other pointers to NULL for tidiness.
    if (!node->is_leaf) {
        for (i = node->num_keys + 1; i < TREE_ORDER; i++)
            node->children_array[i] = nullptr;
    }
    node->num_keys--;

    return node;
}

persistent_ptr<PmseTreeNode> PmseTree::makeTreeRoot(IndexKeyEntry& entry) {
    persistent_ptr<char> obj;
    auto n = make_persistent<PmseTreeNode>(true);

    (n->keys[0]).data = pmemobj_tx_alloc(entry.key.objsize(), 1);
    memcpy(static_cast<void*>((n->keys[0]).data.get()), entry.key.objdata(), entry.key.objsize());
    (n->keys[0]).loc = entry.loc.repr();
    n->num_keys = n->num_keys + 1;
    n->next = nullptr;
    n->previous = nullptr;
    n->parent = nullptr;
    return n;
}

bool PmseTree::nodeIsSafeForOperation(persistent_ptr<PmseTreeNode> node, bool insert) {
    if (insert) {
        if (node->num_keys < (TREE_ORDER)) {
            return true;
        }
        else
            return false;
    } else {
        uint64_t min_keys;
        min_keys = node->is_leaf ? cut(TREE_ORDER - 1) : cut(TREE_ORDER) - 1;

        if (node->num_keys > min_keys) {
            return true;
        }
        else
            return false;
    }
}

persistent_ptr<PmseTreeNode> PmseTree::locateLeafWithKeyPM(
                persistent_ptr<PmseTreeNode> node, IndexKeyEntry& entry,
                const BSONObj& ordering, std::list<nvml::obj::shared_mutex*>& locks,
                persistent_ptr<PmseTreeNode>& lockNode, bool insert) {
    uint64_t i = 0;
    int64_t cmp;
    persistent_ptr<PmseTreeNode> current = _root;
    persistent_ptr<PmseTreeNode> child;

    if (current == nullptr)
            return nullptr;

    if (current->is_leaf) {
        (current->_pmutex).lock();
        if (current != _root) {
            (current->_pmutex).unlock();
            current = _root;
        } else {
            locks.push_back(&(_root->_pmutex));
            return _root;
        }
    }
    (current->_pmutex).lock_shared();
    if (current != _root) {
        (current->_pmutex).unlock_shared();
        (_root->_pmutex).lock_shared();
        current = _root;
    }
    while (!current->is_leaf) {
        i = 0;
        while (i < current->num_keys) {
            cmp = IndexKeyEntry_PM::compareEntries(entry, current->keys[i], ordering);
            if (cmp >= 0) {
                i++;
            } else {
                break;
            }
        }
    if (current->children_array[i]->is_leaf) {
        (current->children_array[i]->_pmutex).lock();
        locks.push_back(&(current->children_array[i]->_pmutex));
    }
    else {
        (current->children_array[i]->_pmutex).lock_shared();
    }

    current = current->children_array[i];
    current->parent->_pmutex.unlock_shared();
    }
    if (!nodeIsSafeForOperation(current, insert)) {
        unlockTree(locks);
        current = _root;
        (current->_pmutex).lock();
        while (current != _root)
        {
            (current->_pmutex).unlock();
            current = _root;
            (current->_pmutex).lock();
        }
        locks.push_back(&(current->_pmutex));
        if (current == nullptr)
            return nullptr;

        while (!current->is_leaf) {
            i = 0;
            while (i < current->num_keys) {
                cmp = IndexKeyEntry_PM::compareEntries(entry, current->keys[i], ordering);
                if (cmp >= 0) {
                    i++;
                } else {
                    break;
                }
            }
            child = current->children_array[i];
            (child->_pmutex).lock();
            current = child;
            if (nodeIsSafeForOperation(current, insert)) {
                unlockTree(locks);
            }
            locks.push_back(&(current->_pmutex));
        }
    }
    if (current->next) {
        current->next->_pmutex.lock();
        lockNode = current->next;
    }
    return current;
}


/*
 * Insert leaf into correct place.
 */
Status PmseTree::insertKeyIntoLeaf(persistent_ptr<PmseTreeNode> node,
                                   IndexKeyEntry& entry,
                                   const BSONObj& _ordering) {
    uint64_t i, insertion_point = 0;

    while (insertion_point < node->num_keys &&
                    IndexKeyEntry_PM::compareEntries(entry, node->keys[insertion_point], _ordering) > 0) {
            insertion_point++;
    }

    for (i = node->num_keys; i > insertion_point; i--) {
        node->keys[i] = node->keys[i - 1];
    }

    node->keys[insertion_point].data = pmemobj_tx_alloc(entry.key.objsize(), 1);
    memcpy(static_cast<void*>((node->keys[insertion_point]).data.get()), entry.key.objdata(), entry.key.objsize());
    node->keys[insertion_point].loc = entry.loc.repr();
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
                pool_base pop, persistent_ptr<PmseTreeNode> node,
                IndexKeyEntry& entry, const BSONObj& _ordering,
                std::list<nvml::obj::shared_mutex*>& locks) {
    persistent_ptr<PmseTreeNode> new_leaf;
    IndexKeyEntry_PM new_entry;
    uint64_t insertion_index = 0;
    uint64_t i, j, split;
    persistent_ptr<PmseTreeNode> new_root;
    new_leaf = make_persistent<PmseTreeNode>(true);
    new_leaf->_pmutex.lock();
    IndexKeyEntry_PM temp_keys_array[TREE_ORDER + 1];
    while (insertion_index < node->num_keys &&
                    IndexKeyEntry_PM::compareEntries(entry, node->keys[insertion_index], _ordering) > 0) {
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
    }

    /*
     * Fill free slot with inserted key
     */
    temp_keys_array[insertion_index].data = pmemobj_tx_alloc(entry.key.objsize(), 1);
    memcpy(static_cast<void*>((temp_keys_array[insertion_index]).data.get()), entry.key.objdata(), entry.key.objsize());
    temp_keys_array[insertion_index].loc = entry.loc.repr();
    /*
     * Now copy from temp array to new and to old
     */
    node->num_keys = 0;
    for (i = 0; i < split; i++) {
        node->keys[i] = temp_keys_array[i];
        node->num_keys = node->num_keys + 1;
    }
    /*
     * Copy rest of keys to new node
     */
    for (i = split, j = 0; i < (TREE_ORDER + 1); i++, j++) {
        new_leaf->keys[j] = temp_keys_array[i];
        new_leaf->num_keys = new_leaf->num_keys + 1;
    }
    /*
     * Update pointers next, previous
     */
    new_leaf->next = node->next;
    if (node->next) {
        node->next->previous = new_leaf;
    }
    node->next = new_leaf;
    new_leaf->previous = node;

    /*
     * Update parents
     */
    new_leaf->parent = node->parent;
    new_entry = new_leaf->keys[0];
    new_root = insertIntoNodeParent(pop, _root, node, new_entry, new_leaf);
    if(new_root!=_root)
    {
        new_root->_pmutex.lock();
        locks.push_back(&(new_root->_pmutex));
    }
    new_leaf->_pmutex.unlock();
    if (node == _last)
        _last = new_leaf;
    return new_root;
}

uint64_t PmseTree::getLeftIndex(persistent_ptr<PmseTreeNode> parent,
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
                IndexKeyEntry_PM& new_key, persistent_ptr<PmseTreeNode> right) {
    uint64_t i;
    for (i = n->num_keys; i > left_index; i--) {
        n->children_array[i + 1] = n->children_array[i];
        n->keys[i] = n->keys[i - 1];
    }
    n->children_array[left_index + 1] = right;
    (n->keys[left_index]).data = pmemobj_tx_alloc(new_key.getBSON().objsize(), 1);
    memcpy(static_cast<void*>((n->keys[left_index]).data.get()), new_key.data.get(), new_key.getBSON().objsize());
    (n->keys[left_index]).loc = new_key.loc;

    n->num_keys = n->num_keys + 1;
    return root;
}

persistent_ptr<PmseTreeNode> PmseTree::insertToNodeAfterSplit(
                pool_base pop, persistent_ptr<PmseTreeNode> root,
                persistent_ptr<PmseTreeNode> old_node, uint64_t left_index,
                IndexKeyEntry_PM& new_key, persistent_ptr<PmseTreeNode> right) {
    uint64_t i = 0, j, split;
    IndexKeyEntry_PM k_prime;
    persistent_ptr<PmseTreeNode> new_node;
    persistent_ptr<PmseTreeNode> child;
    persistent_ptr<PmseTreeNode> new_root;
    new_node = make_persistent<PmseTreeNode>(false);
    persistent_ptr<PmseTreeNode> temp_children_array[TREE_ORDER + 2];
    IndexKeyEntry_PM temp_keys_array[TREE_ORDER + 1];

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
    (temp_keys_array[left_index]).data = pmemobj_tx_alloc(new_key.getBSON().objsize(), 1);
    memcpy(static_cast<void*>((temp_keys_array[left_index]).data.get()),
                               new_key.data.get(), new_key.getBSON().objsize());
    (temp_keys_array[left_index]).loc = new_key.loc;

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

    IndexKeyEntry_PM entryPM = temp_keys_array[split-1];;
    if (entryPM.data)
       pmemobj_tx_free(entryPM.data.raw());

    return new_root;
}

/*
 * Inserts a new node into tree structure.
 * Returns root after tree modification.
 */
persistent_ptr<PmseTreeNode> PmseTree::insertIntoNodeParent(
                pool_base pop, persistent_ptr<PmseTreeNode> root,
                persistent_ptr<PmseTreeNode> left, IndexKeyEntry_PM& key,
                persistent_ptr<PmseTreeNode> right) {
    persistent_ptr<PmseTreeNode> parent = left->parent;
    uint64_t left_index;
    /*
     * Check if parent exist. If not, create new one.
     */
    if (parent == nullptr) {
        return allocateNewRoot(pop, left, key, right);
    }
    /*
     * There is parent, insert key into it.
     */
    /*
     * Find place in parent where to insert new key
     */
    left_index = getLeftIndex(parent, left);
    /*
     * If there is slot for new key - just insert
     */
    if (parent->num_keys < TREE_ORDER) {
        return insertKeyIntoNode(pop, root, parent, left_index, key, right);
    }
    /*
     * There is no slot for new key - we need to split
     */
    return insertToNodeAfterSplit(pop, root, parent, left_index, key, right);
}

/*
 * Allocate node for new root and fill it with key.
 */
persistent_ptr<PmseTreeNode> PmseTree::allocateNewRoot(
                pool_base pop, persistent_ptr<PmseTreeNode> left,
                IndexKeyEntry_PM& new_key, persistent_ptr<PmseTreeNode> right) {
    persistent_ptr<PmseTreeNode> new_root;
    new_root = make_persistent<PmseTreeNode>(false);
    (new_root->keys[0]).data = pmemobj_tx_alloc(new_key.getBSON().objsize(), 1);
    memcpy(static_cast<void*>((new_root->keys[0]).data.get()), new_key.data.get(), new_key.getBSON().objsize());
    (new_root->keys[0]).loc = new_key.loc;

    new_root->children_array[0] = left;
    new_root->children_array[1] = right;
    new_root->num_keys = new_root->num_keys + 1;
    new_root->parent = nullptr;
    left->parent = new_root;
    right->parent = new_root;
    return new_root;
}

void PmseTree::unlockTree(std::list<nvml::obj::shared_mutex*>& locks) {
    std::list<nvml::obj::shared_mutex*>::const_iterator iterator;
    try {
        for (iterator = locks.begin(); iterator != locks.end(); ++iterator) {
            (*iterator)->unlock();
        }
        locks.erase(locks.begin(), locks.end());
    }catch(std::exception &e) {}
}

Status PmseTree::insert(pool_base pop, IndexKeyEntry& entry,
                        const BSONObj& ordering, bool dupsAllowed) {
    persistent_ptr<PmseTreeNode> node;
    Status status = Status::OK();
    uint64_t i;
    int64_t cmp;
    std::list<nvml::obj::shared_mutex*> locks;
    persistent_ptr<PmseTreeNode> lockNode;
    if (!_root) {
        stdx::lock_guard<nvml::obj::mutex> guard(globalMutex);
        if(!_root){
            // root not allocated yet
            try {
                transaction::exec_tx(pop, [this, &entry] {
                    _root = makeTreeRoot(entry);
                    _first = _root;
                    _last = _root;
                });
            } catch (std::exception &e) {
                log() << "Index: " << e.what();
                status = Status(ErrorCodes::CommandFailed, e.what());
            }
            return status;
        }
    }
    node = locateLeafWithKeyPM(_root, entry, ordering, locks, lockNode, true);
    /*
     * Duplicate key check
     */
    if (!dupsAllowed) {
        for (i = 0; i < node->num_keys; i++) {
            cmp = entry.key.woCompare(node->keys[i].getBSON(),
                                          ordering, false);
            if (cmp == 0) {
                if (node->keys[i].loc != entry.loc.repr()) {
                    StringBuilder sb;
                    sb << "Duplicate key error ";
                    sb << "dup key: " << entry.key.toString();
                    unlockTree(locks);
                    return Status(ErrorCodes::DuplicateKey, sb.str());
                } else {
                    break;
                }
            }
        }
    }

    /*
     * There is place for new value
     */
    if (node->num_keys < (TREE_ORDER)) {
        try {
            transaction::exec_tx(pop, [this, &status, &node, &entry, ordering] {
                status = insertKeyIntoLeaf(node, entry, ordering);
            });
        } catch (std::exception &e) {
            log() << "Index: " << e.what();
            if (lockNode) {
               lockNode->_pmutex.unlock();
            }
            return Status(ErrorCodes::CommandFailed, e.what());
        }
        unlockTree(locks);
        if (lockNode) {
           lockNode->_pmutex.unlock();
       }
        return status;
    }

    /*
     * splitting
     */
    try {
        transaction::exec_tx(pop, [this, pop, &node, &entry, ordering, &locks] {
            _root = splitFullNodeAndInsert(pop, node, entry, ordering, locks);
        });
    } catch (std::exception &e) {
        log() << "Index: " << e.what();
        if (lockNode) {
           lockNode->_pmutex.unlock();
        }
        return Status(ErrorCodes::CommandFailed, e.what());
    }
    if (lockNode) {
       lockNode->_pmutex.unlock();
    }
    unlockTree(locks);
    return Status::OK();
}

uint64_t PmseTree::countElements() {
    if (!isEmpty()) {
        auto leaf = _first;
        uint64_t counter = 0;
        while (leaf) {
            counter += leaf->num_keys;
            leaf = leaf->next;
        }
        return counter;
    }
    return 0;
}

bool PmseTree::isEmpty() {
    return _first == nullptr;
}

}  // namespace mongo
