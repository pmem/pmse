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

#include "pmse_tree.h"
#include "pmse_sorted_data_interface.h"

namespace mongo {

void PmseTree::remove(pool_base pop, BSONObj& key, const RecordId& loc,
                      bool dupsAllowed, const BSONObj& ordering) {

    persistent_ptr<PmseTreeNode> node;
    RecordId key_record;
    uint64_t recordIndex;
    uint64_t i;
    int64_t cmp;
    _ordering = ordering;

    if (!root) {
        return;
    }

    //find node with key
    node = locateLeafWithKey(key, _ordering);
    //find place in node
    for (i = 0; i < node->num_keys; i++) {
        key_record = node->values_array[i];
        cmp = key.woCompare(node->keys[i].getBSON(), _ordering, false);
        if (cmp == 0) {
            key_record = node->values_array[i];
            recordIndex = i;
            if (dupsAllowed) {
                if (key_record.repr() != loc.repr()) {
                    while ((key.woCompare(node->keys[i].getBSON(), _ordering,
                                    false) == 0)
                                    && (node->values_array[i]).repr()
                                                    != loc.repr()) {
                        if (i > 0) {
                            i--;
                        } else {
                            if (node->previous) {
                                node = node->previous;
                                i = node->num_keys - 1;
                            } else {
                                return;
                            }
                        }
                    }
                }
            }
            /*
             * This should be removed
             */
            key_record = node->values_array[i];
            break;
        }
    }

    /*
     * Remove value
     */

    root = deleteEntry(pop, key, node, i);
}

persistent_ptr<PmseTreeNode> PmseTree::deleteEntry(
                pool_base pop, BSONObj& key, persistent_ptr<PmseTreeNode> node,
                uint64_t index) {
    uint64_t min_keys;
    int64_t neighbor_index;
    int64_t k_prime_index;
    BSONObj_PM k_prime;
    uint64_t capacity;
    persistent_ptr<PmseTreeNode> neighbor;

    // Remove key and pointer from node.

    node = removeEntryFromNode(key, node, index);
    modified = true;

    if (node == root) {
        return adjustRoot(root);
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
        return root;

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
        return coalesceNodes(pop, root, node, neighbor, neighbor_index, k_prime);

    else
        return redistributeNodes(pop, root, node, neighbor, neighbor_index,
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
                int64_t k_prime_index, BSONObj_PM k_prime) {

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
                n->values_array[i] = n->values_array[i - 1];
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
        } else {
            n->values_array[0] = neighbor->values_array[neighbor->num_keys - 1];
            n->keys[0] = neighbor->keys[neighbor->num_keys - 1];

            BSONObj_PM bsonPM;
            persistent_ptr<char> obj;
            transaction::exec_tx(pop,
                            [&] {
                                obj = pmemobj_tx_alloc(n->keys[0].getBSON().objsize(), 1);
                                memcpy( (void*)obj.get(), n->keys[0].getBSON().objdata(), n->keys[0].getBSON().objsize());
                            });

            bsonPM = (n->parent->keys[k_prime_index]);
            if (bsonPM.data.raw().off != 0)
                pmemobj_tx_free(bsonPM.data.raw());

            bsonPM.data = obj;
            n->parent->keys[k_prime_index].data = bsonPM.data;

        }
    }

    /* Case: n is the leftmost child.
     * Take a key-pointer pair from the neighbor to the right.
     * Move the neighbor's leftmost key-pointer pair
     * to n's rightmost position.
     */

    else {
        if (n->is_leaf) {
            n->keys[n->num_keys] = neighbor->keys[0];
            n->values_array[n->num_keys] = neighbor->values_array[0];

            BSONObj_PM bsonPM;
            persistent_ptr<char> obj;
            transaction::exec_tx(pop,
                            [&] {
                                obj = pmemobj_tx_alloc(neighbor->keys[1].getBSON().objsize(), 1);
                                memcpy( (void*)obj.get(), neighbor->keys[1].getBSON().objdata(), neighbor->keys[1].getBSON().objsize());
                            });

            bsonPM = (n->parent->keys[k_prime_index]);
            if (bsonPM.data.raw().off != 0)
                pmemobj_tx_free(bsonPM.data.raw());

            bsonPM.data = obj;

            n->parent->keys[k_prime_index].data = bsonPM.data;
        } else {
            n->keys[n->num_keys] = k_prime;
            n->children_array[n->num_keys + 1] = neighbor->children_array[0];
            tmp = n->children_array[n->num_keys + 1];
            tmp->parent = n;

            BSONObj_PM bsonPM;
            bsonPM = (n->parent->keys[k_prime_index]);

            n->parent->keys[k_prime_index].data = neighbor->keys[0].data;
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
                neighbor->values_array[i] = neighbor->values_array[i + 1];
            }
        }
    }

    /* n now has one more key and one more pointer;
     * the neighbor has one fewer of each.
     */

    n->num_keys++;
    neighbor->num_keys--;

    if (_cursor.node == neighbor) {
        _cursor.node = n;
        _cursor.index = 0;
    }

    return root;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
persistent_ptr<PmseTreeNode> PmseTree::coalesceNodes(
                pool_base pop, persistent_ptr<PmseTreeNode> root,
                persistent_ptr<PmseTreeNode> n,
                persistent_ptr<PmseTreeNode> neighbor, int64_t neighbor_index,
                BSONObj_PM k_prime) {
    uint64_t i, j, neighbor_insertion_index, n_end;
    persistent_ptr<PmseTreeNode> tmp;
    BSONObj k_prime_temp;

    /* Swap neighbor with node if node is on the
     * extreme left and neighbor is to its right.
     */

    if (neighbor_index == -1) {
        tmp = n;
        n = neighbor;
        neighbor = tmp;
    }

    /* Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that n and neighbor have swapped places
     * in the special case of n being a leftmost child.
     */

    neighbor_insertion_index = neighbor->num_keys;

    /* Case:  nonleaf node.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */

    if (!n->is_leaf) {

        /* Append k_prime.
         */
        BSONObj_PM bsonPM;
        persistent_ptr<char> obj;

        obj = pmemobj_tx_alloc(k_prime.getBSON().objsize(), 1);
        memcpy((void*) obj.get(), k_prime.getBSON().objdata(),
                        k_prime.getBSON().objsize());

        bsonPM.data = obj;
        neighbor->keys[neighbor_insertion_index].data = bsonPM.data;
        neighbor->num_keys++;

        n_end = n->num_keys;

        for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
            neighbor->keys[i] = n->keys[j];
            neighbor->children_array[i] = n->children_array[j];
            neighbor->num_keys++;
            n->num_keys--;
        }

        /* The number of pointers is always
         * one more than the number of keys.
         */

        neighbor->children_array[i] = n->children_array[j];

        /* All children must now point up to the same parent.
         */

        for (i = 0; i < neighbor->num_keys + 1; i++) {
            tmp = neighbor->children_array[i];
            tmp->parent = neighbor;
        }
    }

    /* In a leaf, append the keys and pointers of
     * n to the neighbor.
     * Set the neighbor's last pointer to point to
     * what had been n's right neighbor.
     */

    else {

        for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++) {
            neighbor->keys[i] = n->keys[j];
            neighbor->values_array[i] = n->values_array[j];
            neighbor->num_keys++;
        }
        if (n->next) {
            n->next->previous = neighbor;
        }
        neighbor->next = n->next;
    }

    k_prime_temp = k_prime.getBSON();
    for (i = 0; i < n->parent->num_keys; i++) {
        int cmp = k_prime_temp.woCompare(n->parent->keys[i].getBSON(),
                        _ordering, false);
        if (cmp == 0) {
            break;
        }
    }

    if (_cursor.node == n) {
        _cursor.node = neighbor;
        _cursor.index = 0;
    }

    root = deleteEntry(pop, k_prime_temp, n->parent, i);

    delete_persistent<BSONObj_PM[TREE_ORDER]>(n->keys);
    if (n->is_leaf) {
        delete_persistent<RecordId[TREE_ORDER]>(n->values_array);
    }
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

    uint64_t i;

    /* Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.
     * If n is the leftmost child, this means
     * return -1.
     */
    for (i = 0; i <= node->parent->num_keys; i++)
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
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.

    else {
        new_root = nullptr;
    }

    delete_persistent<BSONObj_PM[TREE_ORDER]>(root->keys);
    delete_persistent<RecordId[TREE_ORDER]>(root->values_array);

    delete_persistent<PmseTreeNode>(root);
    return new_root;

}

persistent_ptr<PmseTreeNode> PmseTree::removeEntryFromNode(
                BSONObj& key, persistent_ptr<PmseTreeNode> node,
                uint64_t index) {
    uint64_t i, num_pointers;
    // Remove the key and shift other keys accordingly.
    i = index;

    BSONObj_PM bsonPM;
    bsonPM = (node->keys[i]);
    pmemobj_tx_free(bsonPM.data.raw());

    i = index;
    for (++i; i < node->num_keys; i++) {
        node->keys[i - 1] = node->keys[i];

    }
    // Remove the pointer and shift other pointers accordingly.
    i = index;
    if (node->is_leaf) {
        num_pointers = node->num_keys;
        for (++i; i < num_pointers; i++) {
            node->values_array[i - 1] = node->values_array[i];
        }
    } else {
        num_pointers = node->num_keys + 1;
        i++;
        for (++i; i < num_pointers; i++) {
            node->children_array[i - 1] = node->children_array[i];
        }
    }

    // Set the other pointers to NULL for tidiness.
    if (!node->is_leaf)
        for (i = node->num_keys + 1; i < TREE_ORDER; i++)
            node->children_array[i] = nullptr;

    node->num_keys--;

    return node;
}

persistent_ptr<PmseTreeNode> PmseTree::locateLeafWithKey(
                BSONObj& key, const BSONObj& _ordering) {
    uint64_t i;
    int64_t cmp;
    bool wasEqual = false;
    auto current = root;

    if (current == nullptr)
        return current;
    while (!current->is_leaf) {
        i = 0;
        while (i < current->num_keys) {

            cmp = key.woCompare(current->keys[i].getBSON(), _ordering, false);
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

persistent_ptr<PmseTreeNode> PmseTree::makeTreeRoot(BSONObj_PM& key,
                                                    const RecordId& loc) {
    auto n = make_persistent<PmseTreeNode>(true);
    (n->keys[0]).data = key.data;
    n->values_array[0] = loc;
    n->num_keys = n->num_keys + 1;
    n->next = nullptr;
    n->previous = nullptr;
    n->parent = nullptr;
    return n;
}

persistent_ptr<PmseTreeNode> PmseTree::locateLeafWithKeyPM(
                BSONObj_PM& key, const BSONObj& _ordering) {
    uint64_t i;
    int64_t cmp;
    bool wasEqual = false;
    auto current = root;

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

    node->keys[insertion_point].data = key.data;
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
                pool_base pop, persistent_ptr<PmseTreeNode> node,
                BSONObj_PM& key, const RecordId& loc,
                const BSONObj& _ordering) {
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
                                    _ordering, false) > 0) {
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
    temp_keys_array[insertion_index].data = key.data;
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
    if (node->next)
        node->next->previous = new_leaf;
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

    BSONObj_PM bsonPM;

    bsonPM = old_node->keys[split - 1];
    if (bsonPM.data.raw().off != 0)
        pmemobj_tx_free(bsonPM.data.raw());

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
                persistent_ptr<PmseTreeNode> left, BSONObj_PM& key,
                persistent_ptr<PmseTreeNode> right) {
    persistent_ptr<PmseTreeNode> parent;
    BSONObj_PM newKey;

    uint64_t left_index;
    parent = left->parent;

    persistent_ptr<char> obj;

    transaction::exec_tx(pop,
                    [&] {
                        obj = pmemobj_tx_alloc(key.getBSON().objsize(), 1);
                        memcpy( (void*)obj.get(), key.getBSON().objdata(), key.getBSON().objsize());

                    });

    newKey.data = obj;

    /*
     * Check if parent exist. If not, create new one.
     */
    if (parent == nullptr)
        return allocateNewRoot(pop, left, newKey, right);

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
        return insertKeyIntoNode(pop, root, parent, left_index, newKey, right);
    }
    /*
     * There is no slot for new key - we need to split
     */
    return insertToNodeAfterSplit(pop, root, parent, left_index, newKey, right);
}
/*
 * Allocate node for new root and fill it with key.
 */
persistent_ptr<PmseTreeNode> PmseTree::allocateNewRoot(
                pool_base pop, persistent_ptr<PmseTreeNode> left,
                BSONObj_PM& new_key, persistent_ptr<PmseTreeNode> right) {
    auto new_root = make_persistent<PmseTreeNode>(false);

    new_root->keys[0].data = new_key.data;
    new_root->children_array[0] = left;
    new_root->children_array[1] = right;
    new_root->num_keys = new_root->num_keys + 1;
    new_root->parent = nullptr;
    left->parent = new_root;
    right->parent = new_root;

    return new_root;
}

Status PmseTree::insert(pool_base pop, BSONObj_PM& key, const RecordId& loc,
                        const BSONObj& _ordering, bool dupsAllowed) {

    persistent_ptr<PmseTreeNode> node;
    Status status = Status::OK();

    if (!root)   //root not allocated yet
    {
        try {
            transaction::exec_tx(pop, [&] {
                root = makeTreeRoot(key,loc);
                first = root;
                last = root;
            });
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
        }
        return Status::OK();
    }
    node = locateLeafWithKeyPM(key, _ordering);
    /*
     * There is place for new value
     */
    if (node->num_keys < (TREE_ORDER)) {
        try {
            transaction::exec_tx(pop, [&] {
                status = insertKeyIntoLeaf(node,key,loc,_ordering);
            });
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
        }
        return status;
    }

    /*
     * splitting
     */
    try {
        transaction::exec_tx(pop, [&] {
            root = splitFullNodeAndInsert(pop,node,key,loc,_ordering);
        });
    } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
    }
    return Status::OK();
}

}
