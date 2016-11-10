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

#include "pmse_sorted_data_interface.h"
#include "errno.h"
#include "libpmemobj++/transaction.hpp"

namespace mongo {

class PmseCursor final : public SortedDataInterface::Cursor {
public:
    PmseCursor(OperationContext* txn, bool isForward,
             persistent_ptr<PmseTree> tree, const BSONObj& ordering,
             const bool unique) :
                                    _forward(isForward),
                                    _ordering(ordering),
                                    _root(tree->root),
                                    _first(tree->first),
                                    _last(tree->last),
                                    _unique(unique),
                                    _inf(0) {
        cursorType = EOO;

        _endPosition = 0;

        BSONObjBuilder minBob;
        minBob.append("", -std::numeric_limits<double>::infinity());
        min = minBob.obj();

        BSONObjBuilder maxBob;
        maxBob.append("", std::numeric_limits<double>::infinity());
        max = maxBob.obj();
    };

    /*
     * Find leaf which may contain key that we are looking for
     */
    persistent_ptr<PmseTreeNode> find_leaf(persistent_ptr<PmseTreeNode> node,
                                      const BSONObj& key,
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
                cmp = key.woCompare(current->keys[i].getBSON(), _ordering,
                false);
                if (cmp > 0) {
                    i++;
                } else {
                    if (cmp == 0) {
                        /*
                         * Tricky: support for un-unique keys
                         * If first and second node are equal to looked key , it means that we should enter between them
                         */
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

    void setEndPosition(const BSONObj& key, bool inclusive) {
        uint64_t i;
        int cmp;

        if (inclusive) {
            cursorType = key.firstElementType();
        } else {
            cursorType = EOO;
        }
        if (key == max) {
            // This means scan to end of index.
            _endPosition = nullptr;
            return;
        }

        if (key == min) {
            // This means scan to end of index.
            _endPosition = 0;
            return;
        }

        if (key.firstElementType() == MaxKey) {
            _endPosition = nullptr;
            return;
        }

        /*
         * Find leaf node where key may exist
         */
        persistent_ptr<PmseTreeNode> node = find_leaf(_root, key, _ordering);

        if (node == nullptr) {
            _endPosition = nullptr;
            return;
        }
        /*
         * Find place in leaf where key may exist
         */

        for (i = 0; i < node->num_keys; i++) {
            cmp = key.woCompare(node->keys[i].getBSON(), _ordering, false);
            if (cmp <= 0)
                break;
        }
        if (!inclusive) {
            if (_forward) {
                if (i == node->num_keys) {
                    /*
                     * Key is in next node. Move to next node.
                     */
                    if (node->next) {
                        node = node->next;
                        _endPosition = &(node->keys[0]);
                    } else {
                        _endPosition = nullptr;
                    }

                    return;
                } else {
                    /*
                     * Key is in this node
                     */
                    _endPosition = &(node->keys[i]);
                    return;
                }
            }            //if(_forward)
            else {
                if (cmp == 0) { //find last element from many non-unique
                    while (key.woCompare(node->keys[i].getBSON(), _ordering,
                    false) == 0) {

                        _endPosition = &(node->keys[i]);
                        /*
                         * There are next keys - increment i
                         */
                        if (i < (node->num_keys - 1)) {
                            i++;
                        } else {
                            /*
                             * Move to next node - if it exist
                             */
                            if (node->next != nullptr) {

                                node = node->next;
                                i = 0;
                            } else {
                                _inf = MAX_END;
                                return;
                            }
                        }

                    }
                } else {
                    if (i == node->num_keys) {
                        _endPosition = &(node->keys[i - 1]);

                        return;
                    } else {
                        /*
                         * Key is in this node
                         */
                        _endPosition = &(node->keys[i]);
                        return;
                    }
                }
                return;
            }
        } else {

            if (_forward) {

                /*
                 * Move forward until key is not equal to the looked one
                 */
                if (cmp == 0) {

                    while (key.woCompare(node->keys[i].getBSON(), _ordering,
                    false) == 0) {
                        /*
                         * There are next keys - increment i
                         */
                        if (i < (node->num_keys - 1)) {
                            i++;
                        } else {
                            /*
                             * Move to next node - if it exist
                             */
                            if (node->next != nullptr) {

                                node = node->next;
                                i = 0;
                            } else {
                                _endPosition = nullptr;
                                return;
                            }
                        }

                    }
                }

                if (i == node->num_keys) {
                    if (node->next) {
                        node = node->next;
                        _endPosition = &(node->keys[0]);
                    } else {
                        _endPosition = nullptr;
                    }
                    return;
                } else {
                    _endPosition = &(node->keys[i]);
                    return;
                }
            } //if(_forward){
            else {
                //move backward till first element
                if (cmp == 0) {
                    while (key.woCompare(node->keys[i].getBSON(), _ordering,
                    false) == 0) {
                        /*
                         * There are previous keys - increment i
                         */
                        if (i > 0) {
                            i--;
                        } else {
                            /*
                             * Move to prev node - if it exist
                             */
                            if (node->previous != nullptr) {

                                node = node->previous;
                                i = node->num_keys - 1;
                            } else {
                                _inf = MIN_END;
                                return;
                                //break;
                            }
                        }
                    }
                    _endPosition = &(node->keys[i]);
                } else {
                    if (node->previous == nullptr) {
                         _inf = MIN_END;
                    }
                }

                return;
            }
        }
    }
    ;

    boost::optional<IndexKeyEntry> next(RequestedInfo parts = kKeyAndLoc) {
        /*
         * Advance cursor in leaves
         */

        /*
         * Forward
         */
        if (_forward) {
            /*
             * There are next keys - increment index
             */
            if (_cursor.index < (_cursor.node->num_keys - 1)) {
                _cursor.index++;

                if (_endPosition
                                && (_cursor.node->keys[_cursor.index].getBSON()
                                                == (*_endPosition).getBSON())) {
                    return {};
                }
                if (correctType(_cursor.node->keys[_cursor.index].getBSON()))
                    return IndexKeyEntry(
                                    _cursor.node->keys[_cursor.index].getBSON(),
                                    _cursor.node->values_array[_cursor.index]);
            } else {
                /*
                 * Move to next node - if it exist
                 */
                if (_cursor.node->next != nullptr) {

                    _cursor.node = _cursor.node->next;
                    _cursor.index = 0;
                    if (_endPosition
                                    && (_cursor.node->keys[_cursor.index].getBSON()
                                                    == (*_endPosition).getBSON())) {
                        return {};
                    }
                    if (correctType(
                                    _cursor.node->keys[_cursor.index].getBSON()))
                        return IndexKeyEntry(
                                        _cursor.node->keys[_cursor.index].getBSON(),
                                        _cursor.node->values_array[_cursor.index]);
                } else {
                    return {};
                }
            }
            return {};
        } //if(_forward)
        else {
            /*
             * There are next keys - increment index
             */
            if (_cursor.index > 0) {
                _cursor.index--;
                if (_endPosition
                                && (_cursor.node->keys[_cursor.index].getBSON()
                                                == (*_endPosition).getBSON())) {
                    return {};
                }
                if (correctType(_cursor.node->keys[_cursor.index].getBSON()))
                    return IndexKeyEntry(
                                    _cursor.node->keys[_cursor.index].getBSON(),
                                    _cursor.node->values_array[_cursor.index]);
            } else {
                /*
                 * Move to prev node - if it exist
                 */
                if (_cursor.node->previous != nullptr) {

                    _cursor.node = _cursor.node->previous;
                    _cursor.index = _cursor.node->num_keys - 1;
                    if (_endPosition
                                    && (_cursor.node->keys[_cursor.index].getBSON()
                                                    == (*_endPosition).getBSON())) {
                        return {};
                    }
                    if (correctType(
                                    _cursor.node->keys[_cursor.index].getBSON()))
                        return IndexKeyEntry(
                                        _cursor.node->keys[_cursor.index].getBSON(),
                                        _cursor.node->values_array[_cursor.index]);
                } else {
                    return {};
                }
            }
            return {};
        }
    }
    ;

    bool previous() {
        if (_previousCursor.index == 0) {
            /*
             * It is first element, move to prev node
             */
            if (_previousCursor.node->previous) {
                _previousCursor.node = _previousCursor.node->previous;
                _previousCursor.index = _previousCursor.node->num_keys - 1;
                return true;
            } else {
                /*
                 * There are no more prev
                 */
                return false;
            }
        } else {
            _previousCursor.index = _previousCursor.index - 1;
            return true;
        }

    }

    /*
     * Check if types of two BSONObjs are comparable
     */
    bool correctType(BSONObj record) {
        bool result = false;

        BSONType typeRecord = record.firstElementType();
        if (cursorType == typeRecord)
            return true;

        if (cursorType == MinKey || cursorType == MaxKey
                        || cursorType == Undefined) {
            return true;
        }
        switch (cursorType) {
        case NumberDouble:
        case NumberInt:
        case NumberLong:
        case NumberDecimal:
            if ((typeRecord == NumberDouble) || (typeRecord == NumberInt)
                            || (typeRecord == NumberLong)
                            || (typeRecord == NumberDecimal))
                result = true;
            break;
        case Object:
            if (typeRecord == Object)
                result = true;
            break;
        case String:
            if (typeRecord == String)
                result = true;
            break;
        case jstOID:
            if (typeRecord == jstOID)
                result = true;
            break;
        case Bool:
            if (typeRecord == Bool)
                result = true;
            break;
        case Date:
            if (typeRecord == Date)
                result = true;
            break;
        default:
            std::cout << "not supported ";
            std::cout << std::endl;
        }
        return result;
    }

    boost::optional<IndexKeyEntry> seek(const BSONObj& key,
                                        bool inclusive,
                                        RequestedInfo parts = kKeyAndLoc) {
        uint64_t i = 0;
        int cmp;
        if (cursorType == EOO) {
            cursorType = key.firstElementType();
        }


        persistent_ptr<PmseTreeNode> node;
        boost::optional<IndexKeyEntry> nextValue;

        if (key == min) {
            _cursor.node = _first;
            _cursor.index = 0;
            if (_endPosition
                            && (_cursor.node->keys[_cursor.index].getBSON()
                                            == (*_endPosition).getBSON()))
                return {};
            return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
                            _cursor.node->values_array[_cursor.index]);
        }
        //only in backward
        if (key == max) {
            if (_endPosition && _inf == MAX_END && !inclusive)
                return {};

            _cursor.node = _last;
            _cursor.index = (_cursor.node)->num_keys - 1;
            if (_endPosition
                            && (_cursor.node->keys[_cursor.index].getBSON()
                                            == (*_endPosition).getBSON()))
                return {};
            return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
                            _cursor.node->values_array[_cursor.index]);
        }

        node = find_leaf(_root, key, _ordering);

        if (node == NULL)
            return NULL;

        /*
         * Check if in current node exist value that is equal or bigger than input key
         */
        for (i = 0; i < node->num_keys; i++) {
            cmp = key.woCompare(node->keys[i].getBSON(), _ordering, false);
            if (cmp <= 0) {
                break;
            }
        }
        /*
         * Check if bigger or equal element was found : i will be > than num_keys
         * If element was not found: return the last one
         */
        if (i == node->num_keys) {
            _cursor.node = node;
            _cursor.index = i - 1;
            if (_forward) {
                nextValue = next(parts);
                if (!nextValue.is_initialized()) {
                    return {};
                }
            }
            return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
                            _cursor.node->values_array[_cursor.index]);
        }

        /*
         * Check if it is equal.
         * If it is not equal then return bigger one or empty key if it is bigger than end position
         * Check if next object has correct type. If not, go to next one.
         */
        if (cmp != 0) {
            _cursor.node = node;
            _cursor.index = i;

            if (_endPosition && (node->keys[i].getBSON() == (*_endPosition).getBSON())) {
                return {};
            }

            /*
             * Check object type. If wrong return next.
             * For "Backward" direction return previous object because we are just after bigger one.
             */
            if (correctType(_cursor.node->keys[_cursor.index].getBSON())) {
                if (_forward) {
                    return IndexKeyEntry(
                                    _cursor.node->keys[_cursor.index].getBSON(),
                                    _cursor.node->values_array[_cursor.index]);
                } else {
                    return next();
                }
            } else
                return next();

        }
        /*
         * So it is equal element
         */
        /*
         * If inclusive - return next not-equal element (while)
         */
        if (!inclusive) {
            _cursor.node = node;
            _cursor.index = i;
            while (key.woCompare(_cursor.node->keys[_cursor.index].getBSON(),
                            _ordering, false) == 0) {
                nextValue = next(parts);
                if (!nextValue.is_initialized()) {
                    return {};
                }
            }
            return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
                            _cursor.node->values_array[_cursor.index]);
        }

        /*
         * It is not inclusive.
         * Check if is first element in this node
         */
        if (_forward) {
            if (i != 0) {
                /*
                 * It is not first element. Return it.
                 */
                _cursor.node = node;
                _cursor.index = i;
                return IndexKeyEntry(
                                _cursor.node->keys[_cursor.index].getBSON(),
                                _cursor.node->values_array[_cursor.index]);
            } else {
                /*
                 * It is first element. We should check previous nodes (non-unique keys) - forward
                 */
                _cursor.node = node;
                _cursor.index = i;
                _previousCursor.node = node;
                _previousCursor.index = i;
                previous();
                /*
                 * Get previous until are not equal
                 */
                while (!key.woCompare(
                                _previousCursor.node->keys[_previousCursor.index].getBSON(),
                                _ordering, false)) {
                    _cursor.node = _previousCursor.node;
                    _cursor.index = _previousCursor.index;
                    if (!previous()) {
                        /*
                         * There are no more prev
                         */
                        break;
                    }

                }
                return IndexKeyEntry(
                                _cursor.node->keys[_cursor.index].getBSON(),
                                _cursor.node->values_array[_cursor.index]);

            }
        }                //if(_forward){
        else {
            while (key.woCompare(node->keys[i].getBSON(), _ordering, false) == 0) {
                _cursor.node = node;
                _cursor.index = i;
                /*
                 * There are next keys - increment i
                 */
                if (i < (node->num_keys - 1)) {
                    i++;
                } else {
                    /*
                     * Move to next node - if it exist
                     */
                    if (node->next != nullptr) {

                        node = node->next;
                        i = 0;
                    }
                }

            }
            return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
                            _cursor.node->values_array[_cursor.index]);
        }

    };

    boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
                                        RequestedInfo parts = kKeyAndLoc) {
        return {};
    };

    boost::optional<IndexKeyEntry> seekExact(const BSONObj& key,
                                             RequestedInfo parts = kKeyAndLoc) {
        auto kv = seek(key, true, kKeyAndLoc);
        if (kv && kv->key.woCompare(key, BSONObj(), /*considerFieldNames*/
        false) == 0)
            return kv;
        return {};
    };

    void save() {
    };

    void saveUnpositioned() {
    };

    void restore() {
    };

    void detachFromOperationContext() {
    };

    void reattachToOperationContext(OperationContext* opCtx) {
    };

private:
    const bool _forward;
    const BSONObj& _ordering;
    persistent_ptr<PmseTreeNode> _root;
    persistent_ptr<PmseTreeNode> _first;
    persistent_ptr<PmseTreeNode> _last;
    const bool _unique;
    BSONType cursorType;
    /*
     * Marks end position for seek and next. Set by setEndPosition().
     * */
    BSONObj_PM* _endPosition;
    uint64_t _inf;
    bool _isEOF = true;
    /*
     * Cursor used for iterating with next until "_endPosition"
     */

    struct CursorObject {
        persistent_ptr<PmseTreeNode> node;
        uint64_t index;
    };
    CursorObject _cursor;
    CursorObject _previousCursor;
    BSONObj min;
    BSONObj max;
    BSONObj_PM end_min_pm;
    BSONObj_PM end_max_pm;

};

PmseSortedDataInterface::PmseSortedDataInterface(
                StringData ident, const IndexDescriptor* desc, StringData dbpath) {
    filepath = dbpath;
    std::string filename = filepath.toString() + ident.toString();
    _desc = desc;

    if (access(filename.c_str(), F_OK) != 0) {
        pm_pool = pool<PmseTree>::create(filename.c_str(), "pmStore",
                        10 * PMEMOBJ_MIN_POOL, 0666);
    } else {
        pm_pool = pool<PmseTree>::open(filename.c_str(), "pmStore");

    }
    tree = pm_pool.get_root();
}


/*
 * Insert new (Key,RecordID) into Sorted Index into correct place. Placement must be chosen basing on key value.
 *
 */
Status PmseSortedDataInterface::insert(OperationContext* txn,
                                          const BSONObj& key,
                                          const RecordId& loc,
                                          bool dupsAllowed) {
    BSONObj owned = key.getOwned();
    BSONObj_PM bsonPM;

    persistent_ptr<char> obj;

    auto pop = pool_base(pm_pool);
    transaction::exec_tx(pop,
                    [&] {
                        obj = pmemobj_tx_alloc(owned.objsize(), 1);
                        memcpy( (void*)obj.get(), owned.objdata(), owned.objsize());

                    });
    bsonPM.data = obj;

    return tree->insert(pop, bsonPM, loc, _desc->keyPattern(), dupsAllowed);
}

/*
 * Remove given record from Sorted Index *
 */
void PmseSortedDataInterface::unindex(OperationContext* txn,
                                         const BSONObj& key,
                                         const RecordId& loc,
                                         bool dupsAllowed) {
    BSONObj owned = key.getOwned();
    tree->remove(pm_pool, owned, loc, dupsAllowed);

}

std::unique_ptr<SortedDataInterface::Cursor> PmseSortedDataInterface::newCursor(
                OperationContext* txn,
                bool isForward) const {
    return stdx::make_unique<PmseCursor>(txn, isForward, tree,
                    _desc->keyPattern(), _desc->unique());
}

class PMStoreSortedDataBuilderInterface : public SortedDataBuilderInterface {
    MONGO_DISALLOW_COPYING(PMStoreSortedDataBuilderInterface)
    ;

public:

    PMStoreSortedDataBuilderInterface(PmseSortedDataInterface* index,
                                      OperationContext* txn) :
                    _index(index), _txn(txn) {
    }

    virtual Status addKey(const BSONObj& key, const RecordId& loc) {
        return _index->insert(_txn, key, loc, true);
    }

    void commit(bool mayInterrupt) {
    }
private:
    PmseSortedDataInterface* _index;
    OperationContext* _txn;

};

SortedDataBuilderInterface* PmseSortedDataInterface::getBulkBuilder(
                OperationContext* txn,
                bool dupsAllowed) {
    return new PMStoreSortedDataBuilderInterface(this, txn);
}

}
