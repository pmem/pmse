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

#include "pmse_index_cursor.h"

#include "mongo/util/log.h"

namespace mongo {

enum BehaviorIfFieldIsEqual {
    normal = '\0',
    less = 'l',
    greater = 'g',
};

PmseCursor::PmseCursor(OperationContext* txn, bool isForward,
                       persistent_ptr<PmseTree> tree, const BSONObj& ordering,
                       const bool unique) :
                                _forward(isForward),
                                _ordering(ordering),
                                _first(tree->first),
                                _last(tree->last),
                                _unique(unique),
                                _tree(tree),
                                _inf(0) {
    cursorType = EOO;

    _endPosition = 0;
    _wasMoved = false;
    _eofRestore = false;

    BSONObjBuilder minBob;
    minBob.append("", -std::numeric_limits<double>::infinity());
    min = minBob.obj();

    BSONObjBuilder maxBob;
    maxBob.append("", std::numeric_limits<double>::infinity());
    max = maxBob.obj();
}

/*
 * Find leaf which may contain key that we are looking for
 */
persistent_ptr<PmseTreeNode> PmseCursor::find_leaf(
                persistent_ptr<PmseTreeNode> node, const BSONObj& key,
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
            cmp = key.woCompare(current->keys[i].getBSON(), _ordering, false);
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

void PmseCursor::setEndPosition(const BSONObj& key, bool inclusive) {
    uint64_t i;
    int cmp;

    if (!_tree->root) {
        return;
    }

    if (inclusive) {
        cursorType = key.firstElementType();
    } else {
        cursorType = EOO;
    }
    if (SimpleBSONObjComparator::kInstance.evaluate(key == max)) {

        // This means scan to end of index.
        _endPosition = nullptr;
        return;
    }

    if (SimpleBSONObjComparator::kInstance.evaluate(key == min)) {
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
    persistent_ptr<PmseTreeNode> node = find_leaf(_tree->root, key, _ordering);

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
                while (key.woCompare(node->keys[i].getBSON(), _ordering, false)
                                == 0) {

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

                while (key.woCompare(node->keys[i].getBSON(), _ordering, false)
                                == 0) {
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
                while (key.woCompare(node->keys[i].getBSON(), _ordering, false)
                                == 0) {
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

boost::optional<IndexKeyEntry> PmseCursor::next(
                RequestedInfo parts = kKeyAndLoc) {
    boost::optional<IndexKeyEntry> entry;
    /**
     * Find next correct value for cursor
     */
    entry = iterateToNext(parts);
    if(!entry.is_initialized())
    {
        return entry;
    }

    /**
     * Move to next and remember it
     */
    if(!_wasMoved)
    {
        moveToNext();
        _wasMoved = true;
    }
    if(_cursor.node.raw_ptr()->off != 0)
    {
        _cursorKey = _cursor.node->keys[_cursor.index].getBSON();
        _cursorId = _cursor.node->values_array[_cursor.index];
        //remember next value
    }
    else
    {
        _eofRestore = true;
    }
    return entry;
}

boost::optional<IndexKeyEntry> PmseCursor::iterateToNext(
                RequestedInfo parts = kKeyAndLoc) {
    /*
     * Advance cursor in leaves
     */

    persistent_ptr<PmseTreeNode> node;
    CursorObject _previousCursor;
    uint64_t i;
    int64_t cmp;
    bool found = false;
    bool foundKey = false;

    if (!_tree->root)
    {
        return boost::none;
    }
    if (_cursor.node == nullptr)
    {
        return boost::none;
    }
    if (_eofRestore)
    {
        _eofRestore= false;
        return boost::none;
    }

    /**
     * Restore location of cursor after tree modification.
     */
    if(_wasRestore)
    {
        node = find_leaf(_tree->root, _cursorKey, _ordering);
        for (i = 0; i < node->num_keys; i++) {
            cmp = _cursorKey.woCompare(node->keys[i].getBSON(), _ordering, false);
            if (cmp == 0)
            {
                foundKey = true;
                if( _cursorId == node->values_array[i])
                {
                    found = true;
                    break;
                }
            }
            if(cmp<0)   //stop iterating when found bigger
            {
                break;
            }
        }
        if(found)
        {
            _cursor.node = node;
            _cursor.index = i;
            found = false;
        }
        else
        {
            if(foundKey) //key was found, but with wrong ID - check previous
            {
                _previousCursor.node = node;
                _previousCursor.index = i;
                while(previous(_previousCursor))
                {
                    cmp = _cursorKey.woCompare(_previousCursor.node->keys[_previousCursor.index].getBSON(), _ordering, false);
                    if(cmp==0 && _cursorId==_previousCursor.node->values_array[_previousCursor.index])
                    {
                        _cursor.node = _previousCursor.node;
                        _cursor.index = _previousCursor.index;
                        break;
                    }
                }
            }
            else
            {
                _cursor.node = node;
                _cursor.index = i;
            }
        }
        _wasMoved = true;
        _wasRestore = false;
    }

    /**
     * Do step forward
     */
    if(!_wasMoved)
    {
        moveToNext();
    }
    _wasMoved = false;

    /**
     * Iterate position to next correct value if needed
     */
    do{
        if(_cursor.node)
        {
            if (_endPosition && (SimpleBSONObjComparator::kInstance.evaluate(
                                            _cursor.node->keys[_cursor.index].getBSON()
                                                            == _endPosition->getBSON())))
            {
                return boost::none;
            }
            if (correctType(_cursor.node->keys[_cursor.index].getBSON())){
                return IndexKeyEntry(
                            _cursor.node->keys[_cursor.index].getBSON(),
                            _cursor.node->values_array[_cursor.index]);
            }
            moveToNext();
        }
        else
        {
            return boost::none;
        }
    }
    while(true);

    return boost::none;
}

bool PmseCursor::previous(CursorObject& _cursor) {
    if (_cursor.index == 0) {
        /*
         * It is first element, move to prev node
         */
        if (_cursor.node->previous) {
            _cursor.node = _cursor.node->previous;
            _cursor.index = _cursor.node->num_keys - 1;
            return true;
        } else {
            /*
             * There are no more prev
             */
            return false;
        }
    } else {
        _cursor.index = _cursor.index - 1;
        return true;
    }

}

/*
 * Check if types of two BSONObjs are comparable
 */
bool PmseCursor::correctType(BSONObj record) {
    bool result = false;

    BSONType typeRecord = record.firstElementType();

    if (cursorType == typeRecord)
        return true;

    if (cursorType == MinKey || cursorType == MaxKey || cursorType == Undefined
                    || cursorType == EOO || cursorType == jstNULL) {
        return true;
    }
    switch (cursorType) {
    case NumberDouble:
    case NumberInt:
    case NumberLong:
    case NumberDecimal:
    case Array:
        if ((typeRecord == NumberDouble) || (typeRecord == NumberInt)
                        || (typeRecord == NumberLong)
                        || (typeRecord == NumberDecimal) || (typeRecord == Array))
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
        log() << "not supported";
    }
    return result;
}

void PmseCursor::moveToNext() {
    if (_forward) {
        /*
         * There are next keys - increment index
         */
        if (_cursor.index < (_cursor.node->num_keys - 1)) {
            _cursor.index++;
        } else {
            /*
             * Move to next node - if it exist
             */
            if (_cursor.node->next != nullptr) {

                _cursor.node = _cursor.node->next;
                _cursor.index = 0;
            } else {
                _cursor.node = nullptr;
            }
        }
    } else {
        /*
         * There are next keys - increment index
         */
        if (_cursor.index > 0) {
            _cursor.index--;
        } else {
            /*
             * Move to prev node - if it exist
             */
            if (_cursor.node->previous != nullptr) {

                _cursor.node = _cursor.node->previous;
                _cursor.index = _cursor.node->num_keys - 1;
            } else {
                _cursor.node = nullptr;
            }
        }
    }
}

boost::optional<IndexKeyEntry> PmseCursor::seek(
                const BSONObj& key, bool inclusive, RequestedInfo parts =
                                kKeyAndLoc) {
    boost::optional<IndexKeyEntry> entry;
    const auto discriminator = inclusive ? KeyString::kInclusive : KeyString::kExclusiveBefore;
    /**
     * Remember current search result to return it
     */
    entry = seekInTree(key, discriminator, parts);
    /**
     * Remember next value to find it on next in case of deletion
     */
    if(_cursor.node.raw_ptr()->off != 0)
    {
        moveToNext();
        _wasMoved = true;
    }
    if(_cursor.node.raw_ptr()->off != 0) {
        _cursorKey = _cursor.node->keys[_cursor.index].getBSON();
        _cursorId = _cursor.node->values_array[_cursor.index];
        //remember next value
    }
    else {
        _eofRestore = true;
    }
    return entry;
}


boost::optional<IndexKeyEntry> PmseCursor::seekInTree(
                const BSONObj& key, KeyString::Discriminator discriminator, RequestedInfo parts =
                                kKeyAndLoc) {
    CursorObject _previousCursor;
    uint64_t i = 0;
    int cmp;

    _returnValue = {};

    if (!_tree->root) {
        return boost::none;
    }

    if (cursorType == EOO) {
        cursorType = key.firstElementType();
    }

    persistent_ptr<PmseTreeNode> node;

    if (SimpleBSONObjComparator::kInstance.evaluate(key == min)) {
        _cursor.node = _first;
        _cursor.index = 0;
        if (_endPosition
                        && SimpleBSONObjComparator::kInstance.evaluate(
                                        _cursor.node->keys[_cursor.index].getBSON()
                                                        == _endPosition->getBSON()))
            return boost::none;

        return IndexKeyEntry(
                        _cursor.node->keys[_cursor.index].getBSON(),
                        _cursor.node->values_array[_cursor.index]);
    }
    //only in backward
    if (SimpleBSONObjComparator::kInstance.evaluate(key == max)) {
        if (_endPosition && _inf == MAX_END && (discriminator != KeyString::kInclusive))
            return boost::none;

        _cursor.node = _last;
        _cursor.index = (_cursor.node)->num_keys - 1;
        if (_endPosition
                        && SimpleBSONObjComparator::kInstance.evaluate(
                                        _cursor.node->keys[_cursor.index].getBSON()
                                                        == _endPosition->getBSON()))
            return boost::none;

        return IndexKeyEntry(
                        _cursor.node->keys[_cursor.index].getBSON(),
                        _cursor.node->values_array[_cursor.index]);
    }

    node = find_leaf(_tree->root, key, _ordering);

    if (node == NULL)
        return boost::none;

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
            moveToNext();
            if (!_cursor.node) {
                return boost::none;
            }
        }
        if (_endPosition
                        && SimpleBSONObjComparator::kInstance.evaluate(
                                        _cursor.node->keys[_cursor.index].getBSON()
                                                        == _endPosition->getBSON())) {
            return boost::none;
        }
        else{
            return IndexKeyEntry(
                        _cursor.node->keys[_cursor.index].getBSON(),
                        _cursor.node->values_array[_cursor.index]);
        }
    }

    /*
     * Check if it is equal.
     * If it is not equal then return bigger one or empty key if it is bigger than end position
     * Check if next object has correct type. If not, go to next one.
     */
    if (cmp != 0) {
        _cursor.node = node;
        _cursor.index = i;

        if (_endPosition
                        && SimpleBSONObjComparator::kInstance.evaluate(
                                        node->keys[i].getBSON()
                                                        == _endPosition->getBSON())) {
            return boost::none;
        }

        /*
         * Check object type. If wrong return next.
         * For "Backward" direction return previous object because we are just after bigger one.
         */
        if (correctType(
                        _cursor.node->keys[_cursor.index].getBSON())) {
            if (_forward) {
                return IndexKeyEntry(
                                _cursor.node->keys[_cursor.index].getBSON(),
                                _cursor.node->values_array[_cursor.index]);
            } else {
                moveToNext();
                return IndexKeyEntry(
                        _cursor.node->keys[_cursor.index].getBSON(),
                        _cursor.node->values_array[_cursor.index]);
            }
        } else
        {
            moveToNext();
            return IndexKeyEntry(
                        _cursor.node->keys[_cursor.index].getBSON(),
                        _cursor.node->values_array[_cursor.index]);
        }
    }
    /*
     * So it is equal element
     */
    /*
     * If not inclusive - return next not-equal element (while)
     */
    if (discriminator != KeyString::kInclusive) {
        _cursor.node = node;
        _cursor.index = i;
        while (key.woCompare(
                        _cursor.node->keys[_cursor.index].getBSON(),
                        _ordering, false) == 0) {
            moveToNext();
            if (!_cursor.node) {
                return boost::none;
            }
        }
        return IndexKeyEntry(
                        _cursor.node->keys[_cursor.index].getBSON(),
                        _cursor.node->values_array[_cursor.index]);
    }

    /*
     * It is inclusive.
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
            previous(_previousCursor);
            /*
             * Get previous until are not equal
             */
            while (!key.woCompare(
                            _previousCursor.node->keys[_previousCursor.index].getBSON(),
                            _ordering, false)) {
                _cursor.node = _previousCursor.node;
                _cursor.index = _previousCursor.index;
                if (!previous(_previousCursor)) {
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
        return IndexKeyEntry(
                        _cursor.node->keys[_cursor.index].getBSON(),
                        _cursor.node->values_array[_cursor.index]);
    }

}

boost::optional<IndexKeyEntry> PmseCursor::seek(
                const IndexSeekPoint& seekPoint,
                RequestedInfo parts = kKeyAndLoc) {
    boost::optional<IndexKeyEntry> entry;
    BSONObj key = IndexEntryComparison::makeQueryObject(seekPoint, _forward);
    auto discriminator = KeyString::kInclusive;

    BSONObjIterator lhsIt(key);
    while (lhsIt.more())
    {
        const BSONElement l = lhsIt.next();
        BehaviorIfFieldIsEqual lEqBehavior = BehaviorIfFieldIsEqual(l.fieldName()[0]);
        if (lEqBehavior)
        {
            if(lEqBehavior == greater)
            {
                discriminator = KeyString::kExclusiveBefore;
            }
        }
    }
    entry = seekInTree(key, discriminator, parts);
    if(_cursor.node.raw_ptr()->off != 0)
    {
        moveToNext();
        _wasMoved = true;
    }
    if(_cursor.node.raw_ptr()->off != 0) {
        _cursorKey = _cursor.node->keys[_cursor.index].getBSON();
        _cursorId = _cursor.node->values_array[_cursor.index];
        //remember next value
    }
    else {
        _eofRestore = true;
    }
    return entry;
}

boost::optional<IndexKeyEntry> PmseCursor::seekExact(
                const BSONObj& key, RequestedInfo parts = kKeyAndLoc) {
    auto kv = seek(key, true, kKeyAndLoc);
    if (kv && kv->key.woCompare(key, BSONObj(), false) == 0)
        return kv;
    return boost::none;
}

void PmseCursor::save() {
}

void PmseCursor::saveUnpositioned() {
}

void PmseCursor::restore() {
    if(_eofRestore)
        return;

    _wasRestore=true;
}

void PmseCursor::detachFromOperationContext() {
}

void PmseCursor::reattachToOperationContext(OperationContext* opCtx) {
}
}
