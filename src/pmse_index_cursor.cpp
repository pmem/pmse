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

#include <shared_mutex>
#include <limits>

#include "mongo/util/log.h"

namespace mongo {

enum BehaviorIfFieldIsEqual {
    normal = '\0',
    less = 'l',
    greater = 'g',
};

//const BSONObj PmseCursor::min = BSON("" << -std::numeric_limits<double>::infinity());
//const BSONObj PmseCursor::max = BSON("" << std::numeric_limits<double>::infinity());
//const PmseCursor::IndexKeyEntry_PM min;
//const PmseCursor::IndexKeyEntry_PM max;

PmseCursor::PmseCursor(OperationContext* txn, bool isForward,
                       persistent_ptr<PmseTree> tree, const BSONObj& ordering,
                       const bool unique)
    : _forward(isForward),
      _ordering(ordering),
      _first(tree->_first),
      _last(tree->_last),
      _unique(unique),
      _tree(tree),
      cursorType(EOO),
      _inf(0),
      _endPositionIsDataEnd(false),
      _locateFoundDataEnd(false),
      _wasMoved(false),
      _eofRestore(false) {}



/*
 * Find leaf which may contain key that we are looking for
 */
persistent_ptr<PmseTreeNode> PmseCursor::find_leaf(
                persistent_ptr<PmseTreeNode> node, IndexKeyEntry& entry,
                const BSONObj& ordering) {
    uint64_t i;
    int64_t cmp;
//    bool wasEqual = false;
    persistent_ptr<PmseTreeNode> current = node;

    if (current == nullptr)
        return current;
    while (!current->is_leaf) {
        i = 0;
        while (i < current->num_keys) {
            cmp = IndexKeyEntry_PM::compareEntries(entry, current->keys[i], ordering);
            //cmp = key.woCompare(current->keys[i].getBSON(), _ordering, false);
            if (cmp > 0) {
                i++;
            } else {
                break;
            }
        }
        current = current->children_array[i];
    }

    return current;
}

//void PmseCursor::setEndPosition(const BSONObj& key, bool inclusive) {
//    uint64_t i;
//    int cmp;
//    std::shared_lock<nvml::obj::shared_mutex> lock(_tree->_pmutex);
//    if (!_tree->_root) {
//        return;
//    }
//
//    if (inclusive) {
//        cursorType = key.firstElementType();
//    } else {
//        cursorType = EOO;
//    }
//    if (SimpleBSONObjComparator::kInstance.evaluate(key == max)) {
//        // This means scan to end of index.
//        _endPosition = nullptr;
//        return;
//    }
//
//    if (SimpleBSONObjComparator::kInstance.evaluate(key == min)) {
//        // This means scan to end of index.
//        _endPosition = 0;
//        return;
//    }
//
//    if (key.firstElementType() == MaxKey) {
//        _endPosition = nullptr;
//        return;
//    }
//    /*
//     * Find leaf node where key may exist
//     */
//    persistent_ptr<PmseTreeNode> node = find_leaf(_tree->_root, key, _ordering);
//
//    if (node == nullptr) {
//        _endPosition = nullptr;
//        return;
//    }
//    /*
//     * Find place in leaf where key may exist
//     */
//
//    for (i = 0; i < node->num_keys; i++) {
//        cmp = key.woCompare(node->keys[i].getBSON(), _ordering, false);
//        if (cmp <= 0)
//            break;
//    }
//    if (!inclusive) {
//        if (_forward) {
//            if (i == node->num_keys) {
//                /*
//                 * Key is in next node. Move to next node.
//                 */
//                if (node->next) {
//                    node = node->next;
//                    _endPosition = &(node->keys[0]);
//                } else {
//                    _endPosition = nullptr;
//                }
//
//                return;
//            } else {
//                /*
//                 * Key is in this node
//                 */
//                _endPosition = &(node->keys[i]);
//                return;
//            }
//        } else {
//            if (cmp == 0) {  // find last element from many non-unique
//                while (key.woCompare(node->keys[i].getBSON(), _ordering, false) == 0) {
//                    _endPosition = &(node->keys[i]);
//                    /*
//                     * There are next keys - increment i
//                     */
//                    if (i < (node->num_keys - 1)) {
//                        i++;
//                    } else {
//                        /*
//                         * Move to next node - if it exist
//                         */
//                        if (node->next != nullptr) {
//                            node = node->next;
//                            i = 0;
//                        } else {
//                            _inf = MAX_END;
//                            return;
//                        }
//                    }
//                }
//            } else {
//                if (i == node->num_keys) {
//                    _endPosition = &(node->keys[i - 1]);
//
//                    return;
//                } else {
//                    /*
//                     * Key is in this node
//                     */
//                    _endPosition = &(node->keys[i]);
//                    return;
//                }
//            }
//            return;
//        }
//    } else {
//        if (_forward) {
//            /*
//             * Move forward until key is not equal to the looked one
//             */
//            if (cmp == 0) {
//                while (key.woCompare(node->keys[i].getBSON(), _ordering, false)
//                                == 0) {
//                    /*
//                     * There are next keys - increment i
//                     */
//                    if (i < (node->num_keys - 1)) {
//                        i++;
//                    } else {
//                        /*
//                         * Move to next node - if it exist
//                         */
//                        if (node->next != nullptr) {
//                            node = node->next;
//                            i = 0;
//                        } else {
//                            _endPosition = nullptr;
//                            return;
//                        }
//                    }
//                }
//            }
//
//            if (i == node->num_keys) {
//                if (node->next) {
//                    node = node->next;
//                    _endPosition = &(node->keys[0]);
//                } else {
//                    _endPosition = nullptr;
//                }
//                return;
//            } else {
//                _endPosition = &(node->keys[i]);
//                return;
//            }
//        } else {
//            // move backward till first element
//            if (cmp == 0) {
//                while (key.woCompare(node->keys[i].getBSON(), _ordering, false)
//                                == 0) {
//                    /*
//                     * There are previous keys - increment i
//                     */
//                    if (i > 0) {
//                        i--;
//                    } else {
//                        /*
//                         * Move to prev node - if it exist
//                         */
//                        if (node->previous != nullptr) {
//                            node = node->previous;
//                            i = node->num_keys - 1;
//                        } else {
//                            _inf = MIN_END;
//                            return;  // break;
//                        }
//                    }
//                }
//                _endPosition = &(node->keys[i]);
//            } else {
//                if (node->previous == nullptr) {
//                    _inf = MIN_END;
//                }
//            }
//            return;
//        }
//    }
//}


//Find entry in tree which is equal or bigger to input entry
//Locates input cursor on that entry
//Sets _locateFoundDataEnd when result is after last entry in tree
bool PmseCursor::lower_bound(IndexKeyEntry entry, CursorObject& cursor) {
    uint64_t i = 0;
    int64_t cmp;
    persistent_ptr<PmseTreeNode> current = _tree->_root;

    while (!current->is_leaf) {
        i = 0;
        while (i < current->num_keys) {
            cmp = IndexKeyEntry_PM::compareEntries(entry, current->keys[i], _ordering);
            std::cout <<"cmp="<<cmp <<std::endl;
            if (cmp > 0) {
                i++;
            } else {
                break;
            }
        }
        current = current->children_array[i];
    }
    for(i=0;i<current->num_keys;i++) {
        std::cout <<"lower_bound, found node,key["<<i<<"]= "<<( current->keys[i]).getBSON().toString() <<std::endl;

    }


    i=0;
    while (i < current->num_keys && IndexKeyEntry_PM::compareEntries(entry, current->keys[i], _ordering) > 0) {
        std::cout <<"leaf i="<<i<<" cmp="<<IndexKeyEntry_PM::compareEntries(entry, current->keys[i], _ordering)<<std::endl;
            i++;
    }
    std::cout <<"i="<<i << std::endl;
    //Iterated to end of node without finding bigger value
    //It means: return next
    if(i==current->num_keys) {
        if(current->next) {
            cursor.node = current->next;
            cursor.index = 0;
            return true;
        }
//            return &(current->next->keys[0]);

//        if(_forward)
        _locateFoundDataEnd = true;
//            _endPositionIsDataEnd = true;
//        else
//            _endPositionIsMin = true;
//        return boost::none;
        return false;
    }
    cursor.node = current;
    cursor.index = i;
    return true;
//    return &(current->keys[i]);
}

//int64_t PmseCursor::compareEntries(BSONObj& leftBSON, RecordId& leftLoc, IndexKeyEntry& rightEntry, const BSONObj& ordering){
//    int cmp;
//    cmp = leftBSON.woCompare(rightEntry.key, ordering, false);
//    if(cmp!=0)
//        return cmp;
//    //when entries keys are equal, compare RecordID
//    //std::cout <<"compare entries: equal keys, left="<<leftEntry.loc.repr()<< " right="<<rightEntry.loc <<std::endl;
//    if(leftLoc.repr()<rightEntry.loc.repr())
//        return -1;
//    else if(leftLoc.repr() > rightEntry.loc.repr())
//        return 1;
//    else return 0;
////    return leftEntry.loc.repr()-rightEntry.loc;
//}

bool PmseCursor::atOrPastEndPointAfterSeeking() {
    if (_isEOF)
        return true;
    if (!_endState)
        return false;

//    const int cmp = PmseCursor::compareEntries((_cursor.node->keys[_cursor.index]).getBSON(),RecordId((_cursor.node->keys[_cursor.index]).loc), _endState->query, _ordering);
    int cmp;
    cmp = (_cursor.node->keys[_cursor.index]).getBSON().woCompare(_endState->query.key,_ordering, false);
    if(cmp==0){
        if((_cursor.node->keys[_cursor.index]).loc<_endState->query.loc.repr())
            cmp = -1;
        else if((_cursor.node->keys[_cursor.index]).loc > _endState->query.loc.repr())
            cmp = 1;
        else cmp = 0;
    }
    if (_forward) {
        // We may have landed after the end point.
        return cmp > 0;
    } else {
        // We may have landed before the end point.
        return cmp < 0;
    }
}

void PmseCursor::locate(const BSONObj& key, const RecordId& loc) {
    bool locateFound;
    CursorObject locateCursor;
    _isEOF = false;
    const auto query = IndexKeyEntry(key, loc);
    locateFound = lower_bound(query, locateCursor);
    if (_forward) {
        std::cout <<"locate forward" << std::endl;
        if(_locateFoundDataEnd) {
            _locateFoundDataEnd = false;
            _isEOF = true;
        }
        if(locateFound){
            _cursor.node = locateCursor.node;
            _cursor.index = locateCursor.index;
        }
    }
    else {  //manage backward
        std::cout <<"locate backward" << std::endl;
        if(_locateFoundDataEnd) {
            _locateFoundDataEnd = false;
            std::cout <<"locate, cursor key["<<_last->num_keys-1<<"]= "<<( _last->keys[_last->num_keys-1]).getBSON().toString() <<std::endl;
            _cursor.node = _last;
            _cursor.index = _last->num_keys-1;
        }
        else {
            _cursor.node = locateCursor.node;
            _cursor.index = locateCursor.index;
            std::cout <<"locate, cursor key["<<_cursor.index<<"]= "<<( _cursor.node->keys[_cursor.index]).getBSON().toString() <<std::endl;
            int cmp;
                cmp = (_cursor.node->keys[_cursor.index]).getBSON().woCompare(query.key,_ordering, false);
                if(cmp==0){
                    if((_cursor.node->keys[_cursor.index]).loc<query.loc.repr())
                        cmp = -1;
                    else if((_cursor.node->keys[_cursor.index]).loc > query.loc.repr())
                        cmp = 1;
                    else cmp = 0;
                }
            if(cmp) {
                moveToNext();
            }
        }
//        if (_it == _data.end() || _data.value_comp().compare(*_it, query) > 0)
//            moveToNext();  // sets _isEOF if there is nothing more to return.
    }


    if (atOrPastEndPointAfterSeeking())
        _isEOF = true;


//    if(_locateFoundDataEnd){
//        _endPositionIsDataEnd = true;
//        _locateFoundDataEnd = false;
//    }
}

void PmseCursor::seekEndCursor() {
    CursorObject endCursor;
    bool found;

    if (!_endState || !_tree->_root)
        return;

    found = lower_bound(_endState->query, endCursor);
    if(_locateFoundDataEnd) {
        _endPositionIsDataEnd = true;
        _locateFoundDataEnd = false;
    }
    if (!_forward) {
        std::cout << "seekEndCursor, !forward, end cursor="<< endCursor.index << std::endl;
        if(_endPosition)
            std::cout <<"_endPos exist" <<std::endl;
        if(found)
        {
            std::cout <<"found" <<std::endl;
        }
        std::cout << "endptr="<< endCursor.node.raw_ptr()->off <<std::endl;
        std::cout << "_first="<< _first.raw_ptr()->off <<std::endl;

        // lower_bound lands us on or after query. Reverse cursors must be on or before.
        if( (endCursor.node == _first) && (endCursor.index == 0) ) {

            std::cout <<"node=first" <<std::endl;

            _endPositionIsDataEnd = true;
            return;
        }
        int cmp;
            cmp = (endCursor.node->keys[endCursor.index]).getBSON().woCompare(_endState->query.key,_ordering, false);
            if(cmp==0){
                if((endCursor.node->keys[endCursor.index]).loc<_endState->query.loc.repr())
                    cmp = -1;
                else if((endCursor.node->keys[endCursor.index]).loc > _endState->query.loc.repr())
                    cmp = 1;
                else cmp = 0;
            }
        if(cmp>0) {
            if (endCursor.index > 0) {
                endCursor.index--;
            } else {
                /*
                 * Move to prev node - if it exist
                 */
                if (endCursor.node->previous != nullptr) {
                    endCursor.node = endCursor.node->previous;
                    endCursor.index = endCursor.node->num_keys - 1;
                } else {
                    endCursor.node = nullptr;
                }
            }
        }

//                if (it == _data.end() || _data.value_comp().compare(*it, _endState->query) > 0) {
//                    if (it == _data.begin()) {
//                        it = _data.end();  // all existing data in range.
//                    } else {
//                        --it;
//                    }
//                }
    }

//            if (it != _data.end())
//                dassert(compareKeys(it->key, _endState->query.key) >= 0);
    if(found)
    {
        _endPosition = IndexKeyEntry(endCursor.node->keys[endCursor.index].getBSON(),RecordId(endCursor.node->keys[endCursor.index].loc));
//                _endPosition.get().key = endCursor.node->keys[endCursor.index].getBSON();
//                _endPosition.get().loc. = RecordId(endCursor.node->keys[endCursor.index].loc);
    }
}


void PmseCursor::setEndPosition(const BSONObj& key, bool inclusive){

//    int cmp = key.woCompare(max, _ordering, false);

    std::cout <<"setEndPosition key = "<< key.toString()<<" type="<<key.firstElementType()<<std::endl;// <<" cmp="<<cmp <<std::endl;
    std::cout <<"inclusive = " << inclusive << std::endl;
    if (key.isEmpty()) {
        // This means scan to end of index.
        _endState = boost::none;
        return;
    }

    // NOTE: this uses the opposite min/max rules as a normal seek because a forward
    // scan should land after the key if inclusive and before if exclusive.
    _endState = EndState(stripFieldNames(key),
                         _forward == inclusive ? RecordId::max() : RecordId::min());
    std::cout <<"setEndPosition key:rec = "<< _endState->query.loc.repr() <<std::endl;

    seekEndCursor();
    if(_endPosition)
        std::cout <<"end position = "<< _endPosition.get().key.toString()<< " loc="<<_endPosition.get().loc <<std::endl;

    if(_endPositionIsDataEnd)
        std::cout <<"end position = data end"<<std::endl;


}

bool PmseCursor::atEndPoint() {
//    return _endState && (_cursor.node->keys[_cursor.index]).getBSON() == _endState->query.key && (_cursor.node->keys[_cursor.index]).loc == _endState->query.loc;
//    return ((SimpleBSONObjComparator::kInstance.evaluate(
//                                                _cursor.node->keys[_cursor.index].getBSON() ==
//                                                _endPosition.getBSON())) && (_cursor.node->keys[_cursor.index]).loc == _endPosition.loc);
    if(_endPosition && (IndexKeyEntry_PM::compareEntries(_endPosition.get(),_cursor.node->keys[_cursor.index], _ordering) ==0))
        return true;
    return false;
    //TODO: Compare pointers or store endPosition value and compare it
//    return _cursor.node->keys[_cursor.index] == _endPosition;
}

// Advances once in the direction of the scan, updating _isEOF as needed.
// Does nothing if already _isEOF.
//void PmseCursor::advance() {
//    if (_isEOF)
//        return;
//    if (_forward) {
//        if (_it != _data.end())
//            ++_it;
//        if (_it == _data.end() || atEndPoint())
//            _isEOF = true;
//    } else {
//        if (_it == _data.begin() || _data.empty()) {
//            _isEOF = true;
//        } else {
//            --_it;
//        }
//        if (atEndPoint())
//            _isEOF = true;
//    }
//}

boost::optional<IndexKeyEntry> PmseCursor::next(
                RequestedInfo parts = kKeyAndLoc) {
//    boost::optional<IndexKeyEntry> entry;
//    std::shared_lock<nvml::obj::shared_mutex> lock(_tree->_pmutex);
//    /**
//     * Find next correct value for cursor
//     */
//    entry = iterateToNext(parts);
//    if (!entry.is_initialized()) {
//        return entry;
//    }
//
//    /**
//     * Move to next and remember it
//     */
//    if (!_wasMoved) {
//        moveToNext();
//        _wasMoved = true;
//    }
//    if (_cursor.node.raw_ptr()->off != 0) {
//        _cursorKey = _cursor.node->keys[_cursor.index].getBSON();
//        _cursorId = RecordId(_cursor.node->keys[_cursor.index].loc);
//        // remember next value
//    } else {
//        _eofRestore = true;
//    }
//    return entry;

//    if (_lastMoveWasRestore) {
//        // Return current position rather than advancing.
//        _lastMoveWasRestore = false;
//    } else {
//        std::cout <<" next, before advance"<<*_it << std::endl;
        moveToNext();
        if(!_cursor.node)
            return boost::none;
        std::cout <<" next="<<_cursor.node->keys[_cursor.index].getBSON() << " loc="<<_cursor.node->keys[_cursor.index].loc << std::endl;
        if (atEndPoint())
            _isEOF = true;
//    }

    if (_isEOF)
        return {};

//    std::cout <<" next return="<<*_it << std::endl;
    return IndexKeyEntry((_cursor.node->keys[_cursor.index]).getBSON(),RecordId((_cursor.node->keys[_cursor.index]).loc));
}

boost::optional<IndexKeyEntry> PmseCursor::iterateToNext(RequestedInfo parts = kKeyAndLoc) {
    /*
     * Advance cursor in leaves
     */
    persistent_ptr<PmseTreeNode> node;
    CursorObject _previousCursor;
//    uint64_t i;
//    int64_t cmp;
//    bool found = false;
//    bool foundKey = false;

    if (!_tree->_root || _cursor.node == nullptr) {
        return boost::none;
    }
    if (_eofRestore) {
        _eofRestore = false;
        return boost::none;
    }

    /**
     * Restore location of cursor after tree modification.
     */
//    if (_wasRestore) {
//        node = find_leaf(_tree->_root, _cursorKey, _ordering);
//        for (i = 0; i < node->num_keys; i++) {
//            cmp = IndexKeyEntry_PM::compareEntries(_cursorKey, node->keys[i], _ordering);
//            //cmp = _cursorKey.woCompare(node->keys[i].getBSON(), _ordering, false);
//            if (cmp == 0) {
//                foundKey = true;
////                if (_cursorId == node->values_array[i]) {
////                    found = true;
////                    break;
////                }
//            }
//            if (cmp < 0) {
//                // stop iterating when found bigger
//                break;
//            }
//        }
//        if (found) {
//            _cursor.node = node;
//            _cursor.index = i;
//            found = false;
//        } else {
//            if (foundKey) {
//                // key was found, but with wrong ID - check previous
//                _previousCursor.node = node;
//                _previousCursor.index = i;
//                while (previous(_previousCursor)) {
//                    cmp = IndexKeyEntry_PM::compareEntries(_cursorKey, node->keys[i], _ordering);
//                    cmp = _cursorKey.woCompare(_previousCursor.node->keys[_previousCursor.index].getBSON(),
//                                               _ordering, false);
//                    if (cmp == 0 && _cursorId == _previousCursor.node->values_array[_previousCursor.index]) {
//                        _cursor.node = _previousCursor.node;
//                        _cursor.index = _previousCursor.index;
//                        break;
//                    }
//                }
//            } else {
//                _cursor.node = node;
//                _cursor.index = i;
//            }
//        }
//        _wasMoved = true;
//        _wasRestore = false;
//    }

    /**
     * Do step forward
     */
//    if (!_wasMoved) {
//        moveToNext();
//    }
//    _wasMoved = false;
//
//    /**
//     * Iterate position to next correct value if needed
//     */
//    do {
//        if (_cursor.node) {
//            if (_endPosition && (SimpleBSONObjComparator::kInstance.evaluate(
//                                            _cursor.node->keys[_cursor.index].getBSON() ==
//                                            _endPosition->getBSON()))) {
//                return boost::none;
//            }
//            if (correctType(_cursor.node->keys[_cursor.index].getBSON())) {
//                return IndexKeyEntry(
//                            _cursor.node->keys[_cursor.index].getBSON(),
//                            RecordId(_cursor.node->keys[_cursor.index].loc));
//            }
//            moveToNext();
//        } else {
//            return boost::none;
//        }
//    } while (true);
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
        case Array: {
            if ((typeRecord == NumberDouble) || (typeRecord == NumberInt) ||
                (typeRecord == NumberLong) ||
                (typeRecord == NumberDecimal) ||
                (typeRecord == Array))
                result = true;
            break;
        }
        case Object: {
            if (typeRecord == Object)
                result = true;
            break;
        }
        case String: {
            if (typeRecord == String)
                result = true;
            break;
        }
        case jstOID: {
            if (typeRecord == jstOID)
                result = true;
            break;
        }
        case Bool: {
            if (typeRecord == Bool)
                result = true;
            break;
        }
        case Date: {
            if (typeRecord == Date)
                result = true;
            break;
        }
        default: {
            log() << "not supported";
        }
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

boost::optional<IndexKeyEntry> PmseCursor::seek(const BSONObj& key,
                                                bool inclusive,
                                                RequestedInfo parts = kKeyAndLoc) {
//    boost::optional<IndexKeyEntry> entry;
    std::shared_lock<nvml::obj::shared_mutex> lock(_tree->_pmutex);
    std::cout <<"seek key = "<< key.toString()<<" type="<<key.firstElementType()<<std::endl;// <<" cmp="<<cmp <<std::endl;
    std::cout <<"inclusive = " << inclusive << std::endl;
    if (!_tree->_root)
        return {};
    if (key.isEmpty()) {

        if(inclusive){
            _cursor.node = _first;
            _cursor.index = 0;
        }
        else{
            _cursor.node = _last;
            _cursor.index = (_cursor.node)->num_keys - 1;
            return {};
        }
    }
    else {
        const BSONObj query = stripFieldNames(key);
        locate(query, _forward == inclusive ? RecordId::min() : RecordId::max());
        if(!_isEOF) {
            std::cout<<"seek found ="<< (_cursor.node->keys[_cursor.index]).getBSON().toString()  <<std::endl;
        }
        else
            std::cout<<"seek not found " <<std::endl;
//        _lastMoveWasRestore = false;
        if (_isEOF)
            return {};
//        dassert(inclusive ? compareKeys(_it->key, query) >= 0
//                          : compareKeys(_it->key, query) > 0);
    }
//        _it = inclusive ? _data.begin() : _data.end();
//        _isEOF = (_it == _data.end());
//        if (_isEOF) {
//            return {};
//        }

//    const auto discriminator = inclusive ? KeyString::kInclusive : KeyString::kExclusiveBefore;
//
//    IndexKeyEntry seekEntry(key.getOwned(), RecordId(inclusive ? 0 : LLONG_MAX));
//    std::cout << "inclusive = " <<inclusive <<std::endl;
//    /**
//     * Remember current search result to return it
//     */
//    entry = seekInTree(seekEntry, discriminator, parts);
//    /**
//     * Remember next value to find it on next in case of deletion
//     */
//    if (_cursor.node.raw_ptr()->off != 0) {
//        moveToNext();
//        _wasMoved = true;
//    }
//    if (_cursor.node.raw_ptr()->off != 0) {
//        _cursorKey = _cursor.node->keys[_cursor.index].getBSON();
//        _cursorId = RecordId(_cursor.node->keys[_cursor.index].loc);
//        // remember next value
//    } else {
//        _eofRestore = true;
//    }
    return IndexKeyEntry((_cursor.node->keys[_cursor.index]).getBSON(),RecordId((_cursor.node->keys[_cursor.index]).loc));
}


//boost::optional<IndexKeyEntry> PmseCursor::seekInTree(
//                IndexKeyEntry& entry, KeyString::Discriminator discriminator, RequestedInfo parts =
//                                kKeyAndLoc) {
//    CursorObject _previousCursor;
//    uint64_t i = 0;
//    int cmp;
//
//    _returnValue = {};
//
//    if (!_tree->_root) {
//        return boost::none;
//    }
//
//    if (cursorType == EOO) {
//        cursorType = entry.key.firstElementType();
//    }
//
//    persistent_ptr<PmseTreeNode> node;
//
//    if (SimpleBSONObjComparator::kInstance.evaluate(entry.key == min)) {
//        _cursor.node = _first;
//        _cursor.index = 0;
//        if (_endPosition
//                        && SimpleBSONObjComparator::kInstance.evaluate(
//                                        _cursor.node->keys[_cursor.index].getBSON()
//                                                        == _endPosition->getBSON()))
//            return boost::none;
//
//        return IndexKeyEntry(
//                        _cursor.node->keys[_cursor.index].getBSON(),
//                        RecordId(_cursor.node->keys[_cursor.index].loc));
//    }
//    // only in backward
//    if (SimpleBSONObjComparator::kInstance.evaluate(entry.key == max)) {
//        if (_endPosition && _inf == MAX_END && (discriminator != KeyString::kInclusive))
//            return boost::none;
//
//        _cursor.node = _last;
//        _cursor.index = (_cursor.node)->num_keys - 1;
//        if (_endPosition && SimpleBSONObjComparator::kInstance.evaluate(_cursor.node->keys[_cursor.index].getBSON() ==
//                                                                        _endPosition->getBSON())) {
//            return boost::none;
//        }
//        return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
//                            RecordId(_cursor.node->keys[_cursor.index].loc));
//    }
//
//    node = find_leaf(_tree->_root, entry, _ordering);
//
//    if (node == NULL)
//        return boost::none;
//
//    /*
//     * Check if in current node exist value that is equal or bigger than input key
//     */
//    for (i = 0; i < node->num_keys; i++) {
//        //cmp = key.woCompare(node->keys[i].getBSON(), _ordering, false);
//        cmp = IndexKeyEntry_PM::compareEntries(entry, node->keys[i], _ordering);
//        if (cmp <= 0) {
//            break;
//        }
//    }
//    /*
//     * Check if bigger or equal element was found : i will be > than num_keys
//     * If element was not found: return the last one
//     */
//    if (i == node->num_keys) {
//        _cursor.node = node;
//        _cursor.index = i - 1;
//        if (_forward) {
//            moveToNext();
//            if (!_cursor.node) {
//                return boost::none;
//            }
//        }
//        if (_endPosition && SimpleBSONObjComparator::kInstance.evaluate(
//                                        _cursor.node->keys[_cursor.index].getBSON()
//                                                        == _endPosition->getBSON())) {
//            return boost::none;
//        } else {
//            return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
//                                RecordId(_cursor.node->keys[_cursor.index].loc));
//        }
//    }
//
//    /*
//     * Check if it is equal.
//     * If it is not equal then return bigger one or empty key if it is bigger than end position
//     * Check if next object has correct type. If not, go to next one.
//     */
//    if (cmp != 0) {
//        _cursor.node = node;
//        _cursor.index = i;
//
//        if (_endPosition && SimpleBSONObjComparator::kInstance.evaluate(node->keys[i].getBSON() ==
//                                                                        _endPosition->getBSON())) {
//            return boost::none;
//        }
//        /*
//         * Check object type. If wrong return next.
//         * For "Backward" direction return previous object because we are just after bigger one.
//         */
//        if (correctType(_cursor.node->keys[_cursor.index].getBSON())) {
//            if (_forward) {
//                return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
//                                RecordId(_cursor.node->keys[_cursor.index].loc));
//            } else {
//                moveToNext();
//                return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
//                                RecordId(_cursor.node->keys[_cursor.index].loc));
//            }
//        } else {
//            moveToNext();
//            return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
//                            RecordId(_cursor.node->keys[_cursor.index].loc));
//        }
//    }
//    /*
//     * So it is equal element
//     */
//    /*
//     * If not inclusive - return next not-equal element (while)
//     */
//    if (discriminator != KeyString::kInclusive) {
//        _cursor.node = node;
//        _cursor.index = i;
//
//        while (IndexKeyEntry_PM::compareEntries(entry, _cursor.node->keys[_cursor.index], _ordering) == 0) {
//            moveToNext();
//            if (!_cursor.node) {
//                return boost::none;
//            }
//        }
//        return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
//                        RecordId(_cursor.node->keys[_cursor.index].loc));
//    }
//
//    /*
//     * It is inclusive.
//     * Check if is first element in this node
//     */
//    if (_forward) {
//        if (i != 0) {
//            /*
//             * It is not first element. Return it.
//             */
//            _cursor.node = node;
//            _cursor.index = i;
//            return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
//                            RecordId(_cursor.node->keys[_cursor.index].loc));
//        } else {
//            /*
//             * It is first element. We should check previous nodes (non-unique keys) - forward
//             */
//            _cursor.node = node;
//            _cursor.index = i;
//            _previousCursor.node = node;
//            _previousCursor.index = i;
//            previous(_previousCursor);
//            /*
//             * Get previous until are not equal
//             */
//
//            while (!IndexKeyEntry_PM::compareEntries(entry, _previousCursor.node->keys[_previousCursor.index], _ordering)) {
//                _cursor.node = _previousCursor.node;
//                _cursor.index = _previousCursor.index;
//                if (!previous(_previousCursor)) {
//                    /*
//                     * There are no more prev
//                     */
//                    break;
//                }
//            }
//            return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
//                            RecordId(_cursor.node->keys[_cursor.index].loc));
//        }
//    } else {
//
//        while (IndexKeyEntry_PM::compareEntries(entry, node->keys[i], _ordering) == 0) {
//            _cursor.node = node;
//            _cursor.index = i;
//            /*
//             * There are next keys - increment i
//             */
//            if (i < (node->num_keys - 1)) {
//                i++;
//            } else {
//                /*
//                 * Move to next node - if it exist
//                 */
//                if (node->next != nullptr) {
//                    node = node->next;
//                    i = 0;
//                }
//            }
//        }
//        return IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(),
//                        RecordId(_cursor.node->keys[_cursor.index].loc));
//    }
//}

boost::optional<IndexKeyEntry> PmseCursor::seek(const IndexSeekPoint& seekPoint,
                                                RequestedInfo parts = kKeyAndLoc) {
    boost::optional<IndexKeyEntry> entry;
    std::cout << "seek2" << std::endl;
    if (!_tree->_root)
        return {};
    const BSONObj query = IndexEntryComparison::makeQueryObject(seekPoint, _forward);
    locate(query, _forward ? RecordId::min() : RecordId::max());
//    _lastMoveWasRestore = false;
    if (_isEOF)
        return {};
//    std::shared_lock<nvml::obj::shared_mutex> lock(_tree->_pmutex);
//
//    BSONObj key = IndexEntryComparison::makeQueryObject(seekPoint, _forward);
//    auto discriminator = KeyString::kInclusive;
//
//    BSONObjIterator lhsIt(key);
//    while (lhsIt.more()) {
//        const BSONElement l = lhsIt.next();
//        BehaviorIfFieldIsEqual lEqBehavior = BehaviorIfFieldIsEqual(l.fieldName()[0]);
//        if (lEqBehavior) {
//            if (lEqBehavior == greater) {
//                discriminator = KeyString::kExclusiveBefore;
//            }
//        }
//    }
//    IndexKeyEntry seekEntry(key.getOwned(), RecordId(0));
//    entry = seekInTree(seekEntry, discriminator, parts);
//    if (_cursor.node.raw_ptr()->off != 0) {
//        moveToNext();
//        _wasMoved = true;
//    }
//    if (_cursor.node.raw_ptr()->off != 0) {
//        _cursorKey = _cursor.node->keys[_cursor.index].getBSON();
//        _cursorId = RecordId(_cursor.node->keys[_cursor.index].loc);
//        // remember next value
//    } else {
//        _eofRestore = true;
//    }
    return IndexKeyEntry((_cursor.node->keys[_cursor.index]).getBSON(),RecordId((_cursor.node->keys[_cursor.index]).loc));
}

boost::optional<IndexKeyEntry> PmseCursor::seekExact(
                const BSONObj& key, RequestedInfo parts = kKeyAndLoc) {
    auto kv = seek(key, true, kKeyAndLoc);
    if (kv && kv->key.woCompare(key, BSONObj(), false) == 0)
        return kv;
    return boost::none;
}

void PmseCursor::save() {}

void PmseCursor::saveUnpositioned() {}

void PmseCursor::restore() {
    if (_eofRestore)
        return;
    _wasRestore = true;
}

void PmseCursor::detachFromOperationContext() {}

void PmseCursor::reattachToOperationContext(OperationContext* opCtx) {}

}  // namespace mongo
