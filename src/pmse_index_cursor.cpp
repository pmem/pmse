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

#include "pmse_index_cursor.h"

#include <shared_mutex>
#include <limits>
#include <list>

#include "mongo/util/log.h"

namespace mongo {

PmseCursor::PmseCursor(OperationContext* txn, bool isForward,
                       persistent_ptr<PmseTree> tree, const BSONObj& ordering,
                       const bool unique)
    : _forward(isForward),
      _ordering(ordering),
      _first(tree->_first),
      _last(tree->_last),
      _tree(tree),
      _endPositionIsDataEnd(false),
      _locateFoundDataEnd(false),
      _eofRestore(false) {}

    // Find entry in tree which is equal or bigger to input entry
    // Locates input cursor on that entry
    // Sets _locateFoundDataEnd when result is after last entry in tree
bool PmseCursor::lower_bound(IndexKeyEntry entry, CursorObject& cursor, std::list<nvml::obj::shared_mutex*>& locks) {
    uint64_t i = 0;
    int64_t cmp;
    persistent_ptr<PmseTreeNode> current = _tree->_root;
    (current->_pmutex).lock_shared();
    persistent_ptr<PmseTreeNode> child;
    while (!current->is_leaf) {
        i = 0;
        while (i < current->num_keys) {
            IndexEntryComparison c(Ordering::make(_ordering));
            cmp = c.compare(entry, IndexKeyEntry(current->keys[i].getBSON(), RecordId(current->keys[i].loc)));
            if (cmp >= 0) {
                i++;
            } else {
                break;
            }
        }
        child = current->children_array[i];
        child->_pmutex.lock_shared();
        current->_pmutex.unlock_shared();
        current = child;
    }
    locks.push_back(&(current->_pmutex));
    i = 0;
    IndexEntryComparison c(Ordering::make(_ordering));

    while (i < current->num_keys && c.compare(entry, IndexKeyEntry(current->keys[i].getBSON(), RecordId(current->keys[i].loc))) > 0) {
            i++;
    }
    // Iterated to end of node without finding bigger value
    // It means: return next
    if (i == current->num_keys) {
        if (current->next) {
            cursor.node = current->next;
            cursor.index = 0;
            return true;
        }
        else{
            _locateFoundDataEnd = true;
            return false;
        }
    }
    cursor.node = current;
    cursor.index = i;
    return true;
}

bool PmseCursor::atOrPastEndPointAfterSeeking() {
    if (_isEOF)
        return true;
    if (!_endState)
        return false;
    int cmp;
    cmp = (_cursor.node->keys[_cursor.index]).getBSON().woCompare(_endState->query.key, _ordering, false);
    if (cmp == 0) {
        if ((_cursor.node->keys[_cursor.index]).loc < _endState->query.loc.repr()) {
            cmp = -1;
        } else if ((_cursor.node->keys[_cursor.index]).loc > _endState->query.loc.repr()) {
            cmp = 1;
            } else {
                cmp = 0;
            }
    }
    if (_forward) {
        // We may have landed after the end point.
        return cmp > 0;
    } else {
        // We may have landed before the end point.
        return cmp < 0;
    }
}

void PmseCursor::locate(const BSONObj& key, const RecordId& loc, std::list<nvml::obj::shared_mutex*>& locks) {
    bool locateFound;
    CursorObject locateCursor;
    _isEOF = false;
    const auto query = IndexKeyEntry(key, loc);
    locateFound = lower_bound(query, locateCursor, locks);
    if (_forward) {
        if (_locateFoundDataEnd) {
            _locateFoundDataEnd = false;
            _isEOF = true;
        }
        if (locateFound) {
            _cursor.node = locateCursor.node;
            _cursor.index = locateCursor.index;
        }
    } else {  // manage backward
        if (_locateFoundDataEnd) {
            _locateFoundDataEnd = false;
            _cursor.node = _last;
            _cursor.index = _last->num_keys-1;
        } else {
            _cursor.node = locateCursor.node;
            _cursor.index = locateCursor.index;
            int cmp;
            IndexEntryComparison c(Ordering::make(_ordering));
            cmp = c.compare(IndexKeyEntry(_cursor.node->keys[_cursor.index].getBSON(), RecordId(_cursor.node->keys[_cursor.index].loc)), query);

            if (cmp) {
                moveToNext(locks);
            }
        }
    }
    if (atOrPastEndPointAfterSeeking())
        _isEOF = true;
}

void PmseCursor::seekEndCursor() {
    CursorObject endCursor;
    bool found;

    if (!_endState || !_tree->_root)
        return;
    std::list<nvml::obj::shared_mutex*> locks;
    found = lower_bound(_endState->query, endCursor, locks);
    if (_locateFoundDataEnd) {
        _endPositionIsDataEnd = true;
        _locateFoundDataEnd = false;
    }
    if (!_forward) {
        // lower_bound lands us on or after query. Reverse cursors must be on or before.
        if ( (endCursor.node == _first) && (endCursor.index == 0) ) {
            _endPositionIsDataEnd = true;
            unlockTree(locks);
            return;
        }
        int cmp;
            cmp = (endCursor.node->keys[endCursor.index]).getBSON().woCompare(_endState->query.key, _ordering, false);
            if (cmp == 0) {
                if ((endCursor.node->keys[endCursor.index]).loc < _endState->query.loc.repr()) {
                    cmp = -1;
                } else if ((endCursor.node->keys[endCursor.index]).loc > _endState->query.loc.repr()) {
                    cmp = 1;
                } else {
                    cmp = 0;
                }
            }
        if (cmp > 0) {
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
    }
    if ( found ) {
        _endPosition = IndexKeyEntry(endCursor.node->keys[endCursor.index].getBSON(),
                                    RecordId(endCursor.node->keys[endCursor.index].loc));
    }
    unlockTree(locks);
}


void PmseCursor::setEndPosition(const BSONObj& key, bool inclusive) {
    if (key.isEmpty()) {
        // This means scan to end of index.
        _endState = boost::none;
        return;
    }

    _endState = EndState(stripFieldNames(key),
                         _forward == inclusive ? RecordId::max() : RecordId::min());
    seekEndCursor();
}

bool PmseCursor::atEndPoint() {
    if (_endPosition &&
        (IndexKeyEntry_PM::compareEntries(_endPosition.get(), _cursor.node->keys[_cursor.index], _ordering) == 0))
        return true;
    return false;
}

boost::optional<IndexKeyEntry> PmseCursor::next(
                RequestedInfo parts = kKeyAndLoc) {
    std::list<nvml::obj::shared_mutex *> locks;

    if (_tree->_root == nullptr)
        return {};
    locate(_cursorKey, RecordId(_cursorId), locks);
    IndexEntryComparison c(Ordering::make(_ordering));
    if ( c.compare(IndexKeyEntry(_cursorKey, RecordId(_cursorId)),
                    IndexKeyEntry((_cursor.node->keys[_cursor.index]).getBSON(),
                                    RecordId((_cursor.node->keys[_cursor.index]).loc))) == 0)
        moveToNext(locks);
    if (!_cursor.node) {
        unlockTree(locks);
        return boost::none;
    }
    if (atEndPoint())
        _isEOF = true;
    if (_isEOF) {
        unlockTree(locks);
        return {};
    }
    if (_cursor.node.raw_ptr()->off != 0) {
            _cursorKey = _cursor.node->keys[_cursor.index].getBSON();
            _cursorId = _cursor.node->keys[_cursor.index].loc;
            // remember next value
        } else {
            _eofRestore = true;
        }
    IndexKeyEntry entry((_cursor.node->keys[_cursor.index]).getBSON(),
                        RecordId((_cursor.node->keys[_cursor.index]).loc));
    unlockTree(locks);
    return entry;
}

void PmseCursor::moveToNext(std::list<nvml::obj::shared_mutex*>& locks) {
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
                _cursor.node->next->_pmutex.lock_shared();
                locks.push_back(&(_cursor.node->next->_pmutex));
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
                _cursor.node->previous->_pmutex.lock_shared();
                locks.push_back(&(_cursor.node->previous->_pmutex));
                _cursor.node = _cursor.node->previous;
                _cursor.index = _cursor.node->num_keys - 1;
            } else {
                _cursor.node = nullptr;
            }
        }
    }
}

void PmseCursor::unlockTree(std::list<nvml::obj::shared_mutex*>& locks) {
    std::list<nvml::obj::shared_mutex*>::const_iterator iterator;
    try {
        for (iterator = locks.begin(); iterator != locks.end(); ++iterator) {
            (*iterator)->unlock_shared();
        }
        locks.erase(locks.begin(), locks.end());
    } catch (std::exception &e) {}
}

boost::optional<IndexKeyEntry> PmseCursor::seek(const BSONObj& key,
                                                bool inclusive,
                                                RequestedInfo parts = kKeyAndLoc) {
    if (!_tree->_root)
        return {};
    std::list<nvml::obj::shared_mutex*> locks;

    if (key.isEmpty()) {
        if (inclusive) {
            _cursor.node = _first;
            _cursor.index = 0;
        } else {
            _cursor.node = _last;
            _cursor.index = (_cursor.node)->num_keys - 1;
            return {};
        }
    } else {
        const BSONObj query = stripFieldNames(key);
        locate(query, _forward == inclusive ? RecordId::min() : RecordId::max(), locks);
        if (_isEOF) {
            unlockTree(locks);
            return {};
        }
    }
    if (_cursor.node.raw_ptr()->off != 0) {
        _cursorKey = _cursor.node->keys[_cursor.index].getBSON();
        _cursorId = _cursor.node->keys[_cursor.index].loc;
        // remember next value
    } else {
        _eofRestore = true;
        unlockTree(locks);
        return {};
    }
    IndexKeyEntry entry((_cursor.node->keys[_cursor.index]).getBSON(),
                        RecordId((_cursor.node->keys[_cursor.index]).loc));
    unlockTree(locks);
    return entry;
}

boost::optional<IndexKeyEntry> PmseCursor::seek(const IndexSeekPoint& seekPoint,
                                                RequestedInfo parts = kKeyAndLoc) {
    if (!_tree->_root)
        return {};

    const BSONObj query = IndexEntryComparison::makeQueryObject(seekPoint, _forward);
    auto discriminator = RecordId::min();
    std::list<nvml::obj::shared_mutex*> locks;
    locate(query, _forward ? RecordId::min() : RecordId::max(), locks);

    if (_isEOF) {
        unlockTree(locks);
        return {};
    }

    if (_cursor.node.raw_ptr()->off != 0) {
        _cursorKey = _cursor.node->keys[_cursor.index].getBSON();
        _cursorId = _cursor.node->keys[_cursor.index].loc;
        // remember next value
    } else {
        _eofRestore = true;
        unlockTree(locks);
        return {};
    }
    IndexKeyEntry entry((_cursor.node->keys[_cursor.index]).getBSON(),
                        RecordId((_cursor.node->keys[_cursor.index]).loc));
    unlockTree(locks);
    return entry;
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
    seekEndCursor();
    if (_eofRestore)
        return;
}

void PmseCursor::detachFromOperationContext() {}

void PmseCursor::reattachToOperationContext(OperationContext* opCtx) {}

}  // namespace mongo
