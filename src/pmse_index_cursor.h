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

#ifndef SRC_PMSE_INDEX_CURSOR_H_
#define SRC_PMSE_INDEX_CURSOR_H_

#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/db/storage/key_string.h"

#include "pmse_tree.h"

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

using namespace nvml::obj;

namespace mongo {

class PmseCursor final : public SortedDataInterface::Cursor {
 public:
    PmseCursor(OperationContext* txn, bool isForward,
               persistent_ptr<PmseTree> tree, const BSONObj& ordering,
               const bool unique);

    void setEndPosition(const BSONObj& key, bool inclusive);

    virtual boost::optional<IndexKeyEntry> next(RequestedInfo parts);

    boost::optional<IndexKeyEntry> seek(const BSONObj& key, bool inclusive,
                                        RequestedInfo parts);

    boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
                                        RequestedInfo parts);

    boost::optional<IndexKeyEntry> seekExact(const BSONObj& key,
                                             RequestedInfo parts);

    void save();

    void saveUnpositioned();

    void restore();

    void detachFromOperationContext();

    void reattachToOperationContext(OperationContext* opCtx);

 private:
    virtual boost::optional<IndexKeyEntry> iterateToNext(RequestedInfo parts);

    boost::optional<IndexKeyEntry> seekInTree(IndexKeyEntry& key,
                                              KeyString::Discriminator discriminator,
                                              RequestedInfo parts);

    persistent_ptr<PmseTreeNode> find_leaf(persistent_ptr<PmseTreeNode> node,
                                            IndexKeyEntry& entry,
                                           const BSONObj& _ordering);
    bool hasFieldNames(const BSONObj& obj) {
        BSONForEach(e, obj) {
            if (e.fieldName()[0])
                return true;
        }
        return false;
    }

    BSONObj stripFieldNames(const BSONObj& query) {
        if (!hasFieldNames(query))
            return query;

        BSONObjBuilder bb;
        BSONForEach(e, query) {
            bb.appendAs(e, StringData());
        }
        return bb.obj();
    }

    bool previous(CursorObject&);
    bool correctType(BSONObj record);
    void moveToNext();

    const bool _forward;
    const BSONObj& _ordering;
    persistent_ptr<PmseTreeNode> _first;
    persistent_ptr<PmseTreeNode> _last;
    const bool _unique;
    persistent_ptr<PmseTree> _tree;
    BSONType cursorType;
    /*
     * Marks end position for seek and next. Set by setEndPosition().
     * */
    //BSONObj_PM* _endPosition;
    IndexKeyEntry_PM* _endPosition;
    uint64_t _inf;
    bool _isEOF = true;
    /*
     * Cursor used for iterating with next until "_endPosition"
     */
    CursorObject _cursor;
    CursorObject _returnValue;
    static const BSONObj min;
    static const BSONObj max;
    //BSONObj_PM end_min_pm;
    //BSONObj_PM end_max_pm;



    struct EndState {
        EndState(BSONObj key, RecordId loc) : query(std::move(key), loc) {}

        IndexKeyEntry query;
//        IndexSet::const_iterator it;
    };
    boost::optional<EndState> _endState;
    BSONObj _cursorKey;
    RecordId _cursorId;

    bool _wasMoved;
    bool _eofRestore;
    bool _wasRestore = false;
};
}  // namespace mongo

#endif  // SRC_PMSE_INDEX_CURSOR_H_
