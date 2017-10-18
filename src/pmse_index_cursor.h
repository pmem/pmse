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
    boost::optional<IndexKeyEntry> seekInTree(IndexKeyEntry& key,
                                              KeyString::Discriminator discriminator,
                                              RequestedInfo parts);
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
    void locate(const BSONObj& key, const RecordId& loc, std::list<nvml::obj::shared_mutex*>& locks);
    void unlockTree(std::list<nvml::obj::shared_mutex*>& locks);
    void seekEndCursor();
    bool lower_bound(IndexKeyEntry entry, CursorObject& cursor, std::list<nvml::obj::shared_mutex*>& locks);
    void moveToNext(std::list<nvml::obj::shared_mutex*>& locks);
    bool atOrPastEndPointAfterSeeking();
    bool atEndPoint();
    const bool _forward;
    const BSONObj& _ordering;
    persistent_ptr<PmseTreeNode> _first;
    persistent_ptr<PmseTreeNode> _last;
    persistent_ptr<PmseTree> _tree;
    bool _isEOF = true;
    /*
     * Cursor used for iterating with next until "_endPosition"
     */
    boost::optional<IndexKeyEntry> _endPosition;
    CursorObject _cursor;

    struct EndState {
        EndState(BSONObj key, RecordId loc) : query(std::move(key), loc) {}
        IndexKeyEntry query;
    };
    boost::optional<EndState> _endState;
    BSONObj _cursorKey;
    int64_t _cursorId;
    bool _endPositionIsDataEnd;
    bool _locateFoundDataEnd;
    bool _eofRestore;
};
}  // namespace mongo

#endif  // SRC_PMSE_INDEX_CURSOR_H_
