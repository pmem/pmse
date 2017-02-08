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

#include "pmse_sorted_data_interface.h"
#include "pmse_index_cursor.h"

#include "mongo/util/log.h"

namespace mongo {

PmseSortedDataInterface::PmseSortedDataInterface(StringData ident,
                                                 const IndexDescriptor* desc,
                                                 StringData dbpath,
                                                 std::map<StringData, pool_base> &pool_handler) : _records(0) {
    if (pool_handler.count(ident) > 0) {
        pm_pool = pool<PmseTree>(pool_handler[ident]);
    } else {
        filepath = dbpath;
        std::string filename = filepath.toString() + ident.toString();
        _desc = desc;

        if (access(filename.c_str(), F_OK) != 0) {
            pm_pool = pool<PmseTree>::create(filename.c_str(), "pmse",
                                             10 * PMEMOBJ_MIN_POOL, 0666);
        } else {
            pm_pool = pool<PmseTree>::open(filename.c_str(), "pmse");
        }
    }

    tree = pm_pool.get_root();
}

/*
 * Insert new (Key,RecordID) into Sorted Index into correct place. Placement must be chosen basing on key value.
 *
 */
Status PmseSortedDataInterface::insert(OperationContext* txn,
                                       const BSONObj& key, const RecordId& loc,
                                       bool dupsAllowed) {
    BSONObj_PM bsonPM;
    BSONObj owned = key.getOwned();
    Status status = Status::OK();

    persistent_ptr<char> obj;

    try {
        transaction::exec_tx(pm_pool,
                             [&] {
            obj = pmemobj_tx_alloc(owned.objsize(), 1);
            memcpy( (void*)obj.get(), owned.objdata(), owned.objsize());
        });

        bsonPM.data = obj;
        status = tree->insert(pm_pool, bsonPM, loc, _desc->keyPattern(), dupsAllowed);
        ++_records;
    } catch (std::exception &e) {
        log() << e.what();
    }
    return status;
}

/*
 * Remove given record from Sorted Index *
 */
void PmseSortedDataInterface::unindex(OperationContext* txn, const BSONObj& key,
                                      const RecordId& loc, bool dupsAllowed) {
    BSONObj owned = key.getOwned();
    try {
        transaction::exec_tx(pm_pool,
        [&] {
            tree->remove(pm_pool, owned, loc, dupsAllowed, _desc->keyPattern());
        });
        --_records;
    } catch (std::exception &e) {
        log() << e.what();
    }

}

Status PmseSortedDataInterface::dupKeyCheck(OperationContext* txn, const BSONObj& key,
                                            const RecordId& loc) {
    BSONObj owned = key.getOwned();
    return tree->dupKeyCheck(pm_pool,owned,loc);
}

std::unique_ptr<SortedDataInterface::Cursor> PmseSortedDataInterface::newCursor(
                OperationContext* txn, bool isForward) const {
    return stdx::make_unique <PmseCursor> (txn, isForward, tree, _desc->keyPattern(), _desc->unique());
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
                OperationContext* txn, bool dupsAllowed) {
    return new PMStoreSortedDataBuilderInterface(this, txn);
}

}
