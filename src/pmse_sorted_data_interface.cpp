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

#include "pmse_change.h"
#include "pmse_index_cursor.h"
#include "pmse_sorted_data_interface.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/system/error_code.hpp>
#include <libpmemobj++/mutex.hpp>

#include <map>
#include <string>
#include <utility>

#include "mongo/util/log.h"

namespace mongo {

const int TempKeyMaxSize = 1024;

PmseSortedDataInterface::PmseSortedDataInterface(StringData ident,
                                                 const IndexDescriptor* desc,
                                                 StringData dbpath,
                                                 std::map<std::string, pool_base> *pool_handler)
    : _dbpath(dbpath), _desc(desc) {
    try {
        if (pool_handler->count(ident.toString()) > 0) {
            _pm_pool = pool<PmseTree>((*pool_handler)[ident.toString()]);
        } else {
            std::string filepath = _dbpath.toString() + ident.toString();
            if (!boost::filesystem::exists(filepath)) {
                _pm_pool = pool<PmseTree>::create(filepath.c_str(), "pmse_index",
                                                  10 * PMEMOBJ_MIN_POOL, 0666);
            } else {
                _pm_pool = pool<PmseTree>::open(filepath.c_str(), "pmse_index");
            }
            pool_handler->insert(std::pair<std::string, pool_base>(ident.toString(),
                                                                   _pm_pool));
        }
        _tree = _pm_pool.get_root();
    } catch (std::exception &e) {
        log() << "Error handled: " << e.what();
        throw;
    }
}

/*
 * Insert new (Key,RecordID) into Sorted Index into correct place.
 * Placement must be chosen basing on key value. *
 */
Status PmseSortedDataInterface::insert(OperationContext* txn,
                                       const BSONObj& key, const RecordId& loc,
                                       bool dupsAllowed) {
    BSONObj_PM bsonPM;
    BSONObj owned = key.getOwned();
    Status status = Status::OK();
    persistent_ptr<char> obj;

    if (key.objsize() >= TempKeyMaxSize) {
        std::string msg = mongoutils::str::stream()
            << "PMSE::insert: key too large to index, failing " << ' '
            << key.objsize() << ' ' << key;
        return Status(ErrorCodes::KeyTooLong, msg);
    }

    try {
        transaction::exec_tx(_pm_pool, [&obj, &owned] {
            obj = pmemobj_tx_alloc(owned.objsize(), 1);
            memcpy(static_cast<void*>(obj.get()), owned.objdata(), owned.objsize());
        });

        bsonPM.data = obj;
        status = _tree->insert(_pm_pool, bsonPM, loc, _desc->keyPattern(), dupsAllowed);
        if (status == Status::OK()) {
            ++_tree->_records;
            txn->recoveryUnit()->registerChange(new InsertIndexChange(_tree, _pm_pool, key, loc, dupsAllowed, _desc));
        }
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
        transaction::exec_tx(_pm_pool, [this, &owned, loc, dupsAllowed, txn] {
            if (_tree->remove(_pm_pool, owned, loc, dupsAllowed,
                             _desc->keyPattern(), txn))
                --_tree->_records;
        });
    } catch (std::exception &e) {
        log() << e.what();
    }
}

Status PmseSortedDataInterface::dupKeyCheck(OperationContext* txn,
                                            const BSONObj& key,
                                            const RecordId& loc) {
    BSONObj owned = key.getOwned();
    return _tree->dupKeyCheck(_pm_pool, owned, loc);
}

std::unique_ptr<SortedDataInterface::Cursor> PmseSortedDataInterface::newCursor(
                OperationContext* txn, bool isForward) const {
    return stdx::make_unique <PmseCursor> (txn, isForward, _tree,
                                           _desc->keyPattern(),
                                           _desc->unique());
}

class PmseSortedDataBuilderInterface : public SortedDataBuilderInterface {
    MONGO_DISALLOW_COPYING(PmseSortedDataBuilderInterface);
 public:
    PmseSortedDataBuilderInterface(OperationContext* txn,
                                   PmseSortedDataInterface* index,
                                   bool dupsAllowed)
    : _index(index),
      _txn(txn),
      _dupsAllowed(dupsAllowed) {}

    virtual Status addKey(const BSONObj& key, const RecordId& loc) {
        return _index->insert(_txn, key, loc, _dupsAllowed);
    }

    void commit(bool mayInterrupt) {}
 private:
    PmseSortedDataInterface* _index;
    OperationContext* _txn;
    bool _dupsAllowed;
};

SortedDataBuilderInterface* PmseSortedDataInterface::getBulkBuilder(
                OperationContext* txn, bool dupsAllowed) {
    return new PmseSortedDataBuilderInterface(txn, this, dupsAllowed);
}

}  // namespace mongo
