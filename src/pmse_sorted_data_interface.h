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

#ifndef SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_SORTED_DATA_INTERFACE_H_
#define SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_SORTED_DATA_INTERFACE_H_

#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/bson/bsonobj_comparator.h"

#include <libpmemobj.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>

#include "pmse_tree.h"

using namespace nvml::obj;

namespace mongo {

class PmseSortedDataInterface : public SortedDataInterface {
public:

    PmseSortedDataInterface(StringData ident, const IndexDescriptor* desc,
                            StringData dbpath, std::map<std::string, pool_base> &pool_handler);

    virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* txn,
                                                       bool dupsAllowed) override;

    virtual Status insert(OperationContext* txn, const BSONObj& key,
                          const RecordId& loc, bool dupsAllowed);

    virtual void unindex(OperationContext* txn, const BSONObj& key,
                         const RecordId& loc, bool dupsAllowed);

    virtual Status dupKeyCheck(OperationContext* txn, const BSONObj& key,
                               const RecordId& loc);

    virtual void fullValidate(OperationContext* txn, long long* numKeysOut,
                              ValidateResults* fullResults) const {
        *numKeysOut = tree->_records;
        // TODO: Implement fullValidate
    }

    virtual bool appendCustomStats(OperationContext* txn,
                                   BSONObjBuilder* output, double scale) const {
        // TODO: Implement appendCustomStats
        return false;
    }

    virtual long long getSpaceUsedBytes(OperationContext* txn) const {
        // TODO: Implement getSpaceUsedBytes
        return 0;
    }

    virtual bool isEmpty(OperationContext* txn) {
        return tree->_records == 0 ? true : false;
    }

    virtual Status initAsEmpty(OperationContext* txn) {
        return Status::OK();
    }

    std::unique_ptr<SortedDataInterface::Cursor> newCursor(
                    OperationContext* txn, bool isForward) const;

private:
    void moveToNext();
    StringData filepath;
    pool<PmseTree> pm_pool;
    persistent_ptr<PmseTree> tree;
    const IndexDescriptor* _desc;

};
}
#endif /* SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_SORTED_DATA_INTERFACE_H_ */
