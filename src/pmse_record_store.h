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

#ifndef SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_RECORD_STORE_H_
#define SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_RECORD_STORE_H_


#include "libpmem.h"
#include "libpmemobj.h"
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/utils.hpp>

#include "mongo/platform/basic.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/stdx/memory.h"

#include "pmse_map.h"

using namespace nvml::obj;

namespace mongo {

namespace {
const std::string storeName = "pmse";
}

struct root {
    persistent_ptr<PmseMap<InitData>> kvmap_root_ptr;
};

class PmseRecordCursor final : public SeekableRecordCursor {
public:
    PmseRecordCursor(persistent_ptr<PmseMap<InitData>> mapper);

    boost::optional<Record> next();

    boost::optional<Record> seekExact(const RecordId& id) final;

    void save() final;

    bool restore() final;

    void detachFromOperationContext() final {}

    void reattachToOperationContext(OperationContext* txn) final {}

    void saveUnpositioned();
private:
    persistent_ptr<PmseMap<InitData>> _mapper;
    persistent_ptr<KVPair> _cur;
    persistent_ptr<KVPair> _restorePoint;
    p<bool> _eof = false;
    p<int> actual = 0;
    PMEMoid _currentOid = OID_NULL;
};

class PmseRecordStore : public RecordStore {
public:
    PmseRecordStore(StringData ns, const CollectionOptions& options,
                       StringData dbpath);
    ~PmseRecordStore() {
        try {
            mapPool.close();
        } catch (std::logic_error &e) {
            std::cout << e.what() << std::endl;
        }
    }

    virtual const char* name() const {
        return storeName.c_str();
    }

    virtual void setCappedCallback(CappedCallback*);

    virtual long long dataSize(OperationContext* txn) const {
        return mapper->dataSize();
    }

    virtual long long numRecords(OperationContext* txn) const {
        return (long long) mapper->fillment();
    }

    virtual bool isCapped() const {
        return _options.capped;
    }

    virtual int64_t storageSize(OperationContext* txn,
                                BSONObjBuilder* extraInfo = NULL,
                                int infoLevel = 0) const {
        // TODO: Implement storageSize
        return _storageSize;
    }

    virtual bool findRecord(OperationContext* txn, const RecordId& loc,
                            RecordData* rd) const;

    virtual void deleteRecord(OperationContext* txn, const RecordId& dl);

    virtual StatusWith<RecordId> insertRecord(OperationContext* txn,
                                              const char* data, int len,
                                              bool enforceQuota);

    virtual StatusWith<RecordId> insertRecord(OperationContext* txn,
                                              const DocWriter* doc,
                                              bool enforceQuota) {
        // TODO: Implement record inserting
        std::cout << "Not implemented insertRecord function!" << std::endl;
        _numInserts++;
        return StatusWith<RecordId>(RecordId(6, 4));
    }

    virtual Status insertRecordsWithDocWriter(OperationContext* txn,
                                              const DocWriter* const* docs,
                                              size_t nDocs,
                                              RecordId* idsOut = nullptr) {
        // TODO: Implement insertRecordsWithDocWriter
        std::cout << "Not implemented: insertRecordsWithDocWriter" << std::endl;
        return Status::OK();
    }

    virtual void waitForAllEarlierOplogWritesToBeVisible(OperationContext* txn) const {
        // TODO: Implement insertRecordsWithDocWriter
        std::cout << "Not implemented: waitForAllEarlierOplogWritesToBeVisible" << std::endl;
    }

    virtual Status updateRecord(OperationContext* txn,
                                              const RecordId& oldLocation,
                                              const char* data, int len,
                                              bool enforceQuota,
                                              UpdateNotifier* notifier);

    virtual bool updateWithDamagesSupported() const {
        return false;
    }

    virtual StatusWith<RecordData> updateWithDamages(
                    OperationContext* txn, const RecordId& loc,
                    const RecordData& oldRec, const char* damageSource,
                    const mutablebson::DamageVector& damages) {
        invariant(false);
    }

    std::unique_ptr<SeekableRecordCursor> getCursor(OperationContext* txn,
                                                    bool forward) const final {
        return stdx::make_unique<PmseRecordCursor>(mapper);
    }

    virtual Status truncate(OperationContext* txn) {
        mapper->truncate();
        return Status::OK();
    }

    virtual void temp_cappedTruncateAfter(OperationContext* txn, RecordId end,
                                          bool inclusive) {
    }

    virtual Status validate(OperationContext* txn, bool full, bool scanData,
                            ValidateAdaptor* adaptor, ValidateResults* results,
                            BSONObjBuilder* output) {
        return Status::OK();
    }

    virtual void appendCustomStats(OperationContext* txn,
                                   BSONObjBuilder* result, double scale) const {
        result->appendNumber("numInserts", _numInserts);
    }

    virtual Status touch(OperationContext* txn, BSONObjBuilder* output) const {
        return Status::OK();
    }

    virtual void updateStatsAfterRepair(OperationContext* txn,
                                        long long numRecords,
                                        long long dataSize) {
    }

    /**
     * @return OK if the validate run successfully
     *         OK will be returned even if corruption is found
     *         deatils will be in result
     */
    virtual Status validate(OperationContext* txn,
                            ValidateCmdLevel level,
                            ValidateAdaptor* adaptor,
                            ValidateResults* results,
                            BSONObjBuilder* output) {
        // TODO: Implement validate
        return Status::OK();
    }

private:
    CappedCallback* _cappedCallback;
    int64_t _storageSize = 20480;
    CollectionOptions _options;
    long long _numInserts;
    const StringData _DBPATH;
    pool<root> mapPool;
    persistent_ptr<PmseMap<InitData>> mapper;
};
}
#endif /* SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_RECORD_STORE_H_ */
