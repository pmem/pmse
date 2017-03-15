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

#ifndef SRC_PMSE_RECORD_STORE_H_
#define SRC_PMSE_RECORD_STORE_H_

#include "pmse_map.h"

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/utils.hpp>

#include <cmath>
#include <string>
#include <map>

#include "mongo/platform/basic.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/stdx/memory.h"

using namespace nvml::obj;

namespace mongo {

namespace {
const std::string storeName = "pmse";
const uint64_t baseSize = 20480;
}

class PmseRecordCursor final : public SeekableRecordCursor {
 public:
    PmseRecordCursor(persistent_ptr<PmseMap<InitData>> mapper, bool forward);

    boost::optional<Record> next();

    boost::optional<Record> seekExact(const RecordId& id) final;

    void save() final;

    bool restore() final;

    void detachFromOperationContext() final {}

    void reattachToOperationContext(OperationContext* txn) final {}

    void saveUnpositioned();

 private:
    void moveToNext(bool inNext = true);
    void moveToLast();
    void moveBackward();
    bool checkPosition();

    persistent_ptr<PmseMap<InitData>> _mapper;
    persistent_ptr<KVPair> _before;
    persistent_ptr<KVPair> _cur;
    persistent_ptr<KVPair> _restorePoint;
    p<bool> _eof = false;
    p<bool> _isCapped;
    p<bool> _forward;
    p<bool> _lastMoveWasRestore;
    p<bool> _positionCheck;
    p<int64_t> actualListNumber = -1;
    p<int64_t> _actualAfterRestore = 0;
    p<uint64_t> position;
    PMEMoid _currentOid = OID_NULL;
};

class PmseRecordStore : public RecordStore {
 public:
    PmseRecordStore(StringData ns, StringData ident,
                    const CollectionOptions& options,
                    StringData dbpath,
                    std::map<std::string, pool_base> *pool_handler);

    ~PmseRecordStore() = default;

    virtual const char* name() const {
        return storeName.c_str();
    }

    virtual void setCappedCallback(CappedCallback* cb);

    virtual long long dataSize(OperationContext* txn) const {
        return _mapper->dataSize();
    }

    virtual long long numRecords(OperationContext* txn) const {
        return (int64_t)_mapper->fillment();
    }

    virtual bool isCapped() const {
        return _options.capped;
    }

    virtual int64_t storageSize(OperationContext* txn,
                                BSONObjBuilder* extraInfo = NULL,
                                int infoLevel = 0) const {
        return _storageSize;
    }

    virtual bool findRecord(OperationContext* txn, const RecordId& loc,
                            RecordData* rd) const;

    virtual void deleteRecord(OperationContext* txn, const RecordId& dl);

    virtual StatusWith<RecordId> insertRecord(OperationContext* txn,
                                              const char* data, int len,
                                              bool enforceQuota);

    virtual Status insertRecordsWithDocWriter(OperationContext* txn,
                                              const DocWriter* const* docs,
                                              size_t nDocs,
                                              RecordId* idsOut = nullptr);

    virtual void waitForAllEarlierOplogWritesToBeVisible(OperationContext* txn) const;

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
        return stdx::make_unique<PmseRecordCursor>(_mapper, forward);
    }

    virtual Status truncate(OperationContext* txn) {
        if (!_mapper->truncate()) {
            return Status(ErrorCodes::OperationFailed, "Truncate error");
        }
        return Status::OK();
    }

    virtual void temp_cappedTruncateAfter(OperationContext* txn, RecordId end,
                                          bool inclusive);

    virtual Status validate(OperationContext* txn, bool full, bool scanData,
                            ValidateAdaptor* adaptor, ValidateResults* results,
                            BSONObjBuilder* output) {
        return Status::OK();
    }

    virtual void appendCustomStats(OperationContext* txn,
                                   BSONObjBuilder* result, double scale) const {
        if (_mapper->isCapped()) {
            result->appendNumber("capped", true);
            result->appendNumber("maxSize", floor(_mapper->getMax() / scale));
            result->appendNumber("max", _mapper->getMaxSize());
        } else {
            result->appendNumber("capped", false);
        }
        result->appendNumber("numInserts", _mapper->fillment());
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
                            BSONObjBuilder* output);

 private:
    void deleteCappedAsNeeded(OperationContext* txn);
    CappedCallback* _cappedCallback;
    int64_t _storageSize = baseSize;
    CollectionOptions _options;
    const StringData _DBPATH;
    pool<root> mapPool;
    persistent_ptr<PmseMap<InitData>> _mapper;
};
}  // namespace mongo
#endif  // SRC_PMSE_RECORD_STORE_H_
