/*
 * Copyright 2014-2018, Intel Corporation
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

#ifndef SRC_PMSE_ENGINE_H_
#define SRC_PMSE_ENGINE_H_

#include <libpmemobj.h>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>

#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "pmse_list.h"
#include "pmse_recovery_unit.h"

#include "mongo/db/storage/kv/kv_engine.h"

namespace mongo {

class JournalListener;

using namespace pmem::obj;

struct ident_entry {
    persistent_ptr<ident_entry> next;
    p<uint64_t> value;
};

class PmseEngine : public KVEngine {
 public:
    explicit PmseEngine(std::string dbpath);

    virtual ~PmseEngine();

    virtual RecoveryUnit* newRecoveryUnit() {
        return new PmseRecoveryUnit();
    }

    virtual Status createRecordStore(OperationContext* opCtx,
                                     StringData ns,
                                     StringData ident,
                                     const CollectionOptions& options);

    virtual std::unique_ptr<RecordStore> getRecordStore(OperationContext* opCtx,
                                                        StringData ns,
                                                        StringData ident,
                                                        const CollectionOptions& options);

    virtual Status createSortedDataInterface(OperationContext* opCtx,
                                             StringData ident,
                                             const IndexDescriptor* desc);

    virtual SortedDataInterface* getSortedDataInterface(OperationContext* opCtx,
                                                        StringData ident,
                                                        const IndexDescriptor* desc);

    virtual Status dropIdent(OperationContext* opCtx, StringData ident);

    virtual bool supportsDocLocking() const {
        return true;
    }

    virtual Status beginBackup(OperationContext* txn) {
        return Status::OK();
    }

    virtual void endBackup(OperationContext* txn) {
        return;
    }

    virtual bool supportsDirectoryPerDB() const {
        return false;
    }

    virtual bool isDurable() const {
        return true;
    }

    virtual bool isEphemeral() const {
        return false;
    }

    virtual int64_t getIdentSize(OperationContext* opCtx, StringData ident) {
        // TODO(kfilipek): Implement getIdentSize
        return 1;
    }

    virtual Status repairIdent(OperationContext* opCtx, StringData ident) {
        return Status::OK();
    }

    virtual bool hasIdent(OperationContext* opCtx, StringData ident) const {
        return _identList->hasKey(ident.toString().c_str());
    }

    std::vector<std::string> getAllIdents(OperationContext* opCtx) const {
        return _identList->getKeys();
    }

    virtual void cleanShutdown() {
        // If not clean shutdown start scanning all collections
        _identList->safeShutdown();
    }

    void setJournalListener(JournalListener* jl) final {}

 private:
    stdx::mutex _pmutex;
    bool _needCheck;
    std::map<std::string, pool_base> _poolHandler;
    std::shared_ptr<void> _catalogInfo;
    std::string _dbPath;
    PMEMobjpool *pm_pool = NULL;
    const StringData _kIdentFilename = "pmkv.pm";
    pool<ListRoot> pop;
    persistent_ptr<PmseList> _identList;
};
}  // namespace mongo

#endif  // SRC_PMSE_ENGINE_H_
