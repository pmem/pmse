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

#ifndef SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_ENGINE_H_
#define SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_ENGINE_H_

#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>

#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/db/storage/recovery_unit_noop.h"
#include "pmse_list.h"

#include <libpmemobj.h>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>

namespace mongo {

class JournalListener;

using namespace nvml::obj;

struct ident_entry {
    persistent_ptr<ident_entry> next;
    p<uint64_t> value;
};

class PmseEngine : public KVEngine {
public:
    PmseEngine(std::string dbpath) : _DBPATH(dbpath) {
        std::cout << "createRecordStore constructor\n";
        std::string path = _DBPATH+_IDENT_FILENAME.toString();
        if (!boost::filesystem::exists(path)) {
            std::cout << "Create pool..." << std::endl;
            pop = pool<PmseList>::create(path, "identList", PMEMOBJ_MIN_POOL,
                                             S_IRWXU);
            std::cout << "Create pool end" << std::endl;
        } else {
            pop = pool<PmseList>::open(path, "identList");
            std::cout << "Open pool..." << std::endl;
        }

        try {
            identList = pop.get_root();
        } catch (std::exception& e) {
            std::cout << "Error while creating PMStore engine:" << e.what() << std::endl;
        };
        identList->setPool(pop);
    }
    virtual ~PmseEngine() {
        pop.close();
    }

    virtual RecoveryUnit* newRecoveryUnit() {
        // TODO: Implement RecoveryUnit
        return new RecoveryUnitNoop();
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
        // TODO: Implement getIdentSize
        return 1;
    }

    virtual Status repairIdent(OperationContext* opCtx, StringData ident) {
        return Status::OK();
    }

    virtual bool hasIdent(OperationContext* opCtx, StringData ident) const {
        std::cout << "hasIdent" << std::endl;
        return identList->hasKey(ident.toString().c_str());
    }

    std::vector<std::string> getAllIdents(OperationContext* opCtx) const {
        std::cout << "getAllIdents" << std::endl;
        return identList->getKeys();
    }

    virtual void cleanShutdown() {};

    void setJournalListener(JournalListener* jl) final {}

private:
    std::shared_ptr<void> _catalogInfo;
    const std::string _DBPATH;
    PMEMobjpool *pm_pool = NULL;
    const StringData _IDENT_FILENAME = "pmkv.pm";
    pool<PmseList> pop;
    persistent_ptr<PmseList> identList;
};
}

#endif /* SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_ENGINE_H_ */
