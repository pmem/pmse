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

#include "pmse_engine.h"
#include "pmse_record_store.h"
#include "pmse_sorted_data_interface.h"

#include <cstdlib>
#include <string>

#include "mongo/platform/basic.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/db/storage/ephemeral_for_test/ephemeral_for_test_record_store.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/stdx/memory.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/util/log.h"

#include <boost/algorithm/string/predicate.hpp>

namespace mongo {

PmseEngine::PmseEngine(std::string dbpath) : _dbPath(dbpath) {
    if(!boost::algorithm::ends_with(dbpath, "/")) {
        _dbPath = _dbPath +"/";
    }
    std::string path = _dbPath+_kIdentFilename.toString();
    if (!boost::filesystem::exists(path)) {
        pop = pool<ListRoot>::create(path, "pmse_identlist", 4 * PMEMOBJ_MIN_POOL,
                                      0664);
        log() << "Engine pool created";
    } else {
        pop = pool<ListRoot>::open(path, "pmse_identlist");
        log() << "Engine pool opened";
    }

    try {
        auto root = pop.get_root();
        if (!root->list_root_ptr) {
            transaction::exec_tx(pop, [this, &root] {
                root->list_root_ptr = make_persistent<PmseList>(pop);
            });
        }
        _identList = root->list_root_ptr;
    } catch (std::exception& e) {
        log() << "Error while creating PMSE engine:" << e.what() << std::endl;
    }
    _identList->setPool(pop);
    if(!_identList->isAfterSafeShutdown()) {
        _needCheck = true;
    } else {
        _needCheck = false;
    }
    _identList->resetState();
}

PmseEngine::~PmseEngine() {
    for (auto p : _poolHandler) {
        p.second.close();
    }
    pop.close();
}

Status PmseEngine::createRecordStore(OperationContext* opCtx, StringData ns, StringData ident,
                                     const CollectionOptions& options) {
    stdx::lock_guard<stdx::mutex> lock(_pmutex);
    auto status = Status::OK();
    try {
        _identList->insertKV(ident.toString().c_str(), ns.toString().c_str());
        auto record_store = stdx::make_unique<PmseRecordStore>(ns, ident, options, _dbPath, &_poolHandler);
    } catch(std::exception &e) {
        status = Status(ErrorCodes::OutOfDiskSpace, e.what());
    }
    return status;
}

std::unique_ptr<RecordStore> PmseEngine::getRecordStore(OperationContext* opCtx,
                                                        StringData ns,
                                                        StringData ident,
                                                        const CollectionOptions& options) {
    persistent_ptr<PmseMap<InitData>> _mapper;
    pool<root> mapPoolOld = pool<root>((_poolHandler)[ident.toString()]);
    auto mapper_root = mapPoolOld.get_root();
    _mapper = mapper_root->kvmap_root_ptr;
    _mapper->storeCounters();
    _identList->update(ident.toString().c_str(), ns.toString().c_str());
    return stdx::make_unique<PmseRecordStore>(ns, ident, options, _dbPath,
                                              &_poolHandler, (_needCheck ? true : false));
}

Status PmseEngine::createSortedDataInterface(OperationContext* opCtx,
                                             StringData ident,
                                             const IndexDescriptor* desc) {
    stdx::lock_guard<stdx::mutex> lock(_pmutex);
    try {
        _identList->insertKV(ident.toString().c_str(), "");
        auto sorted_data_interface = PmseSortedDataInterface(ident, desc, _dbPath, &_poolHandler);
    } catch (std::exception &e) {
        return Status(ErrorCodes::OutOfDiskSpace, e.what());
    }
    return Status::OK();
}

SortedDataInterface* PmseEngine::getSortedDataInterface(OperationContext* opCtx,
                                                        StringData ident,
                                                        const IndexDescriptor* desc) {
    return new PmseSortedDataInterface(ident, desc, _dbPath, &_poolHandler);
}

Status PmseEngine::dropIdent(OperationContext* opCtx, StringData ident) {
    stdx::lock_guard<stdx::mutex> lock(_pmutex);
    boost::filesystem::path path(_dbPath);
    _identList->deleteKV(ident.toString().c_str());
    if (_poolHandler.count(ident.toString()) > 0) {
        _poolHandler[ident.toString()].close();
        _poolHandler.erase(ident.toString());
    }
    boost::filesystem::remove_all(path.string() + ident.toString());
    return Status::OK();
}

}  // namespace mongo
