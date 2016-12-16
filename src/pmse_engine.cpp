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

#include "mongo/platform/basic.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/db/storage/ephemeral_for_test/ephemeral_for_test_record_store.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/stdx/memory.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/util/log.h"

#include "pmse_sorted_data_interface.h"
#include "pmse_record_store.h"
#include "pmse_engine.h"

#include <cstdlib>
#include <iostream>

namespace mongo {

Status PmseEngine::createRecordStore(OperationContext* opCtx, StringData ns, StringData ident,
                                     const CollectionOptions& options) {
    auto status = Status::OK();
    try {
        auto record_store = stdx::make_unique<PmseRecordStore>(ns, options, _DBPATH);
        identList->insertKV(ident.toString().c_str(), ns.toString().c_str());

    } catch(std::exception &e) {
        std::cout << "Create record store error: " << e.what() << std::endl;
        status = Status(ErrorCodes::BadValue, "Persistent memory exhausted");
    }
    return status;
}

std::unique_ptr<RecordStore> PmseEngine::getRecordStore(OperationContext* opCtx,
                                                        StringData ns,
                                                        StringData ident,
                                                        const CollectionOptions& options) {
    return stdx::make_unique<PmseRecordStore>(ns, options, _DBPATH);
}

Status PmseEngine::createSortedDataInterface(OperationContext* opCtx,
                                             StringData ident,
                                             const IndexDescriptor* desc) {
    return Status::OK();
}

SortedDataInterface* PmseEngine::getSortedDataInterface(OperationContext* opCtx,
                                                        StringData ident,
                                                        const IndexDescriptor* desc) {
    return new PmseSortedDataInterface(ident, desc, _DBPATH);
}

Status PmseEngine::dropIdent(OperationContext* opCtx, StringData ident) {
    bool status;
    boost::filesystem::path path(_DBPATH);
    const char* ns = identList->find(ident.toString().c_str(), status);
    identList->deleteKV(ident.toString().c_str());
    if(!std::string(ns).empty()) {
        boost::filesystem::remove_all(path.string()+ns);
    }
    boost::filesystem::remove_all(path.string()+ns+"_mapper");
    boost::filesystem::remove_all(path.string()+ident.toString());
    return Status::OK();
}

}
