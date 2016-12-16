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

#include "mongo/db/storage/record_store.h"
#include "mongo/util/log.h"

#include "pmse_record_store.h"

#include "errno.h"

#include <cstdlib>

#include <libpmemobj++/transaction.hpp>

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/system/error_code.hpp>

using nvml::obj::transaction;

namespace mongo {

PmseRecordStore::PmseRecordStore(StringData ns,
                                       const CollectionOptions& options,
                                       StringData dbpath) :
                RecordStore(ns), _options(options), _DBPATH(dbpath) {
    log() << "ns: " << ns;
    _numInserts = 0;
    std::string filename = _DBPATH.toString() + ns.toString();
    boost::filesystem::path path;
    log() << filename;
    if (ns.toString() == "local.startup_log"
                    && boost::filesystem::exists(filename)) {
        log() << "Delete old startup log";
        boost::filesystem::remove_all(filename);
        boost::filesystem::remove_all(filename + "_mapper");
    }

    std::string mapper_filename = _DBPATH.toString() + ns.toString()
                    + "_mapper";
    if (!boost::filesystem::exists(mapper_filename.c_str())) {
        std::cout << "Mapper create pool..." << std::endl;
        mapPool = pool<root>::create(mapper_filename, "kvmapper",
                        80 * PMEMOBJ_MIN_POOL);
        std::cout << "Create pool end" << std::endl;
    } else {
        std::cout << "Open pool..." << std::endl;
        try {
            mapPool = pool<root>::open(mapper_filename, "kvmapper");
        } catch (std::exception &e) {
            std::cout << "Error handled: " << e.what() << std::endl;
        }
        std::cout << "Open pool end..." << std::endl;
    }
    auto mapper_root = mapPool.get_root();

    if (!mapper_root->kvmap_root_ptr) {
        transaction::exec_tx(mapPool,[&] {
            mapper_root->kvmap_root_ptr = make_persistent<PmseMap<InitData>>(options.capped, options.cappedMaxDocs, options.cappedSize);
            mapper_root->kvmap_root_ptr->initialize(true);
        });
    } else {
        transaction::exec_tx(mapPool,[&] {
            mapper_root->kvmap_root_ptr->initialize(false);
        });
    }
    try {
        mapper = mapPool.get_root()->kvmap_root_ptr;

    } catch (std::exception& e) {
        std::cout << "Error while creating PMStore engine" << std::endl;
    };
}

StatusWith<RecordId> PmseRecordStore::insertRecord(OperationContext* txn,
                                                      const char* data, int len,
                                                      bool enforceQuota) {
    persistent_ptr<InitData> obj;
    uint64_t id = 0;
    try {
        transaction::exec_tx(mapPool, [&] {
            obj = pmemobj_tx_alloc(sizeof(InitData::size) + len, 1);
            obj->size = len;
            memcpy(obj->data, data, len);
        });
    } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
    }
    if(obj == nullptr)
        return StatusWith<RecordId>(ErrorCodes::InternalError,
                                    "Not allocated memory!");
    id = mapper->insert(obj);
    if(!id)
        return StatusWith<RecordId>(ErrorCodes::OperationFailed,
                                    "Null record Id!");
    return StatusWith<RecordId>(RecordId(id));
}

Status PmseRecordStore::updateRecord(
                OperationContext* txn, const RecordId& oldLocation,
                const char* data, int len, bool enforceQuota,
                UpdateNotifier* notifier) {
    persistent_ptr<InitData> obj;
    try {
        transaction::exec_tx(mapPool, [&] {
            obj = pmemobj_tx_alloc(sizeof(InitData::size) + len, 1);
            obj->size = len;
            memcpy(obj->data, data, len);
            mapper->updateKV(oldLocation.repr(), obj);
        });
    } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
        return Status(ErrorCodes::BadValue, e.what());
    }
    return Status::OK();
}

void PmseRecordStore::deleteRecord(OperationContext* txn,
                                      const RecordId& dl) {
    mapper->remove((uint64_t) dl.repr());
}

void PmseRecordStore::setCappedCallback(CappedCallback*) {
    log() << "Not mocked setCappedCallback";
}

bool PmseRecordStore::findRecord(OperationContext* txn, const RecordId& loc,
                                    RecordData* rd) const {
    persistent_ptr<InitData> obj;
    if(mapper->find((uint64_t) loc.repr(), obj)){
        invariant(obj != nullptr);
        *rd = RecordData(obj->data, obj->size);
        return true;
    }
    return false;
}

PmseRecordCursor::PmseRecordCursor(persistent_ptr<PmseMap<InitData>> mapper) {
    _mapper = mapper;
    _cur = nullptr;
}

boost::optional<Record> PmseRecordCursor::next() {
    if(_eof)
        return boost::none;
    if(_cur != nullptr) {
        if(_cur->next != nullptr) {
            _cur = _cur->next;
        } else {
            _cur = nullptr;
            while(_cur == nullptr && ++actual < _mapper->_size) {
                persistent_ptr<KVPair> head = _mapper->getFirstPtr(actual);
                if(head != nullptr) {
                    _cur = head;
                    break;
                }
            }
        }
    } else {
        while(_cur == nullptr && actual < _mapper->_size) {
            persistent_ptr<KVPair> head = _mapper->getFirstPtr(actual);
            if(head != nullptr) {
                _cur = head;
                break;
            }
            actual++;
        }
    }
    if(_cur == nullptr) {
        _eof = true;
        return boost::none;
    }
    RecordId a((int64_t) _cur->idValue);
    RecordData b(_cur->ptr->data, _cur->ptr->size);
    return { {a,b}};
}

boost::optional<Record> PmseRecordCursor::seekExact(const RecordId& id) {
    persistent_ptr<InitData> obj = nullptr;
    bool status = _mapper->getPair(id.repr(), _cur);
    obj = _cur->ptr;
    if (!status || !obj) {
        return boost::none;
    }
    RecordId a(id.repr());
    RecordData b(obj->data, obj->size);
    return {{a,b}};
}
}

