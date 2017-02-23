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
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/operation_context.h"

#include "pmse_record_store.h"
#include "pmse_change.h"

#include "errno.h"

#include <cstdlib>

#include <libpmemobj++/transaction.hpp>

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/system/error_code.hpp>

using nvml::obj::transaction;

namespace mongo {

PmseRecordStore::PmseRecordStore(StringData ns,
                                 StringData ident,
                                 const CollectionOptions& options,
                                 StringData dbpath,
                                 std::map<std::string, pool_base> &pool_handler) :
                RecordStore(ns), _cappedCallback(nullptr),
                _options(options), _DBPATH(dbpath) {
    log() << "ns: " << ns;
    if(pool_handler.count(ident.toString()) > 0) {
        mapPool = pool<root>( pool_handler[ident.toString()] );
    } else {
        std::string filename = _DBPATH.toString() + ident.toString();
        boost::filesystem::path path;
        log() << filename;
        if (ns.toString() == "local.startup_log"
                        && boost::filesystem::exists(filename)) {
            log() << "Delete old startup log";
            boost::filesystem::remove_all(filename);
        }

        std::string mapper_filename = _DBPATH.toString() + ident.toString();
        if (!boost::filesystem::exists(mapper_filename.c_str())) {
            try {
                mapPool = pool<root>::create(mapper_filename, "kvmapper",
                                             (ns.toString() == "local.startup_log" ||
                                                             ns.toString() == "_mdb_catalog" ? 10 : 80)
                                                             * PMEMOBJ_MIN_POOL);
            } catch (std::exception &e) {
                log() << "Error handled: " << e.what();
                throw;
            }
        } else {
            try {
                mapPool = pool<root>::open(mapper_filename, "kvmapper");
            } catch (std::exception &e) {
                log() << "Error handled: " << e.what();
                throw;
            }
        }
        pool_handler.insert(std::pair<std::string, pool_base>(ident.toString(), mapPool));
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
        _mapper = mapPool.get_root()->kvmap_root_ptr;
    } catch (std::exception& e) {
        log() << "Error while creating PmseRecordStore";
    };
}

StatusWith<RecordId> PmseRecordStore::insertRecord(OperationContext* txn,
                                                      const char* data, int len,
                                                      bool enforceQuota) {
    if (isCapped() && len > (int)_mapper->getMax()) {
        return StatusWith<RecordId>(ErrorCodes::BadValue, "object to insert exceeds cappedMaxSize");
    }
    persistent_ptr<InitData> obj;
    uint64_t id = 0;
    try {
        transaction::exec_tx(mapPool, [&] {
            obj = pmemobj_tx_alloc(sizeof(InitData::size) + len, 1);
            obj->size = len;
            memcpy(obj->data, data, len);
        });
    } catch (std::exception &e) {
        log() << e.what();
    }
    if(obj == nullptr)
        return StatusWith<RecordId>(ErrorCodes::InternalError,
                                    "Not allocated memory!");
    id = _mapper->insert(obj);
    if(!id)
        return StatusWith<RecordId>(ErrorCodes::OperationFailed,
                                    "Null record Id!");
    txn->recoveryUnit()->registerChange(new InsertChange(_mapper, RecordId(id)));
    deleteCappedAsNeeded(txn);
    while(_mapper->dataSize() > _storageSize) {
        _storageSize =  _storageSize + baseSize;
    }
    return StatusWith<RecordId>(RecordId(id));
}

Status PmseRecordStore::updateRecord(OperationContext* txn, const RecordId& oldLocation,
                                     const char* data, int len, bool enforceQuota,
                                     UpdateNotifier* notifier) {
    persistent_ptr<InitData> obj;
    try {
        transaction::exec_tx(mapPool, [&] {
            obj = pmemobj_tx_alloc(sizeof(InitData::size) + len, 1);
            obj->size = len;
            memcpy(obj->data, data, len);
            _mapper->updateKV(oldLocation.repr(), obj, txn);
            deleteCappedAsNeeded(txn);
        });
    } catch (std::exception &e) {
        log() << e.what();
        return Status(ErrorCodes::BadValue, e.what());
    }
    while(_mapper->dataSize() > _storageSize) {
        _storageSize =  _storageSize + baseSize;
    }
    return Status::OK();
}

void PmseRecordStore::deleteRecord(OperationContext* txn,
                                      const RecordId& dl) {
    _mapper->remove((uint64_t) dl.repr(), txn);
}

void PmseRecordStore::setCappedCallback(CappedCallback* cb) {
    _cappedCallback = cb;
}

void PmseRecordStore::temp_cappedTruncateAfter(OperationContext* txn, RecordId end,
                                               bool inclusive) {
    PmseRecordCursor cursor(_mapper, true);
    auto rec = cursor.seekExact(end);
    if(!inclusive)
        rec = cursor.next();
    while(rec != boost::none) {
        RecordId id(rec->id);
        RecordData data(rec->data);
        rec = cursor.next();
        if(_cappedCallback) {
            deleteRecord(txn, id);
            _cappedCallback->aboutToDeleteCapped(txn, id, data);
        }
    }
}

bool PmseRecordStore::findRecord(OperationContext* txn, const RecordId& loc,
                                    RecordData* rd) const {
    persistent_ptr<InitData> obj;
    if(_mapper->find((uint64_t) loc.repr(), obj)){
        invariant(obj != nullptr);
        *rd = RecordData(obj->data, obj->size);
        return true;
    }
    return false;
}

void PmseRecordStore::deleteCappedAsNeeded(OperationContext* txn) {
    while(_mapper->isCapped() && _mapper->removalIsNeeded()) {
        uint64_t idToDelete = _mapper->getCappedFirstId();
        RecordId id(idToDelete);
        RecordData data;
        findRecord(txn, id, &data);
        _mapper->remove(idToDelete);
        uassertStatusOK(_cappedCallback->aboutToDeleteCapped(txn, id, data));
    }
}

Status PmseRecordStore::insertRecordsWithDocWriter(OperationContext* txn,
                                          const DocWriter* const* docs,
                                          size_t nDocs,
                                          RecordId* idsOut) {
    // TODO(kfilipek): Implement insertRecordsWithDocWriter
    log() << "Not implemented: insertRecordsWithDocWriter";
    return Status::OK();
}

void PmseRecordStore::waitForAllEarlierOplogWritesToBeVisible(OperationContext* txn) const {
    // TODO(kfilipek): Implement insertRecordsWithDocWriter
    log() << "Not implemented: waitForAllEarlierOplogWritesToBeVisible";
}

PmseRecordCursor::PmseRecordCursor(persistent_ptr<PmseMap<InitData>> mapper, bool forward) : _forward(forward), _lastMoveWasRestore(false) {
    _mapper = mapper;
    _before = nullptr;
    _cur = nullptr;
}

void PmseRecordCursor::moveToNext(bool inNext) {
    auto cursor = _cur;
    auto listNumber = (actual == -1 ? 0 : static_cast<int64_t>(actual));
    if(cursor != nullptr) {
        if(cursor->next != nullptr) {
            cursor = cursor->next;
        } else {
            cursor = nullptr;
            while(cursor == nullptr && listNumber < _mapper->_size) {
                listNumber++;
                persistent_ptr<KVPair> head = _mapper->getFirstPtr(listNumber);
                if(head != nullptr) {
                    cursor = head;
                    break;
                }
            }
        }
    } else { //cursor == nullptr
        while(cursor == nullptr && listNumber < _mapper->_size) {
            persistent_ptr<KVPair> head = _mapper->getFirstPtr(listNumber);
            if(head != nullptr) {
                cursor = head;
                break;
            }
            listNumber++;
        }
    }

    if(inNext) {
        _cur = cursor;
        actual = listNumber;
    } else {
        _restorePoint = cursor;
        _actualAfterRestore = listNumber;
    }
}

boost::optional<Record> PmseRecordCursor::next() {
    if(_eof)
        return boost::none;
    if(_lastMoveWasRestore) {
        _lastMoveWasRestore = false;
    } else {
        if(_forward)
            moveToNext();
        else
            moveBackward();
    }
    if(_cur == nullptr || _eof) {
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
    if(_cur == nullptr || _cur->ptr == nullptr) {
        return boost::none;
    }
    obj = _cur->ptr;
    if (!status || !obj) {
        return boost::none;
    }
    RecordId a(id.repr());
    RecordData b(obj->data, obj->size);
    return {{a,b}};
}

void PmseRecordCursor::save() {
    moveToNext(false);
}

bool PmseRecordCursor::restore() {
    if(_mapper->isCapped() && _restorePoint == nullptr) {
        _eof = true;
        return false;
    }
    if(_eof)
        return true;
    if(_restorePoint == nullptr) {
        _eof = true;
        return true;
    }
    if(_cur == nullptr) {
        _cur = _restorePoint;
        return true;
    }
    if(!_mapper->hasId(_cur->idValue)) {
        _cur = _restorePoint;
        _restorePoint = nullptr;
        _lastMoveWasRestore = true;
        actual = _actualAfterRestore;
    }
    return true;
}

void PmseRecordCursor::saveUnpositioned() {
    _eof = true;
}

void PmseRecordCursor::moveToLast() {
    if (_mapper->isCapped()) {
        _cur = _mapper->getFirstPtr(0);
        if(_cur && !_forward) {
            while(_cur->next) {
                _cur = _cur->next;
            }
        }
    } else {
        int64_t lastNonEmpty = -1;
        int64_t scope = (actual < 0 ? _mapper->_size : static_cast<int64_t>(actual));
        for (int64_t i = 0; i < scope; ++i) {
            if (_mapper->_list[i]->size())
                lastNonEmpty = i;
        }
        if(lastNonEmpty == -1) {
            _eof = true;
            _cur = nullptr;
            actual = -1;
            return;
        }
        _cur = _mapper->getFirstPtr(lastNonEmpty);
        if(_cur && !_forward) {
            while(_cur->next) {
                _cur = _cur->next;
            }
        }
        actual = lastNonEmpty;
    }
}

void PmseRecordCursor::moveBackward() {
    if(!_eof && _cur) {
        if (_mapper->isCapped()) {
            auto temp = _mapper->getFirstPtr(0);
            _before = nullptr;
            if(temp && !_forward) {
                while(temp->next && temp != _cur) {
                    _before = temp;
                    temp = temp->next;
                }
            }
        } else {
            if (_cur == _mapper->getFirstPtr(actual)) {
                if (actual <= 0) {
                    _before = nullptr;
                    _eof = true;
                } else {
                    moveToLast();
                    _before = _cur;
                }
            } else {
                auto temp = _mapper->getFirstPtr(actual);
                _before = nullptr;
                if(temp && !_forward) {
                    while(temp->next && temp != _cur) {
                        _before = temp;
                        temp = temp->next;
                    }
                }
            }
        }
        _cur = _before;
    } else {
        moveToLast();
    }
    if(_cur == nullptr) {
        _eof = true;
    }
}
}

