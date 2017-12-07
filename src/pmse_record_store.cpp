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

#include "pmse_change.h"
#include "pmse_record_store.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/system/error_code.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/transaction.hpp>

#include <cstdlib>
#include <map>
#include <string>
#include <utility>

#include "mongo/db/storage/record_store.h"
#include "mongo/util/log.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/operation_context.h"

using nvml::obj::transaction;

namespace mongo {

PmseRecordStore::PmseRecordStore(StringData ns,
                                 StringData ident,
                                 const CollectionOptions& options,
                                 StringData dbpath,
                                 std::map<std::string, pool_base> *pool_handler,
                                 bool recoveryNeeded)
    : RecordStore(ns), _cappedCallback(nullptr),
      _options(options), _dbPath(dbpath) {
    log() << "ns: " << ns;
    if (pool_handler->count(ident.toString()) > 0) {
        _mapPool = pool<root>((*pool_handler)[ident.toString()]);
    } else {
        std::string filepath = _dbPath.toString() + ident.toString();
        boost::filesystem::path path;
        log() << filepath;
        if (ns.toString() == "local.startup_log" &&
            boost::filesystem::exists(filepath)) {
            log() << "Delete old startup log";
            boost::filesystem::remove_all(filepath);
        }

        std::string mapper_filename = _dbPath.toString() + ident.toString();
        if (!boost::filesystem::exists(mapper_filename.c_str())) {
            try {
                _mapPool = pool<root>::create(mapper_filename, "pmse_mapper",
                                              (isSystemCollection(ns) ? 600 : 600)
                                              * PMEMOBJ_MIN_POOL, 0664);
            } catch (std::exception &e) {
                log() << "Error handled: " << e.what();
                throw;
            }
        } else {
            try {
                _mapPool = pool<root>::open(mapper_filename, "pmse_mapper");
            } catch (std::exception &e) {
                log() << "Error handled: " << e.what();
                throw;
            }
        }
        pool_handler->insert(std::pair<std::string, pool_base>(ident.toString(),
                                                               _mapPool));
    }

    auto mapper_root = _mapPool.get_root();

    if (!mapper_root->kvmap_root_ptr) {
        transaction::exec_tx(_mapPool, [mapper_root, options, ns] {
            mapper_root->kvmap_root_ptr = make_persistent<PmseMap<InitData>>(options.capped,
                                                                             options.cappedMaxDocs,
                                                                             options.cappedSize,
                                                                             isSystemCollection(ns));
        });
        _mapper = mapper_root->kvmap_root_ptr;
        _mapper->initialize(true);
    } else {
        _mapper = mapper_root->kvmap_root_ptr;
        if (_mapper->isInitialized()) {
            _mapper->initialize(false);
        } else {
            _mapper->initialize(true);
        }
        transaction::exec_tx(_mapPool, [this, recoveryNeeded] {
            if (recoveryNeeded) {
                _mapper->recover();
            } else {
                _mapper->restoreCounters();
            }
        });
    }
}

StatusWith<RecordId> PmseRecordStore::insertRecord(OperationContext* txn,
                                                   const char* data,
                                                   int len,
                                                   Timestamp timestamp,
                                                   bool enforceQuota) {
    if (isCapped() && len > static_cast<int>(_mapper->getMax())) {
        return StatusWith<RecordId>(ErrorCodes::BadValue,
                                    "object to insert exceeds cappedMaxSize");
    }
    persistent_ptr<InitData> obj;
    uint64_t id = 0;
    try {
        transaction::exec_tx(_mapPool, [this, &obj, len, data, &id] {
            obj = pmemobj_tx_alloc(sizeof(InitData::size) + len, 1);
            obj->size = len;
            memcpy(obj->data, data, len);
            id = _mapper->insert(obj);
        });
    } catch (std::exception &e) {
        log() << "RecordStore: " << e.what();
        return StatusWith<RecordId>(ErrorCodes::OperationFailed,
                                    "Insert record error");
    }
    if (!id)
        return StatusWith<RecordId>(ErrorCodes::OperationFailed,
                                    "Null record Id!");
    _mapper->changeSize(len);
    txn->recoveryUnit()->registerChange(new InsertChange(_mapper, RecordId(id), len));
    deleteCappedAsNeeded(txn);
    while (_mapper->dataSize() > _storageSize) {
        _storageSize =  _storageSize + baseSize;
    }
    return StatusWith<RecordId>(RecordId(id));
}

Status PmseRecordStore::updateRecord(OperationContext* txn, const RecordId& oldLocation,
                                     const char* data, int len, bool enforceQuota,
                                     UpdateNotifier* notifier) {
    persistent_ptr<InitData> obj;
    stdx::lock_guard<nvml::obj::mutex> lock(_mapper->_listMutex[oldLocation.repr() % _mapper->getHashmapSize()]);
    try {
        transaction::exec_tx(_mapPool, [&obj, len, data, txn, oldLocation, this] {
            obj = pmemobj_tx_alloc(sizeof(InitData::size) + len, 1);
            obj->size = len;
            memcpy(obj->data, data, len);
            _mapper->updateKV(oldLocation.repr(), obj, txn);
            _mapper->changeSize(obj->size - len);
            deleteCappedAsNeeded(txn);
        });
    } catch (std::exception &e) {
        log() << e.what();
        return Status(ErrorCodes::BadValue, e.what());
    }
    while (_mapper->dataSize() > _storageSize) {
        _storageSize =  _storageSize + baseSize;
    }
    return Status::OK();
}

void PmseRecordStore::deleteRecord(OperationContext* txn,
                                   const RecordId& dl) {
    stdx::lock_guard<nvml::obj::mutex> lock(_mapper->_listMutex[dl.repr() % _mapper->getHashmapSize()]);
    persistent_ptr<KVPair> p;
    if (_mapper->getPair(dl.repr(), &p)) {
        _mapper->remove((uint64_t) dl.repr(), txn);
        _mapper->changeSize(-p->ptr->size);
    }
}

void PmseRecordStore::setCappedCallback(CappedCallback* cb) {
    _cappedCallback = cb;
}

void PmseRecordStore::cappedTruncateAfter(OperationContext* txn, RecordId end,
                                          bool inclusive) {
    PmseRecordCursor cursor(_mapper, true);
    auto rec = cursor.seekExact(end);
    if (!inclusive)
        rec = cursor.next();
    while (rec != boost::none) {
        RecordId id(rec->id);
        RecordData data(rec->data);
        rec = cursor.next();
        if (_cappedCallback) {
            deleteRecord(txn, id);
            _cappedCallback->aboutToDeleteCapped(txn, id, data);
        }
    }
}

bool PmseRecordStore::findRecord(OperationContext* txn, const RecordId& loc,
                                 RecordData* rd) const {
    persistent_ptr<InitData> obj;
    if (_mapper->find((uint64_t) loc.repr(), &obj)) {
        invariant(obj != nullptr);
        *rd = RecordData(obj->data, obj->size);
        return true;
    }
    return false;
}

void PmseRecordStore::deleteCappedAsNeeded(OperationContext* txn) {
    while (_mapper->isCapped() && _mapper->removalIsNeeded()) {
        uint64_t idToDelete = _mapper->getCappedFirstId();
        RecordId id(idToDelete);
        RecordData data;
        findRecord(txn, id, &data);
        _mapper->remove(idToDelete);
        _mapper->changeSize(-data.size());
        uassertStatusOK(_cappedCallback->aboutToDeleteCapped(txn, id, data));
    }
}

Status PmseRecordStore::insertRecordsWithDocWriter(OperationContext* txn,
                                                   const DocWriter* const* docs,
                                                   const Timestamp* timestamps,
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

PmseRecordCursor::PmseRecordCursor(persistent_ptr<PmseMap<InitData>> mapper, bool forward)
    : _forward(forward),
      _lastMoveWasRestore(false) {
    _mapper = mapper;
    _before = nullptr;
    _cur = nullptr;
}

void PmseRecordCursor::moveToNext(bool inNext) {
    auto cursor = _cur;
    auto listNumber = (_actualListNumber == -1 ? 0 : static_cast<int64_t>(_actualListNumber));
    if (listNumber != -1 && _lastMoveWasRestore) {  // Cursor points to wrong position
        _lastMoveWasRestore = false;
        auto item = _mapper->getFirstPtr(_actualListNumber);
        while (item != nullptr) {  // Try to find next element
            if (item->position >= _position) {
                _cur = item;
                break;
            }
            item = item->next;
        }
        if (item == nullptr) {
            while (item == nullptr && listNumber < _mapper->_size) {
                persistent_ptr<KVPair> head = _mapper->getFirstPtr(listNumber);
                if (head != nullptr) {
                    item = head;
                    break;
                }
                listNumber++;
            }
        }
        cursor = item;
    } else {  // When nothing change with underlying data cursor can fast jump
        if (cursor != nullptr) {
            if (cursor->next != nullptr) {
                cursor = cursor->next;
            } else {
                cursor = nullptr;
                while (cursor == nullptr && listNumber < _mapper->_size) {
                    listNumber++;
                    persistent_ptr<KVPair> head = _mapper->getFirstPtr(listNumber);
                    if (head != nullptr) {
                        cursor = head;
                        break;
                    }
                }
            }
        } else {  // cursor == nullptr
            while (cursor == nullptr && listNumber < _mapper->_size) {
                persistent_ptr<KVPair> head = _mapper->getFirstPtr(listNumber);
                if (head != nullptr) {
                    cursor = head;
                    break;
                }
                listNumber++;
            }
        }
    }

    if (inNext) {
        _cur = cursor;
        _actualListNumber = listNumber;
    }
    if (_cur != nullptr)
        _position = _cur->position;
    else
        _eof = true;
}

Status PmseRecordStore::validate(OperationContext* txn,
                                 ValidateCmdLevel level,
                                 ValidateAdaptor* adaptor,
                                 ValidateResults* results,
                                 BSONObjBuilder* output) {
    int64_t nInvalid = 0;
    int64_t nRecords = 0;
    uint64_t totalDataSize = 0;

    auto cursor = getCursor(txn, true);
    boost::optional<Record> record;
    Status status = Status::OK();
    while (record = cursor->next()) {
        auto dataSize = record->data.size();
        totalDataSize += dataSize;
        ++nRecords;
        size_t validatedSize;
        status = adaptor->validate(record->id, record->data, &validatedSize);
        if (!status.isOK() || static_cast<size_t>(dataSize) != validatedSize) {
            if (results->valid) {
                results->errors.push_back("detected one or more invalid documents (see logs)");
                results->valid = false;
            }
            ++nInvalid;
            log() << "document: RecordId(" << record->id << ") is corruped";
        }
    }

    if (level == kValidateFull)
        output->append("nInvalidDocuments", nInvalid);

    output->appendNumber("nrecords", _mapper->fillment());
    return Status::OK();
}

boost::optional<Record> PmseRecordCursor::next() {
    if (_eof)
        return boost::none;
    if (_forward)
        moveToNext();
    else
        moveBackward();
    if (_cur == nullptr || _eof) {
        _eof = true;
        return boost::none;
    }
    _position = _cur->position;
    RecordId a((int64_t) _cur->idValue);
    RecordData b(_cur->ptr->data, _cur->ptr->size);
    return {{a, b}};
}

boost::optional<Record> PmseRecordCursor::seekExact(const RecordId& id) {
    persistent_ptr<InitData> obj = nullptr;
    bool status = _mapper->getPair(id.repr(), &_cur);
    if (_cur == nullptr || _cur->ptr == nullptr) {
        return boost::none;
    }
    obj = _cur->ptr;
    if (!status || !obj) {
        return boost::none;
    }
    _position = _cur->position;
    RecordId a(id.repr());
    RecordData b(obj->data, obj->size);
    return {{a, b}};
}

void PmseRecordCursor::save() {
    _positionCheck = true;
}

bool PmseRecordCursor::restore() {
    if (_positionCheck) {
        _positionCheck = false;
        if (checkPosition() && _cur != nullptr && _cur->isDeleted)
            _lastMoveWasRestore = true;
    }
    if (_mapper->isCapped()) {
        _eof = true;
        return false;
    }
    if (_eof)
        return true;
    return true;
}

void PmseRecordCursor::saveUnpositioned() {
    _eof = true;
}

void PmseRecordCursor::moveToLast() {
    if (_mapper->isCapped()) {
        _cur = _mapper->getFirstPtr(0);
        if (_cur && !_forward) {
            while (_cur->next) {
                _cur = _cur->next;
            }
        }
    } else {
        int64_t lastNonEmpty = -1;
        int64_t scope = (_actualListNumber < 0 ? _mapper->_size : static_cast<int64_t>(_actualListNumber));
        for (int64_t i = 0; i < scope; ++i) {
            if (_mapper->_list[i].size())
                lastNonEmpty = i;
        }
        if (lastNonEmpty == -1) {
            _eof = true;
            _cur = nullptr;
            _actualListNumber = -1;
            return;
        }
        _cur = _mapper->getFirstPtr(lastNonEmpty);
        if (_cur && !_forward) {
            while (_cur->next) {
                _cur = _cur->next;
            }
        }
        _actualListNumber = lastNonEmpty;
    }
}

void PmseRecordCursor::moveBackward() {
    if (!_eof && _cur) {
        if (_mapper->isCapped()) {
            auto temp = _mapper->getFirstPtr(0);
            _before = nullptr;
            if (temp && !_forward) {
                while (temp->next && temp != _cur) {
                    _before = temp;
                    temp = temp->next;
                }
            }
        } else {
            if (_cur == _mapper->getFirstPtr(_actualListNumber)) {
                if (_actualListNumber <= 0) {
                    _before = nullptr;
                    _eof = true;
                } else {
                    moveToLast();
                    _before = _cur;
                }
            } else {
                auto temp = _mapper->getFirstPtr(_actualListNumber);
                _before = nullptr;
                if (temp && !_forward) {
                    while (temp->next && temp != _cur) {
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
    if (_cur == nullptr) {
        _eof = true;
    }
}

bool PmseRecordCursor::checkPosition() {
    if (_cur != nullptr && _cur->position != _position) {  // Can come back to list, but with another pos
        return false;
    }
    return true;
}

bool PmseRecordStore::isSystemCollection(const StringData& ns) {
    return ns.toString() == "local.startup_log" ||
           ns.toString() == "admin.system.version" ||
           ns.toString() == "_mdb_catalog";
}

}  // namespace mongo

