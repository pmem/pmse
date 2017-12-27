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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include <libpmemobj++/transaction.hpp>

#include "mongo/db/operation_context.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/util/log.h"

#include "pmse_change.h"
#include "pmse_map.h"

namespace mongo {

TruncateChange::TruncateChange(pool_base pop, PmseMap<InitData> *mapper,
                               RecordId Id, InitData *data, uint64_t dataSize)
        : _mapper(mapper), _Id(Id), _pop(pop), _dataSize(dataSize) {
    _cachedData = static_cast<InitData*>(malloc(sizeof(InitData) + data->size));
    memcpy(_cachedData->data, data->data, data->size);
    _cachedData->size = data->size;
}

void TruncateChange::commit() {}

void TruncateChange::rollback() {
    persistent_ptr<InitData> obj;
    persistent_ptr<KVPair> temp = nullptr;
    try {
       transaction::exec_tx(_pop, [this, &obj, &temp] {
           obj = pmemobj_tx_alloc(sizeof(InitData::size) + _cachedData->size, 1);
           obj->size = _cachedData->size;
           memcpy(obj->data, _cachedData->data, _cachedData->size);
           temp = make_persistent<KVPair>();
           temp->idValue = static_cast<uint64_t>(_Id.repr());
       });
    } catch (std::exception &e) {
       log() << e.what();
    }
    _mapper->insertToFrontKV(temp, obj);
    _mapper->changeSize(_dataSize);
}

DropListChange::DropListChange(pool_base pop, persistent_ptr<PmseListIntPtr[]> list, int size)
        : _pop(pop), _list(list), _size(size) {}

void DropListChange::commit() {}

void DropListChange::rollback() {
    try {
        make_persistent_atomic<PmseListIntPtr[]>(_pop, _list, _size);
    } catch(std::exception &e) {
        log() << e.what();
    }
}

InsertChange::InsertChange(persistent_ptr<PmseMap<InitData>> mapper,
                           RecordId loc, uint64_t dataSize)
    : _mapper(mapper), _loc(loc), _dataSize(dataSize) {}

void InsertChange::commit() {}

void InsertChange::rollback() {
    _mapper->remove((uint64_t) _loc.repr());
    _mapper->changeSize(-_dataSize);
}

RemoveChange::RemoveChange(pool_base pop, InitData* data, uint64_t dataSize)
    : _pop(pop), _dataSize(dataSize) {
    _cachedData = static_cast<InitData*>(malloc(sizeof(InitData) + data->size));
    memcpy(_cachedData->data, data->data, data->size);
    _cachedData->size = data->size;
}
RemoveChange::~RemoveChange() {
    free(_cachedData);
}
void RemoveChange::commit() {}
void RemoveChange::rollback() {
    persistent_ptr<InitData> obj;
    _mapper = pool<root>(_pop).get_root()->kvmap_root_ptr;
    try {
        transaction::exec_tx(_pop, [this, &obj] {
            obj = pmemobj_tx_alloc(sizeof(InitData::size) + _cachedData->size, 1);
            obj->size = _cachedData->size;
            memcpy(obj->data, _cachedData->data, _cachedData->size);
        });
    } catch (std::exception &e) {
        log() << e.what();
    }
    _mapper->insert(obj);
    _mapper->changeSize(_dataSize);
}

UpdateChange::UpdateChange(pool_base pop, uint64_t key, InitData* data, uint64_t dataSize)
        : _pop(pop), _key(key), _dataSize(dataSize) {
    _cachedData = static_cast<InitData*>(malloc(sizeof(InitData) + data->size));
    memcpy(_cachedData->data, data->data, data->size);
    _cachedData->size = data->size;
}
UpdateChange::~UpdateChange() {
    free(_cachedData);
}
void UpdateChange::commit() {}
void UpdateChange::rollback() {
    persistent_ptr<InitData> obj;
    try {
        _mapper = pool<root>(_pop).get_root()->kvmap_root_ptr;
        transaction::exec_tx(_pop, [this, &obj] {
            obj = pmemobj_tx_alloc(sizeof(InitData::size) + _cachedData->size, 1);
            obj->size = _cachedData->size;
            memcpy(obj->data, _cachedData->data, _cachedData->size);
        });
        _mapper->updateKV(_key, obj);
        auto rd = RecordData(obj->data, obj->size);
        _mapper->changeSize(rd.size() - _dataSize);
    } catch (std::exception &e) {
        log() << e.what();
    }
}
InsertIndexChange::InsertIndexChange(persistent_ptr<PmseTree> tree,
                                     pool_base pop, BSONObj key,
                                     RecordId loc, bool dupsAllowed,
                                     const IndexDescriptor* desc)
        : _tree(tree), _pop(pop), _key(key), _loc(loc),
          _dupsAllowed(dupsAllowed), _desc(desc) {}

void InsertIndexChange::commit() {}

void InsertIndexChange::rollback() {
    try {
        transaction::exec_tx(_pop, [this] {
            IndexKeyEntry entry(_key.getOwned(), _loc);
            _tree->remove(_pop, entry, _dupsAllowed, _desc->keyPattern());
        });
    } catch (std::exception &e) {
        log() << e.what();
    }
}

RemoveIndexChange::RemoveIndexChange(pool_base pop, BSONObj key, RecordId loc,
                                     bool dupsAllowed, BSONObj ordering)
        : _pop(pop), _key(key), _loc(loc),
          _dupsAllowed(dupsAllowed), _ordering(ordering) {}
void RemoveIndexChange::commit() {}
void RemoveIndexChange::rollback() {
    Status status = Status::OK();
    IndexKeyEntry entry(_key.getOwned(), _loc);
    status = _tree->insert(_pop, entry, _ordering, _dupsAllowed);
}

}  // namespace mongo
