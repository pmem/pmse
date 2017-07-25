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

/*
 * pmstore_map.h
 *
 *  Created on: Sep 22, 2016
 *      Author: kfilipek
 */

#ifndef SRC_PMSE_MAP_H_
#define SRC_PMSE_MAP_H_

#include "pmse_list_int_ptr.h"
#include "pmse_change.h"

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/detail/pexceptions.hpp>

#include <limits>

namespace mongo {

const uint64_t CAPPED_SIZE = 1;
const uint64_t HASHMAP_SIZE = 1'000'000u;

class PmseRecordCursor;

template<typename T>
class PmseMap {
    friend PmseRecordCursor;

 public:
    PmseMap() = default;

    PmseMap(bool isCapped, uint64_t maxDoc, uint64_t sizeOfColl, uint64_t size = HASHMAP_SIZE)
        : _size(isCapped ? CAPPED_SIZE : size), _isCapped(isCapped) {
        _maxDocuments = maxDoc;
        _sizeOfCollection = sizeOfColl;
        try {
            _list = make_persistent<persistent_ptr<PmseListIntPtr>[]>(_size);
        } catch (std::exception &e) {
            std::cout << "PmseMap: " << e.what() << std::endl;
        }
    }

    ~PmseMap() = default;

    uint64_t insert(persistent_ptr<T> value) {
        auto id = getNextId();
        if (!id) {
            return 0;
        }
        stdx::lock_guard<nvml::obj::mutex> lock(_listMutex[id->idValue % _size]);
        if (!insertKV(id, value)) {
            return 0;
        }
        ++_hashmapSize;
        return id->idValue;
    }

    uint64_t getCappedFirstId() {
        if (isCapped())
            return getFirstPtr(0)->idValue;
        return 0;
    }

    bool removalIsNeeded() {
        if (isCapped()) {
            if ((uint64_t)_dataSize > _sizeOfCollection) {
                return true;
            }
            if ((_maxDocuments != 0) && (_list[0]->_size > _maxDocuments))  // number of items exceed
                return true;
        }
        return false;
    }

    bool insertKV(const persistent_ptr<KVPair> &id, persistent_ptr<T> value) {  // internal use
        try {
            _list[id->idValue % _size]->insertKV(id, value);
        } catch (std::exception &e) {
            std::cout << "KVMapper: " << e.what() << std::endl;
            return false;
        }
        return true;  // correctly added
    }

    bool insertToFrontKV(const persistent_ptr<KVPair> &id, persistent_ptr<T> value) {  // internal use
        try {
            _list[id->idValue % _size]->insertKV(id, value, true);
        } catch (std::exception &e) {
            std::cout << "KVMapper: " << e.what() << std::endl;
            return false;
        }
        return true;  // correctly added
    }

    bool updateKV(uint64_t id, persistent_ptr<T> value, OperationContext* txn = nullptr) {
        try {
            _list[id % _size]->update(id, value, txn);
        } catch (std::exception &e) {
            std::cout << "KVMapper: " << e.what() << std::endl;
            return false;
        }
        return true;
    }

    bool hasId(uint64_t id) {
        return _list[id % _size]->hasKey(id);
    }

    bool find(uint64_t id, persistent_ptr<T> *value) {
        return _list[id % _size]->find(id, value);
    }

    bool getPair(uint64_t id, persistent_ptr<KVPair> *value) {
        return _list[id % _size]->getPair(id, value);
    }

    bool remove(uint64_t id, OperationContext* txn = nullptr) {
        _hashmapSize--;
        persistent_ptr<KVPair> toDeleted;
        _list[id % _size]->deleteKV(id, toDeleted, txn);
        moveToDeleted(toDeleted, _deleted);
        return true;
    }

    void initialize(bool firstRun) {
        pop = pool_by_vptr(this);
        for (int i = 0; i < _size; i++) {
            if (firstRun) {
                try {
                    transaction::exec_tx(pop, [this, i] {
                        _list[i] = make_persistent<PmseListIntPtr>();
                    });
                } catch(std::exception &e) {
                    std::cout << e.what() << std::endl;
                }
            }
            _list[i]->setPool();
        }
    }

    void deinitialize() {
        // TODO(kfilipek): deallocate all resources
    }

    uint64_t fillment() {
        if (_isCapped)
            return _list[0]->size();
        return _hashmapSize;
    }

    bool truncate(OperationContext* txn) {
        bool status = true;
        try {
            transaction::exec_tx(pop, [this, txn] {
                for (int i = 0; i < _size; i++) {
                    _list[i]->clear(txn, this);
                    if (txn)
                        txn->recoveryUnit()->registerChange(new DropListChange(pop, _list, i));
                    delete_persistent<PmseListIntPtr>(_list[i]);
                    _list[i] = nullptr;
                }
                initialize(true);
            });
            _counter = 0;
            _hashmapSize = 0;
            _dataSize = 0;
        } catch (nvml::transaction_alloc_error &e) {
            std::cout << e.what() << std::endl;
            status = false;
        } catch (nvml::transaction_scope_error &e) {
            std::cout << e.what() << std::endl;
            status = false;
        }
        return status;
    }

    int64_t dataSize() {
        return _dataSize;
    }

    void changeSize(uint64_t size) {
        _dataSize += size;
    }

    bool isCapped() const {
        return _isCapped;
    }

    uint64_t getMax() const {
        return _sizeOfCollection;
    }

    uint64_t getMaxSize() const {
        return _maxDocuments;
    }

    void moveToDeleted(persistent_ptr<KVPair> &item, persistent_ptr<KVPair> &list) {
        stdx::lock_guard<nvml::obj::mutex> guard(_pmutex);
        if (list != nullptr) {
            item->next = list;
            item->isDeleted = true;
            list = item;
        } else {
            item->next = nullptr;
            item->isDeleted = true;
            list = item;
        }
    }

    int getHashmapSize() {
        return _size;
    }

    nvml::obj::mutex _listMutex[HASHMAP_SIZE];

 private:
    const int _size;
    const bool _isCapped;
    pool_base pop;
    p<uint64_t> _dataSize = 0;
    p<uint64_t> _counter = 0;
    p<uint64_t> _hashmapSize = 0;
    p<uint64_t> _maxDocuments;
    p<uint64_t> _sizeOfCollection;
    persistent_ptr<persistent_ptr<PmseListIntPtr>[]> _list;

    nvml::obj::mutex _pmutex;
    persistent_ptr<KVPair> _deleted;

    persistent_ptr<KVPair> getFirstPtr(int listNumber) {
        if (listNumber < _size)
            return _list[listNumber]->_head;
        return {};
    }

    persistent_ptr<KVPair> getNextId() {
            persistent_ptr<KVPair> temp = nullptr;
            if (_deleted == nullptr) {
                if (_counter != std::numeric_limits<uint64_t>::max()-1) {
                    this->_counter++;
                    try {
                        transaction::exec_tx(pop, [this, &temp] {
                            temp = make_persistent<KVPair>();
                        });
                        temp->idValue = _counter;
                    } catch (std::exception &e) {
                        std::cout << "Next id generation: " << e.what() << std::endl;
                        return nullptr;
                    }
                } else {
                    return nullptr;
                }
            } else {
                stdx::lock_guard<nvml::obj::mutex> guard(_pmutex);
                temp = _deleted;
                _deleted = _deleted->next;
                temp->isDeleted = false;
                return temp;
            }
            return temp;
        }
};

struct root {
    persistent_ptr<PmseMap<InitData>> kvmap_root_ptr;
};
}  // namespace mongo
#endif  // SRC_PMSE_MAP_H_
