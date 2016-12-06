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

/*
 * pmstore_map.h
 *
 *  Created on: Sep 22, 2016
 *      Author: kfilipek
 */

#ifndef SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_MAP_H_
#define SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_MAP_H_

#include "pmse_list_int_ptr.h"
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_array.hpp>

using namespace nvml::obj;

namespace mongo {

const uint64_t CAPPED_SIZE = 1;

class PmseRecordCursor;

template<typename T>
class PmseMap {
    friend PmseRecordCursor;
public:
    PmseMap() = default;

    PmseMap(bool isCapped, uint64_t maxDoc, uint64_t sizeOfColl, uint64_t size = 1000)
            : _size(isCapped ? CAPPED_SIZE : size), _isCapped(isCapped) {
        _maxDocuments = maxDoc;
        _sizeOfCollection = sizeOfColl;
        _list = make_persistent<persistent_ptr<PmseListIntPtr>[]>(_size);
    }

    ~PmseMap() = default;

    uint64_t insert(persistent_ptr<T> value) {
        uint64_t id = getNextId();
        if (!insertKV(id, value)) {
            return -1;
        }
        _hashmapSize++;
        return id;
    }

    bool insertKV(int id, persistent_ptr<T> value) { //internal use
        if (_isCapped) {
            if (!hasId(id)) {
                _list[id % _size]->insertKV_capped(id, value, _isCapped,
                                                   _maxDocuments, _sizeOfCollection);
            } else
                return false;
        } else {
            if (!hasId(id)) {
                _list[id % _size]->insertKV(id, value);
            } else {
                return false;
            }
        }
        return true; //correctly added
    }

    bool updateKV(uint64_t id, persistent_ptr<T> value) {
        if (hasId(id)) {
            _list[id % _size]->update(id, value);
        } else {
            return false;
        }
        return true;
    }

    bool hasId(uint64_t id) {
        return _list[id % _size]->hasKey(id);
    }

    bool find(uint64_t id, persistent_ptr<T> *value) {
        persistent_ptr<InitData> obj;
        if (_list[id % _size]->find(id, obj)) {
            *value = obj;
            return true;
        }
        *value = nullptr;
        return false;
    }

    bool remove(uint64_t id) {
        _list[id % _size]->deleteKV(id, _deleted);
        _hashmapSize--;
        return true;
    }

    void initialize(bool firstRun) {
        for (int i = 0; i < _size; i++) {
            if (firstRun)
                _list[i] = make_persistent<PmseListIntPtr>();
            _list[i]->setPool();
        }
    }

    void deinitialize() {
        //TODO: deallocate all resources
    }

    uint64_t fillment() {
        return _hashmapSize;
    }

private:
    const int _size;
    const bool _isCapped;
    p<uint64_t> _counter = 0;
    p<uint64_t> _hashmapSize = 0;
    p<uint64_t> _maxDocuments;
    p<uint64_t> _sizeOfCollection;
    p<uint64_t> _counterCapped = 0;
    persistent_ptr<persistent_ptr<PmseListIntPtr>[]> _list;
    persistent_ptr<KVPair> _deleted;

    persistent_ptr<KVPair> getFirstPtr(int listNumber) {
        if (listNumber < _size)
            return _list[listNumber]->head;
        return {};
    }

    uint64_t getNextId() {
        if(_deleted != nullptr) {
            pool_base pop;
            pop = pool_by_vptr(this);
            auto temp = _deleted;
            uint64_t id = 0;
            transaction::exec_tx(pop, [&] {
                id = _deleted->idValue;
                _deleted = _deleted->next;
                delete_persistent<KVPair>(temp);
            });
            return id;
        } else {
            if(_counter != std::numeric_limits<uint64_t>::max()-1)
                this->_counter++;
            else
                return 0;
        }
        return _counter;
    }
};
}
#endif /* SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_MAP_H_ */
