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
 * pmstore_list_int_ptr.cpp
 *
 *  Created on: Sep 22, 2016
 *      Author: kfilipek
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "pmse_list_int_ptr.h"
#include "pmse_change.h"

#include "mongo/db/storage/recovery_unit.h"

#include <exception>
#include <mutex>

#include "mongo/util/log.h"

namespace mongo {

PmseListIntPtr::PmseListIntPtr() : counter(1) {
    pop = pool_by_vptr(this);
}

PmseListIntPtr::~PmseListIntPtr() {
}

void PmseListIntPtr::setPool() {
    pop = pool_by_vptr(this);
}

uint64_t PmseListIntPtr::size() {
    return _size;
}

void PmseListIntPtr::insertKV(const persistent_ptr<KVPair> &key,
                              const persistent_ptr<InitData> &value, bool insertToFront) {
    try {
        transaction::exec_tx(pop, [this, &key, &value, insertToFront] {
            if (insertToFront){
                key->ptr = value;
                key->next = nullptr;
                if(head != nullptr){
                    key->next = head;
                    head = key;
                }
                else{
                    head = key;
                    tail = head;
                }
                _size++;
            } else {
                key->ptr = value;
                key->next = nullptr;
                if (head != nullptr) {
                    tail->next = key;
                    tail = key;
                } else {
                    head = key;
                    tail = head;
                }
                _size++;
            }
        });
    } catch (std::exception &e) {
        log() << "KVMapper: " << e.what();
    }
}

int64_t PmseListIntPtr::deleteKV(uint64_t key,
                                 persistent_ptr<KVPair> &deleted,
                                 OperationContext* txn) {
    auto before = head;
    int64_t sizeFreed = 0;
    for (auto rec = head; rec != nullptr; rec = rec->next) {
        if (rec->idValue == key) {
            transaction::exec_tx(pop, [this, &deleted, &before,
                                       &sizeFreed, &rec, &txn] {
                if (before != head) {
                    before->next = rec->next;
                    if (before->next == nullptr)
                        tail = before;
                    before.flush();
                } else {
                    if (head == rec) {
                        head = rec->next;
                    } else {
                        before->next = rec->next;
                        if (rec->next != nullptr) {
                            tail = rec->next;
                        } else {
                            tail = before;
                        }
                    }
                    if (head == nullptr) {
                        tail = head;
                    }
                }
                _size--;
                deleted = rec;
                if (txn) {
                    auto rd = RecordData(deleted->ptr->data, deleted->ptr->size);
                    txn->recoveryUnit()->registerChange(new RemoveChange(pop, (deleted->ptr).get(), rd.size()));
                }
                sizeFreed = pmemobj_alloc_usable_size(deleted->ptr.raw());
                delete_persistent<InitData>(deleted->ptr);
            });
            break;
        } else {
            before = rec;
        }
    }
    return sizeFreed;
}

bool PmseListIntPtr::hasKey(uint64_t key) {
    for (auto rec = head; rec != nullptr; rec = rec->next) {
        if (rec->idValue == key) {
            return true;
        }
    }
    return false;
}

bool PmseListIntPtr::find(uint64_t key, persistent_ptr<InitData> &item_ptr) {
    for (auto rec = head; rec != nullptr; rec = rec->next) {
        if (rec->idValue == key) {
            item_ptr = rec->ptr;
            return true;
        }
    }
    item_ptr = nullptr;
    return false;
}

bool PmseListIntPtr::getPair(uint64_t key, persistent_ptr<KVPair> &item_ptr) {
    for (auto rec = head; rec != nullptr; rec = rec->next) {
        if (rec->idValue == key) {
            item_ptr = rec;
            return true;
        }
    }
    item_ptr = nullptr;
    return false;
}

void PmseListIntPtr::update(uint64_t key,
                            const persistent_ptr<InitData> &value, OperationContext* txn) {
    for (auto rec = head; rec != nullptr; rec = rec->next) {
        if (rec->idValue == key) {
            if (rec->ptr != nullptr) {
                if (txn) {
                    txn->recoveryUnit()->registerChange(new UpdateChange(pop, key, (rec->ptr).get(), rec->ptr->size));
                }
                try {
                    transaction::exec_tx(pop, [&rec] {
                        delete_persistent<InitData>(rec->ptr);
                    });
                } catch(std::exception &e) {
                    log() << e.what();
                }
            }
            rec->ptr = value;
            return;
        }
    }
}

void PmseListIntPtr::clear(OperationContext* txn, PmseMap<InitData> *_mapper) {
    if (!head)
        return;
    transaction::exec_tx(pop, [this, txn, _mapper] {
        for (auto rec = head; rec != nullptr;) {
            if (txn)
                txn->recoveryUnit()->registerChange(new TruncateChange(pop, _mapper, RecordId(rec->idValue),
                                (rec->ptr).get(), rec->ptr->size));
            auto temp = rec->next;
            delete_persistent<KVPair>(rec);
            rec = temp;
        }
        head = nullptr;
        _size = 0;
    });
}

uint64_t PmseListIntPtr::getNextId() {
    return counter++;
}

}  // namespace mongo
