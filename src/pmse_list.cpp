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
 * pmstorekvmapper.cpp
 *
 *  Created on: Mar 24, 2016
 *      Author: kfilipek
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "pmse_list.h"

#include <exception>
#include <string>
#include <vector>

#include "mongo/stdx/mutex.h"

namespace mongo {

void PmseList::insertKV(const char key[], const  char value[]) {
    stdx::lock_guard<nvml::obj::mutex> lock(_pmutex);
    transaction::exec_tx(pool_obj, [this, key, value] {
        persistent_ptr<KVPair> pair;
        try {
             pair = make_persistent<KVPair>();
        } catch (std::exception &e) {
            std::cout << "Can't allocate memory in pmstore_list" << std::endl;
        }
        struct _values temp;
        strcpy(temp.id, key);
        strcpy(temp.value, value);
        pair->kv = temp;
        pair->next = nullptr;
        if (head != nullptr) {
            tail->next = pair;
            tail = pair;
        } else {
            head = pair;
            tail = head;
        }
    });
}

void PmseList::deleteKV(const char key[]) {
    stdx::lock_guard<nvml::obj::mutex> lock(_pmutex);
    auto before = head;
    for (auto rec = head; rec != nullptr; rec = rec->next) {
        if (strcmp(rec->kv.get_ro().id, key) == 0) {
            if (before != head) {
                before->next = rec->next;
                if (before->next == nullptr) {
                    tail = before;
                }
                before.flush();
            } else {
                if (head == rec) {
                    head = rec->next;
                } else {
                    before->next = rec->next;
                }
                if (head == nullptr) {
                    tail = head;
                }
            }
            pmemobj_free(rec.raw_ptr());
            break;
        } else {
            before = rec;
        }
    }
}

bool PmseList::hasKey(const char key[]) {
    for (auto rec = head; rec != nullptr; rec = rec->next) {
        if (strcmp(rec->kv.get_ro().id, key) == 0) {
            return true;
        }
    }
    return false;
}

std::vector<std::string> PmseList::getKeys() {
    stdx::lock_guard<nvml::obj::mutex> lock(_pmutex);
    std::vector<std::string> names;
    for (auto rec = head; rec != nullptr; rec = rec->next) {
        names.push_back(rec->kv.get_ro().id);
    }
    return names;
}

std::string PmseList::findFirstValue(const char value[]) {
    for (auto rec = head; rec != nullptr; rec = rec->next) {
        if (strcmp(rec->kv.get_ro().value, value) == 0) {
            return rec->kv.get_ro().id;
        }
    }
    return "";
}

const char* PmseList::find(const char key[], bool &status) {
    stdx::lock_guard<nvml::obj::mutex> lock(_pmutex);
    for (auto rec = head; rec != nullptr; rec = rec->next) {
        if (strcmp(rec->kv.get_ro().id, key) == 0) {
            status = true;
            return rec->kv.get_ro().value;
        }
    }
    status = false;
    return "";
}

void PmseList::update(const char key[], const char value[]) {
    stdx::lock_guard<nvml::obj::mutex> lock(_pmutex);
    for (auto rec = head; rec != nullptr; rec = rec->next) {
        if (strcmp(rec->kv.get_ro().id, key) == 0) {
            struct _values temp;
            strcpy(temp.id, key);
            strcpy(temp.value, value);
            rec->kv = temp;
            return;
        }
    }
}

void PmseList::clear() {
    stdx::lock_guard<nvml::obj::mutex> lock(_pmutex);
    if (!head)
        return;
    transaction::exec_tx(pool_obj, [this] {
        for (auto rec = head; rec != nullptr; rec = rec->next) {
            auto temp = rec->next;
            pmemobj_tx_free(rec.raw());
            rec = temp;
        }
        head = nullptr;
    });
}

void PmseList::setPool(pool<ListRoot> pool_obj) {
    this->pool_obj = pool_obj;
}

bool PmseList::isAfterSafeShutdown() {
    return _afterSafeShutdown;
}

void PmseList::resetState() {
    transaction::exec_tx(pool_obj, [this] {
        _afterSafeShutdown = false;
    });
}

void PmseList::safeShutdown() {
    transaction::exec_tx(pool_obj, [this] {
        _afterSafeShutdown = true;
    });
}

}  // namespace mongo
