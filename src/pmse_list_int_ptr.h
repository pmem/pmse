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
 * pmse_list_int_ptr.h
 *
 *  Created on: Sep 22, 2016
 *      Author: kfilipek
 */

#ifndef SRC_PMSE_LIST_INT_PTR_H_
#define SRC_PMSE_LIST_INT_PTR_H_

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

#include <vector>

#include "mongo/db/operation_context.h"

using namespace nvml::obj;

namespace mongo {
struct InitData {
    uint64_t size;
    char data[];
};

struct _pair {
    p<uint64_t> idValue;
    persistent_ptr<InitData> ptr;
    persistent_ptr<_pair> next;
    p<uint64_t> position;
};

template<class InitData>
class PmseMap;

typedef struct _pair KVPair;

class PmseListIntPtr {
    template<typename T>
    friend class PmseMap;
 public:
    PmseListIntPtr();
    ~PmseListIntPtr();
    void insertKV(const persistent_ptr<KVPair> &key,
                  const persistent_ptr<InitData> &value, bool insertToFront = false);
    bool find(uint64_t key, persistent_ptr<InitData> *item_ptr);
    bool getPair(uint64_t key, persistent_ptr<KVPair> *item_ptr);
    void update(uint64_t key, const persistent_ptr<InitData> &value, OperationContext* txn);
    int64_t deleteKV(uint64_t key, persistent_ptr<KVPair> &deleted, OperationContext* txn);
    bool hasKey(uint64_t key);
    void clear(OperationContext* txn, PmseMap<InitData> *_mapper);
    void setPool();
    uint64_t size();
    uint64_t getNextId();

 private:
    persistent_ptr<KVPair> getHead() {
        return head;
    }
    persistent_ptr<KVPair> head;
    persistent_ptr<KVPair> tail;
    persistent_ptr<KVPair> deleted;
    p<uint64_t> _counter;
    p<uint64_t> _size;
    pool_base pop;
    p<uint64_t> actualSizeOfCollection = 0;
};
}  // namespace mongo
#endif  // SRC_PMSE_LIST_INT_PTR_H_
