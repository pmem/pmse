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
 * pmstorekvmapper.h
 *
 *  Created on: Mar 24, 2016
 *      Author: kfilipek
 */


#ifndef SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_LIST_H_
#define SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_LIST_H_

#include <vector>

#include <libpmemobj.h>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/transaction.hpp>

#include <mutex>

using namespace nvml::obj;

namespace mongo {

struct _values {
    char id[256];
    char value[256];
};

class PmseList {
public:
    struct _pair {
        p<struct _values> kv;
        persistent_ptr<_pair> next;
    };
    typedef struct _pair KVPair;
    PmseList(pool<PmseList> obj) : pool_obj(obj) {}
    ~PmseList() {};
    void insertKV(const char key[], const  char value[]);
    void deleteKV(const char key[]);
    void update(const char key[], const char value[]);
    bool hasKey(const char key[]);
    std::vector<std::string> getKeys();
    const char* find(const char key[], bool &status);
    void clear();
    void setPool(pool<PmseList> pool_obj);
private:
    persistent_ptr<KVPair> head;
    persistent_ptr<KVPair> tail;
    p<uint64_t> counter;
    pool<PmseList> pool_obj;
    std::mutex _pmutex;
    PmseList() {}
};

}
#endif /* SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_LIST_H_ */
