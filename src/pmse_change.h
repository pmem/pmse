/*
 * Copyright 2014-2020, Intel Corporation
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

#ifndef SRC_PMSE_CHANGE_H_
#define SRC_PMSE_CHANGE_H_

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

#include "pmse_list_int_ptr.h"
#include "pmse_tree.h"

#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/record_data.h"
#include "mongo/db/record_id.h"

namespace mongo {
template<typename T>
class PmseMap;

class TruncateChange: public RecoveryUnit::Change {
 public:
    TruncateChange(pool_base pop, PmseMap<InitData> *mapper, RecordId Id, InitData *data, uint64_t dataSize);
    virtual void rollback();
    virtual void commit();
 private:
    PmseMap<InitData> *_mapper;
    RecordId _Id;
    InitData *_cachedData;
    pool_base _pop;
    persistent_ptr<KVPair> _key;
    uint64_t _dataSize;
};

class DropListChange: public RecoveryUnit::Change {
 public:
    DropListChange(pool_base pop, persistent_ptr<PmseListIntPtr[]> list, int size);
    virtual void rollback();
    virtual void commit();
 private:
    pool_base _pop;
    persistent_ptr<PmseListIntPtr[]> _list;
    int _size;
};

class InsertChange : public RecoveryUnit::Change {
 public:
    InsertChange(persistent_ptr<PmseMap<InitData>> mapper, RecordId loc, uint64_t dataSize);
    virtual void rollback();
    virtual void commit();
 private:
    persistent_ptr<PmseMap<InitData>> _mapper;
    const RecordId _loc;
    uint64_t _dataSize;
};

class RemoveChange : public RecoveryUnit::Change {
 public:
    RemoveChange(pool_base pop, InitData* data, uint64_t dataSize);
    ~RemoveChange();
    virtual void rollback();
    virtual void commit();
 private:
    pool_base _pop;
    InitData *_cachedData;
    uint64_t _dataSize;
    persistent_ptr<PmseMap<InitData>> _mapper;
};

class UpdateChange : public RecoveryUnit::Change {
 public:
    UpdateChange(pool_base pop, uint64_t key, InitData* data, uint64_t dataSize);
    ~UpdateChange();
    virtual void rollback();
    virtual void commit();
 private:
    pool_base _pop;
    uint64_t _key;
    InitData *_cachedData;
    uint64_t _dataSize;
    persistent_ptr<PmseMap<InitData>> _mapper;
};

class InsertIndexChange : public RecoveryUnit::Change {
 public:
    InsertIndexChange(persistent_ptr<PmseTree> tree, pool_base pop,
                      BSONObj key, RecordId loc, bool dupsAllowed,
                      const IndexDescriptor* desc);
    virtual void rollback();
    virtual void commit();
 private:
    persistent_ptr<PmseTree> _tree;
    pool_base _pop;
    BSONObj _key;
    RecordId _loc;
    bool _dupsAllowed;
    const IndexDescriptor*_desc;
};

class RemoveIndexChange : public RecoveryUnit::Change {
 public:
    RemoveIndexChange(persistent_ptr<PmseTree> tree, pool_base pop, BSONObj key, RecordId loc,
                      bool dupsAllowed, BSONObj ordering);
    virtual void rollback();
    virtual void commit();
 private:
    persistent_ptr<PmseTree> _tree;
    pool_base _pop;
    BSONObj _key;
    RecordId _loc;
    bool _dupsAllowed;
    BSONObj _ordering;
};

}  // namespace mongo
#endif  // SRC_PMSE_CHANGE_H_
