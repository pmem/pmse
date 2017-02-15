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

#ifndef SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_CHANGE_H_
#define SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_CHANGE_H_

#include "libpmem.h"
#include "libpmemobj.h"
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

#include "pmse_map.h"
#include "pmse_tree.h"

#include "mongo/db/record_id.h"
#include "mongo/db/index/index_descriptor.h"

namespace mongo {

class InsertChange : public RecoveryUnit::Change
{
public:
    InsertChange(persistent_ptr<PmseMap<InitData>> mapper, RecordId loc);
    virtual void rollback();
    virtual void commit();
private:
    persistent_ptr<PmseMap<InitData>> _mapper;
    const RecordId _loc;
};

class RemoveChange : public RecoveryUnit::Change
{
public:
    RemoveChange(pool_base pop, InitData* data);
    ~RemoveChange();
    virtual void rollback();
    virtual void commit();
private:
    pool_base _pop;
    InitData *_cachedData;
    persistent_ptr<PmseMap<InitData>> _mapper;

};

class UpdateChange : public RecoveryUnit::Change
{
public:
    UpdateChange(pool_base pop, uint64_t key, InitData* data);
    ~UpdateChange();
    virtual void rollback();
    virtual void commit();
private:
    pool_base _pop;
    uint64_t _key;
    InitData *_cachedData;
    persistent_ptr<PmseMap<InitData>> _mapper;

};

class InsertIndexChange : public RecoveryUnit::Change
{
public:
    InsertIndexChange(persistent_ptr<PmseTree> tree,pool<PmseTree> pm_pool,BSONObj key,RecordId loc,bool dupsAllowed,const IndexDescriptor* desc);
    virtual void rollback();
    virtual void commit();
private:
    persistent_ptr<PmseTree> _tree;
    pool<PmseTree> _pm_pool;
    BSONObj _key;
    RecordId _loc;
    bool _dupsAllowed;
    const IndexDescriptor*_desc;

};

}
#endif /* SRC_MONGO_DB_MODULES_PMSTORE_SRC_PMSE_CHANGE_H_ */