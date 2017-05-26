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

#ifndef SRC_PMSE_RECOVERY_UNIT_H_
#define SRC_PMSE_RECOVERY_UNIT_H_

#include <vector>

#include "mongo/db/storage/recovery_unit.h"

namespace mongo {

class PmseRecoveryUnit : public RecoveryUnit {
 public:
    PmseRecoveryUnit() = default;

    virtual void beginUnitOfWork(OperationContext* opCtx);

    virtual void commitUnitOfWork();

    virtual void abortUnitOfWork();

    virtual bool waitUntilDurable();

    virtual void abandonSnapshot();

    virtual SnapshotId getSnapshotId() const;

    virtual void registerChange(Change* change);

    virtual void* writingPtr(void* data, size_t len);

    virtual void setRollbackWritesDisabled();

 private:
    typedef std::shared_ptr<Change> ChangePtr;
    typedef std::vector<ChangePtr> Changes;
    Changes _changes;
};

}  // namespace mongo
#endif  // SRC_PMSE_RECOVERY_UNIT_H_
