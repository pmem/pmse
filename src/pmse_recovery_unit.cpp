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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/util/log.h"

#include "pmse_recovery_unit.h"

namespace mongo {

void PmseRecoveryUnit::commitUnitOfWork() {
    try {
        auto end = _changes.end();
        for (auto it = _changes.begin(); it != end; ++it) {
            (*it)->commit();
        }
        _changes.clear();
    } catch (...) {
        throw;
    }
}

void PmseRecoveryUnit::abortUnitOfWork() {
    try {
        auto end = _changes.rend();
        for (auto it = _changes.rbegin(); it != end; ++it) {
            (*it)->rollback();
        }
        _changes.clear();
    } catch (...) {
        throw;
    }
}
void PmseRecoveryUnit::beginUnitOfWork(OperationContext* opCtx) {}

bool PmseRecoveryUnit::waitUntilDurable() {
    return true;
}

void PmseRecoveryUnit::abandonSnapshot() {}

SnapshotId PmseRecoveryUnit::getSnapshotId() const {
    return SnapshotId();
}

void PmseRecoveryUnit::registerChange(Change* change) {
    _changes.push_back(ChangePtr(change));
}

void* PmseRecoveryUnit::writingPtr(void* data, size_t len) {
    return nullptr;
}

void PmseRecoveryUnit::setRollbackWritesDisabled() {}

}  // namespace mongo
