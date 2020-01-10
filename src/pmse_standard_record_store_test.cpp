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

#include <cstdlib>
#include <memory>
#include <string>

#include "mongo/platform/basic.h"
#include "mongo/base/checked_cast.h"
#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/json.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/kv/kv_engine_test_harness.h"
#include "mongo/db/storage/kv/kv_prefix.h"
#include "mongo/db/storage/record_store_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/clock_source_mock.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/scopeguard.h"

#include "mongo/db/modules/pmse/src/pmse_engine.h"
#include "mongo/db/modules/pmse/src/pmse_record_store.h"
#include "mongo/db/modules/pmse/src/pmse_recovery_unit.h"

#include <libpmemobj.h>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

namespace mongo {

using std::unique_ptr;
using std::string;

class PmseHarnessHelper final : public RecordStoreHarnessHelper {
 public:
    PmseHarnessHelper() : _dbpath("psmem_0"), _engine(_dbpath.path()) {
    }

    ~PmseHarnessHelper() {
    }

    virtual std::unique_ptr<RecordStore> newNonCappedRecordStore() {
        return newNonCappedRecordStore("a.b");
    }

    virtual std::unique_ptr<RecordStore> newNonCappedRecordStore(const std::string& ns) {
        PmseRecoveryUnit* ru =
            dynamic_cast<PmseRecoveryUnit*>(_engine.newRecoveryUnit());
        OperationContextNoop opCtx(ru);
        string uri = "table:" + ns;

        CollectionOptions options;
        options.capped = false;
        options.cappedSize = -1;
        options.cappedMaxDocs = -1;

        std::map<std::string, pool_base> pool_handler;
        auto ret = stdx::make_unique<PmseRecordStore>(
            ns, "pool_test", options, _dbpath.path() + "/", &pool_handler);

        return std::move(ret);
    }

    virtual std::unique_ptr<RecordStore> newCappedRecordStore(
        int64_t cappedSizeBytes, int64_t cappedMaxDocs) final {
        return newCappedRecordStore("a.b", cappedSizeBytes, cappedMaxDocs);
    }

    virtual std::unique_ptr<RecordStore> newCappedRecordStore(
        const std::string& ns, int64_t cappedMaxSize, int64_t cappedMaxDocs) {
        PmseRecoveryUnit* ru =
            dynamic_cast<PmseRecoveryUnit*>(_engine.newRecoveryUnit());
        OperationContextNoop opCtx(ru);
        string uri = "table:a.b";

        CollectionOptions options;
        options.capped = true;
        options.cappedSize = cappedMaxSize;
        options.cappedMaxDocs = cappedMaxDocs;

        std::map<std::string, pool_base> pool_handler;
        auto ret = stdx::make_unique<PmseRecordStore>(
            ns, "pool_test", options, _dbpath.path() + "/", &pool_handler);

        return std::move(ret);
    }

    virtual std::unique_ptr<RecoveryUnit> newRecoveryUnit() final {
        return std::unique_ptr<RecoveryUnit>(_engine.newRecoveryUnit());
    }

    virtual bool supportsDocLocking() final {
        return true;
    }

 private:
    unittest::TempDir _dbpath;
    ClockSourceMock _cs;

    PmseEngine _engine;
};

std::unique_ptr<HarnessHelper> makeHarnessHelper() {
    return stdx::make_unique<PmseHarnessHelper>();
}

MONGO_INITIALIZER(RegisterHarnessFactory)(InitializerContext* const) {
    mongo::registerHarnessHelperFactory(makeHarnessHelper);
    return Status::OK();
}

}  // mongo
