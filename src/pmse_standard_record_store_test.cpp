/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
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
