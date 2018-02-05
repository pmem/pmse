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

#include <memory>
#include <string>

#include "mongo/platform/basic.h"
#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/json.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/kv/kv_prefix.h"
#include "mongo/db/storage/sorted_data_interface_test_harness.h"
#include "mongo/db/storage/kv/kv_engine_test_harness.h"
#include "mongo/db/storage/record_store_test_harness.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"

#include "mongo/db/modules/pmse/src/pmse_record_store.h"
#include "mongo/db/modules/pmse/src/pmse_recovery_unit.h"
#include "mongo/db/modules/pmse/src/pmse_sorted_data_interface.h"

#include <libpmemobj.h>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

namespace mongo {

class PmseSortedDataInterfaceHarnessHelper final
    : public SortedDataInterfaceHarnessHelper {
 public:
    PmseSortedDataInterfaceHarnessHelper() : _dbpath("psmem_0") {
    }

    ~PmseSortedDataInterfaceHarnessHelper() final {
    }

    std::unique_ptr<SortedDataInterface> newSortedDataInterface(
        bool unique) final {
        std::string ns = "test.pmse";
        OperationContextNoop opCtx(newRecoveryUnit().release());
        BSONObj spec;

        spec = BSON("key" << BSON("a" << 1) << "name"
                          << "testIndex"
                          << "ns" << ns << "unique" << unique);

        IndexDescriptor desc(NULL, "", spec);

        std::map<std::string, pool_base> pool_handler;

        return stdx::make_unique<PmseSortedDataInterface>(
            "pool_test", &desc, _dbpath.path() + "/", &pool_handler);
    }

    std::unique_ptr<RecoveryUnit> newRecoveryUnit() final {
        return stdx::make_unique<PmseRecoveryUnit>();
    }

 private:
    unittest::TempDir _dbpath;
};

std::unique_ptr<HarnessHelper> makeHarnessHelper() {
    return stdx::make_unique<PmseSortedDataInterfaceHarnessHelper>();
}

MONGO_INITIALIZER(RegisterHarnessFactory)(InitializerContext* const) {
    mongo::registerHarnessHelperFactory(makeHarnessHelper);
    return Status::OK();
}
}  // namespace mongo
