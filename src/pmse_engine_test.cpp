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

#include "mongo/platform/basic.h"

#include <libpmemobj.h>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include "mongo/base/init.h"
#include "mongo/db/modules/pmse/src/pmse_engine.h"
#include "mongo/db/modules/pmse/src/pmse_record_store.h"
#include "mongo/db/storage/kv/kv_engine_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/util/clock_source_mock.h"

namespace mongo {
namespace {

class PmseKVHarnessHelper : public KVHarnessHelper {
 public:
  PmseKVHarnessHelper() : _dbpath("psmem_0") {
    _engine.reset(new PmseEngine(_dbpath.path() + "/"));
  }

  virtual ~PmseKVHarnessHelper() {
    _engine.reset(NULL);
  }

  virtual KVEngine* restartEngine() {
    _engine.reset(NULL);
    _engine.reset(new PmseEngine(_dbpath.path() + "/"));
    return _engine.get();
  }

  virtual KVEngine* getEngine() {
    return _engine.get();
  }

 private:
  const std::unique_ptr<ClockSource> _cs = stdx::make_unique<ClockSourceMock>();
  unittest::TempDir _dbpath;
  std::unique_ptr<PmseEngine> _engine;
};

std::unique_ptr<KVHarnessHelper> makeHelper() {
  return stdx::make_unique<PmseKVHarnessHelper>();
}

MONGO_INITIALIZER(RegisterKVHarnessFactory)(InitializerContext*) {
  KVHarnessHelper::registerFactory(makeHelper);
  return Status::OK();
}

}  // namespace
}  // namespace mongo
