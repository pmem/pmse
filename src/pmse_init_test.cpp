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

#include "mongo/platform/basic.h"
#include "mongo/db/json.h"
#include "mongo/db/modules/pmse/src/pmse_record_store.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_engine_metadata.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

class PmseEngineFactoryTest : public mongo::unittest::Test {
 private:
    virtual void setUp() {
        ServiceContext* globalEnv = getGlobalServiceContext();
        ASSERT_TRUE(globalEnv);
        ASSERT_TRUE(getGlobalServiceContext()->isRegisteredStorageEngine("pmse"));
        std::unique_ptr<StorageFactoriesIterator> sfi(
            getGlobalServiceContext()->makeStorageFactoriesIterator());
        ASSERT_TRUE(sfi);
        bool found = false;
        while (sfi->more()) {
            const StorageEngine::Factory* currentFactory = sfi->next();
            if (currentFactory->getCanonicalName() == "pmse") {
                found = true;
                factory = currentFactory;
                break;
            }
            found = true;
        }
        ASSERT_TRUE(found);
    }

    virtual void tearDown() {
        factory = NULL;
    }

 protected:
    const StorageEngine::Factory* factory;
};

void _testValidateMetadata(const StorageEngine::Factory* factory,
                           const BSONObj& metadataOptions, bool directoryPerDB,
                           bool directoryForIndexes,
                           ErrorCodes::Error expectedCode) {
    // It is fine to specify an invalid data directory for the metadata
    // as long as we do not invoke read() or write().
    StorageEngineMetadata metadata("no_such_directory");
    metadata.setStorageEngineOptions(metadataOptions);

    StorageGlobalParams storageOptions;
    storageOptions.directoryperdb = directoryPerDB;

    Status status = factory->validateMetadata(metadata, storageOptions);
    if (expectedCode != status.code()) {
        FAIL(str::stream() << "Unexpected StorageEngine::Factory::validateMetadata "
                            "result. Expected: "
                           << ErrorCodes::errorString(expectedCode) << " but got "
                           << status.toString()
                           << " instead. metadataOptions: " << metadataOptions
                           << "; directoryPerDB: " << directoryPerDB);
  }
}

// Do not validate fields that are not present in metadata.
TEST_F(PmseEngineFactoryTest, ValidateMetadataEmptyOptions) {
    _testValidateMetadata(factory, BSONObj(), false, false, ErrorCodes::OK);
    _testValidateMetadata(factory, BSONObj(), false, true, ErrorCodes::OK);
    _testValidateMetadata(factory, BSONObj(), true, false, ErrorCodes::OK);
    _testValidateMetadata(factory, BSONObj(), false, false, ErrorCodes::OK);
}

TEST_F(PmseEngineFactoryTest, ValidateMetadataDirectoryPerDB) {
    _testValidateMetadata(factory, fromjson("{directoryPerDB: 123}"), false,
                            false, ErrorCodes::FailedToParse);
    _testValidateMetadata(factory, fromjson("{directoryPerDB: false}"), false,
                            false, ErrorCodes::OK);
    _testValidateMetadata(factory, fromjson("{directoryPerDB: false}"), true,
                            false, ErrorCodes::InvalidOptions);
    _testValidateMetadata(factory, fromjson("{directoryPerDB: true}"), false,
                            false, ErrorCodes::InvalidOptions);
    _testValidateMetadata(factory, fromjson("{directoryPerDB: true}"), true,
                            false, ErrorCodes::OK);
}

void _testCreateMetadataOptions(const StorageEngine::Factory* factory,
                                bool directoryPerDB, bool directoryForIndexes) {
    StorageGlobalParams storageOptions;
    storageOptions.directoryperdb = directoryPerDB;

    BSONObj metadataOptions = factory->createMetadataOptions(storageOptions);

    BSONElement directoryPerDBElement =
        metadataOptions.getField("directoryPerDB");
    ASSERT_TRUE(directoryPerDBElement.isBoolean());
    ASSERT_EQUALS(directoryPerDB, directoryPerDBElement.boolean());
}

TEST_F(PmseEngineFactoryTest, CreateMetadataOptions) {
    _testCreateMetadataOptions(factory, false, false);
    _testCreateMetadataOptions(factory, false, true);
    _testCreateMetadataOptions(factory, true, false);
    _testCreateMetadataOptions(factory, true, true);
}

}  // namespace mongo
