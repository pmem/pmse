# -*- mode: python -*-
Import("env")

env = env.Clone()

env.InjectMongoIncludePaths()

env.Library(
    target= 'storage_pmse_base',
    source= [
        'src/pmse_engine.cpp',
        'src/pmse_record_store.cpp',
        'src/pmse_list_int_ptr.cpp',
        'src/pmse_list.cpp',
        'src/pmse_sorted_data_interface.cpp',
        'src/pmse_tree.cpp',
        'src/pmse_index_cursor.cpp',
        'src/pmse_recovery_unit.cpp',
        'src/pmse_change.cpp'
        ],
    LIBDEPS= [
        '$BUILD_DIR/mongo/base',
        '$BUILD_DIR/mongo/db/namespace_string',
        '$BUILD_DIR/mongo/db/catalog/collection_options',
        '$BUILD_DIR/mongo/db/storage/ephemeral_for_test/ephemeral_for_test_record_store',
        '$BUILD_DIR/mongo/db/storage/kv/kv_storage_engine',

        ],
    SYSLIBDEPS=[
        "pmem",
        "pmemobj",
         ]
    )

env.Library(
    target='storage_pmse',
    source=[
        'src/pmse_init.cpp',
    ],
    LIBDEPS=[
        'storage_pmse_base',
    ],
    LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/db/serveronly']
)

env.CppUnitTest(
    target='pmse_init_test',
    source=['src/pmse_init_test.cpp'],
	LIBDEPS=['$BUILD_DIR/mongo/db/serveronly']
)

env.CppUnitTest(
    target= 'pmse_engine_test',
    source= [
        'src/pmse_engine_test.cpp'
    ],
	LIBDEPS= [
        '$BUILD_DIR/mongo/db/storage/kv/kv_engine_test_harness',
        '$BUILD_DIR/mongo/s/client/sharding_client',
        '$BUILD_DIR/mongo/base',
        '$BUILD_DIR/mongo/db/bson/dotted_path_support',
        '$BUILD_DIR/mongo/db/catalog/collection',
        '$BUILD_DIR/mongo/db/catalog/collection_options',
        '$BUILD_DIR/mongo/db/concurrency/lock_manager',
        '$BUILD_DIR/mongo/db/concurrency/write_conflict_exception',
        '$BUILD_DIR/mongo/db/index/index_descriptor',
        '$BUILD_DIR/mongo/db/mongod_options',
        '$BUILD_DIR/mongo/db/namespace_string',
        '$BUILD_DIR/mongo/db/repl/repl_settings',
        '$BUILD_DIR/mongo/db/server_options_core',
        '$BUILD_DIR/mongo/db/service_context',
        '$BUILD_DIR/mongo/db/storage/index_entry_comparison',
        '$BUILD_DIR/mongo/db/storage/journal_listener',
        '$BUILD_DIR/mongo/db/storage/key_string',
        '$BUILD_DIR/mongo/db/storage/kv/kv_prefix',
        '$BUILD_DIR/mongo/db/storage/oplog_hack',
        '$BUILD_DIR/mongo/db/storage/storage_options',
        '$BUILD_DIR/mongo/util/concurrency/ticketholder',
        '$BUILD_DIR/mongo/util/elapsed_tracker',
        '$BUILD_DIR/mongo/util/processinfo',
        'storage_pmse_base'
    ]
)

env.Library(
    target= 'additional_pmse_record_store_tests',
    source= [
        'src/pmse_record_store_test.cpp',
    ],
    LIBDEPS= [
        '$BUILD_DIR/mongo/base',
        '$BUILD_DIR/mongo/db/bson/dotted_path_support',
        '$BUILD_DIR/mongo/db/catalog/collection',
        '$BUILD_DIR/mongo/db/catalog/collection_options',
        '$BUILD_DIR/mongo/db/concurrency/lock_manager',
        '$BUILD_DIR/mongo/db/concurrency/write_conflict_exception',
        '$BUILD_DIR/mongo/db/index/index_descriptor',
        '$BUILD_DIR/mongo/db/mongod_options',
        '$BUILD_DIR/mongo/db/namespace_string',
        '$BUILD_DIR/mongo/db/repl/repl_settings',
        '$BUILD_DIR/mongo/db/server_options_core',
        '$BUILD_DIR/mongo/db/service_context',
        '$BUILD_DIR/mongo/db/storage/index_entry_comparison',
        '$BUILD_DIR/mongo/db/storage/journal_listener',
        '$BUILD_DIR/mongo/db/storage/key_string',
        '$BUILD_DIR/mongo/db/storage/kv/kv_prefix',
        '$BUILD_DIR/mongo/db/storage/oplog_hack',
        '$BUILD_DIR/mongo/db/storage/storage_options',
        '$BUILD_DIR/mongo/util/concurrency/ticketholder',
        '$BUILD_DIR/mongo/util/elapsed_tracker',
        '$BUILD_DIR/mongo/util/processinfo',
        'storage_pmse_base',
        '$BUILD_DIR/mongo/db/storage/kv/kv_engine_core',
        '$BUILD_DIR/mongo/db/storage/record_store_test_harness',
        '$BUILD_DIR/mongo/util/clock_source_mock'
    ]
)

env.CppUnitTest(
    target= 'storage_pmse_record_store_test',
    source= [
        'src/pmse_standard_record_store_test.cpp'
    ],
    LIBDEPS= [
        'additional_pmse_record_store_tests'
    ]
)

env.CppUnitTest(
    target= 'pmse_sorted_data_interface_test',
    source= [
        'src/pmse_sorted_data_interface_test.cpp',
    ],
    LIBDEPS= [
        '$BUILD_DIR/mongo/db/storage/kv/kv_engine_core',
        '$BUILD_DIR/mongo/db/storage/sorted_data_interface_test_harness',
        '$BUILD_DIR/mongo/s/client/sharding_client',
        '$BUILD_DIR/mongo/base',
        '$BUILD_DIR/mongo/db/bson/dotted_path_support',
        '$BUILD_DIR/mongo/db/catalog/collection',
        '$BUILD_DIR/mongo/db/catalog/collection_options',
        '$BUILD_DIR/mongo/db/concurrency/lock_manager',
        '$BUILD_DIR/mongo/db/concurrency/write_conflict_exception',
        '$BUILD_DIR/mongo/db/index/index_descriptor',
        '$BUILD_DIR/mongo/db/mongod_options',
        '$BUILD_DIR/mongo/db/namespace_string',
        '$BUILD_DIR/mongo/db/repl/repl_settings',
        '$BUILD_DIR/mongo/db/server_options_core',
        '$BUILD_DIR/mongo/db/service_context',
        '$BUILD_DIR/mongo/db/storage/index_entry_comparison',
        '$BUILD_DIR/mongo/db/storage/journal_listener',
        '$BUILD_DIR/mongo/db/storage/key_string',
        '$BUILD_DIR/mongo/db/storage/kv/kv_prefix',
        '$BUILD_DIR/mongo/db/storage/oplog_hack',
        '$BUILD_DIR/mongo/db/storage/storage_options',
        '$BUILD_DIR/mongo/util/concurrency/ticketholder',
        '$BUILD_DIR/mongo/util/elapsed_tracker',
        '$BUILD_DIR/mongo/util/processinfo',
        'storage_pmse_base'
    ]
)
