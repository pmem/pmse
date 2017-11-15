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
        source=['src/pmse_init_test.cpp',
                ]
        )
		
env.CppUnitTest(
        target='pmse_engine_test',
        source=['src/pmse_engine_test.cpp',
                ]
        )
		
env.CppUnitTest(
        target='pmse_record_store_test',
        source=['src/pmse_record_store_test.cpp',
                ]
        )
		
env.CppUnitTest(
        target='pmse_sorted_data_interface_test',
        source=['src/pmse_sorted_data_interface_test.cpp',
                ]
        )