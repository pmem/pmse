## Crash tests for MongoDB & PMSE

There are two ways to run crash tests:

Run single test using a test specific file:

	./crashtest_insert_01.sh
	
Run main script with test parameters:

	./run_test.sh -i 1
	
There are three test groups:

	-i for insert
	-u for update
	-d for delete
	
Main script can also run all tests one by one with parameter -a[ll]:

    ./run_test.sh -a

Default dbpath is:

	/mnt/psmem_0/

It is possible to specify a different dbpath by parameter -m:

	./run_test.sh -m /my/db/path/ -i 1

or

	./crashtest_insert_01.sh -m /my/db/path/

There are also log files in 'log' directory, each test will generate two files, for example in case of insert 01;

	crashtest_insert_01_gdb_log.txt
	crashtest_insert_01_shell_log.txt

The first one with 'gdb' in filename contains log from gdb and mongod server.
Second file with 'shell' in filename contains log from mongo shell.