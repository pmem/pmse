## Persistent Memory Storage Engine

This module is designed to natively support operations on Persistent Memory within MongoDB

To use this module, in the `mongo` repository directory do the following:

    mkdir -p src/mongo/db/modules/
    ln -sf ~/pmse src/mongo/db/modules/pmse

To build you will need to run

    scons

To clean:
    scons -c

Start `mongod` using the `--storageEngine=pmse` option and `--dbpath=xxx`, where xxx is path to mounted DAX device.

