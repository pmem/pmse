# PMSE - Persistent Memory Storage Engine for MongoDB

## What is PMSE?
Persistent Memory Storage Engine is fully compatible with NVDIMM’s alternative storage engine for MongoDB, that empower optimal usage of persistent memory. Storage engine doesn’t do any snapshots or journaling, everything is immediately stored as persistent. 

## Dependencies
-	[Persistent Memory Development Kit (PMDK)](https://github.com/pmem/pmdk)
-	[MongoDB](https://github.com/mongodb/mongo)

## Building
To use this module, in the mongo repository directory do the following:
```
mkdir -p src/mongo/db/modules/
ln -sf ~/pmse src/mongo/db/modules/pmse
```
Then you can compile:
```
scons LIBPATH=path_to_libraries --dbg=off --opt=on core
```
Typical library path is /usr/local/lib/ and it depends on system you use.
To clean after building:
```
scons –c
```

## Tips for building
To speed up build time with **–j** option and number of threads, behavior is the same as make.
Also user can specify what need to build, typical configuration need only core features to run server and client, possible are: **core**, **all**, **unittests**.

## Server running
The last thing you need is to replace the path to persistent memory or DAX device:
```
./mongod --storageEngine=pmse --dbpath=/path/to/pm_device
```

## Benchmarking
If you want to do some benchmarks just go to the utils folder and read README.md file.

## Authors
-	Adrian Nidzgorski
-	Jakub Schmiegel
-	Krzysztof Filipek
-	Maciej Maciejewski
