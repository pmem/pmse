# PMSE - Persistent Memory Storage Engine for MongoDB

## What is PMSE?
Persistent Memory Storage Engine is fully compatible with NVDIMM’s alternative storage engine for MongoDB, that empower optimal usage of persistent memory. Storage engine doesn’t do any snapshots or journaling, everything is immediately stored as persistent. 

## Dependencies
-	[Persistent Memory Development Kit (PMDK)](https://github.com/pmem/pmdk)
-	[MongoDB](https://github.com/mongodb/mongo)

## Building
Our engine is compatible with MongoDB 3.5.13, so please checkout to proper tag: **git checkout r3.5.13**
Be sure you have satisfied all dependencies for MongoDB and PMSE, especially PIP requirements and PMDK.
First, try to compile MongoDB using this: https://github.com/mongodb/mongo/wiki/Build-Mongodb-From-Source 
To use PMSE module with MongoDB, in the mongo repository directory do the following:
```
cd ~/mongo
mkdir -p src/mongo/db/modules/
ln -sf ~/pmse src/mongo/db/modules/pmse
```
Then you can compile:
```
scons LIBPATH=path_to_libraries --dbg=off --opt=on core
```
Some operating systems have newer version of GCC so you shoud use GCC-5, for this purpose use CC and CXX flags for scons:
```
scons CC=gcc-5 CXX=g++-5 LIBPATH=/usr/local/lib --dbg=off --opt=on core
```

Typical library path for PMDK is /usr/local/lib/ or /usr/local/lib64/ and it depends on system you use. 
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
