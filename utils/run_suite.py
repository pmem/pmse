#!/usr/bin/python
import json
import os
import subprocess

#comment
# SUITE write_workload
# THREADS 1 2 4 8 16 32 48 64 96
# JOURNALING enabled/disabled
# RECORDS 1000
# OPERATIONS 100
# READ_PROPORTION 0.0
# UPDATE_PROPORTION 0.0
# INSERT_PROPORTION 1.0
# YCSB_NUMA 1
# DROP_BEFORE
# ENDSUITE

#GET PATHS FROM CONFIG FILE
PATH_TO_MONGO = ''
PATH_TO_YCSB = ''

path_configuration = open("path_configuration.txt", "r")
for line in path_configuration:
    if line.startswith('YCSB_PATH='):
        arg = line.split("=")
        if len(arg) > 1:
            PATH_TO_YCSB = arg[1].replace('\n','')
        else:
            raise NameError('No path in YCSB_PATH!')
    elif line.startswith('MONGO_PATH='):
        arg = line.split("=")
        if len(arg) > 1:
            PATH_TO_MONGO = arg[1].replace('\n','')
        else:
            raise NameError('No path in MONGO_PATH!')
            
if not os.path.isdir(PATH_TO_MONGO):
    raise NameError('Wrong path to MONGO!')
elif not os.path.isdir(PATH_TO_YCSB):
    raise NameError('Wrong path to YCSB!')

class Test:
    def __init__(self):
        self.testName = ""
        self.threads = []
        self.journaling = ""
        self.records = 0
        self.operations = 0
        self.read_proportion = -1.0
        self.update_proportion = -1.0
        self.insert_proportion = -1.0
        self.ycsb_numa = -1
        self.drop_before = -1
        self.create_after_drop = -1
        self.is_load = -1
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
                          sort_keys=True, indent=4)

def getArgs(str):
    arguments = []
    for i in range(1, len(str)):
        arguments.append(str[i])
    return arguments

KEYWORDS = set(["THREADS", "JOURNALING", "RECORDS", "OPERATIONS",
                "READ_PROPORTION", "LOAD",
                "UPDATE_PROPORTION", "INSERT_PROPORTION", "YCSB_NUMA",
                "SUITE", "ENDSUITE", "DROP_BEFORE", "CREATE_AFTER_DROP"]) #Add keyword if you need to extend implementation

# open meta file
with open("test_suite.txt", "r") as configfile:
    configurations = []
    for line in configfile:
        splittedLine = line.split()
        if line == '\n' or line.startswith('#'):
            continue
        if len(set.intersection(KEYWORDS, splittedLine)) != 1:
            print splittedLine
            raise NameError('Too many keywords in single line!')

        #get args if exists
        args = getArgs(splittedLine)
        
        #if line starts from keyword we must read arguments
        if splittedLine[0] == "SUITE":
            configurations.append(Test())
            configurations[len(configurations)-1].testName = args[0]
        elif splittedLine[0] == "THREADS":
            configurations[len(configurations)-1].threads = args
        elif splittedLine[0] == "LOAD":
            configurations[len(configurations)-1].is_load = 1
        elif splittedLine[0] == "JOURNALING":
            if args[0] == "enabled":
                configurations[len(configurations)-1].journaling = "journaled" #according to YCSB documentation
            elif args[0] == "disabled":
                configurations[len(configurations)-1].journaling = "acknowledged" #according to YCSB documentation
            else:
                raise NameError('Unrecognized argument')
        elif splittedLine[0] == "RECORDS":
            configurations[len(configurations)-1].records = args[0]
        elif splittedLine[0] == "OPERATIONS":
            configurations[len(configurations)-1].operations = args[0]
        elif splittedLine[0] == "READ_PROPORTION":
            configurations[len(configurations)-1].read_proportion = args[0]
        elif splittedLine[0] == "UPDATE_PROPORTION":
            configurations[len(configurations)-1].update_proportion = args[0]
        elif splittedLine[0] == "INSERT_PROPORTION":
            configurations[len(configurations)-1].insert_proportion = args[0]
        elif splittedLine[0] == "YCSB_NUMA":
            configurations[len(configurations)-1].ycsb_numa = args[0]
        elif splittedLine[0] == "DROP_BEFORE":
            configurations[len(configurations)-1].drop_before = 1
        elif splittedLine[0] == "CREATE_AFTER_DROP":
            configurations[len(configurations)-1].create_after_drop = 1
        elif splittedLine[0] == "ENDSUITE":
            continue
        else:
            raise NameError('Unrecognized keyword')
configfile.close()

print 'Script read those tests:'
i = 1
for conf in configurations:
    print '{:>20} {:<12}'.format('Test#: ', str(i))
    print '{:>20} {:<12}'.format("Name: ", conf.testName)
    print '{:>20} {:<12}'.format("Threads: " ,str(conf.threads))
    print '{:>20} {:<12}'.format("Journaling: ", conf.journaling)
    print '{:>20} {:<12}'.format("Records: ", conf.records)
    print '{:>20} {:<12}'.format("Operation: ", conf.operations)
    print '{:>20} {:<12}'.format("Read proportion: ", str(conf.read_proportion))
    print '{:>20} {:<12}'.format("Update proportion: ", str(conf.update_proportion))
    print '{:>20} {:<12}'.format("Insert proportion: ", str(conf.insert_proportion))
    print '{:>20} {:<12}'.format("NUMA for YCSB: ", conf.ycsb_numa)
    print ""
    i = i + 1

# PUT CONFIGURATION TO FILE IN PROPER PATH
results_directory = "results/"
if not os.path.exists(results_directory):
    os.makedirs(results_directory)
i = 1
with open(results_directory + '/configurations.json', 'w') as jsonconfig:
    for conf in configurations:
        jsonconfig.write(conf.toJSON() + '\n')
        if not os.path.exists(results_directory + conf.testName + '/'):
                os.makedirs(results_directory + conf.testName + '/')
        with open(results_directory + conf.testName + '/test_description.txt', 'a') as test_description:
            test_description.write('{:>20} {:<12}'.format('Test#: ', str(i)) + '\n') #   'Test #' + str(i)
            test_description.write('{:>20} {:<12}'.format("Name: ", conf.testName) + '\n')
            test_description.write('{:>20} {:<12}'.format("Threads: " ,str(conf.threads)) + '\n')
            test_description.write('{:>20} {:<12}'.format("Journaling: ", conf.journaling) + '\n')
            test_description.write('{:>20} {:<12}'.format("Records: ", conf.records) + '\n')
            test_description.write('{:>20} {:<12}'.format("Operation: ", conf.operations) + '\n')
            test_description.write('{:>20} {:<12}'.format("Read proportion: ", str(conf.read_proportion)) + '\n')
            test_description.write('{:>20} {:<12}'.format("Update proportion: ", str(conf.update_proportion)) + '\n')
            test_description.write('{:>20} {:<12}'.format("Insert proportion: ", str(conf.insert_proportion)) + '\n')
            test_description.write('{:>20} {:<12}'.format("NUMA for YCSB: ", conf.ycsb_numa) + '\n')
            test_description.write('\n')
        i = i + 1

# run specified configurations
generated_commands = []
for test in configurations:
    command_prefix = ''
    command_suffix = ''
    
    command_prefix = './run_workload.sh ' + test.testName
    
    if not test.is_load == 1:
        command_prefix += ' run '
        if test.journaling == 'journaled':
            command_suffix += " true "
        else:
            command_suffix += " false "
    else:
        command_prefix += ' load ' #there is no need to do journaling while load
        command_suffix += ' false '

    command_suffix += test.records + ' ' + test.operations + ' '
    command_suffix += test.read_proportion + ' ' + test.update_proportion + ' ' + test.insert_proportion + ' '
    if test.ycsb_numa == -1:
        print 'NUMA node is not set for test: ' + test.testName + '.'
    command_suffix += test.ycsb_numa

    for thread_no in test.threads:
        # DROP BEFORE LOAD PHASE
        if test.drop_before == 1 or test.create_after_drop == 1 or test.is_load == 1:
            generated_commands.append(PATH_TO_MONGO + 'mongo ' + PATH_TO_MONGO + 'drop_table.js')
        if test.create_after_drop == 1:
            generated_commands.append(PATH_TO_MONGO + 'mongo ' + PATH_TO_MONGO + 'create_table.js')

        # DROP&CREATE BEFORE NEXT INSERTS
        generated_commands.append(command_prefix + thread_no + command_suffix + ' ' + PATH_TO_YCSB)

# Generate script
with open('testplan.sh','w') as testplan:
    testplan.write('#!/bin/bash\n')
    for x in generated_commands:
        testplan.write(x + '\n')
