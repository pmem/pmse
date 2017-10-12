#!/usr/bin/env python3
#
# Copyright 2017, Intel Corporation
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in
#       the documentation and/or other materials provided with the
#       distribution.
#
#     * Neither the name of the copyright holder nor the names of its
#       contributors may be used to endorse or promote products derived
#       from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from argparse import ArgumentParser
from os import listdir, linesep
from os.path import isfile, join
from subprocess import run, check_output


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Run jstests/core tests with resmoke.py")
    parser.add_argument("-m", "--mongo-root", required=True,
                        help="Path to mongo source root directory.abs")
    parser.add_argument("-d", "--dbpath", required=True,
                        help="Directory where database is created.")
    parser.add_argument("--use-core-arg", action='store_true',
                        help="--suites=core argument is used.")
    parser.add_argument("--timeout", type=int, default=5 *
                        60, help="Test case timeout in seconds.")

    args = parser.parse_args()

    test_dir = '{}/jstests/core'.format(args.mongo_root)
    db_dir = args.dbpath

    tests = [test for test in listdir(test_dir) if isfile(
        join(test_dir, test)) and test.endswith('js')]

    test_binary = ['{}/buildscripts/resmoke.py'.format(args.mongo_root)]
    test_args = ['--continueOnFailure',
                 '--storageEngine=pmse', '--dbpath={}'.format(db_dir)]
    if args.use_core_arg:
        test_args.append('--suites=core')

    failed = []
    for test in sorted(tests):
        print("{0}Running {1}:".format(linesep, test))
        cmd = test_binary + test_args
        cmd.append(join(test_dir, test))

        completed_process = run(cmd, cwd=args.mongo_root, timeout=args.timeout)
        if completed_process.returncode != 0:
            failed.append(test)

        run('rm -rf {}/job0'.format(db_dir), shell=True)

    print("{0}Out of {1} tests {2} failed:".format(linesep, len(tests), len(failed)))
    for test in failed:
        print(test)
