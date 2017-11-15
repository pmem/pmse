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
from subprocess import run, TimeoutExpired, STDOUT, PIPE, DEVNULL
from time import perf_counter
from collections import OrderedDict


def get_tests_for_suite(suite, mongo_root, test_binary):
    cmd = test_binary + ['--suites={}'.format(suite), '-n']
    proc = run(cmd, stdout=PIPE, cwd=mongo_root)
    out = proc.stdout.decode('utf-8').splitlines()

    tests = [join(mongo_root, line) for line in out if line.startswith(
        'jstests') and line.endswith('.js')]

    return tests

if __name__ == '__main__':
    parser = ArgumentParser(
        description='Run jstests/core tests with resmoke.py')
    parser.add_argument('-m', '--mongo-root', required=True,
                        help='Path to mongo source root directory.abs')
    parser.add_argument('-d', '--dbpath', required=True,
                        help='Directory where database is created.')
    parser.add_argument('-s', '--suite', required=True, help='Suite to run.')
    parser.add_argument('--timeout', type=int, default=5 *
                        60, help='Test case timeout in seconds.')
    parser.add_argument(
        '-t',
        '--tests',
        nargs='+',
        help='Tests from selected suite to run, default: run all.')
    args = parser.parse_args()

    test_dir = join(args.mongo_root, 'jstests', args.suite)
    test_binary = [join(args.mongo_root, 'buildscripts', 'resmoke.py')]
    test_args = ['--continueOnFailure',
                 '--storageEngine=pmse',
                 '--suites={}'.format(args.suite),
                 '--dbpath={}'.format(args.dbpath)]

    if args.tests:
        tests = args.tests
    else:
        tests = get_tests_for_suite(args.suite, args.mongo_root, test_binary)

    failed = []
    passed_warnings = OrderedDict()
    timeout = []
    out = ''

    margin = len(max(tests, key=len)) + 8
    for test in sorted(tests):
        cmd = test_binary + test_args
        cmd.append(join(test_dir, test))
        print_output = False
        skipped = False

        print('{} ...'.format(test).ljust(margin), end='', flush=True)

        start = perf_counter()
        try:
            proc = run(cmd, stderr=STDOUT, stdout=PIPE,
                       cwd=args.mongo_root, timeout=args.timeout)
        except TimeoutExpired:
            run('pgrep mongod | xargs kill -9',
                shell=True, stdout=DEVNULL, stderr=STDOUT)
            timeout.append(test)
            print('TIMEOUT', end='')
        else:
            out = proc.stdout.decode('utf-8')
            if proc.returncode == 0:
                if "No tests ran" in out:
                    print('SKIPPED', end='')
                    skipped = True
                else:
                    print('PASSED', end='')
            elif 'were skipped, 0 failed, 0 errored' in out:
                print('PASSED WITH WARNINGS. Test exited with code {}'.format(
                    proc.returncode), end='')
                passed_warnings[test] = proc.exitcode
                print_output = True
            else:
                print('FAILED', end='')
                failed.append(test)
                print_output = True
        finally:
            elapsed_ms = (perf_counter() - start) * 1000
            print('\t{0:.3f} [ms]'.format(elapsed_ms))
            if print_output:
                print(out)
            if not skipped:
                run('rm -r {}/job0'.format(args.dbpath), shell=True)

    if not failed and not timeout:
        print('All tests passed')
    else:
        print('{0}Out of {1} tests {2} failed:'.format(
            linesep, len(tests), len(failed) + len(timeout)))
        for test in failed:
            print(test)
        for test in timeout:
            print('{} (TIMEOUT)'.format(test))

    if passed_warnings:
        print('{} tests passed but exited with non-zero code:'.format(len(passed_warnings)))
        for test, returncode in passed_warnings.items():
            print('{0} ({1})'.format(test, returncode))
