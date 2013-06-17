# Copyright 2013 MemSQL, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use
# this file except in compliance with the License.  You may obtain a copy of the
# License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations under the License.

# Shared testing functions

import sys
import os
import subprocess
import argparse
import logging

from memsql import tools

def setup_databases():
    """Creates the databases on both ends"""

    parser = argparse.ArgumentParser(description='Ditto testing script')
    parser.add_argument('filepath', type=str, help="The path to a .sql file to run")
    parser.add_argument('database', type=str, help="The name of the database to test replication on")
    parser.add_argument('--log', dest='loglevel', type=str,
                        help="Set the logging verbosity with one of the\
                        following options (in order of increasing verbosity):\
                        DEBUG, INFO, WARNING, ERROR, CRITICAL", default="DEBUG")
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)

    filepath = os.path.expanduser(os.path.normpath(args.filepath))

    # Connects without the helper functions to avoid reparsing the
    # arguments and failing because of the --log option
    conn1 = tools.Connection(user='root', host='127.0.0.1:3307', database='')
    conn2 = tools.Connection(user='root', host='127.0.0.1:3306', database='')

    conn1.execute('drop database if exists %s' % args.database)
    conn1.execute('create database %s' % args.database)
    conn1.execute('use %s' % args.database)
    # Resets the binlog before running mysql
    conn1.execute('reset master')

    conn2.execute('drop database if exists %s' % args.database)
    conn2.execute('create database %s' % args.database)
    conn2.execute('use %s' % args.database)

    return filepath, args.database, args.loglevel

def run_mysql(filepath, dbname):
    """Runs MySQL on the given file"""
    mysql_arglist = ["mysql", dbname, "--user=root", "--port=3307", "--force"]
    logging.debug('executing: %s < %s' % (' '.join(mysql_arglist), filepath))
    # Runs mysql on the file
    p = subprocess.Popen(mysql_arglist, stdin=subprocess.PIPE)
    p.communicate(open(filepath).read())
