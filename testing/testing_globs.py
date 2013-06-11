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

from memsql import common

def setup_databases():
    """Creates the databases on both ends"""
    if len(sys.argv) != 3:
        sys.exit(sys.argv[0] + " requires the path to a .sql file and the name of the\
        replication database as arguments")

    filepath = os.path.expanduser(os.path.normpath(sys.argv[1]))
    dbname = sys.argv[2]

    common.query_both('', 'drop database if exists ' + dbname)
    common.query_both('', 'create database ' + dbname)
    common.query_both('', 'use ' + dbname)
    return filepath, dbname

def run_mysql(filepath, dbname):
    """Runs MySQL on the given file"""
    filepath = os.path.expanduser(os.path.normpath(sys.argv[1]))
    dbname = sys.argv[2]
    mysql_arglist = ["mysql", dbname, "--user=root", "--port=3307", "--force"]
    print 'executing:', ' '.join(mysql_arglist), '<', filepath
    # Runs mysql on the file
    p = subprocess.Popen(mysql_arglist, stdin=subprocess.PIPE)
    p.communicate(open(filepath).read())
