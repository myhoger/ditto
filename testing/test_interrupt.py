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

# Tests only the binlog listening part of ditto, interrupting the
# binlog replaying repeatedly. It flushes the MySQL binlog and runs
# the .sql file on the MySQL side. It splits up the queries in the
# binlog, and runs chunks at a time, killing the proccess after a
# certain number of queries and re-resuming until the queries are
# done.

import subprocess, threading
from memsql import common, tools
from testing_globs import *

filepath, dbname = setup_databases()
# Resets the binlog before playing the file
tools.ConnectToMySQL().execute('reset master')
run_mysql(filepath, dbname)

# A threading object to facilitate running a command with a timeout.
# From http://stackoverflow.com/questions/1191374/subprocess-with-timeout
class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        def target():
            print 'Thread started'
            self.process = subprocess.Popen(self.cmd)
            print 'executing:', ' '.join(self.cmd)
            self.process.communicate()
            print 'Thread finished'

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            print 'Terminating process'
            self.process.terminate()
            thread.join()
        return self.process.returncode

command = Command(["python", "../scripts/test_replication.py", dbname,
                   "--no-dump", "--no-blocking", "--ignore-ditto-lock"])

timelimit = 3
# Keeps running the command until the exit code is 0
ret = command.run(timelimit)
while ret != 0:
    ret = command.run(timelimit)
