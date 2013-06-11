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

# Tests only the mysqldump part of ditto. It runs the .sql file on the
# mysql side then runs test_replication.py to check the replication
# onto MemSQL.

import subprocess
from testing_globs import *

filepath, dbname = setup_databases()
run_mysql(filepath, dbname)

# Runs test_replication
ditto_arglist = ["python", "../scripts/test_replication.py", dbname, "--no-listen"]
print 'executing:', ' '.join(ditto_arglist)
subprocess.call(ditto_arglist)
