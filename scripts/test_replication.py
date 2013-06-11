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

# This script replicates the current MySQL binlog of a specific database into
# MemSQL and makes sure that the tables in the specified database match

from replication_utils import *
from replication_globs import *
import signal
import sys
import argparse

if __name__ == '__main__':

    parser = command_line_parser()
    # Adds some testing-specific arguments to the script
    parser.add_argument('--no-listen', dest='no_listen', action='store_true',
                        default=False,
                        help="Don't read the binlog at all after connecting")

    args = parser.parse_args()
    stream, memsql_conn = connect_to_databases(args)

    def equality_checker():
        if check_equality(args, memsql_conn) == True:
            print 'Replication successful'
        else:
            print 'Failure'

    if not args.no_listen:
        # Since blocking=False, binlog_listen will not close the
        # connections before exiting
        binlog_listen(stream, memsql_conn)
        equality_checker()
        close_connections(stream, memsql_conn)
    else:
        equality_checker()
        close_connections(stream, memsql_conn)
