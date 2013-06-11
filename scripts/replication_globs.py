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

# Various globs of code that are shared between replication scripts

from replication_utils import *
import signal
import sys

def connect_to_databases(args):
    """Returns connections to MySQL (as a stream) and MemSQL"""
    stream = connect_to_mysql_stream(args)
    memsql_conn = connect_to_memsql(args, stream)
    memsql_conn.set_print_queries(True)
    return stream, memsql_conn

def binlog_listen(stream, memsql_conn):
    """Listens to the binlog on stream, executing every query it receives
on the MemSQL connection. It also updates the binlog position after
every query so that it can resume in case of interruption. Upon
receiving a SIGINT, it closes the stream and unoccupies the database.

    """

    # Tries to close the stream and unoccupy the database upon getting
    # SIGTERM, SIGABRT, or SIGINT. On SIGINT, it won't exit the
    # program, but on SIGTERM and SIGABRT it will.

    def signal_handler(signum, frame):
        print 'killed'
        close_connections(stream, memsql_conn)
        sys.exit(1)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGABRT, signal_handler)

    try:
        print 'listening'
        # Reads the binlog and executes the retrieved queries in MemSQL
        for binlogevent in stream:
            queries = process_binlogevent(binlogevent)
            # Runs the queries in MemSQL
            for q in queries:
                try:
                    memsql_conn.execute(q[0], *q[1])
                except Exception as e:
                    print 'error:', e
            record_stream_binlog_pos(memsql_conn, stream)
        # If blocking on the stream is False, the above for loop will
        # exit, and the function will return WITHOUT closing the
        # stream or memsql_conn
    except KeyboardInterrupt:
        close_connections(stream, memsql_conn)

def check_equality(args, memsql_conn):
    """Creates a connection to MySQL and checks that the specified
    database is the same in both MySQL and MemSQL. Returns True if the
    databases match and False otherwise

    """
    mysql_settings = {'host': args.host+':'+str(args.port), 'user': args.user,
                      'database': args.database, 'password': args.password}
    mysql_conn = memsql_database.Connection(**mysql_settings)
    tables = []
    for row in mysql_conn.query('show tables'):
        tables.extend(row.values())

    for t in tables:
        print 'Testing table', t
        try:
            memsql_database.compare_assert(mysql_conn, memsql_conn,
                                           'select * from '+t, enforce_order=False)
        except AssertionError as e:
            print 'AssertionError:', e
            return False
    # All the tables matched
    return True

def close_connections(stream, memsql_conn):
    """Closes the stream and removes the ditto lock"""
    stream.close()
    unoccupy_ditto_info(memsql_conn)
