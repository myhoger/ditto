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

import MySQLdb
import MySQLdb.converters
import memsql_database

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import *
from pymysqlreplication.event import *

import argparse
import datetime
import subprocess
import os
import binascii
import re

def fix_object(value):

    """Fixes python objects so that they can be properly inserted into SQL queries"""

    # Needs to turn it into a regular string, since MySQLdb doesn't escape
    # unicode values properly
    if isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value

def compare_items((k, v)):
    """Converta a column-value pair to an equality comparison (uses IS for NULL)"""
    if v == None:
        return '`%s` IS %%s'%k
    else:
        return '`%s`=%%s'%k

def parse_commandline():
        """Parses the commandline-arguments that one could enter to a script replicating MySQL to MemSQL"""

        parser = argparse.ArgumentParser(description='Replicate a MySQL database to MemSQL')
        parser.add_argument('database', help='Database to use')
        parser.add_argument('--host', dest='host', type=str, help='Host where the MySQL database server is located', default='127.0.0.1')
        parser.add_argument('--memsql-host', dest='memsql_host', type=str, help='Host where the MemSQL database server will be located', default='')
        parser.add_argument('--user', dest='user', type=str, help='MySQL Username to log in as', default='root')
        parser.add_argument('--memsql-user', dest='memsql_user', type=str, help='MemSQL username to log in as', default='')
        parser.add_argument('--password', dest='password', type=str, help='MySQL Password to use', default='')
        parser.add_argument('--memsql-password', dest='memsql_password', type=str, help='MemSQL password to use', default='')
        parser.add_argument('--port', dest='port', type=int, help='MySQL port to use', default=3307)
        parser.add_argument('--memsql-port', dest='memsql_port', type=int, help='MemSQL port to use', default=3306)
        parser.add_argument('--no-dump', dest='no_dump', action='store_true',
                            default=False, help="Don't run mysqldump before reading (expects schema to already be set up)")
        parser.add_argument('--resume-from-end', dest='resume_from_end', action='store_true',
                            default=False, help="Even if the binlog replication was interrupted, start from the end of the current binlog")
        args = parser.parse_args()
        return args

def get_mysql_settings(args):
    return {'host':args.host, 'user':args.user, 'passwd':args.password,
            'db': args.database, 'port':args.port}

def get_memsql_settings(args):
    memhost = args.host if not args.memsql_host else args.memsql_host
    memuser = args.user if not args.memsql_user else args.memsql_user
    mempassword = args.password if not args.memsql_password else args.memsql_password
    return {'host': memhost+':'+str(args.memsql_port), 'user': memuser,
            'database':args.database, 'password': mempassword}

def getbinlogpos(stream):
    return stream._BinLogStreamReader__log_pos

def connect_to_mysql_stream(args, blocking=True):
    """Returns an iterator through the latest MySQL binlog

    Expects that the `args' argument was obtained from the
    parse_commandline() function (or something very similar)
    """

    mysql_settings = get_mysql_settings(args)

    ##server_id is your slave identifier. It should be unique
    ##blocking: True if you want to block and wait for the next event at the end of the stream
    server_id = int(binascii.hexlify(os.urandom(4)), 16) # A random 4-byte int
    stream = BinLogStreamReader(connection_settings = mysql_settings, resume_stream=True,
                    server_id = server_id, blocking = blocking, only_events =
                    [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent])

    return stream

def record_stream_binlog_pos(memsql_conn, stream):
    log_pos = stream._BinLogStreamReader__log_pos
    memsql_conn.execute('UPDATE ditto_pos SET pos=%s', log_pos)
def record_current_binlog_pos(memsql_conn, stream):
    (_, log_pos) = stream.get_binlog_pos()
    memsql_conn.execute('UPDATE ditto_pos SET pos=%s', log_pos)

def connect_to_memsql(args, stream):
    """Connects to a MemSQL instance to replicate to

    Expects that the `args' argument was obtained from the
    parse_commandline() function (or something very similar)
    """

    # Dumps database based on flags
    mysql_settings = get_mysql_settings(args)
    if not args.no_dump:
        # Dump with mysqldump
        dumpcommand = ['mysqldump', '--user='+args.user, '--host='+args.host,
            '--port='+str(args.port), '--database', args.database, '--force']
        if args.password:
            dumpcommand.append('--password='+args.password)
        dumpcommand.append('--master-data=2')
        print 'executing: {0}'.format(' '.join(dumpcommand))
        p = subprocess.Popen(dumpcommand, stdout=subprocess.PIPE)
        dump = p.communicate()[0]

        # Run mysql client (connected to memsql) on file
        mysqlcommand = ['mysql', '--user='+args.user, '--host='+args.host,
            '--port='+str(args.memsql_port), '--force']
        if args.password:
            mysqlcommand.append('--password='+args.password)

        print 'executing: {0}'.format(' '.join(mysqlcommand))
        p = subprocess.Popen(mysqlcommand, stdin=subprocess.PIPE)
        p.communicate(input=dump)

        # Sets the binlog position to the value specified in the dump
        binlog_pos = int(re.search('MASTER_LOG_POS=(.*);', dump).group(1))
        stream._BinLogStreamReader__connect_to_stream(custom_log_pos = binlog_pos)

    memsql_settings = get_memsql_settings(args)
    memsql_conn = memsql_database.Connection(**memsql_settings)

    # Creates the ditto_pos table that holds the log position of the
    # next query to be read
    memsql_conn.execute('CREATE TABLE IF NOT EXISTS ditto_pos(pos int)')
    if args.no_dump:
        # If there is a position in ditto_pos and the resume_from_end
        # flag is not set, use that. Else, record the current position
        q = memsql_conn.query('SELECT * FROM ditto_pos')
        if len(q) == 0:
            # Can't run record_current_binlog_pos, since it uses UPDATE
            (_, log_pos) = stream.get_binlog_pos()
            memsql_conn.execute('INSERT INTO ditto_pos(pos) VALUES (%s)', log_pos)
        elif not args.resume_from_end:
            log_pos = q[0]['pos']
            stream._BinLogStreamReader__connect_to_stream(custom_log_pos = int(log_pos))
        else:
            record_current_binlog_pos(memsql_conn, stream)
    else:
        # Record the binlog_pos from mysqldump
        memsql_conn.execute('DELETE from ditto_pos')
        memsql_conn.execute('INSERT INTO ditto_pos(pos) VALUES (%s)', binlog_pos)

    return memsql_conn

def process_binlogevent(binlogevent):
        """Extracts the query/queries from the given binlogevent"""

        # Each query is a pair with a string and a list of parameters for
        # format specifiers
        queries = []

        if isinstance(binlogevent, QueryEvent):
            if binlogevent.query != 'BEGIN': # BEGIN events don't matter
                queries.append( (binlogevent.query, []) )
        else:
            for row in binlogevent.rows:
                if isinstance(binlogevent, WriteRowsEvent):
                    query = ('INSERT INTO {0}({1}) VALUES ({2})'.format(
                                binlogevent.table,
                                ', '.join(map(lambda k: '`%s`'%k, row['values'].keys())),
                                ', '.join(['%s'] * len(row['values']))
                                ),
                                map(fix_object, row['values'].values())
                            )
                elif isinstance(binlogevent, DeleteRowsEvent):
                    query = ('DELETE FROM {0} WHERE {1} LIMIT 1'.format(
                                binlogevent.table,
                                ' AND '.join(map(compare_items, row['values'].items()))
                                ),
                                map(fix_object, row['values'].values())
                            )
                elif isinstance(binlogevent, UpdateRowsEvent):
                    query = ('UPDATE {0} SET {1} WHERE {2} LIMIT 1'.format(
                                binlogevent.table,
                                ', '.join(['`%s`=%%s'%k for k in row['after_values'].keys()]),
                                ' AND '.join(map(compare_items, row['before_values'].items()))
                                ),
                                map(fix_object, row['after_values'].values() + row['before_values'].values())
                            )
                queries.append(query) # It should never be the case that query wasn't created

        return queries

