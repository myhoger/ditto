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

# Utility functions for the replication scripts

import MySQLdb
import MySQLdb.converters
import memsql_database

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import *
from pymysqlreplication.event import *

import argparse
import subprocess
import os
import binascii
import re
import sys
import logging

def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL
    queries

    """

    # Needs to turn it into a regular string, since MySQLdb doesn't escape
    # unicode values properly
    if isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value

def compare_items((k, v)):
    """Converta a column-value pair to an equality comparison (uses IS for
    NULL)

    """
    if v == None:
        return '`%s` IS %%s'%k
    else:
        return '`%s`=%%s'%k

def command_line_parser():
        """Returns a command line parser used for ditto scripts"""

        parser = argparse.ArgumentParser(
            description='Replicate a MySQL database to MemSQL')
        parser.add_argument('database', help='Database to use')
        parser.add_argument('--host', dest='host', type=str,
                            help='Host where the MySQL database server is located',
                            default='127.0.0.1')
        parser.add_argument('--memsql-host', dest='memsql_host', type=str,
                            help='Host where the MemSQL server will be located',
                            default='')
        parser.add_argument('--user', dest='user', type=str,
                            help='MySQL Username to log in as', default='root')
        parser.add_argument('--memsql-user', dest='memsql_user', type=str,
                            help='MemSQL username to log in as', default='')
        parser.add_argument('--password', dest='password', type=str,
                            help='MySQL Password to use', default='')
        parser.add_argument('--memsql-password', dest='memsql_password', type=str,
                            help='MemSQL password to use', default='')
        parser.add_argument('--port', dest='port', type=int,
                            help='MySQL port to use', default=3307)
        parser.add_argument('--memsql-port', dest='memsql_port', type=int,
                            help='MemSQL port to use', default=3306)
        parser.add_argument('--no-dump', dest='no_dump', action='store_true',
                            help="Don't run mysqldump before reading\
                            (expects schema to already be set up)", default=False)
        parser.add_argument('--ignore-ditto-lock', dest='ignore_ditto_lock',
                            action='store_true', help="If the ditto lock is set in\
                            the database being replicated, ignore it and proceed\
                            anyways (this is intended to be used only if the ditto\
                            lock is incorrectly set when no ditto processes are\
                            active, not to allow multiple ditto processes to function\
                            simultaneously)", default=False)
        parser.add_argument('--resume-from-end', dest='resume_from_end',
                            action='store_true', help="Even if the binlog\
                            replication was interrupted, start from the end of\
                            the current binlog rather than resuming from the interruption",
                            default=False)
        parser.add_argument('--resume-from-start', dest='resume_from_start',
                            action='store_true', help="Start from the beginning\
                            of the current binlog, regardless of the current position", default=False)
        parser.add_argument('--no-blocking', dest='no_blocking', action='store_true',
                            default=False, help="Don't wait for more events on\
                            the binlog after getting to the end")
        parser.add_argument('--log', dest='loglevel', type=str,
                            help="Set the logging verbosity with one of the\
                            following options (in order of increasing verbosity):\
                            DEBUG, INFO, WARNING, ERROR, CRITICAL", default="DEBUG")
        return parser

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
    return stream.log_pos
def setbinlogpos(stream, pos):
    stream.log_pos = pos

def connect_to_mysql_stream(args):
    """Returns an iterator through the latest MySQL binlog

    Expects that the `args' argument was obtained from the
    command_line_parser() parser (or something very similar)

    """

    mysql_settings = get_mysql_settings(args)

    ##server_id is your slave identifier. It should be unique
    ##blocking: True if you want to block and wait for the next event at the end of the stream
    server_id = int(binascii.hexlify(os.urandom(4)), 16) # A random 4-byte int

    # If the resume_from_end flag is set, it will set the stream to
    # the latest master binlog position. If the resume_from_start flag
    # is set, it will be set to the beginning of the binlog. Both
    # options cannot be set together.
    if args.resume_from_end and args.resume_from_start:
        sys.exit("Cannot set both --resume_from_end and --resume_from_start")

    stream = BinLogStreamReader(connection_settings = mysql_settings,
                                resume_stream= args.resume_from_end,
                                server_id = server_id,
                                blocking = not args.no_blocking,
                                only_events = [DeleteRowsEvent, WriteRowsEvent,
                                               UpdateRowsEvent, QueryEvent])
    return stream

def record_stream_binlog_pos(memsql_conn, stream):
    """Records the binlog position that the stream is currently at"""
    log_pos = getbinlogpos(stream)
    memsql_conn.execute('UPDATE ditto_info SET pos=%s', log_pos)
def record_master_binlog_pos(memsql_conn, stream):
    """Records the binlog position that the master is currently at"""
    (_, log_pos) = stream.get_master_binlog_pos()
    memsql_conn.execute('UPDATE ditto_info SET pos=%s', log_pos)
def unoccupy_ditto_info(memsql_conn):
    """Sets the in_use value in ditto_info to 0, thereby freeing up the
    database to other ditto processes"""
    memsql_conn.execute("UPDATE ditto_info SET in_use=0")

def connect_to_memsql(args, stream):
    """Connects to a MemSQL instance to replicate to

    Expects that the `args' argument was obtained from the
    command_line_parser() parser (or something very similar)

    """

    logging.basicConfig(level=args.loglevel)
    # Dumps database based on flags
    mysql_settings = get_mysql_settings(args)
    if not args.no_dump:
        # Dump with mysqldump
        dumpcommand = ['mysqldump', '--user='+args.user, '--host='+args.host,
            '--port='+str(args.port), '--database', args.database, '--force']
        if args.password:
            dumpcommand.append('--password='+args.password)
        dumpcommand.append('--master-data=2')
        logging.debug('executing: {0}'.format(' '.join(dumpcommand)))
        p = subprocess.Popen(dumpcommand, stdout=subprocess.PIPE)
        dump = p.communicate()[0]

        # Run mysql client (connected to memsql) on file
        mysqlcommand = ['mysql', '--user='+args.user, '--host='+args.host,
            '--port='+str(args.memsql_port), '--force']
        if args.password:
            mysqlcommand.append('--password='+args.password)

        logging.debug('executing: {0}'.format(' '.join(mysqlcommand)))
        p = subprocess.Popen(mysqlcommand, stdin=subprocess.PIPE)
        p.communicate(input=dump)

        # Sets the binlog position to the value specified in the dump
        binlog_pos = int(re.search('MASTER_LOG_POS=(.*);', dump).group(1))
        stream.connect_to_stream(custom_log_pos = binlog_pos)

    memsql_settings = get_memsql_settings(args)
    memsql_conn = memsql_database.Connection(**memsql_settings)

    # Creates the ditto_info table that holds the log position of the
    # next query to be read and a boolean indicating whether the
    # database is in use. If the boolean is 0, the database is open,
    # and the function continues. Else, the database is being used by
    # another ditto process, and the current one aborts, provided
    # ignore_ditto_lock isn't True.
    memsql_conn.execute('CREATE TABLE IF NOT EXISTS ditto_info(pos int, in_use int unique key)')
    # Checks for usage. If it's open, we set in_use to 1
    q = memsql_conn.query('SELECT * FROM ditto_info')
    ditto_lock_errmsg = 'This database is already in use by another ditto process. If you wish to run ditto anyways, run it with the --ignore-ditto-lock flag.'
    if len(q) != 0 and int(q[0]['in_use']) != 0 and not args.ignore_ditto_lock:
        sys.exit(ditto_lock_errmsg)
    else:
        # Tries to aquire the lock by inserting a 1 into the in_use
        # column. If it fails with a dup key error, it means the lock
        # has already been acquired and we fail. If it succeeds, it
        # deletes the old row with the 0 lock. If there was no row at
        # all, it acquires the lock and sets the initial ditto
        # position to the beginning of the binlog, triggering the same
        # behavior as --resume-from-end
        try:
            if len(q) == 0:
                ditto_pos = stream.starting_binlog_pos
                memsql_conn.execute("INSERT INTO ditto_info values (%s, 1)", ditto_pos)
            elif len(q) == 1:
                ditto_pos = int(q[0]['pos'])
                memsql_conn.execute("INSERT INTO ditto_info values (%s, 1)", ditto_pos)
                memsql_conn.execute("DELETE FROM ditto_info where in_use=0")
            else:
                sys.exit('ditto_info table cannot have more than one row')
        except MySQLdb.DatabaseError as e:
            logging.error(e)
            # 1062 is the dup key error code
            if e[0] == 1062 and args.ignore_ditto_lock:
                logging.debug('Continuing anyways')
            else:
                sys.exit(ditto_lock_errmsg)

    # If the resume_from_end flag is set, record the latest master
    # position. If the resume_from_start flag is set, record the
    # initial binlog position. If either of these flags were set, the
    # correct binlog position should have already been set in the
    # stream by the connect_to_mysql_stream function
    if args.resume_from_end:
        record_master_binlog_pos(memsql_conn, stream)
    elif args.resume_from_start:
        setbinlogpos(stream, stream.starting_binlog_pos)
        record_stream_binlog_pos(memsql_conn, stream)
    elif args.no_dump:
        # We look to ditto_info for the binlog position. We should
        # have gotten the ditto_pos value above.
        log_pos = ditto_pos
        stream.connect_to_stream(
            custom_log_pos = int(log_pos))
        record_stream_binlog_pos(memsql_conn, stream)
    else:
        # Record the binlog_pos from mysqldump
        stream.connect_to_stream(
            custom_log_pos = binlog_pos)
        record_stream_binlog_pos(memsql_conn, stream)

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
