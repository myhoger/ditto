Ditto
============================================

Ditto is a tool that lets you easily replicate your existing MySQL data into
MemSQL.

Dependencies
============
Ditto requires the following software:

* MySQL &lt;= 5.5 (5.6 is not supported)
* Python 2.7
* MemSQL
* MySQL-Python
* mysqldump


Installation
=============

The replication scripts come bundled with a modified version of the
[python-mysql-replication](https://github.com/noplay/python-mysql-replication)
library that must be installed before we can replicate.

Enter the python-mysql-replication directory:

    $ python setup.py install 

MySQL server settings
=========================

In your MySQL server configuration file (usually at /etc/mysql/my.cnf) you need
to enable replication. Make sure the following options are set as below:

    [mysqld]
    server-id		 = 1
    log_bin			 = /var/log/mysql/mysql-bin.log
    expire_logs_days = 10
    max_binlog_size  = 100M
    binlog-format    = row

Usage
=====

The replication script is ``replicate.py``. To run the script,
enter the scripts directory:
    
    $ python replicate.py [database]

This script will recreate the specified database in MemSQL. By
default, it first runs ``mysqldump`` on the database. Then it waits
for queries on the current binlog that pertain to the specified
database and runs them on MemSQL. To stop the program, send it an
interrupt. There are a number of settings you can tweak at the command
line:

    $ python replicate.py --help

    usage: replicate.py [-h] [--host HOST] [--memsql-host MEMSQL_HOST] [--user USER]
                    [--memsql-user MEMSQL_USER] [--password PASSWORD]
                    [--memsql-password MEMSQL_PASSWORD] [--port PORT] [--memsql-port MEMSQL_PORT]
                    [--no-dump] [--ignore-ditto-lock] [--resume-from-end] [--resume-from-start]
                    [--no-blocking] [--log LOGLEVEL] [--mysqldump-file MYSQLDUMP_FILE]
                    database

    Replicate a MySQL database to MemSQL

    positional arguments:
      database              Database to use

    optional arguments:
      -h, --help            show this help message and exit
      --host HOST           Host where the MySQL database server is located
      --memsql-host MEMSQL_HOST
                            Host where the MemSQL server will be located
      --user USER           MySQL Username to log in as
      --memsql-user MEMSQL_USER
                            MemSQL username to log in as
      --password PASSWORD   MySQL Password to use
      --memsql-password MEMSQL_PASSWORD
                            MemSQL password to use
      --port PORT           MySQL port to use
      --memsql-port MEMSQL_PORT
                            MemSQL port to use
      --no-dump             Don't run mysqldump before reading (expects schema to already be set up)
      --ignore-ditto-lock   If the ditto lock is set in the database being replicated, ignore it and
                            proceed anyways (this is intended to be used only if the ditto lock is
                            incorrectly set when no ditto processes are active, not to allow multiple
                            ditto processes to function simultaneously)
      --resume-from-end     Even if the binlog replication was interrupted, start from the end of the
                            current binlog rather than resuming from the interruption
      --resume-from-start   Start from the beginning of the current binlog, regardless of the current
                            position
      --no-blocking         Don't wait for more events on the binlog after getting to the end
      --log LOGLEVEL        Set the logging verbosity with one of the following options (in order of
                            increasing verbosity): DEBUG, INFO, WARNING, ERROR, CRITICAL
      --mysqldump-file MYSQLDUMP_FILE
                            Specify a file to get the mysqldump from, rather than having ditto running
                            mysqldump itself

The scripts directory also contains ``test_replication.py``. This
script will replicate a specific database from MySQL to MemSQL using
mysqldump and the current binlog and then verify that the content of
all tables in the MySQL database match those in the newly created
MemSQL database. It can be run in the same manner as ``replicate.py``.
To print out all queries in the current binlog, run the
``dump_queries.py`` script.

Licence
=========

Copyright 2013 MemSQL, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License.  You may obtain a copy of the
License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied.  See the License for the
specific language governing permissions and limitations under the License.

The ``memsql_database.py`` file is a modified version of the original file that
is part of Facebook's Tornado project.
