-- Copyright 2013 MemSQL, Inc.

-- Licensed under the Apache License, Version 2.0 (the "License"); you may not use
-- this file except in compliance with the License.  You may obtain a copy of the
-- License at

--     http://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software distributed
-- under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
-- CONDITIONS OF ANY KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations under the License.

drop table if exists insert_values;
create table insert_values(a int primary key auto_increment, b int);
insert into insert_values (b) values (10);
insert into insert_values (b) value (20);
insert insert_values (a,b) values (5,20);
insert into insert_values (b) values (60);
insert insert_values (a,b) value (4,60);
insert insert_values (b) value (70);
insert into insert_values (a,b) values (1,70), (9, 10), (33, 382);
insert into insert_values (a,b) value (3,99990), (129, 3823);
insert insert_values (a) values (92), (10) on duplicate key update a = a - 1;
insert insert_values (b) value (60), (932) on duplicate key update b = b + 3;

insert high_priority into insert_values (b) values (10);
insert high_priority into insert_values (b) value (20);
insert high_priority insert_values (a,b) values (5,20);
insert high_priority into insert_values (b) values (60);
insert high_priority insert_values (a,b) value (4,60);
insert high_priority insert_values (b) value (70);
insert high_priority into insert_values (a,b) values (1,70), (9, 10), (33, 382);
insert high_priority into insert_values (a,b) value (3,99990), (129, 3823);
insert high_priority insert_values (a) values (92), (10) on duplicate key update a = a - 1;
insert high_priority insert_values (b) value (60), (932) on duplicate key update b = b + 3;

insert low_priority into insert_values (b) values (10);
insert low_priority into insert_values (b) value (20);
insert low_priority insert_values (a,b) values (5,20);
insert low_priority into insert_values (b) values (60);
insert low_priority insert_values (a,b) value (4,60);
insert low_priority insert_values (b) value (70);
insert low_priority into insert_values (a,b) values (1,70), (9, 10), (33, 382);
insert low_priority into insert_values (a,b) value (3,99990), (129, 3823);
insert low_priority insert_values (a) values (92), (10) on duplicate key update a = a - 1;
insert low_priority insert_values (b) value (60), (932) on duplicate key update b = b + 3;
