-- Copyright 2013 MemSQL, Inc.

-- Licensed under the Apache License, Version 2.0 (the "License"); you may not use
-- this file except in compliance with the License.  You may obtain a copy of the
-- License at

--     http://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software distributed
-- under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
-- CONDITIONS OF ANY KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations under the License.

drop table if exists delete_uu;
create table delete_uu (id int primary key, a int, b int, key(b));

insert into delete_uu values (1,8,6);
insert into delete_uu values (2,1,3);
insert into delete_uu values (3,6,8);
insert into delete_uu values (4,2,7);
insert into delete_uu values (5,5,0);
insert into delete_uu values (6,1,2);
insert into delete_uu values (7,4,4);
insert into delete_uu values (8,7,9);
insert into delete_uu values (9,3,7);

delete from delete_uu where id in (3,5,3);
insert into delete_uu values (3,6,8);
insert into delete_uu values (5,5,0);

-- no row should match
delete from delete_uu where id in (2,7,4) and b in (1, 5, 8);

-- verify that only rows 2 and 7 are deleted
delete from delete_uu where id in (2,7,4,2) and b < 5;
insert into delete_uu values (2,1,3);
insert into delete_uu values (7,4,4);

-- verify that only rows 5 and 7 are deleted
delete from delete_uu where id >= 5 and id <= 7 and a <> 1;
insert into delete_uu values (5,5,0);
insert into delete_uu values (7,4,4);

delete from delete_uu where (b > 2 and b <= 5 and a < 5) or (b >= 5 and b < 9 and a < 5);
