-- Copyright 2013 MemSQL, Inc.

-- Licensed under the Apache License, Version 2.0 (the "License"); you may not use
-- this file except in compliance with the License.  You may obtain a copy of the
-- License at

--     http://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software distributed
-- under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
-- CONDITIONS OF ANY KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations under the License.

DROP TABLE IF EXISTS multi_upsert_1;
CREATE TABLE multi_upsert_1 (
    a int primary key
);
insert into multi_upsert_1 values (1), (2), (3) on duplicate key update a = a + 1;
insert into multi_upsert_1 values (1), (2), (3) on duplicate key update a = a + 1;
insert into multi_upsert_1 values (1), (2), (3) on duplicate key update a = a + 5;

-- hit dup key errors inside a single multi-insert statement.  We had bugs with row lock look-up failing.
--
drop table if exists t;
create table t(c1 int, c2 int, unique key(c2));
insert into t values (1,1),(1,1),(1,2),(1,3) on duplicate key update c2 = c2 +1;
select * from t;
