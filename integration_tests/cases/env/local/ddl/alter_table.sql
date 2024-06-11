--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

DROP TABLE IF EXISTS `05_alter_table_t0`;

CREATE TABLE `05_alter_table_t0`(a int, t timestamp NOT NULL, dic string dictionary, TIMESTAMP KEY(t)) ENGINE = Analytic with (enable_ttl='false', update_mode='OVERWRITE');
INSERT INTO TABLE `05_alter_table_t0`(a, t, dic) values(1, 1 , "d1");
SELECT * FROM `05_alter_table_t0`;

-- doesn't support rename
ALTER TABLE `05_alter_table_t0` RENAME TO `t1`;

ALTER TABLE `05_alter_table_t0` add COLUMN (b string);
DESCRIBE TABLE `05_alter_table_t0`;
INSERT INTO TABLE `05_alter_table_t0`(a, b, t, dic) values (2, '2', 2, "d2");
SELECT * FROM `05_alter_table_t0`;

ALTER TABLE `05_alter_table_t0` add COLUMN (add_dic string dictionary);
DESCRIBE TABLE `05_alter_table_t0`;
INSERT INTO TABLE `05_alter_table_t0` (a, b, t, dic, add_dic)
    VALUES (2, '2', 2, "d11", "d22"),
    (3, '3', 3, "d22", "d33");


SELECT * FROM `05_alter_table_t0`;

-- doesn't support drop column
ALTER TABLE `05_alter_table_t0` DROP COLUMN b;
DESCRIBE TABLE `05_alter_table_t0`;
SELECT * FROM `05_alter_table_t0`;

-- try to enable layered memtable with invalid 0 mutable switch threshold
ALTER TABLE `05_alter_table_t0` MODIFY SETTING layered_enable='true',layered_mutable_switch_threshold='0';

-- try to enable layered memtable for overwrite mode table
ALTER TABLE `05_alter_table_t0` MODIFY SETTING layered_enable='true',layered_mutable_switch_threshold='3MB';

DROP TABLE `05_alter_table_t0`;
