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

DROP TABLE IF EXISTS `insert_into_select_table1`;

CREATE TABLE `insert_into_select_table1` (
    `timestamp` timestamp NOT NULL,
    `value` int,
    `name` string,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);

INSERT INTO `insert_into_select_table1` (`timestamp`, `value`, `name`)
VALUES
    (1, 100, "s1"),
    (2, 200, "s2"),
    (3, 300, "s3");

DROP TABLE IF EXISTS `insert_into_select_table2`;

CREATE TABLE `insert_into_select_table2` (
    `timestamp` timestamp NOT NULL,
    `value` int,
    `name` string NULL,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);

INSERT INTO `insert_into_select_table2` (`timestamp`, `value`)
SELECT `timestamp`, `value`
FROM `insert_into_select_table1`;

SELECT `timestamp`, `value`, `name`
FROM `insert_into_select_table2`;

DROP TABLE `insert_into_select_table1`;

DROP TABLE `insert_into_select_table2`;
