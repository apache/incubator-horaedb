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

DROP TABLE IF EXISTS `02_function_time_bucket_table`;

CREATE TABLE `02_function_time_bucket_table` (
  `timestamp` timestamp NOT NULL,
  `value` int,
  timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);

INSERT INTO `02_function_time_bucket_table`
(`timestamp`, `value`)
VALUES
    (1659577423000, 1),
    (1659577422000, 2),
    (1659577320000, 3),
    (1659571200000, 4),
    (1659484800000, 5),
    (1656777600000, 6);

-- Test all time granularity.
SELECT time_bucket(`timestamp`, 'P1Y') FROM `02_function_time_bucket_table`;
SELECT time_bucket(`timestamp`, 'P1M') FROM `02_function_time_bucket_table`;
SELECT time_bucket(`timestamp`, 'P1W') FROM `02_function_time_bucket_table`;
SELECT time_bucket(`timestamp`, 'P1D') FROM `02_function_time_bucket_table`;
SELECT time_bucket(`timestamp`, 'PT1H') FROM `02_function_time_bucket_table`;
SELECT time_bucket(`timestamp`, 'PT1M') FROM `02_function_time_bucket_table`;
SELECT time_bucket(`timestamp`, 'PT1S') FROM `02_function_time_bucket_table`;

-- Test various parameters.
-- NOTICE: customizing format is not supported now.
SELECT time_bucket(`timestamp`, 'PT1H', 'yyyy-MM-dd HH:mm:ss') FROM `02_function_time_bucket_table`;
SELECT time_bucket(`timestamp`, 'PT1H', 'yyyy-MM-dd HH:mm:ss', '+0800') FROM `02_function_time_bucket_table`;
SELECT time_bucket(`timestamp`, 'PT1H', 'yyyy-MM-dd HH:mm:ss', '+0800', 'yyyy-MM-dd HH') FROM `02_function_time_bucket_table`;


DROP TABLE `02_function_time_bucket_table`;
