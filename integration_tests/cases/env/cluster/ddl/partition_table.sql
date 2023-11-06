DROP TABLE IF EXISTS `partition_table_t`;

CREATE TABLE `partition_table_t`(
                                    `name`string TAG,
                                    `id` int TAG,
                                    `value` double NOT NULL,
                                    `t` timestamp NOT NULL,
                                    TIMESTAMP KEY(t)
) PARTITION BY KEY(name) PARTITIONS 4 ENGINE = Analytic with (enable_ttl='false');

SHOW CREATE TABLE partition_table_t;

SELECT * from system.public.tables where table_name like "%partition_table_t%" order by table_id;

SELECT * from system.public.tables where partition_info is not null;

INSERT INTO partition_table_t (t, name, value)
VALUES (1651737067000, "ceresdb0", 100),
       (1651737067000, "ceresdb1", 101),
       (1651737067000, "ceresdb2", 102),
       (1651737067000, "ceresdb3", 103),
       (1651737067000, "ceresdb4", 104),
       (1651737067000, "ceresdb5", 105),
       (1651737067000, "ceresdb6", 106),
       (1651737067000, "ceresdb7", 107),
       (1651737067000, "ceresdb8", 108),
       (1651737067000, "ceresdb9", 109),
       (1651737067000, "ceresdb10", 110);

SELECT * from partition_table_t where name = "ceresdb0";

SELECT * from partition_table_t where name = "ceresdb1";

SELECT * from partition_table_t where name = "ceresdb2";

SELECT * from partition_table_t where name = "ceresdb3";

SELECT * from partition_table_t where name in ("ceresdb0", "ceresdb1", "ceresdb2", "ceresdb3", "ceresdb4") order by name;

SELECT * from partition_table_t where name in ("ceresdb5", "ceresdb6", "ceresdb7","ceresdb8", "ceresdb9", "ceresdb10") order by name;

ALTER TABLE partition_table_t ADD COLUMN (b string);

INSERT INTO partition_table_t (t, id, name, value) VALUES (1651737067000, 10, "ceresdb0", 100);

ALTER TABLE partition_table_t MODIFY SETTING enable_ttl='true';

SHOW CREATE TABLE __partition_table_t_0;

SHOW CREATE TABLE __partition_table_t_1;

SHOW CREATE TABLE __partition_table_t_2;

SHOW CREATE TABLE __partition_table_t_3;

DROP TABLE IF EXISTS `partition_table_t`;

SHOW CREATE TABLE partition_table_t;

DROP TABLE IF EXISTS `random_partition_table_t`;

CREATE TABLE `random_partition_table_t`(
                                    `name`string TAG,
                                    `id` int TAG,
                                    `value` double NOT NULL,
                                    `t` timestamp NOT NULL,
                                    TIMESTAMP KEY(t)
) PARTITION BY RANDOM PARTITIONS 4 ENGINE = Analytic with (enable_ttl='false', update_mode="APPEND");

SHOW CREATE TABLE random_partition_table_t;

INSERT INTO random_partition_table_t (t, name, value)
VALUES (1651737067000, "ceresdb0", 100),
       (1651737067000, "ceresdb1", 101),
       (1651737067000, "ceresdb2", 102),
       (1651737067000, "ceresdb3", 103),
       (1651737067000, "ceresdb4", 104),
       (1651737067000, "ceresdb5", 105),
       (1651737067000, "ceresdb6", 106),
       (1651737067000, "ceresdb7", 107),
       (1651737067000, "ceresdb8", 108),
       (1651737067000, "ceresdb9", 109),
       (1651737067000, "ceresdb10", 110);

SELECT * from random_partition_table_t where name = "ceresdb0";

SELECT * from random_partition_table_t where name = "ceresdb5";

DROP TABLE IF EXISTS `random_partition_table_t`;

SHOW CREATE TABLE random_partition_table_t;

DROP TABLE IF EXISTS `random_partition_table_t_overwrite`;

CREATE TABLE `random_partition_table_t_overwrite`(
                                    `name`string TAG,
                                    `id` int TAG,
                                    `value` double NOT NULL,
                                    `t` timestamp NOT NULL,
                                    TIMESTAMP KEY(t)
) PARTITION BY RANDOM PARTITIONS 4 ENGINE = Analytic with (enable_ttl='false', update_mode="OVERWRITE");
