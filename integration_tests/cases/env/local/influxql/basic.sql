DROP TABLE IF EXISTS `h2o_feet`;

CREATE TABLE `h2o_feet` (
    `time` timestamp NOT NULL,
    `level_description` string TAG,
    `location` string TAG,
    `water_level` double NOT NULL,
    timestamp KEY (time)) ENGINE = Analytic WITH (
    enable_ttl = 'false'
);

-- Insert Records:
-- ("2015-08-18T00:00:00Z", "between 6 and 9 feet", "coyote_creek", 8.12),
-- ("2015-08-18T00:00:00Z", "below 3 feet", "santa_monica", 2.064),
-- ("2015-08-18T00:06:00Z", "between 6 and 9 feet", "coyote_creek", 8.005),
-- ("2015-08-18T00:06:00Z", "below 3 feet", "santa_monica", 2.116),
-- ("2015-08-18T00:12:00Z", "between 6 and 9 feet", "coyote_creek", 7.887),
-- ("2015-08-18T00:12:00Z", "below 3 feet", "santa_monica", 2.028);
INSERT INTO h2o_feet(time, level_description, location, water_level)
    VALUES
        (1439827200000, "between 6 and 9 feet", "coyote_creek", 8.12),
        (1439827200000, "below 3 feet", "santa_monica", 2.064),
        (1439827560000, "between 6 and 9 feet", "coyote_creek", 8.005),
        (1439827560000, "below 3 feet", "santa_monica", 2.116),
        (1439827620000, "between 6 and 9 feet", "coyote_creek", 7.887),
        (1439827620000, "below 3 feet", "santa_monica", 2.028);


-- protocol=influxql
SELECT * FROM "h2o_feet";

-- protocol=influxql
SELECT level_description, location, water_level FROM "h2o_feet" where location = 'santa_monica';

DROP TABLE IF EXISTS `h2o_feet`;