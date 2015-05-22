-- Hive doesn't allow hyphens in field names

-- This hive script takes in the emails from Enron and
-- counts the numbers exchanged between each pair of people

-- Get the headers struct, which contains the "from" and "to".
-- except the words "from", "to", and "date" are reserved in Hive
DROP TABLE raw;

CREATE EXTERNAL TABLE raw(
    h STRUCT<hivefrom:STRING,hiveto:STRING>
)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES("mongo.columns.mapping"="{'h.hivefrom':'headers.From',
 'h.hiveto':'headers.To'}")
TBLPROPERTIES('mongo.uri'="mongodb://localhost:27017/mongo_hadoop.messages");


DROP TABLE send_recip;
CREATE TABLE send_recip (
    f STRING,
    t_array ARRAY<STRING>
);

-- Strip the white space from the "hiveto" string
-- Then split the comma delimited string into an array of strings
INSERT OVERWRITE TABLE send_recip
SELECT
    h.hivefrom AS f,
    split(h.hiveto, "\\s*,\\s*")
        AS t_array
FROM raw
WHERE h IS NOT NULL
    AND h.hiveto IS NOT NULL;


DROP TABLE send_recip_explode;
CREATE TABLE send_recip_explode (
    f STRING,
    t STRING,
    num INT
);

-- Explode the array so that every element in the array gets it
-- own row. Then group by the unique "f" and "t" pair
-- to find the number of emails between the sender and receiver
INSERT OVERWRITE TABLE send_recip_explode
SELECT
    f,
    t,
    count(1) AS num
FROM send_recip
    LATERAL VIEW explode(t_array) tmpTable AS t
GROUP BY f, t;


DROP TABLE send_recip_counted;
CREATE EXTERNAL TABLE send_recip_counted (
    id STRUCT<
        t : STRING,
        f : STRING
    >,
    count INT
)
WITH SERDEPROPERTIES ("mongo.columns.mapping"="{'id':'_id'}")
TBLPROPERTIES('mongo.uri'="mongodb://localhost:27017/mongo_hadoop.hive_message_pairs");

-- Final output with the correct format
INSERT INTO TABLE send_recip_counted
SELECT
    named_struct('t', t, 'f', f) AS id,
    num AS count
FROM send_recip_explode;
