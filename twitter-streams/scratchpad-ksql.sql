CREATE STREAM kafka_ksql_reinvent_twitter AS SELECT TIMESTAMPTOSTRING(CreatedAt, 'yyyy-MM-dd HH:mm:ss.SSS') AS CreatedAt, EXTRACTJSONFIELD(user,'$.ScreenName') as ScreenName,Text FROM twitter_raw

CREATE STREAM kafka_ksql_reinvent_twitter AS SELECT TIMESTAMPTOSTRING(CreatedAt, 'yyyy-MM-dd HH:mm:ss.SSS') AS CreatedAt, EXTRACTJSONFIELD(user,'$.Name') AS user_Name, EXTRACTJSONFIELD(user,'$.ScreenName') AS user_ScreenName, EXTRACTJSONFIELD(user,'$.Location') AS user_Location, EXTRACTJSONFIELD(user,'$.Description') AS  user_Description, Text,hashtagentities,lang FROM twitter_raw WHERE LCASE(hashtagentities) LIKE '%reinvent%' AND (LCASE(hashtagentities) LIKE '%ksql%' OR LCASE(hashtagentities) LIKE '%apachekafka%');

CREATE STREAM reinvent_twitter AS SELECT TIMESTAMPTOSTRING(CreatedAt, 'yyyy-MM-dd HH:mm:ss.SSS') AS CreatedAt, EXTRACTJSONFIELD(user,'$.Name') AS user_Name, EXTRACTJSONFIELD(user,'$.ScreenName') AS user_ScreenName, EXTRACTJSONFIELD(user,'$.Location') AS user_Location, EXTRACTJSONFIELD(user,'$.Description') AS  user_Description, Text,hashtagentities,lang FROM twitter_raw WHERE LCASE(hashtagentities) LIKE '%reinvent%';

SELECT CREATEDAT, USER_NAME, TEXT FROM twitter_kafka_ksql_reinvent_ WHERE TEXT LIKE '%FML%';
SELECT CREATEDAT, USER_NAME, TEXT FROM twitter_reinvent;


CREATE TABLE user_tweet_count_reinvent AS SELECT user_screenname, count(*) AS  tweet_count  FROM twitter_reinvent WINDOW TUMBLING (SIZE 1 HOUR) GROUP BY user_screenname;

CREATE TABLE USER_TWEET_COUNT_DISPLAY_reinvent AS SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS') AS WINDOW_START , USER_SCREENNAME, TWEET_COUNT FROM user_tweet_count_reinvent;

create table top_5_reinvent as SELECT WINDOW_START, USER_SCREENNAME, TWEET_COUNT FROM USER_TWEET_COUNT_DISPLAY_reinvent WHERE TWEET_COUNT> 5;

select USER_SCREENNAME, TWEET_COUNT from top_5_reinvent;


from robin blog
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM twitter_raw (CreatedAt BIGINT, Id BIGINT, Text VARCHAR) WITH (KAFKA_TOPIC='twitter_json_01', VALUE_FORMAT='JSON');

SELECT text FROM twitter_raw LIMIT 1;

