-- From https://gamov.io/posts/2018/11/28/who-is-tweeting-about-ksql.html

-- Create structured stream from raw json data that comes from kafka-twitter-connect connector
CREATE STREAM twitter_raw (CreatedAt bigint,Id bigint, Text VARCHAR, SOURCE VARCHAR, Truncated VARCHAR, InReplyToStatusId VARCHAR, InReplyToUserId VARCHAR, InReplyToScreenName VARCHAR, GeoLocation VARCHAR, Place VARCHAR, Favorited VARCHAR, Retweeted VARCHAR, FavoriteCount VARCHAR, User VARCHAR, Retweet VARCHAR, Contributors VARCHAR, RetweetCount VARCHAR, RetweetedByMe VARCHAR, CurrentUserRetweetId VARCHAR, PossiblySensitive VARCHAR, Lang VARCHAR, WithheldInCountries VARCHAR, HashtagEntities VARCHAR, UserMentionEntities VARCHAR, MediaEntities VARCHAR, SymbolEntities VARCHAR, URLEntities VARCHAR) WITH (KAFKA_TOPIC='twitter_json_01',VALUE_FORMAT='JSON');

-- Create stream of tweets from Kubecon
CREATE STREAM twitter_jfokus AS \
    SELECT TIMESTAMPTOSTRING(CreatedAt, 'yyyy-MM-dd HH:mm:ss.SSS') AS CreatedAt,\
    EXTRACTJSONFIELD(user,'$.Name') AS user_Name,\
    EXTRACTJSONFIELD(user,'$.ScreenName') AS user_ScreenName,\
    EXTRACTJSONFIELD(user,'$.Location') AS user_Location,\
    EXTRACTJSONFIELD(user,'$.Description') AS  user_Description,\
    Text, hashtagentities, lang \
    FROM twitter_raw WHERE LCASE(hashtagentities) LIKE '%jfokus%';

CREATE STREAM twitter_jfokus_kafka_ksql AS\
    SELECT TIMESTAMPTOSTRING(CreatedAt, 'yyyy-MM-dd HH:mm:ss.SSS') AS CreatedAt,\
    EXTRACTJSONFIELD(user,'$.Name') AS user_Name,\
    EXTRACTJSONFIELD(user,'$.ScreenName') AS user_ScreenName,\
    EXTRACTJSONFIELD(user,'$.Location') AS user_Location,\
    EXTRACTJSONFIELD(user,'$.Description') AS  user_Description,\
    Text,hashtagentities,lang \
    FROM twitter_raw WHERE LCASE(hashtagentities) LIKE '%jfokus%' AND (LCASE(hashtagentities) LIKE '%ksql%' OR LCASE(hashtagentities) LIKE '%apachekafka%');
    
  
  SELECT USER_NAME, TEXT FROM twitter_jfokus_kafka_ksql WHERE TEXT LIKE '%ksql%';



  CREATE STREAM twitter_raw
(CreatedAt bigint,Id bigint, Text VARCHAR, SOURCE VARCHAR, Truncated VARCHAR, InReplyToStatusId VARCHAR, InReplyToUserId VARCHAR, InReplyToScreenName VARCHAR, GeoLocation VARCHAR, Place VARCHAR, Favorited VARCHAR, Retweeted VARCHAR, FavoriteCount VARCHAR, User VARCHAR, Retweet VARCHAR, Contributors VARCHAR, RetweetCount VARCHAR, RetweetedByMe VARCHAR, CurrentUserRetweetId VARCHAR, PossiblySensitive VARCHAR, Lang VARCHAR, WithheldInCountries VARCHAR, HashtagEntities VARCHAR, UserMentionEntities VARCHAR, MediaEntities VARCHAR, SymbolEntities VARCHAR, URLEntities VARCHAR)
WITH
(KAFKA_TOPIC='twitter_json_01',VALUE_FORMAT='JSON');

CREATE STREAM twitter_jbreak AS
SELECT TIMESTAMPTOSTRING(CreatedAt, 'yyyy-MM-dd HH:mm:ss.SSS') AS CreatedAt, EXTRACTJSONFIELD(user,'$.Name') AS user_Name, EXTRACTJSONFIELD(user,'$.ScreenName') AS user_ScreenName, EXTRACTJSONFIELD(user,'$.Location') AS user_Location, EXTRACTJSONFIELD(user,'$.Description') AS  user_Description, Text, hashtagentities, lang
FROM twitter_raw
WHERE LCASE(hashtagentities) LIKE '%jbreak%';

CREATE STREAM twitter_jbreak_kafka_ksql AS
SELECT TIMESTAMPTOSTRING(CreatedAt, 'yyyy-MM-dd HH:mm:ss.SSS') AS CreatedAt, EXTRACTJSONFIELD(user,'$.Name') AS user_Name, EXTRACTJSONFIELD(user,'$.ScreenName') AS user_ScreenName, EXTRACTJSONFIELD(user,'$.Location') AS user_Location, EXTRACTJSONFIELD(user,'$.Description') AS  user_Description, Text, hashtagentities, lang
FROM twitter_raw
WHERE LCASE(hashtagentities) LIKE '%jbreak%' AND (LCASE(hashtagentities) LIKE '%ksql%' OR LCASE(hashtagentities) LIKE '%apachekafka%');

SHOW STREAMS;

CREATE TABLE USER_TWEET_COUNT_REINVENT AS
SELECT USER_SCREENNAME, count(*) AS  tweet_count
FROM twitter_reinvent WINDOW
TUMBLING
(SIZE 1 HOUR) GROUP BY user_screenname;

CREATE TABLE USER_TWEET_COUNT_DISPLAY_reinvent AS
SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS') AS WINDOW_START , USER_SCREENNAME, TWEET_COUNT
FROM user_tweet_count_reinvent;
CREATE TABLE TOP_5_REINVENT as
SELECT WINDOW_START, USER_SCREENNAME, TWEET_COUNT
FROM USER_TWEET_COUNT_DISPLAY_reinvent
WHERE TWEET_COUNT> 5;



