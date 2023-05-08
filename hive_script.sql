
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

create external table if not EXISTS project.twitter_landing_table (tweet_id string,
tweet_date string,
text string,
retweets int,
impression_count int, 
username string, 
user_id string,
verified boolean, 
followers int,
location string)
PARTITIONED BY (year int ,month int , day int,hour int)
STORED AS PARQUET
LOCATION '/twitter-landing-data';


create external table if not EXISTS project.users_raw (
user_id string,
username string, 
verified boolean, 
followers int,
location string)
PARTITIONED BY (year int ,month int , day int,hour int)
STORED AS PARQUET
LOCATION '/twitter-raw-data/users_raw';




create external table if not EXISTS project.tweets_raw (
tweet_id string,
user_id string,
tweet_date string,
text string,
retweets int,
impression_count int)
PARTITIONED BY (year int ,month int , day int,hour int)
STORED AS PARQUET
LOCATION '/twitter-raw-data/tweets_raw';



msck repair table project.twitter_landing_table;


-- -----------------------  working on Users Dimension --------------------------------------------------

DROP TABLE IF EXISTS temp_table_users; 

-- Create temporary tables to hold merge records
CREATE TABLE temp_table_users LIKE project.users_raw; 

---- update records

INSERT INTO table temp_table_users
SELECT A.user_id  AS user_id, 
CASE WHEN B.username IS NOT NULL THEN B.username ELSE A.username end AS username,
CASE WHEN B.verified IS NOT NULL THEN B.verified ELSE A.verified end AS verified ,
CASE WHEN B.followers IS NOT NULL THEN B.followers ELSE A.followers end AS followers,   
CASE WHEN B.location IS NOT NULL THEN B.location ELSE A.location end AS location,
CASE WHEN B.year IS NOT NULL THEN B.year ELSE A.year end AS year,
CASE WHEN B.month IS NOT NULL THEN B.month ELSE A.month end AS month,
CASE WHEN B.day IS NOT NULL THEN B.day ELSE A.day end AS day,
CASE WHEN B.hour IS NOT NULL THEN B.hour ELSE A.hour end AS hour
FROM project.users_raw AS A 
       LEFT OUTER JOIN ( SELECT user_id, username, verified , followers , location ,tweet_date , year , month , day,hour
FROM (
  SELECT user_id, username, verified , followers , location, tweet_date, year , month , day,hour,
         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY cast(from_utc_timestamp(tweet_date, 'UTC') as timestamp) DESC) as rn
  FROM project.twitter_landing_table
) t
where rn=1) AS B 
                    ON A.user_id = B.user_id ;


--- new records

INSERT INTO temp_table_users 
SELECT B.user_id     AS user_id, 
       B.username   AS username, 
       B.verified  AS verified,
	   B.followers  AS followers,
	   B.location   AS location,
	   B.year , B.month , B.day,B.hour
FROM  ( SELECT user_id, username, verified , followers , location ,tweet_date , year , month , day,hour
FROM (
  SELECT user_id, username, verified , followers , location, tweet_date, year , month , day,hour,
         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY cast(from_utc_timestamp(tweet_date, 'UTC') as timestamp) DESC) as rn
  FROM project.twitter_landing_table
) t
where rn=1) AS B 
       LEFT OUTER JOIN temp_table_users AS A 
                    ON A.user_id = B.user_id 
WHERE  A.user_id IS NULL ;





-- drop users
drop table project.users_raw;


create external table if not EXISTS project.users_raw (
user_id string,
username string, 
verified boolean, 
followers int,
location string)
PARTITIONED BY (year int ,month int , day int,hour int)
STORED AS PARQUET
LOCATION '/twitter-raw-data/users_raw';


--- from temp to users:
insert overwrite table project.users_raw select user_id,username , verified, followers, location , year , month , day,hour from temp_table_users;
          
          
          
-- -----------------------  working on Tweets Dimension --------------------------------------------------


DROP TABLE IF EXISTS temp_table_tweets; 

-- Create temporary tables to hold merge records
CREATE TABLE temp_table_tweets LIKE project.tweets_raw;


INSERT INTO table temp_table_tweets
select t.tweet_id,t.user_id,t.tweet_date,t.text,t.retweets,t.impression_count, t.year , t.month , t.day,t.hour 
from project.twitter_landing_table t
left join project.tweets_raw o
on t.tweet_id=o.tweet_id
where o.tweet_id is null;

insert overwrite table project.tweets_raw select tweet_id,user_id,tweet_date,text,retweets,impression_count, year , month , day,hour from temp_table_tweets;

msck repair table project.tweets_raw;