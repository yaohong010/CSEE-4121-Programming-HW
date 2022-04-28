def query_example():
  """
  This is just an example query to showcase the format of submission
  Please format the query in Bigquery console and paste it here.
  Submission starts from query_one.
  """
  return """
SELECT
 display_name,
 reputation,
 up_votes,
 down_votes
FROM
 `bigquery-public-data.stackoverflow.users`;
  """

def query_one():
    """Query one"""
    # add the formatted query between the triple quotes
    return """
SELECT 
	display_name, 
	reputation, 
	up_votes, 
	down_votes
FROM `bigquery-public-data.stackoverflow.users`
WHERE up_votes > 10000 AND down_votes < 300
ORDER BY reputation DESC
LIMIT 10;
    """

def query_two():
    """Query two"""
    return """
SELECT 
	location, 
	COUNT(id) AS count
FROM `bigquery-public-data.stackoverflow.users`
GROUP BY location
ORDER BY COUNT(id) DESC
LIMIT 10;
    """

def query_three():
    """Query three"""
    return """
SELECT
    CASE 
    WHEN location LIKE '%USA%' OR location LIKE '%United States%' THEN 'USA'
    WHEN location LIKE '%London%' OR location LIKE '%United Kingdom%' THEN 'UK'
    WHEN location LIKE '%France%' THEN 'France'
    WHEN location LIKE '%India%' THEN 'India'
    ELSE location END AS country, 
    COUNT(id) AS num_users
FROM `bigquery-public-data.stackoverflow.users`
WHERE location IS NOT NULL
GROUP BY country
ORDER BY COUNT(id) DESC
LIMIT 10;
    """

def query_four():
    """Query four"""
    return """
SELECT
    EXTRACT(YEAR FROM last_access_date AT TIME ZONE 'UTC') AS last_access_year,
    COUNT(id) AS num_users
FROM `bigquery-public-data.stackoverflow.users`
GROUP BY last_access_year
ORDER BY last_access_year DESC;
    """

def query_five():
    """Query five"""
    return """
SELECT
    id, 
    display_name, 
    last_access_date,
    DATE_DIFF('2021-12-30', EXTRACT(DATE FROM last_access_date AT TIME ZONE 'UTC'), DAY) AS days_since_last_access,
    DATE_DIFF('2021-12-30', EXTRACT(DATE FROM creation_date AT TIME ZONE 'UTC'), DAY) AS days_since_creation
FROM `bigquery-public-data.stackoverflow.users`
ORDER BY days_since_last_access DESC, days_since_creation DESC 
LIMIT 10;
    """

def query_six():
    """Query six"""
    return """
SELECT
    CASE
    WHEN 0 <= reputation AND reputation <= 500 THEN '0-500'
    WHEN 501 <= reputation AND reputation <= 5000 THEN '501-5000'
    WHEN 5001 <= reputation AND reputation <= 50000 THEN '5001-50000'
    WHEN 50001 <= reputation AND reputation <= 500000 THEN '50001-500000'
    WHEN 500000 < reputation THEN '>500000'
    END AS reputation_bucket, 
    ROUND(SUM(up_votes)/SUM(down_votes), 2) AS upvote_ratio,
    COUNT(id) AS num_users
FROM `bigquery-public-data.stackoverflow.users`
GROUP BY reputation_bucket
ORDER BY num_users DESC;
    """

def query_seven():
    """Query seven"""
    return """
SELECT 
    T.tag_name AS tag, 
    COUNT(T.tag_name) AS count
FROM `bigquery-public-data.stackoverflow.tags` T
INNER JOIN 
(
SELECT
    tags,
    EXTRACT(YEAR FROM creation_date AT TIME ZONE 'UTC') AS creation_year
FROM `bigquery-public-data.stackoverflow.posts_questions`
) PQ
ON T.tag_name = PQ.tags
WHERE PQ.creation_year = 2020
GROUP BY T.tag_name
ORDER BY COUNT(T.tag_name) DESC
LIMIT 10;
    """

def query_eight():
    """Query eight"""
    return """
SELECT
    name, 
    COUNT(user_id) AS num_users
FROM `bigquery-public-data.stackoverflow.badges`
WHERE class = 1
GROUP BY name
ORDER BY COUNT(user_id) DESC
LIMIT 10;
    """

def query_nine():
    """Query nine"""
    return """
SELECT
    u.id,
    u.display_name, 
    u.reputation, 
    u.up_votes,
    u.down_votes,
    COUNT(b.name) AS num_gold_badges
FROM `bigquery-public-data.stackoverflow.users` u
INNER JOIN `bigquery-public-data.stackoverflow.badges` b
ON u.id = b.user_id
WHERE b.class = 1
GROUP BY u.id, u.display_name, u.reputation, u.up_votes, u.down_votes
ORDER BY COUNT(b.name) DESC
LIMIT 10;

    """

def query_ten():
    """Query ten"""
    return """
SELECT
    u.id, 
    u.display_name, 
    u.reputation, 
    DATE_DIFF(EXTRACT(DATE FROM b.date AT TIME ZONE 'UTC'), 
              EXTRACT(DATE FROM u.creation_date AT TIME ZONE 'UTC'), DAY) AS num_days
FROM `bigquery-public-data.stackoverflow.users` u
INNER JOIN `bigquery-public-data.stackoverflow.badges` b
ON u.id = b.user_id
WHERE b.name = "Illuminator"
ORDER BY num_days
LIMIT 20;
    """

def query_eleven():
    """Query eleven"""
    return """
SELECT  
    CASE
    WHEN score < 0 THEN '<0'
    WHEN 0 <= score AND score <= 100 THEN '0-100'
    WHEN 101 <= score AND score <= 1000 THEN '101-1000'
    WHEN 1001 <= score AND score <= 10000 THEN '1001-10000'
    WHEN 10000 < score THEN '>10000'
    END AS score_bucket,
    ROUND(AVG(view_count), 2) as avg_num_views
FROM `bigquery-public-data.stackoverflow.posts_questions`
GROUP BY score_bucket
ORDER BY avg_num_views;
    """


def query_twelve():
    """Query twelve"""
    return """
SELECT  
    EXTRACT(DAYOFWEEK FROM creation_date) AS day_name, 
    COUNT(id) AS num_answers
FROM `bigquery-public-data.stackoverflow.posts_answers`
GROUP BY day_name
ORDER BY num_answers DESC;
    """

def query_thirteen():
    """Query thirteen"""
    return """
WITH cte1 as (
SELECT  
    EXTRACT(YEAR FROM creation_date AT TIME ZONE 'UTC') AS year, 
    COUNT(id) AS num_questions, 
FROM `bigquery-public-data.stackoverflow.posts_questions`
GROUP BY year
), 
cte2 as (
SELECT  
    EXTRACT(YEAR FROM creation_date AT TIME ZONE 'UTC') AS year, 
    COUNT(id) AS num_answered, 
FROM `bigquery-public-data.stackoverflow.posts_questions`
WHERE answer_count > 0
GROUP BY year
)
SELECT 
    cte1.year,
    cte1.num_questions,
    ROUND(cte2.num_answered/cte1.num_questions*100, 2) AS percentage_answered
FROM cte1 JOIN cte2 ON cte1.year = cte2.year
ORDER BY cte1.year;
    """

def query_fourteen():
    """Query fourteen"""
    return """
SELECT 
    u.id,
    u.display_name, 
    u.reputation, 
    COUNT(a.id) AS num_answers
FROM `bigquery-public-data.stackoverflow.users` u
LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` a ON u.id = a.owner_user_id
GROUP BY u.id, u.display_name, u.reputation
HAVING COUNT(a.id) > 50
ORDER BY num_answers DESC
LIMIT 20;
    """

def query_fifteen():
    """Query fifteen"""
    return """
SELECT 
    u.id,
    u.display_name, 
    u.reputation, 
    COUNT(a.id) AS num_answers
FROM `bigquery-public-data.stackoverflow.users` u
LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` a ON u.id = a.owner_user_id
LEFT JOIN `bigquery-public-data.stackoverflow.posts_questions` q ON a.parent_id = q.id
WHERE q.tags LIKE '%python%'
GROUP BY u.id, u.display_name, u.reputation
HAVING COUNT(a.id) > 50
ORDER BY num_answers DESC
LIMIT 20;
    """

def query_sixteen():
    """Query sixteen"""
    return """
WITH cte as(
SELECT 
    CASE 
    WHEN score < 0 THEN "<0"
    WHEN score > 10000 THEN ">10000"
    END AS score, 
    AVG(answer_count) AS avg_answers,
    AVG(favorite_count) AS avg_fav_count, 
    AVG(comment_count) AS avg_comments
FROM `bigquery-public-data.stackoverflow.stackoverflow_posts` u
GROUP BY score
)
SELECT
    *
FROM cte
WHERE score = "<0" OR score = ">10000"
ORDER BY score;
    """
