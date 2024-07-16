use new_db;

# CASE STUDY 1

#Jobs reviewed over time:

select ds, ((count(job_id))/(sum(time_spent)/3600)) as JobreviewPerHour
from job_data
group by ds;


# Throughput analysis

select ((count('event'))/(sum(time_spent))) as weekly_rollingaverage 
from job_data; 




#Language share analysis

select  language, count(*)*100/sum(count(*)) over()  as languagePercent
from job_data
where ds<= "11/30/2020 " and ds>= "11/01/2020"
group by language;

# Duplicate rows detection
 
 select ds,job_id,actor_id, count(*) as duplicate
 from job_data
 group by ds,job_id,actor_id
 having count(*) > 1;
 
 select actor_id, count(*) as duplicates
 from job_data
 group by actor_id
 having duplicates >1;
 
  
  
  SELECT 
    job_id, COUNT(*) AS duplicates
FROM
    job_data
GROUP BY job_id
HAVING duplicates > 1;
 
 







# CASE STUDY 2

#  Weekly user engagement 
select week(str_to_date(occurred_at,"%d-%m-%Y %H:%i")) as week,
       count(distinct user_id) as num_of_users
from events
where event_type ='engagement'
group by week;


# User growth analysis


select activated_year, activated_month, num_of_activeUsers,
   sum(num_of_activeUsers) over 
       ( order by activated_year, activated_month ) as cumsum_activeUsers
from
(select year(str_to_date(activated_at,"%d-%m-%Y %H:%i")) as activated_year,
       month(str_to_date(activated_at,"%d-%m-%Y %H:%i")) as activated_month,
        count(distinct user_id) as num_of_activeUsers
        from users
        where state= "active"
        group by activated_year, activated_month
        order by activated_year, activated_month) as growth_table;
         




# Weekly retention analysis
 
 SELECT 
    signup_week AS week_num,
    SUM(CASE WHEN week_nums = 0 THEN 1 ELSE 0 END) AS 'week 0',
    SUM(CASE WHEN week_nums = 1 THEN 1 ELSE 0 END) AS 'week 1',
    SUM(CASE WHEN week_nums = 2 THEN 1 ELSE 0 END) AS 'week 2',
	SUM(CASE WHEN week_nums = 3 THEN 1 ELSE 0 END) AS 'week 3',
    SUM(CASE WHEN week_nums = 4 THEN 1 ELSE 0 END) AS 'week 4',
    SUM(CASE WHEN week_nums = 5 THEN 1 ELSE 0 END) AS 'week 5',
    SUM(CASE WHEN week_nums = 6 THEN 1 ELSE 0 END) AS 'week 6',
    SUM(CASE WHEN week_nums = 7 THEN 1 ELSE 0 END) AS 'week 7', 
    SUM(CASE WHEN week_nums = 8 THEN 1 ELSE 0 END) AS 'week 8',
    SUM(CASE WHEN week_nums = 9 THEN 1 ELSE 0 END) AS 'week 9',
    SUM(CASE WHEN week_nums = 10 THEN 1 ELSE 0 END) AS 'week 10',
    SUM(CASE WHEN week_nums = 11 THEN 1 ELSE 0 END) AS 'week 11',
    SUM(CASE WHEN week_nums = 12 THEN 1 ELSE 0 END) AS 'week 12',
    SUM(CASE WHEN week_nums = 13 THEN 1 ELSE 0 END) AS 'week 13',
    SUM(CASE WHEN week_nums = 14 THEN 1 ELSE 0 END) AS 'week 14',
    SUM(CASE WHEN week_nums = 15 THEN 1 ELSE 0 END) AS 'week 15',
    SUM(CASE WHEN week_nums = 16 THEN 1 ELSE 0 END) AS 'week 16',
    SUM(CASE WHEN week_nums = 17 THEN 1 ELSE 0 END) AS 'week 17'
FROM
    (SELECT 
        a.user_id, b.engagement_week, a.signup_week,
		b.engagement_week - a.signup_week AS week_nums
    FROM
        (SELECT 
               user_id, WEEK(STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i')) AS signup_week
    FROM events
    WHERE event_type = 'signup_flow' AND event_name = 'complete_signup'
    GROUP BY 1 , 2) AS a
    LEFT JOIN 
    (SELECT 
        user_id, WEEK(STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i')) AS engagement_week
    FROM events
    WHERE event_type = 'engagement'
    GROUP BY 1 , 2) AS b  
    ON b.user_id = a.user_id) AS c
GROUP BY signup_week
ORDER BY signup_week;



# Weekly Engagement Per Device:

select year(str_to_date(occurred_at,"%d-%m-%Y %H:%i")) as year,
       week(str_to_date(occurred_at,"%d-%m-%Y %H:%i")) as week,
       device, count(distinct user_id) as user_count
       from events
       where event_type= 'engagement'
       group by 2,3,1
       order by 2,3,1;
       
# Email Engagement Analysis
       
SELECT 
    user_type,
    action,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS num_of_users,
    COUNT(*) / COUNT(DISTINCT user_id) AS averageEvents_per_user
FROM
    email_events
GROUP BY 1 , 2;
       
       
       
       
       
       
       

