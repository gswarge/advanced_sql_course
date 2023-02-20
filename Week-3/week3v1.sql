with web_activity as (
    select 
        event_id
        , session_id
        , event_timestamp
        , parse_json(event_details):recipe_id as recipe_id
        , parse_json(event_details):page as page
        , parse_json(event_details):event as event_name 
    from
        vk_data.events.website_activity 
),

/* Daily Unique Sessions: */
q1 as (
    select
        event_timestamp::date as date
        , count(distinct session_id)  daily_unique_sessions
    from 
        web_activity
    group by 1
    order by 1 desc
),

/* Calculating Daily Avg Session Length 
Step 1: calculate session duration per session per day
*/
q2_1 as (
    select 
        event_timestamp::date as date
        , session_id
        , TIMESTAMPDIFF(second, min(event_timestamp::time), max(event_timestamp::time)) as session_duration_seconds
    from
        web_activity
    group by 1,2
),

q2_final as (
    select
        date
        , round(avg(session_duration_seconds::int),2) avg_session_duration_seconds
    from q2_1
    group by 1
),
/* Avg number of searches completed before displaying a recipe
- essentially count number of search events in the same session  */
q3_1 as (
    select 
        event_timestamp::date as date
        , session_id
        , count_if(event_name='search') as num_of_searches
    from web_activity
    group by 1,2
),
q3_final as (
    select 
        date 
        , avg(num_of_searches) as avg_no_of_searches
    from
        q3_1
    group by 1
),

/* Most viewed recipe for the day */
q4 as (
    select 
        event_timestamp::date as date
        , recipe_id as top_recipe_id
        , count(*) as no_of_views
    from 
        web_activity
    where recipe_id is not null
    group by 1,2
    qualify 
        row_number() over (partition by date order by no_of_views desc) = 1
),

result as (
    select
        q1.date as date
        , q1.daily_unique_sessions
        , q2.avg_session_duration_seconds
        , q3.avg_no_of_searches
        , q4.top_recipe_id
        , q4.no_of_views
    from
        q1 
        left join q2_final as q2 on q2.date = q1.date 
        left join q3_final as q3 on q3.date = q1.date
        left join q4 on q4.date = q1.date

)
select * from result;