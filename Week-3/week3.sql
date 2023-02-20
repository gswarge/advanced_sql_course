
with web_activity as (
    select 
        event_id
        , session_id
        , event_timestamp
        , parse_json(event_details):recipe_id as recipe_id
        , parse_json(event_details):event as event_name 
    from
        vk_data.events.website_activity 
),

/* Calculating  
     -  session duration 
    -   number of search events in a session
*/
session_details as (
    select 
        event_timestamp::date as date
        , session_id
        , TIMESTAMPDIFF(second, min(event_timestamp::time), max(event_timestamp::time)) as session_duration_seconds
        , count_if(event_name='search') as num_of_searches
    from
        web_activity
    group by 1,2
),

/* Calculating
    - Daily Unique Sessions
    - Daily avg number of searches completed in a session, assuming a recipe is displayed after
    - Daily avg session duration
*/
session_metrics as (
    select 
        date 
        , count(distinct session_id)  daily_unique_sessions
        , avg(num_of_searches) as avg_no_of_searches
        , round(avg(session_duration_seconds::int),2) avg_session_duration_seconds
    from
        session_details
    group by 1
),

/* Most viewed recipe for the day */
top_recipe as (
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
        sm.date
        , daily_unique_sessions
        , avg_no_of_searches
        , avg_session_duration_seconds
        , top_recipe_id
        , no_of_views
    from session_metrics as sm
    inner join top_recipe as tr on tr.date = sm.date 
)

select * from result;