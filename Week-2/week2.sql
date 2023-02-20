/* Refactored SQL query, Old sql query at the bottom*/ 
USE DATABASE VK_DATA;

with all_cities as (
    select
        trim(city_name) as city_name
        , trim(state_abbr) as state_abbr
        , geo_location
        , concat(upper(trim(uc.city_name)),',',upper(trim(uc.state_abbr))) as ac_city_state 
    from 
        vk_data.resources.us_cities uc
),
chicago_geolocation as (
    select 
        ac.geo_location
    from all_cities ac
    where upper(ac.city_name) = 'CHICAGO' and upper(ac.state_abbr) = 'IL'
),

gary_geolocation as (
    select 
        ac.geo_location
    from all_cities ac 
    where upper(city_name) = 'GARY' and upper(state_abbr) = 'IN'
),
customer_food_pref as (
    select 
        customer_id
        , count(*)::int as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
),
enriched_customer_data as (
    select 
        cd.first_name || ' ' || cd.last_name as customer_name
        , trim(ca.customer_city) as customer_city
        , trim(ca.customer_state) as customer_state
        , cfp.food_pref_count
        , concat(upper(trim(ca.customer_city)),',',upper(trim(ca.customer_state))) as cust_city_state
    from vk_data.customers.customer_data cd
    inner join vk_data.customers.customer_address ca 
            on cd.customer_id = ca.customer_id
    left join customer_food_pref cfp 
            on cfp.customer_id = cd.customer_id
),

affected_customers as (
    select 
        ecd.*
        , ac.geo_location
    from 
        enriched_customer_data ecd
        left join all_cities ac on ac.ac_city_state = ecd.cust_city_state
    where
        (lower(ecd.customer_city) in ('concord','georgetown','ashland') and lower(ecd.customer_state) = 'ky')
        or (lower(ecd.customer_city) in ('oakland','pleasant hill') and lower(ecd.customer_state) = 'ca')
        or (lower(ecd.customer_city) in ('arlington','brownsville') and lower(ecd.customer_state) = 'tx')
),

final as (
    select 
        ac.customer_name
        , ac.customer_city
        , ac.customer_state
        , ac.food_pref_count
        , (st_distance(ac.geo_location, chic.geo_location) / 1609)::int as chicago_distance_miles
        , (st_distance(ac.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles 
    from
        affected_customers ac
        cross join chicago_geolocation as chic
        cross join gary_geolocation as gary  
    where 
        food_pref_count >= 1
)

select * from final ;


