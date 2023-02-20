select 
        first_name || ' ' || last_name as customer_name,
        ca.customer_city,
        ca.customer_state,
        s.food_pref_count,
        (st_distance(us.geo_location, chic.geo_location) / 1609)::int as chicago_distance_miles,
        (st_distance(us.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
    from vk_data.customers.customer_address as ca
    join vk_data.customers.customer_data c on ca.customer_id = c.customer_id
    left join vk_data.resources.us_cities us 
    on UPPER(rtrim(ltrim(ca.customer_state))) = upper(TRIM(us.state_abbr))
        and trim(lower(ca.customer_city)) = trim(lower(us.city_name))
    join (
        select 
            customer_id,
            count(*) as food_pref_count
        from vk_data.customers.customer_survey
        where is_active = true
        group by 1
    ) s on c.customer_id = s.customer_id
        cross join 
        ( select 
            geo_location
        from vk_data.resources.us_cities 
        where city_name = 'CHICAGO' and state_abbr = 'IL') chic
    cross join 
        ( select 
            geo_location
        from vk_data.resources.us_cities 
        where city_name = 'GARY' and state_abbr = 'IN') gary
    where 
        ((trim(city_name) ilike '%concord%' or trim(city_name) ilike '%georgetown%' or trim(city_name) ilike '%ashland%')
        and customer_state = 'KY')
        or
        (customer_state = 'CA' and (trim(city_name) ilike '%oakland%' or trim(city_name) ilike '%pleasant hill%'))
        or
        (customer_state = 'TX' and (trim(city_name) ilike '%arlington%') or trim(city_name) ilike '%brownsville%')