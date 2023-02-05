/*
Used CTTE's to create various subsets of the data:
1) Performed an Inner join between customer_data and customer_address
2) 
*/
USE DATABASE VK_DATA;
with cust as (
    Select 
        cd.customer_id
        , cd.first_name
        , cd.last_name
        , cd.email
        , concat(upper(trim(ca.customer_city)),',',upper(trim(ca.customer_state))) as cust_city_state
    from
        customers.customer_data cd
        inner join customers.customer_address ca on ca.customer_id = cd.customer_id
),
cities as (
    select 
        ru.city_id
        , ru.geo_location
        , concat(upper(trim(ru.city_name)),',',upper(trim(ru.state_abbr))) as res_city_state
    from
        resources.us_cities ru
),
supp as (
    select 
        *
        , concat(upper(trim(su.supplier_city)),',',upper(trim(su.supplier_state))) as su_city_state 
    from
    SUPPLIERS.SUPPLIER_INFO su
),

suppliers_geo_coded as (
    select 
        supplier_id
        , supplier_name
        , res_city_state as supplier_city_state
        , cities.geo_location
    from supp
    left join cities on cities.res_city_state = supp.su_city_state
), 

eligible_cust_geo_coded as (
    select 
    cust.customer_id
    , cust.first_name
    , cust.last_name
    , cust.email
    , cities.geo_location
from
    cust 
    inner join cities on cust.cust_city_state = cities.res_city_state
),

final as (
select 
    customer_id
    , first_name
    , last_name
    , email
    , supplier_id
    , supplier_name
    , round(ST_DISTANCE(ecgc.geo_location,sgc.geo_location)/1000,2) as distance_in_kms
    , round(ST_DISTANCE(ecgc.geo_location,sgc.geo_location)/1609,2) as distance_in_miles
from
    eligible_cust_geo_coded ecgc
    cross join suppliers_geo_coded sgc  
qualify row_number() over (partition by customer_id order by distance_in_kms) = 1
)

select * from final order by 3,2;

