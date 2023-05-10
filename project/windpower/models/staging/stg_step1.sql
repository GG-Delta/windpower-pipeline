with topic1 as (
    select 
    value::DOUBLE as pressure_msl,
    index::INTEGER as t1_idx,
    record_content:idloc::STRING as t1_id_loc,
    record_metadata:CreateTime::NUMBER as t1_timestamp_unix,
    record_content:elevation::DOUBLE as elevation,
    record_content:latitude::DOUBLE as geo_lat,
    record_content:longitude::DOUBLE as geo_long,
    record_content:timezone_abbreviation::STRING as time_zone,
    to_timestamp(record_metadata:CreateTime::NUMBER,3) as timestamp
    from {{ source('conf_schema', 'PKSQLC_Q71PMALL_STREAM_1787699529') }},
    lateral flatten(input => try_parse_json(record_content:hourly:pressure_msl))
)
, topic2 as (
    select
    value::DOUBLE as temperature_2m,
    index::INTEGER as t2_idx,
    record_content:idloc::STRING as t2_id_loc,
    record_metadata:CreateTime::NUMBER as t2_timestamp_unix
    from {{ source('conf_schema', 'PKSQLC_Q71PMALL_STREAM_1787699529') }},
    lateral flatten(input => try_parse_json(record_content:hourly:temperature_2m))
)
, topic3 as (
    select
    value::DOUBLE as temperature_80m,
    index::INTEGER as t3_idx,
    record_content:idloc::STRING as t3_id_loc,
    record_metadata:CreateTime::NUMBER as t3_timestamp_unix
    from {{ source('conf_schema', 'PKSQLC_Q71PMALL_STREAM_1787699529') }},
    lateral flatten(input => try_parse_json(record_content:hourly:temperature_80m))
)
, topic4 as (
    select
    value::DOUBLE as windgusts_10m,
    index::INTEGER as t4_idx,
    record_content:idloc::STRING as t4_id_loc,
    record_metadata:CreateTime::NUMBER as t4_timestamp_unix
    from {{ source('conf_schema', 'PKSQLC_Q71PMALL_STREAM_1787699529') }},
    lateral flatten(input => try_parse_json(record_content:hourly:windgusts_10m))
)
, topic5 as (
    select
    value::DOUBLE as windspeed_10m,
    index::INTEGER as t5_idx,
    record_content:idloc::STRING as t5_id_loc,
    record_metadata:CreateTime::NUMBER as t5_timestamp_unix
    from {{ source('conf_schema', 'PKSQLC_Q71PMALL_STREAM_1787699529') }},
    lateral flatten(input => try_parse_json(record_content:hourly:windspeed_10m))
)
, topic6 as (
    select
    value::DOUBLE as windspeed_80m,
    index::INTEGER as t6_idx,
    record_content:idloc::STRING as t6_id_loc,
    record_metadata:CreateTime::NUMBER as t6_timestamp_unix
    from {{ source('conf_schema', 'PKSQLC_Q71PMALL_STREAM_1787699529') }},
    lateral flatten(input => try_parse_json(record_content:hourly:windspeed_80m))
)
, topic7 as (
    select
    value::DOUBLE as winddirection_10m,
    index::INTEGER as t7_idx,
    record_content:idloc::STRING as t7_id_loc,
    record_metadata:CreateTime::NUMBER as t7_timestamp_unix
    from {{ source('conf_schema', 'PKSQLC_Q71PMALL_STREAM_1787699529') }},
    lateral flatten(input => try_parse_json(record_content:hourly:winddirection_10m))
)
, topic8 as (
    select
    value::DOUBLE as winddirection_80m,
    index::INTEGER as t8_idx,
    record_content:idloc::STRING as t8_id_loc,
    record_metadata:CreateTime::NUMBER as t8_timestamp_unix
    from {{ source('conf_schema', 'PKSQLC_Q71PMALL_STREAM_1787699529') }},
    lateral flatten(input => try_parse_json(record_content:hourly:winddirection_80m))
)

, base_time as (
    select 
    record_content:hourly:time as col,
    record_content:idloc::STRING as t9_id_loc,
    record_metadata:CreateTime::NUMBER as t9_timestamp_unix
    from {{ source('conf_schema', 'PKSQLC_Q71PMALL_STREAM_1787699529') }}
)
, base_time2 as (
    select
        replace(col,' ','') as col2, -- remove all whitespaces - before, in-between and after,
        t9_id_loc,
        t9_timestamp_unix
    from base_time
)
, splt_time as (
    select
        split(ltrim(trim(trim(col2,'"['),'"]')), ',') as json_array,
        t9_id_loc,
        t9_timestamp_unix
    from base_time2
)

, topic9 as (
    select
    try_to_timestamp(t.value::varchar, 'YYYY-MM-DDTHH24:MI') as time,
    t.value as time_value,
    t.index::INTEGER as t9_idx,
    t9_id_loc,
    t9_timestamp_unix
    from splt_time,
    lateral flatten(input => json_array) as t
)

SELECT 
     elevation
    , geo_lat
    , geo_long
    , time_zone
    , timestamp
    , t1_idx
    , t1_id_loc
    , t1_timestamp_unix
    , pressure_msl
    , temperature_2m
    , temperature_80m
    , windgusts_10m
    , windspeed_10m
    , windspeed_80m
    , winddirection_10m
    , winddirection_80m
    , time --topic9.value -- time_value
FROM 
topic1
left join topic2 on topic1.t1_idx = topic2.t2_idx 
    and topic1.t1_id_loc = topic2.t2_id_loc and topic1.t1_timestamp_unix = topic2.t2_timestamp_unix
left join topic3 on topic1.t1_idx = topic3.t3_idx 
    and topic1.t1_id_loc = topic3.t3_id_loc and topic1.t1_timestamp_unix = topic3.t3_timestamp_unix
left join topic4 on topic1.t1_idx = topic4.t4_idx 
    and topic1.t1_id_loc = topic4.t4_id_loc and topic1.t1_timestamp_unix = topic4.t4_timestamp_unix
left join topic5 on topic1.t1_idx = topic5.t5_idx 
    and topic1.t1_id_loc = topic5.t5_id_loc and topic1.t1_timestamp_unix = topic5.t5_timestamp_unix
left join topic6 on topic1.t1_idx = topic6.t6_idx 
    and topic1.t1_id_loc = topic6.t6_id_loc and topic1.t1_timestamp_unix = topic6.t6_timestamp_unix
left join topic7 on topic1.t1_idx = topic7.t7_idx 
    and topic1.t1_id_loc = topic7.t7_id_loc and topic1.t1_timestamp_unix = topic7.t7_timestamp_unix
left join topic8 on topic1.t1_idx = topic8.t8_idx 
    and topic1.t1_id_loc = topic8.t8_id_loc and topic1.t1_timestamp_unix = topic8.t8_timestamp_unix
left join topic9 on topic1.t1_idx = topic9.t9_idx 
    and topic1.t1_id_loc = topic9.t9_id_loc and topic1.t1_timestamp_unix = topic9.t9_timestamp_unix