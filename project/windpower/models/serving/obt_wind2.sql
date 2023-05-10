{{ dbt_utils.deduplicate(
    relation=ref('obt_wind'),
    partition_by='id_loc, time_hour',
    order_by='timestamp_last desc',
   )
}}