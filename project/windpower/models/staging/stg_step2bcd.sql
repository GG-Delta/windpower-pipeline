with d_past as (
    select * from {{ ref('stg_step2a') }}
    where (date(TIME) = (date(TIMESTAMP) - 1.0))
)
, d_pred as (
    select * from {{ ref('stg_step2a') }}
    where (date(TIME) > date(TIMESTAMP))
)

, obt as (
    select 
    d_pred.ELEVATION AS ELEVATION,
    d_pred.GEO_LAT AS GEO_LAT,
    d_pred.GEO_LONG AS GEO_LONG,
    d_pred.TIME_ZONE AS TIME_ZONE,
    d_pred.TIMESTAMP AS TIMESTAMP_pred,
    d_pred.T1_IDX AS IDX_pred,
    d_pred.T1_ID_LOC AS ID_LOC,
    d_pred.T1_TIMESTAMP_UNIX AS T1_TIMESTAMP_UNIX_pred,
    d_pred.PRESSURE_MSL AS PRESSURE_MSL_pred,
    d_pred.TEMPERATURE_2M AS TEMPERATURE_2M_pred,
    d_pred.TEMPERATURE_80M AS TEMPERATURE_80M_pred,
    d_pred.WINDGUSTS_10M AS WINDGUSTS_10M_pred,
    d_pred.WINDSPEED_10M AS WINDSPEED_10M_pred,
    d_pred.WINDSPEED_80M AS WINDSPEED_80M_pred,
    d_pred.WINDDIRECTION_10M AS WINDDIRECTION_10M_pred,
    d_pred.WINDDIRECTION_80M AS WINDDIRECTION_80M_pred,
    d_pred.TIME AS TIME,
    d_pred.TIMESTAMP_DAY AS TIMESTAMP_DAY_pred,
    d_past.TIMESTAMP AS TIMESTAMP_past,
    d_past.T1_IDX AS IDX_past,
    d_past.T1_TIMESTAMP_UNIX AS T1_TIMESTAMP_UNIX_past,
    d_past.PRESSURE_MSL AS PRESSURE_MSL_past,
    d_past.TEMPERATURE_2M AS TEMPERATURE_2M_past,
    d_past.TEMPERATURE_80M AS TEMPERATURE_80M_past,
    d_past.WINDGUSTS_10M AS WINDGUSTS_10M_past,
    d_past.WINDSPEED_10M AS WINDSPEED_10M_past,
    d_past.WINDSPEED_80M AS WINDSPEED_80M_past,
    d_past.WINDDIRECTION_10M AS WINDDIRECTION_10M_past,
    d_past.WINDDIRECTION_80M AS WINDDIRECTION_80M_past,
    d_past.TIMESTAMP_DAY AS TIMESTAMP_DAY_past
FROM d_pred
LEFT JOIN d_past
  ON (
    d_pred.T1_ID_LOC = d_past.T1_ID_LOC AND
    d_pred.TIME = d_past.TIME
  )

)

select * 
from obt