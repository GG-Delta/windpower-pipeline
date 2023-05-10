select 
    ELEVATION as elevation
    , GEO_LAT as geo_lat
    , GEO_LONG as geo_long
    , TIME_ZONE as time_zone
    , ID_LOC as id_loc
    , LAST_VALUE(TIMESTAMP_PRED) OVER (PARTITION BY ID_LOC ORDER BY TIME ASC) AS timestamp_last
    , TIME as time_hour
    , AVG(PRESSURE_MSL_PRED) OVER (PARTITION BY ID_LOC, TIME) AS avg_pressure_msl_pred
    , AVG(TEMPERATURE_2M_PRED) OVER (PARTITION BY ID_LOC, TIME) AS avg_temperature_2m_pred
    , AVG(TEMPERATURE_80M_PRED) OVER (PARTITION BY ID_LOC, TIME) AS avg_temperature_80m_pred
    , AVG(WINDGUSTS_10M_PRED) OVER (PARTITION BY ID_LOC, TIME) AS avg_windgusts_10m_pred
    , AVG(WINDSPEED_10M_PRED) OVER (PARTITION BY ID_LOC, TIME) AS avg_windspeed_10m_pred
    , AVG(WINDSPEED_80M_PRED) OVER (PARTITION BY ID_LOC, TIME) AS avg_windspeed_80m_pred
    , MSQ_ER_PRESSURE_MSL as msq_er_pressure_msl
    , MSQ_ER_TEMPERATURE_2M as msq_er_temperature_2m
    , MSQ_ER_TEMPERATURE_80M as msq_er_temperature_80m
    , MSQ_ER_WINDGUSTS_10M as msq_er_windgusts_10m
    , MSQ_ER_WINDSPEED_10M as msq_er_windspeed_10m
    , MSQ_ER_WINDSPEED_80M as msq_er_windspeed_80m
from {{ ref('stg_step3')}}