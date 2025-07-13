# get clean AQI data
get_latest_aqi_data = """
        SELECT 
            f.date_fk,
            f.LOCATION_FK,
            f.AQI,
            f.AQI_CATEGORY,
            f.PROMINENT_POLLUTANT,
            f.PM10_AVG_VALUE,
            f.PM25_AVG_VALUE,
            f.SO2_AVG_VALUE,
            f.NO2_AVG_VALUE,
            f.NH3_AVG_VALUE,
            f.CO_AVG_VALUE,
            f.O3_AVG_VALUE,
            l.COUNTRY,
            l.STATE,
            l.CITY,
            l.STATION,
            l.LATITUDE,
            l.LONGITUDE,
            t.RECORD_TS,
            t.AQI_YEAR,
            t.AQI_MONTH
        FROM AQI.DEV.FACT_AQI f
        LEFT JOIN AQI.DEV.DIM_LOCATIONS l
        ON f.LOCATION_FK = l.LOCATION_PK
        LEFT JOIN AQI.DEV.DIM_MEASUREMENT_TIME t
        ON f.date_fk = t.DATE_PK
        ORDER BY t.record_ts DESC
    """


# AQI data aggregated for states
get_aqi_data_per_state = """
    SELECT 
        YEAR,
        MONTH,
        STATE, 
        AQI,
        PM10_AVG, 
        PM25_AVG, 
        SO2_AVG, 
        NO2_AVG, 
        O3_AVG, 
        CO_AVG, 
        NH3_AVG,
        PROMINENT_POLLUTANT, 
        AQI_CATEGORY
    FROM AQI.DEV.MONTHLY_AQI_STATE 
"""
