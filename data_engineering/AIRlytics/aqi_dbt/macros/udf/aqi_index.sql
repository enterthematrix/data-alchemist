{% macro create_calculate_aqi_udf() %}
{% set sql %}
    CREATE OR REPLACE FUNCTION {{ target.database }}.{{ target.schema }}.calculate_aqi_udf(
        pm25 FLOAT, pm10 FLOAT, no2 FLOAT, so2 FLOAT, co FLOAT, o3 FLOAT, nh3 FLOAT
    )
    RETURNS FLOAT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    HANDLER = 'calculate_aqi_udf'
    AS
    $$
def calculate_aqi_udf(pm25, pm10, no2, so2, co, o3, nh3):
    breakpoints = {
        "PM2.5": [(0, 30, 0, 50), (31, 60, 51, 100), (61, 90, 101, 200), (91, 120, 201, 300), (121, 250, 301, 400), (251, 500, 401, 500)],
        "PM10": [(0, 50, 0, 50), (51, 100, 51, 100), (101, 250, 101, 200), (251, 350, 201, 300), (351, 430, 301, 400), (431, 600, 401, 500)],
        "NO2": [(0, 40, 0, 50), (41, 80, 51, 100), (81, 180, 101, 200), (181, 280, 201, 300), (281, 400, 301, 400), (401, 800, 401, 500)],
        "SO2": [(0, 40, 0, 50), (41, 80, 51, 100), (81, 380, 101, 200), (381, 800, 201, 300), (801, 1600, 301, 400), (1601, 2100, 401, 500)],
        "CO": [(0.0, 1.0, 0, 50), (1.1, 2.0, 51, 100), (2.1, 10.0, 101, 200), (10.1, 17.0, 201, 300), (17.1, 34.0, 301, 400), (34.1, 50.0, 401, 500)],
        "O3": [(0, 50, 0, 50), (51, 100, 51, 100), (101, 168, 101, 200), (169, 208, 201, 300), (209, 748, 301, 400), (749, 1000, 401, 500)],
        "NH3": [(0, 200, 0, 50), (201, 400, 51, 100), (401, 800, 101, 200), (801, 1200, 201, 300), (1201, 1800, 301, 400), (1801, 2400, 401, 500)]
    }

    def get_subindex(pollutant, value):
        for (bp_lo, bp_hi, i_lo, i_hi) in breakpoints.get(pollutant, []):
            if value is not None and bp_lo <= value <= bp_hi:
                return ((i_hi - i_lo) / (bp_hi - bp_lo)) * (value - bp_lo) + i_lo
        return None

    values = {
        "PM2.5": pm25,
        "PM10": pm10,
        "NO2": no2,
        "SO2": so2,
        "CO": co,
        "O3": o3,
        "NH3": nh3,
    }

    sub_indices = [
        sub_index for pollutant, val in values.items()
        if val is not None
        for sub_index in [get_subindex(pollutant, val)]
        if sub_index is not None
    ]

    return int(abs(max(sub_indices))) if sub_indices else None
    $$;
        {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
