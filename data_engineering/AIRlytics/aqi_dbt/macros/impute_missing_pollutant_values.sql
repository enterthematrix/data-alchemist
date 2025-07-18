{% macro impute_missing_pollutant_values(pollutant_id) %}
COALESCE(
    TRY_TO_NUMBER({{ pollutant_id }}),
    -- Impute a value from same station
    LAST_VALUE(TRY_TO_NUMBER({{ pollutant_id }})) IGNORE NULLS OVER (
        PARTITION BY STATION ORDER BY RECORD_TS
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    FIRST_VALUE(TRY_TO_NUMBER({{ pollutant_id }})) IGNORE NULLS OVER (
        PARTITION BY STATION ORDER BY RECORD_TS
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ),
    -- Impute a value from a different station in the same city
    LAST_VALUE(TRY_TO_NUMBER({{ pollutant_id }})) IGNORE NULLS OVER (
        PARTITION BY CITY ORDER BY RECORD_TS
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    FIRST_VALUE(TRY_TO_NUMBER({{ pollutant_id }})) IGNORE NULLS OVER (
        PARTITION BY CITY ORDER BY RECORD_TS
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ),
    -- Impute a value from a different station in the same state
    LAST_VALUE(TRY_TO_NUMBER({{ pollutant_id }})) IGNORE NULLS OVER (
        PARTITION BY STATE ORDER BY RECORD_TS
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    FIRST_VALUE(TRY_TO_NUMBER({{ pollutant_id }})) IGNORE NULLS OVER (
        PARTITION BY STATE ORDER BY RECORD_TS
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ),
    -- Any station across all states as a last resort
    LAST_VALUE(TRY_TO_NUMBER(PM10_AVG)) IGNORE NULLS OVER (
        ORDER BY RECORD_TS
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    FIRST_VALUE(TRY_TO_NUMBER(PM10_AVG)) IGNORE NULLS OVER (
        ORDER BY RECORD_TS
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    )
)
{% endmacro %}
