USE DATABASE DATA_ALCHEMIST;
USE SCHEMA DIAMONDS;


select *
     from diamonds_transform
     limit 10;

-- predict with default model version
with test_df as (
     select *
     from diamonds_transform
     limit 10),
preds as (
    SELECT PRICE, DIAMOND_PRICE_PREDICTOR!predict(
        CUT_OE, COLOR_OE, CLARITY_OE, CARAT, DEPTH, TABLE_PCT, X, Y, Z) pred
    FROM test_df)
select price, pred:PREDICTED_PRICE
from preds;

-- predict with specific model version
with test_df as (
    select *
    from diamonds_transform
    limit 10),
preds as (
    WITH v1 AS MODEL DIAMOND_PRICE_PREDICTOR VERSION V1
        SELECT PRICE, v1!predict(
            CUT_OE, COLOR_OE, CLARITY_OE, CARAT, DEPTH, TABLE_PCT, X, Y, Z) pred
        FROM test_df)
select price, pred:PREDICTED_PRICE
from preds;