-- ref: https://docs.snowflake.com/en/user-guide/ml-functions/classification
USE SCHEMA DATA_ALCHEMIST.CORTEX;

-- Setting Up the Data

-- Training Data
-- The training data consists of user interest scores and ratings, along with labels indicating whether the user made a purchase or not.
CREATE OR REPLACE TABLE training_purchase_data AS (
    SELECT
        CAST(UNIFORM(0, 4, RANDOM()) AS VARCHAR) AS user_interest_score,
        UNIFORM(0, 3, RANDOM()) AS user_rating,
        FALSE AS label,
        'not_interested' AS class
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(4, 7, RANDOM()) AS VARCHAR) AS user_interest_score,
        UNIFORM(3, 7, RANDOM()) AS user_rating,
        FALSE AS label,
        'add_to_wishlist' AS class
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(7, 10, RANDOM()) AS VARCHAR) AS user_interest_score,
        UNIFORM(7, 10, RANDOM()) AS user_rating,
        TRUE AS label,
        'purchase' AS class
    FROM TABLE(GENERATOR(rowCount => 100))
);

-- Test Data
-- The test data consists of user interest scores and ratings, but without labels. This data will be used to evaluate the model's performance.
CREATE OR REPLACE table prediction_purchase_data AS (
    SELECT
        CAST(UNIFORM(0, 4, RANDOM()) AS VARCHAR) AS user_interest_score,
        UNIFORM(0, 3, RANDOM()) AS user_rating
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(4, 7, RANDOM()) AS VARCHAR) AS user_interest_score,
        UNIFORM(3, 7, RANDOM()) AS user_rating
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(7, 10, RANDOM()) AS VARCHAR) AS user_interest_score,
        UNIFORM(7, 10, RANDOM()) AS user_rating
    FROM TABLE(GENERATOR(rowCount => 100))
);

-- #################################################### BINARY CLASSIFICATION  ####################################################

-- View for Binary Classification
CREATE OR REPLACE view binary_classification_view AS
    SELECT user_interest_score, user_rating, label
FROM training_purchase_data;

-- create and train the binary classification model
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model_binary(
    INPUT_DATA => SYSTEM$REFERENCE('view', 'binary_classification_view'),
    TARGET_COLNAME => 'label'
);

-- Get the predictions and save to a table
CREATE OR REPLACE TABLE my_binary_predictions AS
SELECT *, model_binary!PREDICT(INPUT_DATA => {*})
    AS predictions FROM prediction_purchase_data;

-- unpack the predictions JSON object
SELECT 
    predictions:class AS predicted_class,
    predictions:probability:False AS false_prob,
    predictions:probability:True AS true_prob
    FROM my_binary_predictions;


CALL model_binary!SHOW_CONFUSION_MATRIX();

-- metrics for each class predicted by the model
CALL model_binary!SHOW_EVALUATION_METRICS();

-- calculates overall (global) metrics for all classes predicted by the model by averaging the per-class metrics calculated by show_evaluation_metrics
CALL model_binary!SHOW_GLOBAL_EVALUATION_METRICS();

-- calculates the feature importance for each feature used in the model 
-- it counts the number of times the model’s trees used each feature to make a decision. 
-- these feature importance scores are then normalized to values between 0 and 1
CALL model_binary!SHOW_FEATURE_IMPORTANCE();

-- #################################################### MULTI-CLASS CLASSIFICATION  ####################################################
-- View for Multi-Class Classification
CREATE OR REPLACE VIEW multiclass_classification_view AS
    SELECT user_interest_score, user_rating, class
FROM training_purchase_data;

CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model_multiclass(
    INPUT_DATA => SYSTEM$REFERENCE('view', 'multiclass_classification_view'),
    TARGET_COLNAME => 'class'
);

-- Get the predictions and save to a table
CREATE OR REPLACE TABLE my_predictions AS
SELECT *, model_multiclass!PREDICT(INPUT_DATA => {*}) AS predictions FROM prediction_purchase_data;

SELECT * FROM my_predictions;

SELECT
    predictions:class AS predicted_class,
    ROUND(predictions:probability:not_interested,4) AS not_interested_class_probability,
    ROUND(predictions['probability']['purchase'],4) AS purchase_class_probability,
    ROUND(predictions['probability']['add_to_wishlist'],4) AS add_to_wishlist_class_probability
FROM my_predictions
LIMIT 5;

CALL model_multiclass!SHOW_EVALUATION_METRICS();

CALL model_multiclass!SHOW_GLOBAL_EVALUATION_METRICS();

CALL model_multiclass!SHOW_CONFUSION_MATRIX();


-- #################################################### CLASSIFICATION USING BANK DATASET  ####################################################
-- create the marketing table using the bank dataset @ data/marketing.csv

-- categorize the data into train and infer groups

CREATE OR REPLACE VIEW marketing_view as (
  SELECT *,
    CASE WHEN UNIFORM(0::float, 1::float, RANDOM()) < .95
    THEN 'train' ELSE 'infer' END AS grp
  FROM marketing);

-- training data view 
CREATE OR REPLACE VIEW marketing_train AS (
SELECT * EXCLUDE grp
FROM marketing_view 
WHERE grp = 'train');

-- inference data view 
CREATE OR REPLACE VIEW marketing_infer AS (
  SELECT * EXCLUDE grp
  FROM marketing_view 
  WHERE grp = 'infer');

SELECT count(*) FROM marketing_infer;
SELECT count(*) FROM marketing_train;


-- create and train the binary classification model
CREATE OR REPLACE snowflake.ml.classification bank_clf(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'marketing_train'),
    TARGET_COLNAME => 'Y',
    CONFIG_OBJECT => {'evaluate': TRUE , 'on_error': 'skip'});
SHOW snowflake.ml.classification;

-- Get the predictions and save to a table

CREATE OR REPLACE TABLE marketing_preds AS (
    SELECT Y,
        preds:class::boolean as pred,
        preds:probability:False as false_proba,
        preds:probability:True as true_proba
    FROM (
        SELECT bank_clf!PREDICT(object_construct(*)) AS preds, Y
        FROM marketing_infer));

SELECT *
FROM marketing_preds
LIMIT 100;

CALL bank_clf!SHOW_EVALUATION_METRICS();

CALL bank_clf!SHOW_GLOBAL_EVALUATION_METRICS();

CALL bank_clf!SHOW_CONFUSION_MATRIX();