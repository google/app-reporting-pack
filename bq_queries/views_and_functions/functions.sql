-- Get number of elements in first non-emtpy array.
CREATE OR REPLACE FUNCTION `{bq_project}.{target_dataset}.GetNumberOfElements` (first_element STRING, second_element STRING, third_element STRING)
RETURNS INT64
AS (
    ARRAY_LENGTH(SPLIT(
        IFNULL(
            IFNULL(first_element, second_element),
            third_element), "|")
        ) - 1
    );

-- Convert millis to human-readable values
CREATE OR REPLACE FUNCTION `{bq_project}.{target_dataset}.NormalizeMillis` (value INT64)
RETURNS FLOAT64
AS (ROUND(value / 1e6, 2)
);
