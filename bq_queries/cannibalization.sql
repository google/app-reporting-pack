CREATE TEMP FUNCTION GetHash(hash_string STRING)
RETURNS STRING
AS (TO_HEX(MD5(hash_string)));

CREATE OR REPLACE TABLE {bq_project}.{target_dataset}.cannibalization
AS (
    SELECT
        campaign_id,
        GetHash(CONCAT(G.geos, G.languages, A.app_id, A.bidding_strategy, A.target_conversions)) AS hash_strict,
        GetHash(CONCAT(G.geos, G.languages, A.app_id, A.bidding_strategy)) AS hash_medium,
        GetHash(CONCAT(G.geos, G.languages, A.app_id)) AS hash_flex,
        GetHash(CONCAT(G.geos, G.languages)) AS hash_broad
    FROM `{bq_project}.{target_dataset}.AppCampaignSettingsView` AS A
    LEFT JOIN `{bq_project}.{target_dataset}.GeoLanguageView` AS G
        USING(campaign_id)
);
