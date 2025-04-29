-- OP Airdrop 4 Analysis - Query 1 - Eligible vs Claimed

with 
--- OP Airdrop 4 Address List Import --- 
-- 1) Downloaded from from Github: https://github.com/ethereum-optimism/op-analytics/tree/main/reference_data/address_lists 
-- 2) Uploaded to Google Sheets and accessed via LiveQuery
-- 3) For Google Sheet data retrieval, credit to charliemarketplace's google sheets demo: https://flipsidecrypto.xyz/charliemarketplace/q/bFnJ2s0TdbVp/google-sheets-demo
res AS (
  SELECT
    livequery.live.udf_api(
      'GET',
      'https://science.flipsidecrypto.xyz/googlesheets/readsheet',
      { 'Content-Type': 'application/json' },
       { 
        'sheets_id' : '1QH7VXuC2ubGi6oBvzln6egEw1bYojWlqv_IQdE4sUhs', 
        'tab_name' : 'op_airdrop_4_simple_list'
      }
    ) AS result
  FROM DUAL
)

, data AS (
  SELECT result:data AS json_result_must_pivot 
  FROM res
)

, eligible AS (
  SELECT 
    LOWER(d.value:"address"::VARCHAR) AS eligible_address, 
    TO_NUMBER(d.value:"multiplier", 38, 16) AS multiplier,
    TO_NUMBER(d.value:"total_op", 38, 16) AS eligible_amount_op
  FROM
    data, 
    LATERAL FLATTEN(input => data.json_result_must_pivot::VARIANT) d 
)

--- Airdrop 4 Claimed --- 
, claimed AS (
  SELECT 
      LOWER(decoded_log:account::STRING) AS claimed_address,
      DATE(block_timestamp) AS claimed_date,
      TO_NUMBER(decoded_log:amount, 38, 16) / 1e18 AS claimed_amount_op
  FROM 
      optimism.core.ez_decoded_event_logs
  WHERE 
      LOWER(contract_address) = lower('0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7')
      AND block_timestamp::DATE >= '2024-02-20'
)

--- Eligible vs Claimed --- 
, el_vs_cl AS (
  SELECT
      claimed.claimed_address,
      claimed.claimed_date,
      claimed.claimed_amount_op,
      eligible.eligible_address,
      eligible.eligible_amount_op AS eligible_amount_op, 
      claimed_amount_op / eligible_amount_op AS claimed_pct_per_address
  FROM eligible
  LEFT JOIN claimed
      ON claimed.claimed_address = eligible.eligible_address
)

--- Claim Rate --- 
SELECT 
    count(DISTINCT claimed_address) AS n_claimed,
    count(DISTINCT eligible_address) AS n_eligible,
    sum(claimed_amount_op) AS total_claimed_op,
    sum(eligible_amount_op) AS total_eligible_op,
    ROUND( (n_claimed / n_eligible)*100, 2) AS total_claim_rate_addresses,
    ROUND( (total_claimed_op / total_eligible_op)*100, 2) AS total_claim_rate_amount
    ,AVG(claimed_pct_per_address) AS avg_claim_rate_amount
FROM 
  el_vs_cl vs
