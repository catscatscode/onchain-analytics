-- Optimism Airdrop 4 Analysis - Query 2 - Claim Distro by Amount

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

--- Claimed by Amount Bucket --- 
-- , el_vs_cl AS (
  SELECT
      -- claimed.claimed_date,
      CASE  
          WHEN eligible.eligible_amount_op < 25 THEN 'A: >= 20 and < 25' -- starts at 20
          WHEN eligible.eligible_amount_op >= 25 AND eligible.eligible_amount_op < 50 THEN 'B: >= 25 and < 50'
          WHEN eligible.eligible_amount_op >= 50 AND eligible.eligible_amount_op < 100 THEN 'C: >= 50 and < 100'
          WHEN eligible.eligible_amount_op >= 100 AND eligible.eligible_amount_op < 500 THEN 'D: >= 100 and < 500'
          WHEN eligible.eligible_amount_op >= 500 AND eligible.eligible_amount_op < 1000 THEN 'E: >= 500 and < 1,000'
          WHEN eligible.eligible_amount_op >= 1000 AND eligible.eligible_amount_op < 5000 THEN 'F: >= 1,000 and < 5,000'
          WHEN eligible.eligible_amount_op >= 5000 AND eligible.eligible_amount_op <= 6000 THEN 'G: 5,000-6,000' -- ends at 6,000
          ELSE 'H: error'
      END AS el_amount_bucket,
      COUNT(eligible.eligible_address) AS n_eligible_addresses,
      COUNT(claimed.claimed_address) AS n_claimed_addresses,
      (n_claimed_addresses / n_eligible_addresses) * 100 AS pct_addresses_claimed,
      SUM(eligible.eligible_amount_op) AS total_eligible_op,
      SUM(claimed.claimed_amount_op) AS total_claimed_op,
      (total_claimed_op / total_eligible_op) * 100 AS pct_op_claimed
  FROM eligible
  LEFT JOIN claimed
      ON claimed.claimed_address = eligible.eligible_address
  GROUP BY 1
;
