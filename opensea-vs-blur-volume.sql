-- Full Analysis https://flipsidecrypto.xyz/mar1na-catscatscode/opensea-vs.-blur-in-february-2024-_LEc55

SELECT 
  date_trunc('week', block_timestamp) AS week,
  platform_name AS marketplace,
  count(DISTINCT tx_hash) AS n_sales,
  count(DISTINCT seller_address) AS n_sellers,
  count(DISTINCT buyer_address) AS n_buyers,
  count(DISTINCT nft_address) AS n_collections,
  sum(price_usd) AS sales_volume_usd,
  sum(platform_fee_usd) AS platform_fees,
  sum(creator_fee_usd) AS creator_fees,
  sum(total_fees_usd) AS total_fees,
  avg(price_usd) as avg_tx_size_usd,
  median(price_usd) as median_tx_size_usd,
  max(price_usd) as max_tx_size_usd,
  min(price_usd) as min_tx_size_usd
FROM ethereum.nft.ez_nft_sales
WHERE block_timestamp::DATE >= current_date - INTERVAL '6 months'
  AND block_timestamp::DATE < '2024-02-05'
GROUP BY week, marketplace
