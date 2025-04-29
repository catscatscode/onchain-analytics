-- Full Analysis https://flipsidecrypto.xyz/fc/resources/new-kid-on-base-network-effects-could-onboard-the-next-billion-into-web3

select 
  block_timestamp::date as date,
  count(tx_hash) as n_tx,
  count(distinct from_address) as n_wallets,
  avg(n_wallets) over(order by date rows between 29 preceding and current row) as n_wallets_avg30
from base.core.fact_transactions
where date >= '2023-08-09' -- public launch
group by 1
;
