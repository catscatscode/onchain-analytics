-- Full Analysis https://flipsidecrypto.xyz/fc/resources/new-kid-on-base-network-effects-could-onboard-the-next-billion-into-web3

with first_tx_base as (
  select
    from_address,
    min(block_timestamp::date) as first_tx_date_base
  from base.core.fact_transactions
  where block_timestamp::date >= '2023-08-09'
  group by 1
),

first_tx_eth as (
  select
    from_address,
    min(block_timestamp::date) as first_tx_date_eth
  from ethereum.core.fact_transactions
  where block_timestamp::date >= '2023-08-09'
  group by 1
),

first_tx_arb as (
  select
    from_address,
    min(block_timestamp::date) as first_tx_date_arb
  from arbitrum.core.fact_transactions
  where block_timestamp::date >= '2023-08-09'
  group by 1
),

first_tx_avax as (
  select
    from_address,
    min(block_timestamp::date) as first_tx_date_avax
  from avalanche.core.fact_transactions
  where block_timestamp::date >= '2023-08-09'
  group by 1
),

first_tx_op as (
  select
    from_address,
    min(block_timestamp::date) as first_tx_date_op
  from optimism.core.fact_transactions
  where block_timestamp::date >= '2023-08-09'
  group by 1
),

first_tx_pol as (
  select
    from_address,
    min(block_timestamp::date) as first_tx_date_pol
  from polygon.core.fact_transactions
  where block_timestamp::date >= '2023-08-09'
  group by 1
),

first_tx_other as (
  select 
    t1.from_address, 
    least(first_tx_date_eth, first_tx_date_arb, first_tx_date_avax, first_tx_date_op, first_tx_date_pol) 
      as first_tx_date_other
  from first_tx_eth t1
  left join first_tx_arb using(from_address)
  left join first_tx_avax t3 using(from_address) 
  left join first_tx_op t4 using(from_address) 
  left join first_tx_pol t5 using(from_address) 
  -- group by 1
),

final as (
select
    t6.from_address,
    t6.first_tx_date_base,
    coalesce(t7.first_tx_date_other, current_date::date) as first_tx_date_other
  from first_tx_base t6
  left join first_tx_other t7 using(from_address)
)

select count(from_address)
from final
where first_tx_date_base < first_tx_date_other
