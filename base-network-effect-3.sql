-- Full Analysis https://flipsidecrypto.xyz/fc/resources/new-kid-on-base-network-effects-could-onboard-the-next-billion-into-web3

select
  t1.to_address as contract,
  count(distinct from_address) as n_wallets,
  case 
    when t1.to_address = '0xcf205808ed36593aa40a44f10c7f7c2f67d4a4d4' then 'friend.tech'
    when t1.to_address = '0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca' then 'USDbC'
    when t1.to_address = '0x50b6ebc2103bfec165949cc946d739d5650d7ae4' then 'stargate'
    when t1.to_address = '0xea2a41c02fa86a4901826615f9796e603c6a4491' then 'zora bridge to base'
    when t1.to_address = '0x1fc10ef15e041c5d3c54042e52eb0c54cb9b710c' then 'base is for builders'
  end as description,
  t2.label_subtype
from base.core.fact_transactions t1
left join base.core.dim_labels t2
on t1.to_address = t2.address
group by 1,3,4
order by n_wallets desc
limit 5;
