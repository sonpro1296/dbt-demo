with prices as (
    select * from {{ source('bank_transactions', 'stocks') }}
),

monthly_prices as (
    select distinct symbol, date_part('month', time) as month, date_part('year', time) as year,
        first_value(open) over(partition by symbol, date_trunc('month', time) order by time) as price
    from prices
    where symbol in (select * from {{ ref('stock_from_start') }})
    
),
last_price as (
    select distinct symbol, 
        first_value(open) over(partition by symbol order by time desc) as price
    from prices
    where symbol in (select * from {{ ref('stock_from_start') }})
),

price_comparison as (
    select 
        mp.symbol,
        sum(mp.price) as sum_buy_price,
        sum(lp.price) as sum_sell_price,
        (sum(lp.price) - sum(mp.price)) / sum(mp.price) * 100 as profit_percentage
    from monthly_prices as mp
    join last_price as lp on mp.symbol = lp.symbol
    group by mp.symbol

)

select * from price_comparison 