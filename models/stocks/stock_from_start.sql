with prices as (
    select * from {{ ref('stock_raw') }}
),

stocks_from_start as (
        select distinct symbol from prices where date_part('month', time) = 1 and date_part('year', time) = 2018 
)

select * from stocks_from_start
