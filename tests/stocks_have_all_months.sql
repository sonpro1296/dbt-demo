with stock_by_month as (
    select distinct symbol, date_part('month', time) as month, date_part('year', time) as year
    from {{ source('bank_transactions', 'stocks') }}
    where symbol in (select * from {{ ref('stock_from_start') }})

)

select symbol, count(*) 
from stock_by_month
group by symbol
having count(*) != 12 * 6
