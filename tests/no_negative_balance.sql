select * from {{ ref('bank_transactions') }}
where balance < 0