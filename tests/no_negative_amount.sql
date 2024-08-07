select * from {{ ref('bank_transactions') }}
where deposit >= 0