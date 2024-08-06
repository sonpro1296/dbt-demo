select * from {{ ref('bank_transactions') }}
where deposit < 0 or withdrawal < 0