select *
from {{ source('bank_transactions', 'stocks') }}
where high / open >= 1.07 or low / open <= 0.93

