select * from
        {{ source('bank_transactions', 'bank_raw') }}

