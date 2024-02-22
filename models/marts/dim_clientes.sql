with
    stg_clientes as (
        select *
        from {{ ref('stg_erp__clientes') }}
    )

select *
from stg_clientes