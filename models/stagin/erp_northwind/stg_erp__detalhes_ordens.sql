with 
    fonte_detalhes_ordens as (
        select
            cast(order_id as int) as id_ordem
            , cast(product_id as int) as id_produto
            , cast(unit_price as numeric) as preco_unidade
            , cast(quantity as int) as quantidade
            , cast(discount as numeric) as desconto
        from {{ source('erp', 'order_details') }}
    )

select *
from fonte_detalhes_ordens