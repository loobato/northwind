with 
    fonte_ordens as (
        select
            cast(order_id as int) as id_ordem
            , cast(customer_id as string) as id_cliente
            , cast(employee_id as int) as id_funcionario
            , cast(ship_via as int) as id_transportadora
            , cast(order_date as date) as dt_ordem
            , cast(required_date as date) as dt_requerida
            , cast(shipped_date as date) as dt_enviada
            , cast(freight as numeric) as frete
            , cast(ship_name as string) as nome_transportadora
            , cast(ship_address as string) as endereco_ordem
            , cast(ship_city as string) as cidade_ordem
            , cast(ship_region as string) as regiao_ordem
            , cast(ship_postal_code as string) as cep_ordem
            , cast(ship_country as string) as pais_ordem
        from {{ source('erp', 'orders') }}
    )

select *
from fonte_ordens
