with
    fonte_produtos as (
        select 
            cast(product_id as int) as id_produto
            , cast(supplier_id as int) as id_fornecedor
            , cast(category_id as int) as id_categoria
            , cast(product_name as string) as nome_produto
            , cast(quantity_per_unit as string) as quantidades_por_un
            , cast(unit_price as numeric) as preco_un
            , cast(units_in_stock as int) as un_estoque
            , cast(units_on_order as int) as un_ordem
            , cast(reorder_level as int) as nivel_abastecimento
            , case
                when discontinued = 1 then 'Sim'
                else 'Não'
            end as is_descondinuado
        from {{ source('erp', 'products') }}
    
    )

    select *
    from fonte_produtos             