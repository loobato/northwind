with
    stg_categorias as (
        select 
            id_categoria
            , nome_categoria
            , descricao_categoria
        from {{ ref('stg_erp__categorias') }}
    )

    , stg_fornecedores as (
        select
            id_fornecedor
            , nome_fornecedor
            , contato_fornecedor
            , contato_funcao
            , endereco_fornecedor
            , cidade_fornecedor
            , regiao_fornecedor
            , pais_fornecedor
            , cep_fornecedor
        from {{ ref('stg_erp__fornecedores') }}
    )

    , stg_produtos as (
        select
            id_produto
            , id_fornecedor
            , id_categoria
            , nome_produto
            , quantidades_por_un
            , preco_un
            , un_estoque
            , un_ordem
            , nivel_abastecimento
            , is_descondinuado
        from {{ ref('stg_erp__produtos') }}
    )

    , joined_tabelas as (
        select
            *
        from stg_produtos
        left join stg_categorias on
            stg_produtos.id_categoria = stg_categorias.id_categoria
        left join stg_fornecedores on
            stg_produtos.id_fornecedor = stg_fornecedores.id_fornecedor

    )

select *
from joined_tabelas