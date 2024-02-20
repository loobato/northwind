with
    stg_ordens as (
        select *
        from {{ ref('stg_erp__ordens') }}
    )
    
    , stg_detalhes_ordens as (
        select *
        from {{ ref('stg_erp__detalhes_ordens') }}
    )

    , joined_tabelas as (
        select
        stg_ordens.id_ordem
        , stg_ordens.id_cliente
        , stg_detalhes_ordens.id_produto
        , stg_ordens.id_funcionario
        , stg_ordens.id_transportadora
        
        , stg_ordens.dt_ordem
        , stg_ordens.dt_requerida
        , stg_ordens.dt_enviada
        , stg_ordens.frete
        , stg_ordens.nome_transportadora
        , stg_ordens.endereco_ordem
        , stg_ordens.cidade_ordem
        , stg_ordens.regiao_ordem
        , stg_ordens.cep_ordem
        , stg_ordens.pais_ordem

        , stg_detalhes_ordens.preco_unidade
        , stg_detalhes_ordens.quantidade
        , stg_detalhes_ordens.desconto

        from stg_detalhes_ordens
        left join stg_ordens on
            stg_detalhes_ordens.id_ordem = stg_ordens.id_ordem
    )

    , criar_chave as (
        select
            cast(id_ordem as string) || '_' || cast(id_produto as string) as sk_pedido_item
            , *
        from joined_tabelas
    )

select *
from criar_chave
