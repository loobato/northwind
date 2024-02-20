with 
    dim_produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , dim_funcionarios as (
        select *
        from {{ ref('dim_funcionarios') }}
    )

    , int_vendas as (
        select *
        from {{ ref('int_vendas__pedidos_itens') }}
    )

    , joined_tabelas as (
        select 
            int_vendas.sk_pedido_item
            , int_vendas.id_ordem
            , int_vendas.id_cliente
            , int_vendas.id_produto
            , int_vendas.id_funcionario
            , int_vendas.id_transportadora

            , int_vendas.dt_ordem
            , int_vendas.dt_requerida
            , int_vendas.dt_enviada

            , int_vendas.frete
            , int_vendas.preco_unidade
            , int_vendas.quantidade
            , int_vendas.desconto            
            , int_vendas.nome_transportadora
            , int_vendas.endereco_ordem
            , int_vendas.cidade_ordem
            , int_vendas.regiao_ordem
            , int_vendas.cep_ordem
            , int_vendas.pais_ordem

            , dim_produtos.nome_produto
            , dim_produtos.is_descondinuado
            , dim_produtos.nome_categoria
            , dim_produtos.nome_fornecedor
            , dim_produtos.pais_fornecedor

            , dim_funcionarios.nome_funcionario
            , dim_funcionarios.nome_gerente
            , dim_funcionarios.cargo_funcionario
            , dim_funcionarios.dt_nascimento_funcionario
            , dim_funcionarios.dt_contratacao

        from int_vendas
        left join dim_produtos on
            int_vendas.id_produto = dim_produtos.id_produto
        left join dim_funcionarios on
            int_vendas.id_funcionario = dim_funcionarios.id_funcionario
    )

    , tranformacoes as (
        select 
            *
            , quantidade * preco_unidade as total_bruto
            , quantidade * preco_unidade * (1 - desconto) as total_liquido 
            , case
                when desconto > 0 then 'Sim'
                else 'Nao'
            end as teve_desconto
            , frete / count(id_ordem) over(partition by id_ordem) as frete_ponderado
        from joined_tabelas
    )

    , select_final as (
        select
            sk_pedido_item
            , id_ordem
            , id_cliente
            , id_produto
            , id_funcionario
            , id_transportadora
            -- DATAS
            , dt_ordem
            , dt_requerida
            , dt_enviada
            -- MÉTRICAS
            --, frete
            , preco_unidade
            , quantidade
            , desconto
            , total_bruto
            , total_liquido
            , teve_desconto
            , frete_ponderado
            -- CATEGORIAS
            , nome_transportadora
            , endereco_ordem
            , cidade_ordem
            , regiao_ordem
            , cep_ordem
            , pais_ordem
            , nome_produto
            , is_descondinuado
            , nome_categoria
            , nome_fornecedor
            , pais_fornecedor
            , nome_funcionario
            , nome_gerente
            , cargo_funcionario
            , dt_nascimento_funcionario
            , dt_contratacao

        from tranformacoes
    )

select *
from select_final   