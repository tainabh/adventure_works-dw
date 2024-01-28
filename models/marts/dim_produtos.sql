with
    stg_detalhes_pedidos_vendas as (
        select 
            sk_pedido_vendas
            , id_pedido_vendas 
            , id_detalhes_pedido
            , id_produto
            , id_oferta_especial
            , quantidade_solicitada 
            , preco_unitario_produto
            , desconto_produto
        from {{ ref('stg_sap__detalhes_pedido_vendas') }}
    )

    , stg_produto as (
        select 
            sk_produto
            , id_produto
            , nome_produto
            , codigo_produto
            , produto_deve_ser_fabricado
            , produto_acabado
            , cor_produto
            , estoque_minimo_produto
            , ponto_reposicao_produto
            , custo_padrao_produto
            , preco_tabelado_produto
            , tamanho_produto
            , codigo_tamanho_produto
            , unidade_medida_peso_produto
            , peso_produto
            , dias_fabricacao_produto
            , linha_produto
            , classe_produto
            , estilo_produto
            , inicio_vendas_produto
            , termino_vendas_produto
        from {{ ref('stg_sap__produto') }}
    )

        , joined_tabelas as (
        select
            --stg_produto.sk_produto
            stg_produto.id_produto
            , stg_detalhes_pedidos_vendas.id_pedido_vendas 
            , stg_produto.nome_produto
            , stg_produto.codigo_produto
            , stg_detalhes_pedidos_vendas.quantidade_solicitada
            , stg_detalhes_pedidos_vendas.preco_unitario_produto
            , stg_detalhes_pedidos_vendas.desconto_produto
            , stg_produto.produto_deve_ser_fabricado
            , stg_produto.produto_acabado
            , stg_produto.cor_produto
            , stg_produto.estoque_minimo_produto
            , stg_produto.ponto_reposicao_produto
            , stg_produto.custo_padrao_produto
            , stg_produto.preco_tabelado_produto
            , stg_produto.tamanho_produto
            , stg_produto.codigo_tamanho_produto
            , stg_produto.unidade_medida_peso_produto
            , stg_produto.peso_produto
            , stg_produto.dias_fabricacao_produto
            , stg_produto.linha_produto
            , stg_produto.classe_produto
            , stg_produto.estilo_produto
            , stg_produto.inicio_vendas_produto
            , stg_produto.termino_vendas_produto   
        from stg_produto
        left join stg_detalhes_pedidos_vendas on 
            stg_produto.id_produto = stg_detalhes_pedidos_vendas.id_produto
    )

       , criar_chave as (
            select
                cast (id_produto as string) || '-' || coalesce (cast (id_pedido_vendas as string), 'sem pedido') || '-' || coalesce (cast (quantidade_solicitada as string), '0') || '-' || coalesce (cast (preco_unitario_produto as string), '0') as sk_dim_produtos
                -- usado coalesce para tratar valores nulos e exibir um valor padrão
                , *             
            from joined_tabelas
       )

select *
from criar_chave
order by id_pedido_vendas desc




