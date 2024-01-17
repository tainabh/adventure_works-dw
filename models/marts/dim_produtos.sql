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
            stg_produto.sk_produto
            , stg_produto.id_produto
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

    , produtos_unicos as ( -- cte criada para ter linhas unicas, sendo uma linha por produto
        select 
            sk_produto
            , id_produto
            , nome_produto
            , codigo_produto
            , sum(quantidade_solicitada) as quantidade_vendida -- soma da quantidade vendida por produto
            , sum (preco_unitario_produto) as valor_total_vendido -- soma do valor total vendido por produto
            , sum (desconto_produto) as desconto_total -- soma do desconto total gerado por produto
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
        from joined_tabelas
        group by
            sk_produto
            , id_produto
            , nome_produto
            , codigo_produto
            --, sum(quantidade_solicitada) as quantidade_vendida -- soma da quantidade vendida por produto
            --, sum (preco_unitario_produto) as valor_total_vendido -- soma do valor total vendido por produto
            --, sum (desconto_produto) as desconto_total -- soma do desconto total gerado por produto
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
    )

select *
from produtos_unicos




