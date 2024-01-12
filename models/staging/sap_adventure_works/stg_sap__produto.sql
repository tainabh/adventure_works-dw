with
    fonte_produto as (
        select
            cast (rowguid as string) as sk_produto -- chave surrogate, identificador global de linha na tabela         
            , cast (productid as int) as id_produto
            , cast (name as string) as nome_produto
            , cast (productnumber as string) as codigo_produto
            , cast (makeflag as string) as produto_deve_ser_fabricado
            , cast (finishedgoodsflag as string) as produto_acabado
            , cast (color as string) as cor_produto
            , cast (safetystocklevel as numeric) as estoque_minimo_produto
            , cast (reorderpoint as numeric) as ponto_reposicao_produto
            , cast (standardcost as numeric) as custo_padrao_produto
            , cast (listprice as numeric) as preco_tabelado_produto
            , cast ("size" as string) as tamanho_produto
            , cast (sizeunitmeasurecode as string) as codigo_tamanho_produto
            , cast (weightunitmeasurecode as string) as unidade_medida_peso_produto
            , cast (weight as numeric) as peso_produto
            , cast (daystomanufacture as int) as dias_fabricacao_produto
            , cast (productline as string) as linha_produto
            , cast (class as string) as classe_produto
            , cast (style as string) as estilo_produto           
            , cast (sellstartdate as string) as inicio_vendas_produto
            , cast (sellenddate as string) as termino_vendas_produto
            --, discontinueddate -  nao aplicavel as regras de negocio            
            --, modifieddate - nao aplicavel as regras de negocio
            --, productsubcategoryid - nao aplicavel as regras de negocio
            --, productmodelid - nao aplicavel as regras de negocio
        from {{ source('sap', 'product') }}
    )

select *
from fonte_produto