with
    fonte_detalhes_pedido_vendas as (
        select
             cast (rowguid as string) as sk_pedido_vendas -- chave surrogate, identificador global de linha na tabela
            , cast (salesorderid as int) as id_pedido_vendas
            , cast (salesorderdetailid as int) as id_detalhes_pedido
            , cast (productid as int) as id_produto
            , cast (specialofferid as int) as id_oferta_especial
            , cast (orderqty as int) as quantidade_solicitada
            , cast (unitprice as numeric) as preco_unitario_produto
            , cast (unitpricediscount as numeric) as desconto_produto           
            --, modifieddate - nao aplicavel as regras de negocio
            --, carriertrackingnumber - nao aplicavel as regras de negocio  
        from {{ source('sap', 'salesorderdetail') }}
    )

select *
from fonte_detalhes_pedido_vendas