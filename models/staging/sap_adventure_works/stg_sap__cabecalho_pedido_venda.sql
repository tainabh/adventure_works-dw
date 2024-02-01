with
    fonte_cabecalho_pedido as (
        select 
            cast (rowguid as string) as sk_pedido -- chave surrogate, identificador global de linha na tabela  
            , cast (salesorderid as int) as id_pedido_venda
            , cast (customerid as int) as id_cliente
            , cast (salespersonid as int) as id_vendedor
            , cast (territoryid as int) as id_territorio
            , cast (billtoaddressid as int) as id_endereco_faturamento
            , cast (shiptoaddressid as int) as id_endereco_entrega
            , cast (shipmethodid as int) as id_forma_entrega
            , cast (creditcardid as int) as id_cartao_credito
            , cast (currencyrateid as int) as id_taxa_cambio
            , cast (purchaseordernumber as string) as codigo_pedido
            , cast (accountnumber as string) as numero_conta
            , cast (revisionnumber as numeric) as numero_revisao
            , cast ((date (orderdate)) as date) as data_pedido
            , cast ((date (duedate)) as date) as prazo_entrega_pedido
            , cast ((date (shipdate)) as date) as data_envio
            , cast (status as int) as status_pedido
            , cast (onlineorderflag as string) as pedido_foi_online      
            , cast (creditcardapprovalcode as string) as codigo_aprovacao_cartao_credito
            , cast (subtotal as numeric) as subtotal_pedido
            , cast (taxamt as numeric) as taxa_pedido
            , cast (freight as numeric) as valor_frete
            , cast (totaldue as numeric) as total_a_receber_pedido
            --, comment            
            --, modifieddate
        from {{ source('sap', 'salesorderheader') }}
    )

select *
from fonte_cabecalho_pedido