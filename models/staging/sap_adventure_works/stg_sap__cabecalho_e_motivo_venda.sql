with
    fonte_cabecalho_e_motivo_venda as (
        select 
            cast (salesorderid as int) as id_pedido_venda
            , cast (salesreasonid as int) as id_motivo_venda
            --, modifieddate - nao aplicavel as regras de negocio
        from {{ source('sap', 'salesorderheadersalesreason') }}
    )

select *
from fonte_cabecalho_e_motivo_venda 
