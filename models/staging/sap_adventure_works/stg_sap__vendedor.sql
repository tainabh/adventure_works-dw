with
    fonte_vendedor as (
        select 
            cast (rowguid as string) as sk_vendedor -- chave surrogate, identificador global de linha na tabela
            , cast (businessentityid as int) as id_empresa
            , cast (territoryid as int) as id_territorio_vendedor
            , cast (salesquota as string) as meta_vendas_vendedor
            , cast (bonus as numeric) as bonus_vendedor
            , cast (commissionpct as numeric) as porcentagem_comissao_vendedor
            , cast (salesytd as numeric) as vendas_acumulada_vendedor
            , cast (saleslastyear as numeric) as vendas_ultimo_ano_vendedor
            --, modifieddate - nao aplicavel as regras de negocio         
            
        from {{ source('sap', 'salesperson') }}
    )

select *
from fonte_vendedor