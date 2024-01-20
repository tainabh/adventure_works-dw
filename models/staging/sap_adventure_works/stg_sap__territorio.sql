with
    fonte_territorio as (
        select 
            cast (territoryid as int) as id_territorio
            , cast (name as string) as nome_pais
            , cast (countryregioncode as string) as codigo_regiao_pais            
            , cast (salesytd as numeric) as total_vendas
            , cast (saleslastyear as numeric) as total_vendas_ano_passado
            , cast (costytd as numeric) as total_despesa
            , cast (costlastyear as numeric) as total_despesa_ano_passado
            --, rowguid
            --, modifieddate
            --, group
        from {{ source('sap', 'salesterritory') }}
    )

select *
from fonte_territorio