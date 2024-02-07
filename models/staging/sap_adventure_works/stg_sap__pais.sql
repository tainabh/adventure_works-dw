with
    fonte_pais as (
        select 
            cast (countryregioncode as string) as codigo_regiao_pais
            , cast (name as string) as nome_pais
            --, modifieddate - nao aplicavel as regras de negocio            
        from {{ source('sap', 'countryregion') }}
    )

select *
from fonte_pais