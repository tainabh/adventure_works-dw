with
    fonte_pais as (
        select 
            cast (rowguid as string) as sk_pais
            , cast (stateprovinceid as int) as id_estado
            , cast (territoryid as int) as id_territorio 
            , cast (name as string) as nome_pais
            , cast (stateprovincecode as string) as codigo_estado
            , cast (countryregioncode as string) as codigo_regiao_pais     
            --, isonlystateprovinceflag - nao aplicavel as regras de negocio
            --, modifieddate - nao aplicavel as regras de negocio
        from {{ source('sap', 'stateprovince') }}
    )

select *
from fonte_pais