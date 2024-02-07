with
    fonte_estado as (
        select 
            cast (rowguid as string) as sk_estado
            , cast (stateprovinceid as int) as id_estado
            , cast (territoryid as int) as id_territorio 
            , cast (name as string) as nome_estado
            , cast (stateprovincecode as string) as codigo_estado
            , cast (countryregioncode as string) as codigo_regiao_pais     
            --, isonlystateprovinceflag - nao aplicavel as regras de negocio
            --, modifieddate - nao aplicavel as regras de negocio
        from {{ source('sap', 'stateprovince') }}
    )

select *
from fonte_estado