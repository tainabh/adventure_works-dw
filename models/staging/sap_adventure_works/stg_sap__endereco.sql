with
    fonte_endereco as (
        select 
            cast (addressid as int) as id_endereco
            , cast (stateprovinceid as int) as id_estado
            , cast (city as string) as nome_cidade
            , cast (addressline1 as string) || ' ' || cast (addressline2 as string) as local_endereco                        
            --, postalcode - nao aplicavel as regras de negocio
            -- , spatiallocation - nao aplicavel as regras de negocio
            -- , rowguid - nao aplicavel as regras de negocio
            -- , modifieddate - nao aplicavel as regras de negocio
            
        from {{ source('sap', 'address') }}
    )

select *
from fonte_endereco