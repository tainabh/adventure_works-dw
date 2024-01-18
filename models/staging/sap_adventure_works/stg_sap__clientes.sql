with
    fonte_cliente as (
        select 
            cast (rowguid as string) as sk_cliente
            , cast (customerid as int) as id_cliente
            , cast (personid as int) as id_pessoa
            , cast (storeid as int) as id_loja
            , cast (territoryid as int) as id_territorio
            --, modifieddate - nao aplicavel as regras de negocio
                        
        from {{ source('sap', 'customer') }}
    )

select *
from fonte_cliente