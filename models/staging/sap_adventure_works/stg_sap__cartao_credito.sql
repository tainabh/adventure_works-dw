with
    fonte_cartao_credito as (
        select 
            cast (creditcardid as int) as id_cartao_credito
            , cast (cardtype as string) as tipo_cartao_credito
            --, cardnumber - nao aplicavel as regraas de negocio
            --, expmonth - nao aplicavel as regraas de negocio
            --, expyear - nao aplicavel as regraas de negocio
            --, modifieddate - nao aplicavel as regraas de negocio
            from {{ source('sap', 'creditcard') }}
    )

select *
from fonte_cartao_credito