with
    fonte_pessoa as (
        select 
            cast (rowguid as string) as sk_pessoa -- chave surrogate, identificador global de linha na tabela
            , cast (businessentityid as int) as id_empresa
            , cast (persontype as string) as categoria_pessoa
            , cast (title as string) || ' ' || cast (firstname as string) || ' ' || cast (middlename as string) || ' ' || cast (lastname as string) as nome_pessoa
            --, additionalcontactinfo - nao aplicavel as regras de negocio 
            --, demographics - nao aplicavel as regras de negocio              
            --, modifieddate - nao aplicavel as regras de negocio 
            --, namestyle - nao aplicavel as regras de negocio 
            --, suffix - nao aplicavel as regras de negocio 
            --, emailpromotion - nao aplicavel as regras de negocio 
            
        from {{ source('sap', 'person') }}
    )

select *
from fonte_pessoa

