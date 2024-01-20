with
    
    stg_pais as (
        select 
            id_territorio || nome_pais as sk_territorio_pais -- chave surrogate criada para efetuar joins
            , nome_pais || codigo_regiao_pais as sk_pais_codigo -- chave surrogate criada para efetuar joins
            , id_pais
            , id_territorio
            , nome_pais
            , codigo_pais
            , codigo_regiao_pais
        from {{ ref('stg_sap__pais') }}
    )

    , stg_endereco as (
        select 
            id_endereco
            , id_pais
            , nome_cidade
            , local_endereco
        from {{ ref('stg_sap__endereco') }}
    )

    , stg_regiao_pais as (
        select 
            nome_pais || codigo_regiao_pais as sk_pais_codigo -- chave surrogate criada para efetuar joins
            , codigo_regiao_pais
            , nome_pais
        from {{ ref('stg_sap__regiao_pais') }}
    )
     
    , joined_tabelas_endereco as ( 
        select
            stg_endereco.id_endereco 
            , stg_endereco.id_pais     
            , stg_pais.id_territorio                
            , stg_endereco.local_endereco
            , stg_endereco.nome_cidade  
            , stg_regiao_pais.nome_pais          
            , stg_pais.codigo_pais
            , stg_regiao_pais.codigo_regiao_pais
                        
        from stg_endereco
        left join stg_pais on
        stg_endereco.id_pais = stg_pais.id_pais 
        left join stg_regiao_pais on
        stg_pais.sk_pais_codigo = stg_regiao_pais.sk_pais_codigo        
    )    

select *
from joined_tabelas_endereco
