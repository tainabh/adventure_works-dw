with
    
    stg_estado as (
        select 
            id_territorio || nome_estado as sk_territorio_estado -- chave surrogate criada para efetuar joins
            , nome_estado || codigo_regiao_pais as sk_estado_codigo -- chave surrogate criada para efetuar joins
            , id_estado
            , id_territorio
            , nome_estado
            , codigo_estado
            , codigo_regiao_pais
        from {{ ref('stg_sap__estado') }}
    )

    , stg_endereco as (
        select 
            id_endereco
            , id_estado
            , nome_cidade
            , local_endereco
        from {{ ref('stg_sap__endereco') }}
    )

    , stg_pais as (
        select 
            nome_pais || codigo_regiao_pais as sk_pais_codigo -- chave surrogate criada para efetuar joins
            , codigo_regiao_pais
            , nome_pais
        from {{ ref('stg_sap__pais') }}
    )
     
    , joined_tabelas_endereco as ( 
        select
            stg_endereco.id_endereco 
            , stg_endereco.id_estado     
            , stg_estado.id_territorio                
            , stg_endereco.local_endereco
            , stg_endereco.nome_cidade  
            , stg_pais.nome_pais 
            , stg_estado.nome_estado
            , stg_estado.codigo_estado
            , stg_pais.codigo_regiao_pais
                        
        from stg_endereco
        left join stg_estado on
        stg_endereco.id_estado = stg_estado.id_estado 
        left join stg_pais on
        stg_estado.codigo_regiao_pais = stg_pais.codigo_regiao_pais      
    )    

select *
from joined_tabelas_endereco