with
    stg_clientes as (
        select 
            sk_cliente
            , id_cliente
            , id_pessoa
            , id_loja
            , id_territorio
        from {{ ref('stg_sap__clientes') }}
    )

    , stg_pessoa as (
        select 
            sk_pessoa
            , id_empresa
            , nome
        from {{ ref('stg_sap__pessoa') }}
    )
   
    , joined_tabelas_cliente_pessoa as (
        select
            stg_clientes.sk_cliente
            , stg_clientes.id_cliente
            , stg_clientes.id_pessoa                 
            , stg_pessoa.id_empresa            
            , stg_pessoa.nome                

        from stg_clientes
        left join stg_pessoa on
        sk_cliente = sk_pessoa               
    )

    select *
    from joined_tabelas_cliente_pessoa

    