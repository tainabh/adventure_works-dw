with
    stg_vendedor as (
        select 
            sk_vendedor
            , id_empresa
            , id_territorio_vendedor
            , meta_vendas_vendedor
            , bonus_vendedor
            , porcentagem_comissao_vendedor
            , vendas_acumulada_vendedor
            , vendas_ultimo_ano_vendedor
        from {{ ref('stg_sap__vendedor') }}
    )

    , stg_funcionario as (
        select 
            sk_funcionario
            , id_empresa
            , id_codigo_nacional
            , id_login
            , cargo_funcionario
            , dt_nascimento_funcionario
            , estado_civil_funcionario
            , genero_funcionario
            , data_contratacao_funcionario
            , eh_assalariado
            , horas_descanso_funcionario
            , horas_licenca_medica_funcionario
            , funcionario_ativo
        from {{ ref('stg_sap__funcionarios') }}
    )

    , stg_pessoa as (
        select 
            sk_pessoa
            , id_empresa
            , nome
        from {{ ref('stg_sap__pessoa') }}
    )

    , joined_tabelas as (
        select
            stg_vendedor.sk_vendedor
            , stg_funcionario.sk_funcionario
            , stg_pessoa.sk_pessoa
            , stg_vendedor.id_empresa
            , stg_vendedor.id_territorio_vendedor
            , stg_funcionario.id_codigo_nacional
            , stg_pessoa.nome     
            , stg_funcionario.cargo_funcionario
            , stg_funcionario.dt_nascimento_funcionario
            , stg_funcionario.genero_funcionario
            , stg_funcionario.data_contratacao_funcionario
            , stg_funcionario.eh_assalariado
            , stg_funcionario.horas_descanso_funcionario
            , stg_funcionario.horas_licenca_medica_funcionario
            , stg_funcionario.funcionario_ativo       
            , stg_vendedor.meta_vendas_vendedor
            , stg_vendedor.bonus_vendedor
            , stg_vendedor.porcentagem_comissao_vendedor
            , stg_vendedor.vendas_acumulada_vendedor
            , stg_vendedor.vendas_ultimo_ano_vendedor            
            --, stg_funcionario.sk_funcionario
            --, stg_funcionario.id_empresa     
            --, stg_pessoa.sk_pessoa
            --, stg_pessoa.id_empresa            
        from stg_vendedor
        left join stg_pessoa on 
        stg_vendedor.id_empresa = stg_pessoa.id_empresa
        left join stg_funcionario on
        stg_vendedor.id_empresa = stg_funcionario.id_empresa
    )

select *
from joined_tabelas