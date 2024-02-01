with
    fonte_funcionarios as (
        select 
            cast ( rowguid as string) as sk_funcionario -- chave surrogate, identificador global de linha na tabela
            , cast (businessentityid as int) as id_empresa
            , cast ( nationalidnumber as int) as id_codigo_nacional
            , cast ( loginid as string) as id_login
            , cast ( jobtitle as string) as cargo_funcionario
            , cast ( birthdate as date) as dt_nascimento_funcionario
            , cast ( maritalstatus as string) as estado_civil_funcionario
            , cast ( gender as string) as genero_funcionario
            , cast ( (date (hiredate)) as date) as data_contratacao_funcionario
            , cast ( salariedflag as string) as eh_assalariado
            , cast ( vacationhours as int) as horas_descanso_funcionario
            , cast ( sickleavehours as int) as horas_licenca_medica_funcionario
            , cast ( currentflag as string) as funcionario_ativo
            --, modifieddate - nao aplicavel as regras de negocio
            --, organizationnode - nao aplicavel as regras de negocio
            
        from {{ source('sap', 'employee') }}
    )

select *
from fonte_funcionarios