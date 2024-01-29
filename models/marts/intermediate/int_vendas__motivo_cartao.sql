with
    stg_cabecalho_pedido_venda as (
        select 
            sk_pedido
            , id_pedido_venda
            , id_cliente
            , id_vendedor
            , id_territorio
            , id_endereco_faturamento
            , id_endereco_entrega
            , id_forma_entrega
            , id_cartao_credito
            , id_taxa_cambio
            , codigo_pedido
            , numero_conta
            , numero_revisao
            , data_pedido
            , prazo_entrega_pedido
            , data_envio
            , status_pedido
            , pedido_foi_online
            , codigo_aprovacao_cartao_credito
            , subtotal_pedido
            , taxa_pedido
            , valor_frete
            , total_a_receber_pedido
        from {{ ref('stg_sap__cabecalho_pedido_venda') }}
    )

    , stg_cartao_credito as (
        select 
            id_cartao_credito
            , tipo_cartao_credito
        from {{ ref('stg_sap__cartao_credito') }}
    )

    , stg_motivo_venda as (
        select 
            id_motivo_venda
            , motivo_venda
            , tipo_motivo_venda
        from {{ ref('stg_sap__motivo_venda') }}
    )

    , stg_cabecalho_e_motivo_venda as (
        select 
            id_pedido_venda
            , id_motivo_venda
        from {{ ref('stg_sap__cabecalho_e_motivo_venda') }}
    )

    , joined_tabelas as (
        select
            --stg_cabecalho_pedido_venda.sk_pedido
            stg_cabecalho_pedido_venda.id_pedido_venda
            , stg_cabecalho_pedido_venda.id_cliente
            , stg_cabecalho_pedido_venda.id_vendedor
            , stg_cabecalho_pedido_venda.id_territorio
            , stg_cabecalho_pedido_venda.id_endereco_faturamento
            , stg_cabecalho_pedido_venda.id_endereco_entrega
            , stg_cabecalho_pedido_venda.id_forma_entrega
            , stg_cabecalho_pedido_venda.id_cartao_credito
            , stg_cabecalho_pedido_venda.id_taxa_cambio
            , stg_motivo_venda.id_motivo_venda
            , stg_motivo_venda.motivo_venda
            , stg_motivo_venda.tipo_motivo_venda
            , stg_cabecalho_pedido_venda.codigo_pedido                   
            , stg_cabecalho_pedido_venda.numero_conta
            , stg_cabecalho_pedido_venda.numero_revisao
            , stg_cabecalho_pedido_venda.data_pedido
            , stg_cabecalho_pedido_venda.prazo_entrega_pedido
            , stg_cabecalho_pedido_venda.data_envio
            , stg_cabecalho_pedido_venda.status_pedido
            , stg_cabecalho_pedido_venda.pedido_foi_online
            , stg_cabecalho_pedido_venda.codigo_aprovacao_cartao_credito
            , stg_cartao_credito.tipo_cartao_credito
            , stg_cabecalho_pedido_venda.subtotal_pedido
            , stg_cabecalho_pedido_venda.taxa_pedido
            , stg_cabecalho_pedido_venda.valor_frete
            , stg_cabecalho_pedido_venda.total_a_receber_pedido
        from stg_cabecalho_pedido_venda
        left join stg_cartao_credito on 
        stg_cabecalho_pedido_venda.id_cartao_credito = stg_cartao_credito.id_cartao_credito
        left join stg_cabecalho_e_motivo_venda on 
        stg_cabecalho_pedido_venda.id_pedido_venda = stg_cabecalho_e_motivo_venda.id_pedido_venda
        left join stg_motivo_venda on 
        stg_cabecalho_e_motivo_venda.id_motivo_venda = stg_motivo_venda.id_motivo_venda
                 
    )

    , criar_chave as (
        select
            cast (id_pedido_venda as string) || '-' || coalesce (motivo_venda, 'motivo indisponivel') as sk_vendas_motivo_cartao
           -- usado coalesce para tratar valores nulos e exibir um valor padrão
            , id_pedido_venda 
            , id_cliente
            , id_vendedor
            , id_territorio
            , id_endereco_faturamento
            , id_endereco_entrega
            , id_forma_entrega
            , id_cartao_credito
            , id_taxa_cambio
            , id_motivo_venda
            , motivo_venda
            , tipo_motivo_venda
            , codigo_pedido                   
            , numero_conta
            , numero_revisao
            , data_pedido
            , prazo_entrega_pedido
            , data_envio
            , status_pedido
            , pedido_foi_online
            , codigo_aprovacao_cartao_credito
            , tipo_cartao_credito
            , subtotal_pedido
            , taxa_pedido
            , valor_frete
            , total_a_receber_pedido
            , row_number() over (partition by id_pedido_venda order by motivo_venda) AS rn
        from joined_tabelas
    )



select *
from criar_chave
order by id_pedido_venda desc
