with
    clientes as (
        select 
            *
        from {{ ref('dim_clientes') }}
    )

    , endereco as (
        select 
            *
        from {{ ref('dim_endereco') }}
    )

    , produtos as (
        select 
            *
        from {{ ref('dim_produtos') }}
    )

    , vendedor as (
        select
            *
        from {{ ref('dim_vendedor') }}
    )

    , int_vendas as (
        select 
            *
        from {{ ref('int_vendas__motivo_cartao') }}
    )

    , joined_tabelas as (
        select 
            
            -- CHAVES --
            int_vendas.sk_vendas_motivo_cartao
            , produtos.sk_dim_produtos
            , int_vendas.id_pedido_venda
            , produtos.id_produto
            , clientes.id_pessoa
            , int_vendas.id_cliente
            , int_vendas.id_vendedor            
            , vendedor.id_territorio_vendedor
            , int_vendas.id_territorio
            , endereco.id_pais
            , vendedor.id_codigo_nacional
            , int_vendas.id_endereco_entrega
            , int_vendas.id_forma_entrega
            , int_vendas.id_cartao_credito
            , int_vendas.id_taxa_cambio
            , int_vendas.id_motivo_venda
           
            -- DATAS --             
            , int_vendas.data_pedido 
            , int_vendas.prazo_entrega_pedido
            , int_vendas.data_envio          
            , vendedor.dt_nascimento_funcionario             
            , vendedor.data_contratacao_funcionario 
            , produtos.inicio_vendas_produto
            , produtos.termino_vendas_produto          
            
            -- MÉTRICAS --        
            , int_vendas.taxa_pedido
            , int_vendas.valor_frete
            , produtos.quantidade_solicitada
            , produtos.preco_unitario_produto
            , produtos.desconto_produto
            , produtos.custo_padrao_produto
            , produtos.preco_tabelado_produto
            , int_vendas.subtotal_pedido            
            , int_vendas.total_a_receber_pedido
            , vendedor.meta_vendas_vendedor
            , vendedor.porcentagem_comissao_vendedor
            , vendedor.vendas_acumulada_vendedor
            , vendedor.vendas_ultimo_ano_vendedor                      
            , vendedor.bonus_vendedor   

            -- CATEGORIAS --
            , clientes.nome as nome_cliente          
            , endereco.local_endereco
            , endereco.nome_cidade
            , endereco.nome_pais
            , endereco.codigo_pais
            , produtos.nome_produto
            , produtos.codigo_produto
            , produtos.cor_produto
            , produtos.estoque_minimo_produto
            , produtos.ponto_reposicao_produto
            , produtos.tamanho_produto
            , produtos.codigo_tamanho_produto
            , produtos.unidade_medida_peso_produto
            , produtos.peso_produto
            , produtos.dias_fabricacao_produto
            , produtos.linha_produto
            , produtos.classe_produto
            , produtos.estilo_produto
            , vendedor.nome as nome_vendedor
            , vendedor.horas_descanso_funcionario
            , vendedor.cargo_funcionario
            , vendedor.genero_funcionario
            , vendedor.eh_assalariado
            , vendedor.horas_licenca_medica_funcionario
            , vendedor.funcionario_ativo        
            , int_vendas.motivo_venda
            , int_vendas.tipo_motivo_venda
            , int_vendas.codigo_pedido
            , int_vendas.numero_conta
            , int_vendas.numero_revisao
            , int_vendas.status_pedido
            , int_vendas.pedido_foi_online
            , int_vendas.codigo_aprovacao_cartao_credito
            , int_vendas.tipo_cartao_credito
            , int_vendas.rn


        from int_vendas
        left join clientes on
            int_vendas.id_cliente = clientes.id_cliente
        left join vendedor on   
            int_vendas.id_vendedor = vendedor.id_empresa
        left join endereco on
            int_vendas.id_endereco_entrega = endereco.id_endereco
        left join produtos on
            int_vendas.id_pedido_venda = produtos.id_pedido_vendas
    )
    
  
    , transformacoes as (
        select 
            *
            , preco_unitario_produto * quantidade_solicitada as valor_negociado
            , preco_unitario_produto * quantidade_solicitada * (1 - desconto_produto) as valor_negociado_liquido 
            , case
                when desconto_produto > 0 then 'Sim'
                else 'Nao'
            end as teve_desconto
            , valor_frete / count (id_pedido_venda) over (partition by id_pedido_venda) as frete_ponderado
            , taxa_pedido / count (id_pedido_venda) over (partition by id_pedido_venda) as taxa_ponderada
            , subtotal_pedido / count (id_pedido_venda) over (partition by id_pedido_venda) as subtotal_ponderado
            , total_a_receber_pedido / count (id_pedido_venda) over (partition by id_pedido_venda) as total_ponderado
            , vendas_acumulada_vendedor / count (id_vendedor) over (partition by id_vendedor) as total_vendas_vendedor
            , vendas_ultimo_ano_vendedor / count (id_vendedor) over (partition by id_vendedor) as total_vendas_ultimo_ano_vendedor
            , bonus_vendedor / count (id_vendedor) over (partition by id_vendedor) as bonus_para_vendedor
            , (preco_unitario_produto * quantidade_solicitada * (1 - desconto_produto)) / count (id_pedido_venda) over (partition by id_pedido_venda) as ticket_medio

        from joined_tabelas
        where rn = 1
    )

    --, self_join_tabelas as (
    --    select 
    --        joined_tabelas.sk_vendas_motivo_cartao
    --        , joined_tabelas.sk_dim_produtos
    --        , joined_tabelas.id_pedido_venda
    --        , joined_tabelas.id_produto
    --        , joined_tabelas.id_pessoa
    --        , joined_tabelas.id_cliente
    --        , joined_tabelas.id_vendedor
    --        , joined_tabelas.id_territorio_vendedor
    --        , joined_tabelas.id_territorio
    --        , joined_tabelas.id_pais
    --        , joined_tabelas.id_codigo_nacional
    --        , joined_tabelas.id_endereco_entrega
    --        , joined_tabelas.id_forma_entrega
    --        , joined_tabelas.id_cartao_credito
    --        , joined_tabelas.id_taxa_cambio
    --        , joined_tabelas.id_motivo_venda
    --        , joined_tabelas.data_pedido
    --        , joined_tabelas.prazo_entrega_pedido
    --        , joined_tabelas.data_envio
    --        , joined_tabelas.dt_nascimento_funcionario
    --        , joined_tabelas.data_contratacao_funcionario
    --        , joined_tabelas.inicio_vendas_produto
    --        , joined_tabelas.termino_vendas_produto
    --        , joined_tabelas.quantidade_solicitada
    --        , joined_tabelas.preco_unitario_produto
    --        , joined_tabelas.desconto_produto
    --        , joined_tabelas.custo_padrao_produto
    --        , joined_tabelas.preco_tabelado_produto
    --        , joined_tabelas.meta_vendas_vendedor
    --        , joined_tabelas.porcentagem_comissao_vendedor
    --        , joined_tabelas.nome_cliente
    --        , joined_tabelas.local_endereco
    --        , joined_tabelas.nome_cidade
    --        , joined_tabelas.nome_pais
    --        , joined_tabelas.codigo_pais
    --        , joined_tabelas.nome_produto
    --        , joined_tabelas.codigo_produto
    --        , joined_tabelas.cor_produto
    --        , joined_tabelas.estoque_minimo_produto
    --        , joined_tabelas.ponto_reposicao_produto
    --        , joined_tabelas.tamanho_produto
    --        , joined_tabelas.codigo_tamanho_produto
    --        , joined_tabelas.unidade_medida_peso_produto
    --        , joined_tabelas.peso_produto
    --        , joined_tabelas.dias_fabricacao_produto
    --        , joined_tabelas.linha_produto
    --        , joined_tabelas.classe_produto
    --        , joined_tabelas.estilo_produto
    --        , joined_tabelas.nome_vendedor
    --        , joined_tabelas.horas_descanso_funcionario
    --        , joined_tabelas.cargo_funcionario
    --        , joined_tabelas.genero_funcionario
    --        , joined_tabelas.eh_assalariado
    --        , joined_tabelas.horas_licenca_medica_funcionario
    --        , joined_tabelas.funcionario_ativo
    --        , joined_tabelas.motivo_venda
    --        , joined_tabelas.tipo_motivo_venda
    --        , joined_tabelas.codigo_pedido
    --        , joined_tabelas.numero_conta
    --        , joined_tabelas.numero_revisao
    --        , joined_tabelas.status_pedido
    --        , joined_tabelas.pedido_foi_online
    --        , joined_tabelas.codigo_aprovacao_cartao_credito
    --        , joined_tabelas.tipo_cartao_credito
    --        , joined_tabelas.rn
    --        , transformacoes.frete_ponderado
    --        , transformacoes.taxa_ponderada
    --        , transformacoes.subtotal_ponderado
    --        , transformacoes.total_ponderado
    --        , transformacoes.total_vendas_vendedor
    --        , transformacoes.total_vendas_ultimo_ano_vendedor
    --        , transformacoes.bonus_para_vendedor
    --        , transformacoes.ticket_medio
    --    from joined_tabelas 
    --    left join transformacoes on
    --        joined_tabelas.sk_vendas_motivo_cartao = transformacoes.sk_vendas_motivo_cartao
    --)
--
    --select *
    --from self_join_tabelas
    --order by id_pedido_venda desc
--
    , select_final as (
        select
            -- CHAVES --
            cast (sk_vendas_motivo_cartao as string) || '-' || cast (sk_dim_produtos as string) as sk_fct_vendas
            , sk_vendas_motivo_cartao
            , sk_dim_produtos
            , id_pedido_venda
            , id_produto
            , id_pessoa
            , id_cliente
            , id_vendedor
            , id_territorio_vendedor
            , id_territorio
            , id_pais
            , id_codigo_nacional
            , id_endereco_entrega
            , id_forma_entrega
            , id_cartao_credito
            , id_taxa_cambio
            , id_motivo_venda

            -- DATAS --
            , data_pedido
            , prazo_entrega_pedido
            , data_envio
            , dt_nascimento_funcionario
            , data_contratacao_funcionario
            , inicio_vendas_produto
            , termino_vendas_produto

            -- METRICAS --
            , taxa_ponderada
            --, taxa_pedido
            , frete_ponderado
            --, valor_frete
            , custo_padrao_produto
            , preco_tabelado_produto
            --, subtotal_pedido
            , subtotal_ponderado
            , total_ponderado
            , quantidade_solicitada
            , preco_unitario_produto
            , teve_desconto
            , desconto_produto            
            --, total_a_receber_pedido
            , valor_negociado
            , valor_negociado_liquido
            , ticket_medio
            , meta_vendas_vendedor
            , porcentagem_comissao_vendedor
            , total_vendas_vendedor
            --, vendas_acumulada_vendedor
            --, vendas_ultimo_ano_vendedor
            , total_vendas_ultimo_ano_vendedor
            , bonus_para_vendedor
            --, bonus_vendedor
       
            
           -- CATEGORIAS --
            , nome_cliente
            , local_endereco
            , nome_cidade
            , nome_pais
            , codigo_pais
            , nome_produto
            , codigo_produto
            , cor_produto
            , estoque_minimo_produto
            , ponto_reposicao_produto
            , tamanho_produto
            , codigo_tamanho_produto
            , unidade_medida_peso_produto
            , peso_produto
            , dias_fabricacao_produto
            , linha_produto
            , classe_produto
            , estilo_produto
            , nome_vendedor
            , horas_descanso_funcionario
            , cargo_funcionario
            , genero_funcionario
            , eh_assalariado
            , horas_licenca_medica_funcionario
            , funcionario_ativo
            , motivo_venda
            , tipo_motivo_venda
            , codigo_pedido
            , numero_conta
            , numero_revisao
            , status_pedido
            , pedido_foi_online
            , codigo_aprovacao_cartao_credito
            , tipo_cartao_credito
            , rn
            
        from transformacoes
        
    )

select *
from select_final
order by id_pedido_venda desc


