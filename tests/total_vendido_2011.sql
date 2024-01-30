with
    vendas_em_2011 as (
        select 
            sum (valor_negociado) as total_bruto_2011
        from {{ ref('fct_vendas') }}
        where data_pedido between '2011-01-01 00:00:00.000' and '2011-12-31 00:00:00.000'
    )

select total_bruto_2011
from vendas_em_2011
where total_bruto_2011 != 12646112.1607