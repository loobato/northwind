/* time comercial tem que as vendas em 1997 foram de $658.388,75 */

with
    vendas_1997 as (
        select sum(total_bruto) as total_bruto_vendido
        from {{ ref('fct_vendas') }}
        where dt_ordem between '1997-01-01' and '1997-12-31'
    )

select total_bruto_vendido
from vendas_1997
where total_bruto_vendido != 658388.75