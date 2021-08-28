with vendas as (
    select 
        sum(quantidade_pedido) as total
    from {{ref('fact_vendas')}}
    where data_pedido between '2011-06-01' and '2012-06-01'
)

select * from vendas
where total != 32347