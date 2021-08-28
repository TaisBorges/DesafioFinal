with cliente as (
    select 
        count(distinct C.id_cliente) as num_clientes
    FROM {{ref('dim_cliente')}} C
        JOIN {{ref('fact_vendas')}} V
            On V.fk_cliente = C.id_cliente
    where data_pedido between '2011-06-01' and '2012-06-01'
)

select * from cliente
where num_clientes != 2476