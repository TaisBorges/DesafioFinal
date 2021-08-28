with produto as (
    select 
        COUNT(distinct V.fk_produto) as qtde_produtos
    from {{ref('dim_produto')}} P
        JOIN {{ref('fact_vendas')}} V
            on P.id_produto = V.fk_produto
    where data_pedido between '2011-06-01' and '2012-06-01'
)

select * from produto
where qtde_produtos != 132