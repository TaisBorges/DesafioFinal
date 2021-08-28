with endereco as (
    select 
        COUNT(distinct V.fk_endereco) as qtde_endereco
    from {{ref('dim_endereco')}} E
        JOIN {{ref('fact_vendas')}} V
            on E.id_endereco = V.fk_endereco
    where data_pedido between '2011-06-01' and '2012-06-01'
)

select * from endereco
where qtde_endereco != 2476