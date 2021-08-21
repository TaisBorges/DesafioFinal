with detalhe_pedido_venda as (
    select 
    salesorderid as id_pedido,        
    salesorderdetailid as id_detalhe_pedido,
    productid as id_produto,
    specialofferid as id_oferta_especial,
    orderqty as quantidade_pedido,
    unitprice as preco_unitario,
    modifieddate as data_modificacao_pedido_venda,
    unitpricediscount as desconto_preco_unitario
    from {{source('desafio_final_aw','salesorderdetail')}}
    )
    
    select *,
    (preco_unitario * (1-desconto_preco_unitario)* quantidade_pedido) as valor_faturado
    from detalhe_pedido_venda