with dados_motivo_venda as (
      select
      id_pedido, -- group by mv.id_cliente,
      id_vendedor,
      id_cliente as fk_cliente,
      id_territorio,
      numero_ordem_compra,
      id_endereco_cobranca,
      id_endereco_entrega as fk_endereco,
      status_pedido, 	-- dado a ser apresentado no BI como status
      data_pedido,	-- dado a ser apresentado no BI como data de venda
      subtotal,
      frete,
      data_vencimento,
      valor_final,
      data_entrega,
      tipo_cartao, -- dado a ser apresentado no BI como tipo de cartão usado
      id_cartao_credito,
      tipo_motivo,	
      nome_motivo_venda, -- dado a ser apresentado no BI como motivo da venda
      id_motivo_venda

      from {{ref ('stg_salesorderhearder')}}
),

    detalhe_pedido_venda as (
      select 
      id_pedido as fk_pedido,
      --row_number() over (order by customerid) as sk_cliente
      id_detalhe_pedido,
      id_produto as fk_produto,
      id_oferta_especial,
      quantidade_pedido,
      preco_unitario,
      data_modificacao_pedido_venda,
      desconto_preco_unitario,
      valor_faturado
      from {{ref('stg_salesorderdetails')}}
),

    cliente as (
      select *
      from {{ref ('dim_cliente')}}
),

    endereco as (
      select *
      from {{ref ('dim_endereco')}}
),

    produto as (
      select *
      from {{ref ('dim_produto')}}
),

          

      dados_calculados as (
        select
          --id_pedido,
          count(id_pedido) as numero_pedidos
          from dados_motivo_venda
          --group by id_pedido
  ),

--       dado_calculados_2 as (
--         select
--            fk_pedido
--            quantidade_pedido,
--            preco_unitario,
--          sum(quantidade_pedido) as quantidade_comprada,
--          sum(preco_unitario) as valor_total_produto,
--          avg(preco_unitario) as ticket_medio
--          from detalhe_pedido_venda
--          group by fk_pedido
--  ),  
      
      dados_venda as (
        select
          mv.id_pedido,-- group by mv.id_cliente,
          mv.id_vendedor,
          mv.fk_cliente,
          mv.id_territorio,
          mv.numero_ordem_compra,
          mv.id_endereco_cobranca,
          mv.fk_endereco,
          mv.status_pedido, 	-- dado a ser apresentado no BI como status
          mv.data_pedido,	-- dado a ser apresentado no BI como data de venda
          mv.subtotal,
          mv.frete,
          mv.data_vencimento,
          mv.valor_final,
          mv.data_entrega,
          mv.tipo_cartao, -- dado a ser apresentado no BI como tipo de cartão usado
          mv.id_cartao_credito,
          mv.tipo_motivo,	
          mv.nome_motivo_venda, -- dado a ser apresentado no BI como motivo da venda
          mv.id_motivo_venda,

          pv.fk_pedido,
          pv.id_detalhe_pedido,
          pv.fk_produto,
          pv.id_oferta_especial,
          pv.quantidade_pedido,
          pv.preco_unitario,
          pv.data_modificacao_pedido_venda,
          pv.desconto_preco_unitario,
          pv.valor_faturado

          from dados_motivo_venda mv
          left join detalhe_pedido_venda pv
          on mv.id_pedido = pv.fk_pedido
          left join cliente c
          on c.id_cliente = mv.fk_cliente
          left join endereco e
          on mv.fk_endereco = e.id_endereco
          left join produto p
          on pv.fk_produto = p.id_produto
          )


select * from dados_venda