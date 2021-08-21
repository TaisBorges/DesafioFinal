with cabecalho_pedido as (
    select
    salesorderid as id_pedido,
    salespersonid as id_vendedor,
    customerid as id_cliente,
    currencyrateid as id_taxa_cambio,
    shipmethodid as id_metodo_entrega,
    territoryid as id_territorio,
    creditcardid as fk_cartao_credito,
    purchaseordernumber as numero_ordem_compra,
    billtoaddressid	as id_endereco_cobranca,
    modifieddate as data_modificacao_cabecalho_pedido,
    taxamt as mt_taxa,
    shiptoaddressid as id_endereco_entrega,
    onlineorderflag	as pedido_online,
    status as status_pedido,
    orderdate as data_pedido,
    creditcardapprovalcode as cartao_aprovado,
    subtotal,
    revisionnumber as numero_revisao,
    freight as frete,
    duedate as data_vencimento,
    totaldue as valor_final,
    shipdate as data_entrega,
    accountnumber as numero_conta

    from {{source('desafio_final_aw','salesorderheader')}}
),

    dados_cartao as(
    select
    cardtype as tipo_cartao,
    creditcardid as id_cartao_credito
    from {{source('desafio_final_aw','creditcard')}}
),

    cabecalho_motivo_vendas as (
        select
        salesreasonid as fk_motivo_venda,
        salesorderid as fk_pedido,
        modifieddate as data_modificacao_cabecalho_motivo_vendas
        from {{source('desafio_final_aw','salesorderheadersalesreason')}}
),

    motivo_venda as (
    select
    reasontype as tipo_motivo,	
    modifieddate as data_modificacao_motivo_venda,
    name as nome_motivo_venda,
    salesreasonid as id_motivo_venda
    from {{source('desafio_final_aw','salesreason')}}
),

    dados_motivo_venda as (
        select
            cp.id_pedido,
            cp.id_vendedor,
            cp.id_cliente,
            cp.id_territorio,
            --cp.fk_cartao_credito,
            cp.numero_ordem_compra,
            cp.id_endereco_cobranca,
            cp.id_endereco_entrega,
            cp.status_pedido,
            cp.data_pedido,
            cp.subtotal,
            cp.frete,
            cp.data_vencimento,
            cp.valor_final,
            cp.data_entrega,
            dc.tipo_cartao,
            dc.id_cartao_credito,
            --cm.fk_motivo_venda,
            --cm.fk_pedido,
            mv.tipo_motivo,	
            mv.nome_motivo_venda,
            mv.id_motivo_venda

        from cabecalho_pedido cp
        left join cabecalho_motivo_vendas cm
        on cm.fk_pedido = cp.id_pedido
        left join dados_cartao dc
        on cp.fk_cartao_credito = dc.id_cartao_credito
        left join motivo_venda mv
        on cm.fk_motivo_venda = mv.id_motivo_venda
    ) 

select * from dados_motivo_venda