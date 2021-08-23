with vendedor as (
    select
    salesquota as cota_venda,
    cast (modifieddate as date) as data_modificacao,
    saleslastyear as venda_ultimo_ano,	
    commissionpct as percentual_comissao,	
    territoryid as id_territorio,
    bonus as bonus,
    businessentityid as id_entidade_negocio,
    salesytd as total_venda_ano
    from {{source('desafio_final_aw', 'salesperson')}}
)
select  * from vendedor
