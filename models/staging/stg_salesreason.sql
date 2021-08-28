with motivo_venda as (
    select
    reasontype as tipo_motivo,	
    cast (modifieddate as date) as data_modificacao_motivo_venda,
    name as nome_motivo_venda,
    salesreasonid as id_motivo_venda
    from {{source('desafio_final_aw','salesreason')}}
)
select * from motivo_venda