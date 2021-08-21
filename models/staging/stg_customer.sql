with dados_cliente as (
    select 
    personid as id_pessoa,
    territoryid as id_territorio,
    storeid as id_estoque,
    customerid as id_cliente
    from {{source('desafio_final_aw','customer')}}
)
select * from dados_cliente