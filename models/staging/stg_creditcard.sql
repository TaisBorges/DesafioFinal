with dados_cartao as(
    select
    cardtype as tipo_cartao,
    creditcardid as id_cartao_credito
    from {{source('desafio_final_aw','creditcard')}}
)
select * from dados_cartao