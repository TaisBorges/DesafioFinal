with dados_produto as (
    select *
      from {{ref ('stg_product')}}
)
select * from dados_produto