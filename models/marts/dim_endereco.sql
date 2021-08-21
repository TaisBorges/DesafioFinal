with dados_localizacao as (
    select *
      from {{ref ('stg_address')}}
)
select * from dados_localizacao