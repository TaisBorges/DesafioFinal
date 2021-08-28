with dados_negocio as (
    select *
    from {{ref ('stg_customer')}}
)
select * from dados_negocio