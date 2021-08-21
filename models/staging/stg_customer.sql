with dados_cliente as (
    select 
    --row_number() over (order by customerid) as sk_cliente, -- chave auto-incremental
    customerid as id_cliente,
    personid as sk_pessoa,
    territoryid as id_territorio
        from {{source('desafio_final_aw','customer')}}
    ),
    
    dados_pessoa as (
        select 
        businessentityid as id_entidade_negocio,
        lastname as ultimo_nome,
        firstname as primeiro_nome,
        persontype as tipo_pessoa,
        modifieddate as data_modificacao_pessoa     
        from {{source('desafio_final_aw','person')}}
    ),

    contato_entidade_negocio as (
        select 
        personid as id_pessoa,
        businessentityid as sk_entidade_negocio,
        modifieddate as id_data_modificacao_contato_entidade_negocio
        from {{source('desafio_final_aw','businessentitycontact')}}
    ),

    endereco_entidade_negocio as (
        select
        addressid as id_endereco,
        businessentityid as sk2_entidade_negocio,
        modifieddate as data_modificacao_entidade_negocio
        from{{source('desafio_final_aw','businessentityaddress')}}
    ),
        
    dados_negocio as (
-- preciso juntar as tabelas e ter a localização da entidade de negócio para relacionar o cliente com cidade, estado, país
        select
        c.id_cliente,
        --c.sk_pessoa,
        c.id_territorio,
        p.id_entidade_negocio,
        p.ultimo_nome,
        p.primeiro_nome,
        p.tipo_pessoa,
        --p.data_modificacao_pessoa,
        en.id_pessoa,
        --en.sk_entidade_negocio,
        --en.id_data_modificacao_contato_entidade_negocio,
        een.id_endereco,
        --een.sk2_entidade_negocio,
        --een.data_modificacao_entidade_negocio,

        from contato_entidade_negocio en
            left join dados_pessoa p
            on en.sk_entidade_negocio = p.id_entidade_negocio
            left join dados_cliente c
            on c.sk_pessoa = en.id_pessoa
            left join endereco_entidade_negocio een
            on een.sk2_entidade_negocio = p.id_entidade_negocio

    )

select * from dados_negocio