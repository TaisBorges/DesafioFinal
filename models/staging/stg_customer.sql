with dados_cliente as (
    select 
    customerid as id_cliente,
    personid as sk_entidade_negocio,
    territoryid as id_territorio
        from {{source('desafio_final_aw','customer')}}
    ),
    
    dados_pessoa as (
        select 
        businessentityid as id_entidade_negocio,
        firstname as primeiro_nome,
        middlename as meio_nome,
        lastname as ultimo_nome,
        namestyle as estilo_nome,
        suffix as sufixo,
        cast (modifieddate as date) as data_modificacao_pessoa,
        emailpromotion as email_promocao,
        title as formacao        
        from {{source('desafio_final_aw','person')}}
    ),
    dados_email as (
        SELECT 
            businessentityid as fk_entidade_negocio,
            emailaddressid as id_endereco_email,
            emailaddress as email
        FROM {{source('desafio_final_aw', 'emailaddress')}}
    ),
    dados_telefone as (
        select 
            businessentityid as id_entidade_negocio,
            phonenumbertypeid as fk_tipo_num_telefone,
            phonenumber as telefone
        FROM {{source('desafio_final_aw', 'personphone')}}
    ),
    dados_tipo_telefone as (
        SELECT 
            phonenumbertypeid as id_tipo_num_telefone,
            name as tipo_telefone,
            cast (modifieddate as date) as data_modificacao_tipo_tel_telefone
        FROM {{source('desafio_final_aw', 'phonenumbertype')}}
    ),

    contato_entidade_negocio as (
        select 
        personid as id_pessoa,
        businessentityid as fk_entidade_negocio,
        contacttypeid as fk_tipo_contato,
        cast (modifieddate as date) as id_data_modificacao_contato_entidade_negocio
        from {{source('desafio_final_aw','businessentitycontact')}}
    ),
    dados_contato_comercial_tipo as (
        select 
            contacttypeid as id_tipo_contato,
            cast (modifieddate as date) as id_data_modificacao_contato_comercial_tipo,
            name as tipo_contato
        from {{source('desafio_final_aw', 'contacttype')}}
    ),
    dados_entidade_negocio as (
        select 
            businessentityid as id_entidade_negocio,
            cast (modifieddate as date) as data_modificacao_entidade_negocio
        from {{source('desafio_final_aw', 'businessentity')}}
    ),
    
    dados_endereco_tipo as (
        select 
            name as nome_endereco,
            addresstypeid as id_tipo_endereco,
            cast (modifieddate as date) as data_modificacao_endereco_tipo
        from {{source('desafio_final_aw','addresstype')}}
    ),
    dados_negocio as (
        select
        en.id_entidade_negocio,
        trim( concat(p.primeiro_nome, ' ', p.ultimo_nome )) as nome_completo,
        --p.tipo_pessoa,
        p.estilo_nome,
        p.sufixo,
        p.data_modificacao_pessoa,
        p.email_promocao,
        p.formacao,
        c.id_cliente,
        c.sk_entidade_negocio,
       -- c.id_territorio,
        --p.data_modificacao_pessoa,
        --en.id_pessoa,
        --en.sk_entidade_negocio,
        --en.id_data_modificacao_contato_entidade_negocio,
        --een.id_endereco,
        --een.sk2_entidade_negocio,
        --een.data_modificacao_entidade_negocio,
        em.email,
        t.telefone,
        tt.tipo_telefone,
        cct.tipo_contato,
        cct.id_tipo_contato,
        cen.fk_tipo_contato

        from dados_entidade_negocio en
            left join dados_pessoa p
            on p.id_entidade_negocio = en.id_entidade_negocio
            left join dados_cliente c
            on en.id_entidade_negocio = c.sk_entidade_negocio
            left join dados_email em
            on em.fk_entidade_negocio = ifnull(p.id_entidade_negocio, en.id_entidade_negocio)
            left join dados_telefone t
            on t.id_entidade_negocio = ifnull(p.id_entidade_negocio, en.id_entidade_negocio)
            left join dados_tipo_telefone tt
            on tt.id_tipo_num_telefone = t.fk_tipo_num_telefone
            left join contato_entidade_negocio cen
            on cen.id_pessoa = ifnull(p.id_entidade_negocio, en.id_entidade_negocio) 
            left join dados_contato_comercial_tipo cct
            on cct.id_tipo_contato = cen.fk_tipo_contato
           
    )

select * from dados_negocio