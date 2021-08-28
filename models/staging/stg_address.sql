with dados_endereco as (
    select
    addressid as id_endereco,
    stateprovinceid as fk_estado,
    city as cidade,
    cast (modifieddate as date) as data_modificacao_endereco,
    postalcode as cep
    from {{source('desafio_final_aw','address')}}
    ),
    dados_estado as (
        select
        stateprovinceid as id_estado,
        territoryid as id_territorio,
        countryregioncode as fk_pais,
        cast (modifieddate as date) as data_modificacao_estado,
        name as estado,
        stateprovincecode as uf_estado        
        from {{source('desafio_final_aw','stateprovince')}}
    ),
    dados_pais as (
        select
        countryregioncode as id_pais,
        cast (modifieddate as date) as data_modificacao_pais,
        name as pais
        from {{source ('desafio_final_aw','countryregion')}}
    ),
    dados_localizacao as (
        select
            en.id_endereco,
            en.cidade,
            --es.id_estado,
            es.estado,
            es.uf_estado,
            es.id_territorio,
            rp.pais,
            rp.id_pais,
            from 
            dados_endereco en
                left join dados_estado es
                on en.fk_estado = es.id_estado
                left join dados_pais rp
                on es.fk_pais = rp.id_pais
    )

select * from dados_localizacao