with dados_produto as (
    select 
    productid as id_produto,
    productmodelid as id_modelo_produto,
    productsubcategoryid as id_subcategoria_produto,
    name as nome_produto, 
    productnumber as numero_produto,
    class as classe_produto,
    style as estilo_produto,
    standardcost as custo_padrao_produto,
    sellstartdate as data_inicio_venda,
    sellenddate as data_venda_final,
    listprice as lista_preco_produto,
    productline as linha_produto,
    cast (modifieddate as date) as data_modificacao_produto,
    weightunitmeasurecode as peso_unidade_medida,
    sizeunitmeasurecode as tamanho_unidade_medida,
    size as tamanho,
    color as cor,    
    weight as peso_produto

    from {{source('desafio_final_aw','product')}}
)
select * from dados_produto