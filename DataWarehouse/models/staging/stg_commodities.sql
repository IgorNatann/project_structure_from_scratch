-- Import 

with source as (
    select
        "Data",
        "Close",
        "simbolo"
    from
        {{ source('dbsales_khd7', 'commodities') }}
),

renamed as (
    select
        cast("Data" as date) as data,
        "Close" as valor_fechamento,
        simbolo
    from
        source
)

select * from renamed
