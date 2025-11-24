-- Import 

with source as (
    select
        "Data",
        "Close",
        "simbolo"
    from
        {{ source ('dbsales_khd7', 'commodities') }}
),

-- Renamed 

renamed as (
    SELECT  
        cast("Data" as date) as Data,
        "Close" as valor_fechamentom,
        simbolo
    FROM
        source
)

-- Query
SELECT * FROM renamed