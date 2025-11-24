-- Staging: commodities
-- Fonte: `dbsales_khd7.commodities`
-- Propósito: importa cotações de commodities e aplica limpeza leve:
--   - converte `Data` para `date`
--   - renomeia `Close` para `valor_fechamento`
-- Pressupostos: `Data` está em formato de data válido; sem normalização de timezone

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
