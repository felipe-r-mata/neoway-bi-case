with raw as (
    select
        cast(cnpj as int64) as cnpj,
        upper(trim(nivel_atividade)) as nivel_atividade
    from {{ source('raw', 'empresas_nivel_atividade') }}
)

select
    cnpj,
    nivel_atividade
from raw
