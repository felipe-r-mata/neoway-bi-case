with raw as (
    select
        cast(cnpj as int64) as cnpj,
        optante_simples,
        optante_simei
    from {{ source('raw', 'empresas_simples') }}
)

select
    cnpj,
    coalesce(cast(optante_simples as bool), false) as optante_simples,
    coalesce(cast(optante_simei   as bool), false) as optante_simei
from raw

