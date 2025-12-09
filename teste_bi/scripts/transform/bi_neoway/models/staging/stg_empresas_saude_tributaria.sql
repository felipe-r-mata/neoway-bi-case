with raw as (
    select
        cast(cnpj as int64) as cnpj,
        upper(trim(saude_tributaria)) as saude_tributaria
    from {{ source('raw', 'empresas_saude_tributaria') }}
)

select
    cnpj,
    saude_tributaria
from raw

