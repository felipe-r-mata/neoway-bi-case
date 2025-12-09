with raw as (
    select
        cast(cnpj as int64) as cnpj,
        upper(trim(empresa_porte)) as empresa_porte
    from {{ source('raw', 'empresas_porte') }}
),

normalized as (
    select
        cnpj,
        case
            when empresa_porte like '%MEI%'              then 'MEI'
            when empresa_porte like 'MICRO%'             then 'MICRO'
            when empresa_porte like 'PEQUENA%'           then 'PEQUENA'
            when empresa_porte like 'MEDIA%'             then 'MEDIA'
            when empresa_porte like 'GRANDE%'            then 'GRANDE'
            else null
        end as porte
    from raw
)

select *
from normalized
