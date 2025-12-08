select
    cnpj,
    optante_simples,
    optante_simei
from {{ source('raw', 'empresas_simples') }}
