select
    cnpj,
    saude_tributaria
from {{ source('raw', 'empresas_saude_tributaria') }}
