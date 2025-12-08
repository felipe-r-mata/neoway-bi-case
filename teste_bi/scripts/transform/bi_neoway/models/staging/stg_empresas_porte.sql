select
    cnpj,
    empresa_porte as porte
from {{ source('raw', 'empresas_porte') }}
