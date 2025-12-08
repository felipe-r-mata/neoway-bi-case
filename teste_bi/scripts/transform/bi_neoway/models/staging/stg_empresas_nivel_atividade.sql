select
    cnpj,
    nivel_atividade
from {{ source('raw', 'empresas_nivel_atividade') }}
