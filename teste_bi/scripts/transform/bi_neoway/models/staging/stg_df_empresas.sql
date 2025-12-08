select
    cnpj,
    cast(dt_abertura as date)           as dt_abertura,
    matriz_empresaMatriz                as fl_matriz,
    cd_cnae_principal                   as cd_cnae_principal,
    de_cnae_principal                   as de_cnae_principal,
    endereco_uf,
    endereco_regiao,
    endereco_mesorregiao,
    endereco_municipio,
    endereco_cep,
    de_ramo_atividade,
    situacao_cadastral
from {{ source('raw', 'df_empresas') }}
