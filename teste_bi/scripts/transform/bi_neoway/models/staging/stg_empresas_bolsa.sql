with base as (
    select
        id,
        cd_acao_rdz,
        nm_empresa,
        setor_economico,
        subsetor,
        segmento,
        segmento_b3,
        nm_segmento_b3,
        cd_acao,
        tx_cnpj,
        vl_cnpj,
        created_at,
        updated_at
    from {{ source('raw', 'empresas_bolsa') }}
)

select
    cd_acao_rdz,                          -- código reduzido, casa com cotacoes_bolsa.cd_acao_rdz
    cd_acao               as ticker,      -- pode ter múltiplos tickers no mesmo raiz (ex.: PETR4, PETR3)
    nm_empresa,
    setor_economico,
    subsetor,
    segmento,
    segmento_b3,
    nm_segmento_b3,
    safe_cast(
        nullif(
            regexp_replace(
                coalesce(tx_cnpj, cast(vl_cnpj as string)),
                r'[^0-9]',
                ''
            ),
            ''
        ) as int64
    ) as cnpj,
    created_at,
    updated_at
from base
