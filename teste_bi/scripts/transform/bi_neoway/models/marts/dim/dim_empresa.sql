with joined as (
    select
        e.cnpj,
        e.dt_abertura,
        e.fl_matriz,
        e.cd_cnae_principal,
        e.de_cnae_principal,
        e.endereco_uf,
        e.endereco_regiao,
        e.endereco_mesorregiao,
        e.endereco_municipio,
        e.endereco_cep,
        e.de_ramo_atividade,
        e.situacao_cadastral,
        p.porte,
        s.optante_simples,
        s.optante_simei,
        n.nivel_atividade,
        t.saude_tributaria
    from {{ ref('stg_df_empresas') }}              e
    left join {{ ref('stg_empresas_porte') }}            p on e.cnpj = p.cnpj
    left join {{ ref('stg_empresas_simples') }}          s on e.cnpj = s.cnpj
    left join {{ ref('stg_empresas_nivel_atividade') }}  n on e.cnpj = n.cnpj
    left join {{ ref('stg_empresas_saude_tributaria') }} t on e.cnpj = t.cnpj
)

select
    -- surrogate key baseada exclusivamente no CNPJ da df_empresas
    farm_fingerprint(cast(cnpj as string)) as sk_empresa,

    cnpj,
    dt_abertura,
    fl_matriz,
    cd_cnae_principal,
    de_cnae_principal,
    de_ramo_atividade,
    situacao_cadastral,
    endereco_uf,
    endereco_regiao,
    endereco_mesorregiao,
    endereco_municipio,
    endereco_cep,
    porte,
    optante_simples,
    optante_simei,
    nivel_atividade,
    saude_tributaria
from joined
