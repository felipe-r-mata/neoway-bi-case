with base_empresas as (
    select
        cnpj,
        dt_abertura,
        fl_matriz,
        cd_cnae_principal,
        de_cnae_principal,
        endereco_uf,
        endereco_regiao,
        endereco_mesorregiao,
        endereco_municipio,
        endereco_cep,
        de_ramo_atividade,
        situacao_cadastral
    from {{ ref('stg_df_empresas') }}
),

joined as (
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
    from {{ ref('stg_df_empresas') }} e
    left join {{ ref('stg_empresas_porte') }}            p on e.cnpj = p.cnpj
    left join {{ ref('stg_empresas_simples') }}          s on e.cnpj = s.cnpj
    left join {{ ref('stg_empresas_nivel_atividade') }}  n on e.cnpj = n.cnpj
    left join {{ ref('stg_empresas_saude_tributaria') }} t on e.cnpj = t.cnpj
)

select
    farm_fingerprint(cast(cnpj as string)) as sk_empresa,

    cnpj,
    dt_abertura,
    fl_matriz,
    cd_cnae_principal,
    de_cnae_principal,
    endereco_uf,
    endereco_regiao,
    endereco_mesorregiao,
    endereco_municipio,
    endereco_cep,
    de_ramo_atividade,
    situacao_cadastral,

    coalesce(porte,            'NÃO INFORMADO') as porte,
    coalesce(nivel_atividade,  'NÃO INFORMADO') as nivel_atividade,
    coalesce(saude_tributaria, 'NÃO INFORMADO') as saude_tributaria,
    coalesce(optante_simples,  false)           as optante_simples,
    coalesce(optante_simei,    false)           as optante_simei

from joined