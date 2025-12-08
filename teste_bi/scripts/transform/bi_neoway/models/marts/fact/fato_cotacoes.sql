with base as (
    select
        data,
        ticker,
        cd_acao_rdz,
        cd_bdi,
        tp_merc,
        nm_empresa_rdz,
        especi,
        prazot,
        moeda_ref,
        vl_abertura,
        vl_maximo,
        vl_minimo,
        vl_medio,
        vl_fechamento,
        vl_mlh_oft_compra,
        vl_mlh_oft_venda,
        quantidade_negocios,
        quantidade_titulos,
        vl_volume,
        vl_exec_opc,
        in_opc,
        dt_vnct_opc,
        ft_cotacao,
        vl_exec_moeda_corrente,
        cd_isin
    from {{ ref('stg_cotacoes_bolsa') }}
),

joined_tempo as (
    select
        b.data,
        t.ano,
        t.mes,
        t.ano_mes,
        t.trimestre,
        t.dia_semana_nome,
        t.dia_semana_num,
        b.ticker,
        b.cd_acao_rdz,
        b.cd_bdi,
        b.tp_merc,
        b.nm_empresa_rdz,
        b.especi,
        b.prazot,
        b.moeda_ref,
        b.vl_abertura,
        b.vl_maximo,
        b.vl_minimo,
        b.vl_medio,
        b.vl_fechamento,
        b.vl_mlh_oft_compra,
        b.vl_mlh_oft_venda,
        b.quantidade_negocios,
        b.quantidade_titulos,
        b.vl_volume,
        b.vl_exec_opc,
        b.in_opc,
        b.dt_vnct_opc,
        b.ft_cotacao,
        b.vl_exec_moeda_corrente,
        b.cd_isin
    from base b
    left join {{ ref('dim_tempo') }} t
        on b.data = t.data
),

joined_empresas_bolsa as (
    select
        j.*,
        eb.cnpj,
        eb.nm_empresa       as nm_empresa_bolsa,
        eb.setor_economico,
        eb.subsetor,
        eb.segmento,
        eb.segmento_b3,
        eb.nm_segmento_b3
    from joined_tempo j
    left join {{ ref('stg_empresas_bolsa') }} eb
        on j.cd_acao_rdz = eb.cd_acao_rdz
),

joined_dim as (
    select
        j.*,
        d.sk_empresa
    from joined_empresas_bolsa j
    left join {{ ref('dim_empresa') }} d
        on j.cnpj = d.cnpj
)

select
    -- chaves
    sk_empresa,
    data,
    ano,
    mes,
    ano_mes,
    trimestre,
    dia_semana_nome,
    dia_semana_num,

    -- papel / empresa na bolsa
    ticker,
    cd_acao_rdz,
    nm_empresa_rdz,
    nm_empresa_bolsa,
    setor_economico,
    subsetor,
    segmento,
    segmento_b3,
    nm_segmento_b3,

    -- métricas de cotação
    cd_bdi,
    tp_merc,
    especi,
    prazot,
    moeda_ref,
    vl_abertura,
    vl_maximo,
    vl_minimo,
    vl_medio,
    vl_fechamento,
    vl_mlh_oft_compra,
    vl_mlh_oft_venda,
    quantidade_negocios,
    quantidade_titulos,
    vl_volume,
    vl_exec_opc,
    in_opc,
    dt_vnct_opc,
    ft_cotacao,
    vl_exec_moeda_corrente,
    cd_isin
from joined_dim
