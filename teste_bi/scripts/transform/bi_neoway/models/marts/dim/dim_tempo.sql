with datas as (
    select
        distinct data
    from {{ ref('stg_cotacoes_bolsa') }}
    where data is not null
),

calendar as (
    select
        data,
        extract(year  from data) as ano,
        extract(month from data) as mes,
        cast(format_date('%Y%m', data) as int64) as ano_mes,
        extract(quarter from data) as trimestre,
        extract(dayofweek from data) as dia_semana_num,
        format_date('%A', data) as dia_semana_nome,

        -- Nome abreviado do mês em PT-BR
        case extract(month from data)
            when 1  then 'Jan'
            when 2  then 'Fev'
            when 3  then 'Mar'
            when 4  then 'Abr'
            when 5  then 'Mai'
            when 6  then 'Jun'
            when 7  then 'Jul'
            when 8  then 'Ago'
            when 9  then 'Set'
            when 10 then 'Out'
            when 11 then 'Nov'
            when 12 then 'Dez'
        end as mes_nome,

        -- Mês/ano no formato "Jan/2022"
        concat(
            case extract(month from data)
                when 1  then 'Jan'
                when 2  then 'Fev'
                when 3  then 'Mar'
                when 4  then 'Abr'
                when 5  then 'Mai'
                when 6  then 'Jun'
                when 7  then 'Jul'
                when 8  then 'Ago'
                when 9  then 'Set'
                when 10 then 'Out'
                when 11 then 'Nov'
                when 12 then 'Dez'
            end,
            '/',
            cast(extract(year from data) as string)
        ) as ano_mes_nome
    from datas
)

select
    data,
    ano,
    mes,
    ano_mes,
    trimestre,
    dia_semana_num,
    dia_semana_nome,
    mes_nome,
    ano_mes_nome
from calendar
