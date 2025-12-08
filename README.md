# Projeto BI – Case Neoway / B3  
### Pipeline de Dados • Modelagem Dimensional • dbt • BigQuery • Power BI

Este repositório contém a solução desenvolvida para o case técnico de Business Intelligence da Neoway/B3, abrangendo todas as etapas do processo de dados: ingestão, transformação, modelagem dimensional e construção de dashboard analítico.

---

## 📁 Estrutura do Repositório

```
teste_bi/
│
├── app/                     # arquivos .pbix
├── files/                   # Diretório padrão do case
├── scripts/
│   ├── transform/
│   │   └── bi_neoway/
│   │       ├── models/
│   │       │   ├── staging/ # Modelos de Stage (stg_)
│   │       │   └── marts/   # Modelos dimensionais e fato (dim_ / fato_)
│   │       ├── macros/      # Funções auxiliares do dbt
│   │       ├── logs/        # Logs gerados pelo dbt
│   │       ├── .dbt/        # Metadados locais do dbt
│   │       ├── dbt_project.yml
│   │       └── packages.yml
│   └── explore_all.py       # Script de exploração (fornecido)
│
├── Processo Seletivo - Prova técnica BI S&M - V2.pdf
└── README.md                # Este arquivo
```

---

## 🏗️ Arquitetura da Solução

A solução foi dividida em quatro fases principais:

### **1. Ingestão dos Dados (RAW Layer)**
Os arquivos CSV fornecidos no case foram carregados diretamente para o **BigQuery** por questões de tempo e praticidade.  
Em um ambiente real, esse processo seria automatizado com:

- Cloud Storage + Cloud Run / Cloud Functions  
- Orquestração com Cloud Composer (Airflow)  
- Versionamento de schemas e Data Quality Checks

---

### **2. Modelagem de Stage (STAGING Layer – dbt)**
Cada tabela RAW foi padronizada no dbt:

- Padronização de nomes de colunas  
- Conversão de tipos  
- Limpeza de dados inconsistentes  
- Criação de colunas derivadas iniciais  

Tabelas criadas em `dw_staging`:

- `stg_df_empresas`
- `stg_empresas_bolsa`
- `stg_empresas_porte`
- `stg_empresas_simples`
- `stg_empresas_saude_tributaria`
- `stg_empresas_nivel_atividade`
- `stg_cotacoes_bolsa`

---

### **3. Modelagem Dimensional (MARTS Layer – dbt)**

Criadas em `dw_marts`, com foco analítico:

#### **Dimensões:**
- `dim_empresa`  
  - Construída *exclusivamente* a partir de df_empresas  
  - Chave substituta `sk_empresa` gerada via FARM_FINGERPRINT do CNPJ  
  - Unificação de porte, optantes, saúde tributária e nível de atividade

- `dim_tempo`  
  - Criada a partir da própria fato  
  - Inclui granularidade Ano, Mês, Trimestre, Dia, Mês Nome e Ano-Mês Nome

#### **Fato:**
- `fato_cotacoes`
  - Granularidade: Ticker + Data  
  - Métricas financeiras derivadas: preços, variação %, volume, negócios  
  - Uso de *LEFT JOIN* com `dim_empresa` via `ticker` (limitado pela base fornecida)

---

## 📊 Dashboard Desenvolvido (Power BI)
Link do relatório publicado:  
🔗 **https://app.powerbi.com/view?r=eyJrIjoiZTQ1MDhjMWEtY2Y0MC00MmVmLWFhZjMtOWM2YTRiZDA4OTI4IiwidCI6ImFhMmRkZTY0LWI4MDItNGNjNC1iNDE3LWJiNjBlMWIxODVlYyJ9**

### ✔ Conteúdos apresentados:
- KPIs macroeconômicos de mercado  
- Análise de variação percentual por ativo  
- Retorno diário e mensal  
- Preço médio por período  
- Filtros interativos: ano, ticker, mês e trimestre  

---

## 🧪 Testes de Qualidade (dbt)

Foram implementados testes de:

- `not_null`
- `unique`
- `relationships`

Aplicados principalmente em:

- chaves naturais  
- chaves substitutas  
- colunas críticas da fato  

---

## 🛠️ Tecnologias Utilizadas

| Camada     | Ferramenta |
|-----------|-------------|
| Data Lake | BigQuery RAW |
| Transform | dbt Core |
| Viz | Power BI |
| Linguagens | SQL, Python |

---

## 📌 Considerações Finais

O objetivo do projeto foi demonstrar:

- capacidade de estruturar um pipeline analítico completo  
- organização de dados em camadas  
- utilização de boas práticas de modelagem dimensional  
- criação de um dashboard claro e guiado por indicadores  

---

## 📞 Contato

**Felipe Mata**  
Disponível para esclarecimentos sobre decisões técnicas, modelagem ou execução do projeto.
