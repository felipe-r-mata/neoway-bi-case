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


O arquivo `BI-Neoway.pbix` contém uma **landing page** de navegação e **3 dashboards** principais:

---

### 🔵 Landing Page – Menu de Navegação

- Tela inicial com identidade visual da Neoway / B3  
- Botões para acesso direto aos painéis:
  - **Visão Geral de Mercado & Desempenho das Empresas**
  - **Segmentação & Perfil das Empresas**
  - **Desempenho Financeiro por Perfil de Empresa**

---

### 1) Visão Geral de Mercado & Desempenho das Empresas

Foco na visão macro do mercado de ações no período analisado.

**Principais KPIs**

- Preço máximo do período  
- Preço do último fechamento  
- Volume total negociado  
- Quantidade de negócios  
- Volatilidade % (dispersão dos retornos)

**Principais visualizações**

- **Variação % de Preço por Ticker** (Top N ativos)
- **Retorno Diário %** ao longo do tempo (com troca de granularidade: Ano, Mês/Ano, Mês, Trimestre, Dia)
- **Preço de Fechamento Médio** por período

**Filtros**

- Ano  
- Código da ação (código B3)

---

### 2) Segmentação & Perfil das Empresas

Painel voltado para a visão demográfica e estrutural do ecossistema empresarial.

**Principais visualizações**

- **Empresas por Estado** – Mapa com concentração geográfica  
- **Distribuição Regional (%)** – participação das regiões no total de empresas  
- **Top 10 Ramos de Atividade** em número de empresas

**Filtros**

- Região  
- Município  
- Faixa de **data de abertura** (slider de 1891 a 2022)

Esse painel permite identificar:
- concentração geográfica,
- maturidade econômica,
- oportunidades por ramo de atividade.

---

### 3) Desempenho Financeiro por Perfil de Empresa

Conecta métricas de mercado ao perfil das empresas em diferentes dimensões.

**Principais KPIs**

- Preço de fechamento médio  
- Preço do último fechamento  
- Volume total negociado  
- Quantidade de empresas associadas  
- Volatilidade %

**Principais visualizações**

- **Preço Médio por Ramo de Atividade**  
- **Preço Médio** por:
  - Região  
  - Porte  
  - Saúde Tributária  
  - Nível de Atividade  
  (seleção via botão/segmentador no topo do gráfico)
- **Volume vs Preço Médio** por segmento (gráfico combinado: colunas + linha)

**Filtros**

- Ano  
- Código da ação (código B3)  
- Região / Município  
- Faixa de data de abertura

Valores sem informação em porte, saúde tributária ou nível de atividade são explicitamente agrupados na categoria **"NÃO INFORMADO"**, preservando a integridade do volume de análise e deixando clara a limitação da base original.

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
