# EVO Cohort Dashboard

Dashboard de análise de churn com dados da API EVO.

## Arquitetura

- **GitHub Actions** — extração diária automática da API EVO
- **Streamlit Cloud** — dashboard online 24/7 (gratuito)
- **Databricks Free Edition** — banco de dados para análises avançadas

## Estrutura

```
├── streamlit_app.py          # Dashboard (roda no Streamlit Cloud)
├── extract_daily.py          # Extração da API EVO
├── requirements.txt          # Dependências Python
├── .github/workflows/        # Automação diária
├── .streamlit/               # Config do Streamlit
├── data/
│   ├── current/              # Dados mais recentes (CSVs)
│   └── monthly/YYYY-MM/      # Histórico mensal
└── notebooks/                # Notebooks para Databricks
```

## Setup rápido

1. Fork este repositório
2. Configure os secrets no GitHub (Settings → Secrets → Actions):
   - `EVO_USERNAME`
   - `EVO_PASSWORD`
3. Execute manualmente o workflow (Actions → Extração diária → Run workflow)
4. Deploy no Streamlit Cloud (share.streamlit.io)
