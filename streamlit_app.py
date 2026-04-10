"""
EVO Cohort — Dashboard de Churn
Streamlit Cloud — fica online 24/7
Lê CSVs de data/current/ (gerados pelo GitHub Actions)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

# =====================================================================
# Configuração da página
# =====================================================================
st.set_page_config(
    page_title="Painel de Churn — EVO Cohort",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# CSS customizado
st.markdown("""
<style>
    .block-container { padding: 1rem 2rem; }
    [data-testid="stMetric"] {
        background: #F7F6F3;
        border-radius: 10px;
        padding: 12px 16px;
    }
    [data-testid="stMetricLabel"] { font-size: 12px; }
    [data-testid="stMetricValue"] { font-size: 24px; font-weight: 700; }
    .stTabs [data-baseweb="tab"] { font-size: 14px; }
</style>
""", unsafe_allow_html=True)

MESES_PT = {
    "01": "Jan", "02": "Fev", "03": "Mar", "04": "Abr",
    "05": "Mai", "06": "Jun", "07": "Jul", "08": "Ago",
    "09": "Set", "10": "Out", "11": "Nov", "12": "Dez",
}
MESES_FULL = {
    "01": "Janeiro", "02": "Fevereiro", "03": "Março", "04": "Abril",
    "05": "Maio", "06": "Junho", "07": "Julho", "08": "Agosto",
    "09": "Setembro", "10": "Outubro", "11": "Novembro", "12": "Dezembro",
}

DATA_DIR = Path("data/current")


# =====================================================================
# Carregar dados
# =====================================================================
@st.cache_data(ttl=3600)
def load_data():
    """Carrega os CSVs da pasta data/current/."""
    d = {}
    files = {
        "contratos": "fato_contratos.csv",
        "planos": "dim_planos.csv",
        "filiais": "dim_filiais.csv",
        "categorias": "dim_categorias.csv",
        "metricas": "metricas_mensais.csv",
        "motivos": "motivos_cancelamento.csv",
        "churn_filial": "churn_por_filial.csv",
        "churn_plano": "churn_por_plano.csv",
    }
    for key, fname in files.items():
        path = DATA_DIR / fname
        if path.exists():
            d[key] = pd.read_csv(path)
        else:
            d[key] = pd.DataFrame()

    return d


def safe_delta(current, previous):
    """Calcula variação percentual segura."""
    if previous and previous != 0:
        pct = (current - previous) / previous * 100
        return f"{pct:+.1f}%"
    return None


# =====================================================================
# App principal
# =====================================================================
def main():
    data = load_data()
    df = data["contratos"]

    if df.empty:
        st.error("⚠️ Nenhum dado encontrado em `data/current/`. Execute a extração primeiro.")
        st.code("python extract_daily.py", language="bash")
        st.info("Ou aguarde a execução automática do GitHub Actions.")
        st.stop()

    # ---- Header ----
    col_title, col_badge = st.columns([4, 1])
    with col_title:
        st.markdown("## 📊 Painel de Churn — EVO Cohort")
    with col_badge:
        last_update = DATA_DIR / ".last_update"
        if last_update.exists():
            ts = last_update.read_text().strip()
            st.caption(f"Atualizado: {ts}")
        else:
            st.caption("Dados locais")

    # ---- Filtros na sidebar ----
    with st.sidebar:
        st.markdown("### Filtros")

        anos = sorted(df["ano_entrada"].dropna().unique())
        ano_sel = st.multiselect("Ano", anos, default=anos)

        filiais = sorted(df["nome_filial"].dropna().unique())
        filial_sel = st.multiselect("Filial", filiais, default=[])

        planos = sorted(df["nome_plano"].dropna().unique())
        plano_sel = st.multiselect("Plano", planos, default=[])

        status_sel = st.radio("Status", ["Todos", "Ativo", "Cancelado"], horizontal=True)

    # ---- Aplicar filtros ----
    df_f = df.copy()
    if ano_sel:
        df_f = df_f[df_f["ano_entrada"].isin([str(a) for a in ano_sel])]
    if filial_sel:
        df_f = df_f[df_f["nome_filial"].isin(filial_sel)]
    if plano_sel:
        df_f = df_f[df_f["nome_plano"].isin(plano_sel)]
    if status_sel == "Ativo":
        df_f = df_f[df_f["status_id"] == 1]
    elif status_sel == "Cancelado":
        df_f = df_f[df_f["status_id"] == 2]

    # ---- KPIs ----
    total = len(df_f)
    ativos = len(df_f[df_f["status_id"] == 1])
    cancelados = len(df_f[df_f["status_id"] == 2])
    churn_rate = round(cancelados / max(total, 1) * 100, 1)
    ticket = df_f["valor_venda"].mean() if "valor_venda" in df_f.columns else 0
    receita = df_f.loc[df_f["status_id"] == 1, "valor_venda"].sum() if "valor_venda" in df_f.columns else 0

    # Tempo médio até cancelamento
    if "dias_ate_cancelamento" in df_f.columns:
        dias_cancel = df_f.loc[
            (df_f["status_id"] == 2) & (df_f["dias_ate_cancelamento"].notna()),
            "dias_ate_cancelamento"
        ].mean()
    else:
        dias_cancel = 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Base ativa", f"{ativos:,.0f}".replace(",", "."))
    c2.metric("Total contratos", f"{total:,.0f}".replace(",", "."))
    c3.metric("Cancelamentos", f"{cancelados:,.0f}".replace(",", "."))
    c4.metric("Taxa de churn", f"{churn_rate}%")
    c5.metric("Ticket médio", f"R$ {ticket:,.0f}".replace(",", ".") if ticket else "R$ 0")
    c6.metric("Dias até cancel.", f"{dias_cancel:.0f}d" if dias_cancel else "-")

    st.divider()

    # ---- Tabs ----
    tab1, tab2, tab3 = st.tabs(["📈 Visão geral", "🔍 Detalhamento", "📊 Comparativo YoY"])

    # ==================== TAB 1: Visão Geral ====================
    with tab1:
        col_chart, col_churn = st.columns([2, 1])

        with col_chart:
            st.markdown("##### Entradas vs cancelamentos por mês")
            # Agrupar por mês
            entradas = df_f[df_f["mes_entrada"].notna()].groupby("mes_entrada").size().reset_index(name="Entradas")
            cancelados_m = df_f[
                (df_f["mes_cancelamento"].notna()) & (df_f["status_id"] == 2)
            ].groupby("mes_cancelamento").size().reset_index(name="Cancelamentos")
            cancelados_m.columns = ["mes_entrada", "Cancelamentos"]

            chart_df = pd.merge(entradas, cancelados_m, on="mes_entrada", how="outer").fillna(0)
            chart_df = chart_df.sort_values("mes_entrada")
            chart_df["label"] = chart_df["mes_entrada"].apply(
                lambda x: f"{MESES_PT.get(x[5:7], x[5:7])}/{x[2:4]}" if isinstance(x, str) and len(x) >= 7 else x
            )

            fig = go.Figure()
            fig.add_bar(x=chart_df["label"], y=chart_df["Entradas"], name="Entradas", marker_color="#378ADD")
            fig.add_bar(x=chart_df["label"], y=chart_df["Cancelamentos"], name="Cancelamentos", marker_color="#E24B4A")
            fig.update_layout(
                barmode="group", height=360, margin=dict(t=10, b=40, l=40, r=20),
                legend=dict(orientation="h", y=-0.15), plot_bgcolor="#fff",
                xaxis=dict(gridcolor="#f0f0ec"), yaxis=dict(gridcolor="#f0f0ec"),
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_churn:
            st.markdown("##### Saldo líquido mensal")
            if not chart_df.empty:
                chart_df["Saldo"] = chart_df["Entradas"] - chart_df["Cancelamentos"]
                colors = ["#1D9E75" if v >= 0 else "#E24B4A" for v in chart_df["Saldo"]]
                fig2 = go.Figure(go.Bar(
                    x=chart_df["label"], y=chart_df["Saldo"],
                    marker_color=colors
                ))
                fig2.update_layout(
                    height=360, margin=dict(t=10, b=40, l=40, r=20),
                    plot_bgcolor="#fff", showlegend=False,
                    xaxis=dict(gridcolor="#f0f0ec"), yaxis=dict(gridcolor="#f0f0ec"),
                )
                st.plotly_chart(fig2, use_container_width=True)

    # ==================== TAB 2: Detalhamento ====================
    with tab2:
        col_f, col_p, col_m = st.columns(3)

        with col_f:
            st.markdown("##### Cancelamentos por filial")
            df_canc = df_f[df_f["status_id"] == 2]
            if not df_canc.empty:
                top_filial = df_canc.groupby("nome_filial").size().nlargest(8).reset_index(name="total")
                fig_f = px.bar(top_filial, y="nome_filial", x="total", orientation="h",
                               color_discrete_sequence=["#E24B4A"])
                fig_f.update_layout(height=320, margin=dict(t=10, b=10, l=10, r=10),
                                    yaxis=dict(title=""), xaxis=dict(title=""), showlegend=False)
                st.plotly_chart(fig_f, use_container_width=True)

        with col_p:
            st.markdown("##### Cancelamentos por plano")
            if not df_canc.empty:
                top_plano = df_canc.groupby("nome_plano").size().nlargest(8).reset_index(name="total")
                fig_p = px.bar(top_plano, y="nome_plano", x="total", orientation="h",
                               color_discrete_sequence=["#7F77DD"])
                fig_p.update_layout(height=320, margin=dict(t=10, b=10, l=10, r=10),
                                    yaxis=dict(title=""), xaxis=dict(title=""), showlegend=False)
                st.plotly_chart(fig_p, use_container_width=True)

        with col_m:
            st.markdown("##### Motivos de cancelamento")
            df_mot = data["motivos"]
            if not df_mot.empty:
                fig_m = px.bar(df_mot.head(8), y="motivo", x="percentual", orientation="h",
                               color_discrete_sequence=["#BA7517"])
                fig_m.update_layout(height=320, margin=dict(t=10, b=10, l=10, r=10),
                                    yaxis=dict(title=""), xaxis=dict(title="% dos cancelamentos"), showlegend=False)
                st.plotly_chart(fig_m, use_container_width=True)

    # ==================== TAB 3: Comparativo YoY ====================
    with tab3:
        st.markdown("##### Comparativo 2025 vs 2026 — meses encerrados")
        now_ym = datetime.now().strftime("%Y-%m")

        # Entradas por mês e ano
        df_ent = df_f[df_f["mes_entrada"].notna()].copy()
        df_ent["ano"] = df_ent["mes_entrada"].str[:4]
        df_ent["num_mes"] = df_ent["mes_entrada"].str[5:7]

        comp_e = df_ent.groupby(["ano", "num_mes"]).size().reset_index(name="entradas")

        # Cancelamentos por mês e ano
        df_can = df_f[(df_f["mes_cancelamento"].notna()) & (df_f["status_id"] == 2)].copy()
        df_can["ano"] = df_can["mes_cancelamento"].str[:4]
        df_can["num_mes"] = df_can["mes_cancelamento"].str[5:7]

        comp_c = df_can.groupby(["ano", "num_mes"]).size().reset_index(name="cancelamentos")

        # Pivotar
        e_pivot = comp_e.pivot(index="num_mes", columns="ano", values="entradas").fillna(0)
        c_pivot = comp_c.pivot(index="num_mes", columns="ano", values="cancelamentos").fillna(0)

        rows = []
        for nm in sorted(set(e_pivot.index) | set(c_pivot.index)):
            e25 = int(e_pivot.loc[nm, "2025"]) if nm in e_pivot.index and "2025" in e_pivot.columns else 0
            e26 = int(e_pivot.loc[nm, "2026"]) if nm in e_pivot.index and "2026" in e_pivot.columns else 0
            c25 = int(c_pivot.loc[nm, "2025"]) if nm in c_pivot.index and "2025" in c_pivot.columns else 0
            c26 = int(c_pivot.loc[nm, "2026"]) if nm in c_pivot.index and "2026" in c_pivot.columns else 0

            var_e = round((e26 - e25) / max(e25, 1) * 100, 1) if e25 else None
            var_c = round((c26 - c25) / max(c25, 1) * 100, 1) if c25 else None
            ch25 = round(c25 / max(e25, 1) * 100, 1) if e25 else 0
            ch26 = round(c26 / max(e26, 1) * 100, 1) if e26 else 0

            rows.append({
                "Mês": MESES_FULL.get(nm, nm),
                "Entradas 25": e25, "Entradas 26": e26,
                "Var. Entradas": f"{var_e:+.1f}%" if var_e is not None else "-",
                "Cancel. 25": c25, "Cancel. 26": c26,
                "Var. Cancel.": f"{var_c:+.1f}%" if var_c is not None else "-",
                "Churn 25": f"{ch25}%", "Churn 26": f"{ch26}%",
            })

        if rows:
            comp_df = pd.DataFrame(rows)
            st.dataframe(comp_df, use_container_width=True, hide_index=True)

            # Gráfico comparativo
            st.markdown("##### Gráfico comparativo — entradas")
            comp_chart = pd.DataFrame(rows)
            fig_comp = go.Figure()
            fig_comp.add_bar(x=comp_chart["Mês"], y=comp_chart["Entradas 25"], name="2025", marker_color="#85B7EB")
            fig_comp.add_bar(x=comp_chart["Mês"], y=comp_chart["Entradas 26"], name="2026", marker_color="#378ADD")
            fig_comp.update_layout(
                barmode="group", height=300, margin=dict(t=10, b=40, l=40, r=20),
                legend=dict(orientation="h", y=-0.2), plot_bgcolor="#fff",
            )
            st.plotly_chart(fig_comp, use_container_width=True)


if __name__ == "__main__":
    main()
