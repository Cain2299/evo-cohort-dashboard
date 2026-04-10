"""
EVO Cohort — Extração diária automática.
Roda via GitHub Actions ou manualmente.
Puxa dados da EVO API e salva CSVs organizados.

Uso manual:
    python extract_daily.py

Via GitHub Actions: roda automaticamente todo dia.
"""

import os
import sys
import json
import time
import sqlite3
import logging
import requests
from pathlib import Path
from datetime import datetime
from requests.auth import HTTPBasicAuth

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# =====================================================================
# Configuração — lê de variáveis de ambiente (GitHub Actions secrets)
# ou de .streamlit/secrets.toml (local)
# =====================================================================
def get_credentials():
    """Busca credenciais de env vars ou secrets.toml."""
    username = os.environ.get("EVO_USERNAME")
    password = os.environ.get("EVO_PASSWORD")

    if not username:
        # Tenta ler do secrets.toml
        secrets_path = Path(".streamlit/secrets.toml")
        if secrets_path.exists():
            text = secrets_path.read_text()
            for line in text.splitlines():
                if "EVO_USERNAME" in line and "=" in line:
                    username = line.split("=", 1)[1].strip().strip('"').strip("'")
                elif "EVO_PASSWORD" in line and "=" in line:
                    password = line.split("=", 1)[1].strip().strip('"').strip("'")

    if not username or not password:
        log.error("Credenciais EVO não encontradas. Configure EVO_USERNAME e EVO_PASSWORD.")
        sys.exit(1)

    return username, password


BASE_URL = "https://evo-integracao-api.w12app.com.br"
CURRENT_DIR = Path("data/current")
MONTHLY_DIR = Path("data/monthly")
DELAY = 0.5
MAX_RETRIES = 3


# =====================================================================
# Cliente HTTP
# =====================================================================
class EvoClient:
    def __init__(self, username, password):
        self.auth = HTTPBasicAuth(username, password)

    def get(self, endpoint, params=None):
        url = f"{BASE_URL}{endpoint}"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, params=params, auth=self.auth, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                log.warning("HTTP %s em %s (tentativa %d/%d)", status, endpoint, attempt, MAX_RETRIES)
                if status == 429:
                    time.sleep(10 * attempt)
                elif status >= 500:
                    time.sleep(2 ** attempt)
                else:
                    raise
            except requests.exceptions.RequestException as e:
                log.warning("Erro conexão %s (tentativa %d/%d): %s", endpoint, attempt, MAX_RETRIES, e)
                time.sleep(2 ** attempt)
        raise RuntimeError(f"Falha após {MAX_RETRIES} tentativas em {endpoint}")

    def get_paginated(self, endpoint, params, take=25, label=""):
        all_records = []
        skip = 0
        page = 1
        while True:
            p = {**params, "take": take, "skip": skip}
            log.info("[%s] pag %d (skip=%d)...", label, page, skip)
            resp = self.get(endpoint, p)
            data = resp.json()
            records = data if isinstance(data, list) else []
            if not records:
                break
            all_records.extend(records)
            log.info("[%s] +%d (total: %d)", label, len(records), len(all_records))
            if len(records) < take:
                break
            skip += take
            page += 1
            time.sleep(DELAY)
        return all_records

    def get_simple(self, endpoint, label=""):
        log.info("[%s] extraindo...", label)
        resp = self.get(endpoint)
        data = resp.json()
        return data if isinstance(data, list) else [data] if data else []


# =====================================================================
# Extração dos endpoints
# =====================================================================
def extract_all(client):
    start = "2025-01-01"
    end = datetime.now().strftime("%Y-%m-%d")
    results = {}

    # === FATO: MemberMembership (ativos) ===
    active = client.get_paginated(
        "/api/v3/membermembership",
        {"statusMemberMembership": 1, "registerDateStart": f"{start}T00:00:00Z", "registerDateEnd": f"{end}T23:59:59Z"},
        take=25, label="MM-Ativos"
    )

    # === FATO: MemberMembership (cancelados) ===
    canceled = client.get_paginated(
        "/api/v3/membermembership",
        {"statusMemberMembership": 2, "cancelDateStart": f"{start}T00:00:00Z", "cancelDateEnd": f"{end}T23:59:59Z"},
        take=25, label="MM-Cancelados"
    )

    for r in active:
        r["_status_label"] = "ativo"
    for r in canceled:
        r["_status_label"] = "cancelado"
    results["membermembership"] = active + canceled

    # === FATO: Sales ===
    results["sales"] = client.get_paginated(
        "/api/v2/sales",
        {"dateSaleStart": start, "dateSaleEnd": end, "showReceivables": "false"},
        take=100, label="Sales"
    )

    # === DIMENSÕES ===
    results["memberships"] = client.get_simple("/api/v1/membership", "Memberships")
    results["branches"] = client.get_simple("/api/v1/configuration", "Branches")

    try:
        results["categories"] = client.get_simple("/api/v1/membership/category", "Categories")
    except Exception as e:
        log.warning("Categories falhou: %s", e)
        results["categories"] = []

    return results


# =====================================================================
# Transformação e salvamento
# =====================================================================
def extract_month(date_str):
    if not date_str:
        return None
    try:
        return date_str[:7]
    except (TypeError, IndexError):
        return None


def safe(record, key, default=None):
    v = record.get(key, default)
    return v if v is not None else default


def build_fato_contratos(records, memberships, branches, categories):
    """Constrói DataFrame da tabela fato principal."""
    m_map = {r.get("idMembership"): r.get("name") for r in memberships}
    b_map = {r.get("idBranch", r.get("id")): r.get("name", r.get("branchName")) for r in branches}
    c_map = {r.get("idCategoryMembership", r.get("id")): r.get("name") for r in categories}

    rows = []
    for r in records:
        ms = safe(r, "membershipStart")
        cd = safe(r, "cancelDate")
        entry_m = extract_month(ms)
        cancel_m = extract_month(cd)

        # Dias até cancelamento
        dias = None
        meses = None
        if ms and cd:
            try:
                d1 = datetime.fromisoformat(ms[:10])
                d2 = datetime.fromisoformat(cd[:10])
                dias = (d2 - d1).days
                meses = dias // 30
            except (ValueError, TypeError):
                pass

        id_m = safe(r, "idMembership")
        id_b = safe(r, "idBranch")
        id_c = safe(r, "idMembershipCategory")

        rows.append({
            "id_member_membership": safe(r, "idMemberMemberShip"),
            "id_member": safe(r, "idMember"),
            "nome_membro": safe(r, "name"),
            "id_membership": id_m,
            "nome_plano": m_map.get(id_m, safe(r, "nameMembership", "Sem plano")),
            "id_branch": id_b,
            "nome_filial": b_map.get(id_b, f"Filial {id_b}"),
            "id_membership_category": id_c,
            "nome_categoria": c_map.get(id_c, "Sem categoria"),
            "valor_venda": safe(r, "saleValue"),
            "data_inicio": ms,
            "data_fim": safe(r, "membershipEnd"),
            "data_venda": safe(r, "saleDate"),
            "data_cancelamento": cd,
            "motivo_cancelamento": safe(r, "reasonCancellation"),
            "status_id": safe(r, "statusMemberMembership"),
            "status": "Ativo" if safe(r, "statusMemberMembership") == 1 else "Cancelado",
            "mes_entrada": entry_m,
            "mes_cancelamento": cancel_m,
            "ano_entrada": entry_m[:4] if entry_m else None,
            "num_mes_entrada": entry_m[5:7] if entry_m else None,
            "dias_ate_cancelamento": dias,
            "meses_ate_cancelamento": meses,
            "contract_type": safe(r, "contractType"),
        })
    return pd.DataFrame(rows)


def build_metricas_mensais(df):
    """Constrói tabela de métricas mensais."""
    entradas = df[df["mes_entrada"].notna()].groupby("mes_entrada").agg(
        entradas=("id_member_membership", "count"),
        membros_novos=("id_member", "nunique"),
        receita=("valor_venda", "sum"),
        ticket_medio=("valor_venda", "mean"),
    ).reset_index().rename(columns={"mes_entrada": "mes"})

    cancelamentos = df[
        (df["mes_cancelamento"].notna()) & (df["status_id"] == 2)
    ].groupby("mes_cancelamento").agg(
        cancelamentos=("id_member_membership", "count"),
        membros_cancelados=("id_member", "nunique"),
    ).reset_index().rename(columns={"mes_cancelamento": "mes"})

    metricas = pd.merge(entradas, cancelamentos, on="mes", how="outer").fillna(0)
    metricas = metricas.sort_values("mes")
    metricas["ano"] = metricas["mes"].str[:4]
    metricas["mes_num"] = metricas["mes"].str[5:7]
    metricas["saldo_liquido"] = metricas["entradas"] - metricas["cancelamentos"]
    metricas["taxa_churn_pct"] = (metricas["cancelamentos"] / metricas["entradas"].replace(0, 1) * 100).round(1)
    return metricas


def build_motivos(df):
    total = len(df[df["status_id"] == 2])
    if total == 0:
        return pd.DataFrame(columns=["motivo", "quantidade", "percentual"])
    mot = df[df["status_id"] == 2].groupby(
        df["motivo_cancelamento"].fillna("Não informado")
    ).size().reset_index(name="quantidade")
    mot.columns = ["motivo", "quantidade"]
    mot["percentual"] = (mot["quantidade"] / total * 100).round(1)
    return mot.sort_values("quantidade", ascending=False)


def build_churn_filial(df):
    return df[
        (df["status_id"] == 2) & (df["mes_cancelamento"].notna())
    ].groupby(["mes_cancelamento", "nome_filial"]).size().reset_index(name="cancelamentos").rename(
        columns={"mes_cancelamento": "mes", "nome_filial": "nome_filial"}
    ).sort_values(["mes", "cancelamentos"], ascending=[True, False])


def build_churn_plano(df):
    return df[
        (df["status_id"] == 2) & (df["mes_cancelamento"].notna())
    ].groupby(["mes_cancelamento", "nome_plano"]).size().reset_index(name="cancelamentos").rename(
        columns={"mes_cancelamento": "mes"}
    ).sort_values(["mes", "cancelamentos"], ascending=[True, False])


def save_csvs(df, metricas, motivos, churn_fil, churn_pla, memberships_raw, branches_raw, categories_raw):
    """Salva todos os CSVs em data/current/ e data/monthly/YYYY-MM/."""
    CURRENT_DIR.mkdir(parents=True, exist_ok=True)

    # Current (para o Streamlit)
    df.to_csv(CURRENT_DIR / "fato_contratos.csv", index=False, encoding="utf-8-sig")
    metricas.to_csv(CURRENT_DIR / "metricas_mensais.csv", index=False, encoding="utf-8-sig")
    motivos.to_csv(CURRENT_DIR / "motivos_cancelamento.csv", index=False, encoding="utf-8-sig")
    churn_fil.to_csv(CURRENT_DIR / "churn_por_filial.csv", index=False, encoding="utf-8-sig")
    churn_pla.to_csv(CURRENT_DIR / "churn_por_plano.csv", index=False, encoding="utf-8-sig")

    # Dimensões
    pd.DataFrame(memberships_raw).to_csv(CURRENT_DIR / "dim_planos.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(branches_raw).to_csv(CURRENT_DIR / "dim_filiais.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(categories_raw).to_csv(CURRENT_DIR / "dim_categorias.csv", index=False, encoding="utf-8-sig")

    # Timestamp
    (CURRENT_DIR / ".last_update").write_text(datetime.now().strftime("%d/%m/%Y %H:%M"))

    # Monthly backup
    month_dir = MONTHLY_DIR / datetime.now().strftime("%Y-%m")
    month_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(month_dir / "fato_contratos.csv", index=False, encoding="utf-8-sig")
    metricas.to_csv(month_dir / "metricas_mensais.csv", index=False, encoding="utf-8-sig")

    log.info("CSVs salvos em %s e %s", CURRENT_DIR, month_dir)


# =====================================================================
# Main
# =====================================================================
def main():
    log.info("=" * 50)
    log.info("EVO Cohort — Extração diária")
    log.info("=" * 50)

    username, password = get_credentials()
    client = EvoClient(username, password)

    log.info("Extraindo dados da API EVO...")
    raw = extract_all(client)

    log.info("Transformando dados...")
    df = build_fato_contratos(
        raw["membermembership"],
        raw["memberships"],
        raw["branches"],
        raw["categories"],
    )
    metricas = build_metricas_mensais(df)
    motivos = build_motivos(df)
    churn_fil = build_churn_filial(df)
    churn_pla = build_churn_plano(df)

    log.info("Salvando CSVs...")
    save_csvs(df, metricas, motivos, churn_fil, churn_pla,
              raw["memberships"], raw["branches"], raw["categories"])

    log.info("Registros: %d contratos, %d meses de métricas", len(df), len(metricas))
    log.info("Extração concluída!")


if __name__ == "__main__":
    main()
