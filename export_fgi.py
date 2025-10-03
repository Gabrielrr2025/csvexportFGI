#!/usr/bin/env python3
"""
Exporta histórico do Fear & Greed Index (FGI) para CSV local.
Fonte oficial: https://api.alternative.me/fng/
"""

import pandas as pd
import requests
from datetime import datetime
import os

CSV_FILE = "fear_greed.csv"

def fetch_fgi_api() -> pd.DataFrame:
    """Baixa histórico completo da API alternative.me"""
    url = "https://api.alternative.me/fng/"
    params = {"limit": 0, "format": "json", "date_format": "us"}
    headers = {"User-Agent": "FGI-Exporter/1.0"}

    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        payload = r.json()
        data = payload.get("data", [])
        rows = []
        for d in data:
            ts = d.get("timestamp")
            val = d.get("value")
            if ts is None or val is None:
                continue
            try:
                ts_int = int(ts)
                day = datetime.utcfromtimestamp(ts_int).date()
                rows.append({"date": pd.to_datetime(day), "FGI": float(val)})
            except Exception:
                continue

        if not rows:
            return pd.DataFrame(columns=["date", "FGI"])

        fgi = (
            pd.DataFrame(rows)
            .drop_duplicates(subset=["date"])
            .sort_values("date")
            .set_index("date")
        )
        fgi["FGI"] = fgi["FGI"].astype(float)
        return fgi
    except Exception as e:
        print(f"⚠️ Erro ao buscar API: {e}")
        return pd.DataFrame(columns=["date", "FGI"])

def update_local_csv(new_data: pd.DataFrame):
    """Sobrescreve sempre o CSV com os dados mais recentes"""
    if new_data.empty:
        # Garante que um CSV vazio seja criado para evitar erro no commit
        pd.DataFrame(columns=["date", "FGI"]).to_csv(CSV_FILE, index=False)
        print(f"⚠️ CSV criado vazio (sem dados disponíveis): {CSV_FILE}")
    else:
        new_data.to_csv(CSV_FILE, date_format="%Y-%m-%d")
        print(f"✅ CSV atualizado: {CSV_FILE} ({len(new_data)} linhas)")

if __name__ == "__main__":
    fgi = fetch_fgi_api()
    update_local_csv(fgi)


