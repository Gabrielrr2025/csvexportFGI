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
    fgi = pd.DataFrame(rows).drop_duplicates(subset=["date"]).sort_values("date").set_index("date")
    return fgi

def update_local_csv(new_data: pd.DataFrame, csv_file: str = CSV_FILE):
    """Atualiza CSV local com novos dados"""
    if os.path.exists(csv_file):
        old = pd.read_csv(csv_file, parse_dates=["date"]).set_index("date")
        combined = pd.concat([old, new_data]).drop_duplicates().sort_index()
    else:
        combined = new_data
    combined.to_csv(csv_file, date_format="%Y-%m-%d")
    print(f"✅ CSV atualizado: {csv_file} ({len(combined)} linhas)")

if __name__ == "__main__":
    try:
        fgi = fetch_fgi_api()
        if fgi.empty:
            print("⚠️ API não retornou dados.")
        else:
            update_local_csv(fgi)
    except Exception as e:
        print(f"❌ Erro ao baixar dados: {e}")
