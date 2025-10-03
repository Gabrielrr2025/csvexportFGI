#!/usr/bin/env python3
"""
Exporta histórico do Fear & Greed Index (FGI) para CSV local.
Fonte oficial: https://api.alternative.me/fng/
"""

import pandas as pd
import requests
from datetime import datetime
import sys

CSV_FILE = "fear_greed.csv"

def fetch_fgi_api() -> pd.DataFrame:
    """Baixa histórico completo da API alternative.me"""
    url = "https://api.alternative.me/fng/"
    # Remove date_format para receber timestamp Unix (mais confiável)
    params = {"limit": 0, "format": "json"}
    headers = {"User-Agent": "FGI-Exporter/1.0"}

    try:
        print("📡 Conectando à API alternative.me...")
        r = requests.get(url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        payload = r.json()
        data = payload.get("data", [])
        
        if not data:
            print("⚠️ API retornou lista vazia")
            return pd.DataFrame(columns=["date", "FGI"])

        print(f"✅ API retornou {len(data)} registros")
        
        rows = []
        for d in data:
            ts = d.get("timestamp")
            val = d.get("value")
            if ts is None or val is None:
                continue
            
            try:
                # Tenta converter timestamp Unix (número inteiro)
                try:
                    ts_int = int(ts)
                    day = datetime.utcfromtimestamp(ts_int).date()
                except ValueError:
                    # Se falhar, tenta parsear string no formato "MM-DD-YYYY"
                    day = datetime.strptime(ts, "%m-%d-%Y").date()
                
                rows.append({"date": pd.to_datetime(day), "FGI": float(val)})
            except Exception as e:
                print(f"⚠️ Erro ao processar linha (ts={ts}): {e}")
                continue

        if not rows:
            print("❌ Nenhum dado válido após processamento")
            return pd.DataFrame(columns=["date", "FGI"])

        fgi = (
            pd.DataFrame(rows)
            .drop_duplicates(subset=["date"])
            .sort_values("date")
            .set_index("date")
        )
        fgi["FGI"] = fgi["FGI"].astype(float)
        
        print(f"✅ Dados processados: {len(fgi)} dias únicos")
        print(f"📅 Período: {fgi.index.min()} até {fgi.index.max()}")
        
        return fgi
        
    except requests.exceptions.Timeout:
        print("❌ ERRO: Timeout ao conectar na API (>30s)")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO na requisição HTTP: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ ERRO inesperado: {e}")
        sys.exit(1)

def update_local_csv(new_data: pd.DataFrame):
    """Sobrescreve sempre o CSV com os dados mais recentes"""
    if new_data.empty:
        print("❌ ERRO: Não há dados para salvar no CSV")
        sys.exit(1)
    else:
        new_data.to_csv(CSV_FILE, date_format="%Y-%m-%d")
        print(f"✅ CSV salvo: {CSV_FILE} ({len(new_data)} linhas)")
        
        # Validação final
        try:
            test = pd.read_csv(CSV_FILE)
            if len(test) < 100:  # Esperamos pelo menos 100 dias de histórico
                print(f"⚠️ AVISO: CSV tem apenas {len(test)} linhas (esperado: >100)")
        except Exception as e:
            print(f"❌ ERRO ao validar CSV: {e}")
            sys.exit(1)

if __name__ == "__main__":
    print("=" * 50)
    print("🚀 Iniciando exportação do Fear & Greed Index")
    print("=" * 50)
    
    fgi = fetch_fgi_api()
    update_local_csv(fgi)
    
    print("=" * 50)
    print("✅ Exportação concluída com sucesso!")
    print("=" * 50)
