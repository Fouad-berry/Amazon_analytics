"""
analysis/data_quality.py — Rapport de qualité des données
Génère un rapport complet sur la qualité du dataset raw et processed.
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("data/amazon_analytics.db")


def sep(t): print(f"\n{'═'*60}\n  {t}\n{'═'*60}")


def report_raw():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM raw_orders", conn)
    conn.close()

    sep("RAPPORT QUALITÉ — RAW")
    print(f"Lignes totales     : {len(df):,}")
    print(f"Colonnes           : {df.shape[1]}")
    print(f"\nValeurs manquantes :")
    null_report = df.isnull().sum()
    for col, count in null_report[null_report > 0].items():
        pct = count / len(df) * 100
        print(f"  {col:25s} : {count:,} ({pct:.2f}%)")
    if null_report.sum() == 0:
        print("  Aucune valeur manquante ✓")

    print(f"\nDoublons (order_id) : {df.duplicated(subset=['order_id']).sum():,}")
    print(f"\nPrix négatifs       : {(df['price'] < 0).sum():,}")
    print(f"Revenue négatif     : {(df['total_revenue'] < 0).sum():,}")
    print(f"Rating hors [0,5]   : {(~df['rating'].between(0,5)).sum():,}")
    print(f"Discount hors [0,100]: {(~df['discount_percent'].between(0,100)).sum():,}")


def report_processed():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM processed_orders", conn)
    conn.close()

    sep("RAPPORT QUALITÉ — PROCESSED")
    print(f"Lignes totales    : {len(df):,}")
    print(f"Colonnes          : {df.shape[1]}")
    print(f"\nValeurs manquantes (colonnes critiques) :")
    critical = ["order_date","product_category","price","total_revenue","quantity_sold","rating"]
    for col in critical:
        nulls = df[col].isnull().sum()
        status = "✓" if nulls == 0 else f"⚠ {nulls}"
        print(f"  {col:25s} : {status}")

    print(f"\nDistribution catégories :")
    for cat, cnt in df["product_category"].value_counts().items():
        print(f"  {cat:20s} : {cnt:,} ({cnt/len(df)*100:.1f}%)")

    print(f"\nDistribution régions :")
    for reg, cnt in df["customer_region"].value_counts().items():
        print(f"  {reg:20s} : {cnt:,} ({cnt/len(df)*100:.1f}%)")

    sep("STATS CLÉS")
    print(f"Revenue total     : ${df['total_revenue'].sum():,.2f}")
    print(f"Revenue moyen     : ${df['total_revenue'].mean():.2f}")
    print(f"Prix moyen        : ${df['price'].mean():.2f}")
    print(f"Remise moyenne    : {df['discount_percent'].mean():.1f}%")
    print(f"Rating moyen      : {df['rating'].mean():.2f}")
    print(f"Produits uniques  : {df['product_id'].nunique():,}")
    print(f"Période           : {df['order_date'].min()} → {df['order_date'].max()}")
    print(f"\nRapport généré le : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    report_raw()
    report_processed()