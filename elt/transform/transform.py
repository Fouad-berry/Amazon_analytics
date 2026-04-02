"""
ELT — TRANSFORM
Nettoyage, typage, enrichissement et feature engineering sur raw_orders.
Produit la table processed_orders prête pour les marts.
"""

import pandas as pd
import sqlite3
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TRANSFORM] %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path("data/amazon_analytics.db")


def load_raw() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM raw_orders", conn)
    conn.close()
    log.info(f"{len(df):,} lignes chargées depuis raw_orders")
    return df


# ── Nettoyage ──────────────────────────────────────────────────────────────

def clean(df: pd.DataFrame) -> pd.DataFrame:
    log.info("--- Nettoyage ---")
    n0 = len(df)

    # Supprimer doublons sur order_id
    df = df.drop_duplicates(subset=["order_id"])
    log.info(f"Doublons supprimés : {n0 - len(df)}")

    # Supprimer lignes avec colonnes critiques nulles
    critical = ["order_date", "product_category", "price", "total_revenue", "quantity_sold"]
    df = df.dropna(subset=critical)

    # Valeurs aberrantes
    df = df[df["price"]          > 0]
    df = df[df["total_revenue"]  > 0]
    df = df[df["quantity_sold"]  > 0]
    df = df[df["discount_percent"].between(0, 100)]
    df = df[df["rating"].between(0, 5)]

    # Harmoniser les chaînes
    df["product_category"] = df["product_category"].str.strip()
    df["customer_region"]  = df["customer_region"].str.strip()
    df["payment_method"]   = df["payment_method"].str.strip()

    log.info(f"Lignes après nettoyage : {len(df):,} (supprimées : {n0 - len(df)})")
    return df


# ── Enrichissement ─────────────────────────────────────────────────────────

def enrich(df: pd.DataFrame) -> pd.DataFrame:
    log.info("--- Enrichissement ---")

    # Typage dates
    df["order_date"] = pd.to_datetime(df["order_date"])

    # Colonnes temporelles
    df["year"]         = df["order_date"].dt.year
    df["month"]        = df["order_date"].dt.month
    df["month_name"]   = df["order_date"].dt.strftime("%B")
    df["quarter"]      = df["order_date"].dt.quarter.map({1:"Q1",2:"Q2",3:"Q3",4:"Q4"})
    df["week"]         = df["order_date"].dt.isocalendar().week.astype(int)
    df["day_of_week"]  = df["order_date"].dt.day_name()
    df["is_weekend"]   = df["order_date"].dt.dayofweek >= 5

    # Métriques dérivées
    df["revenue_per_unit"]   = (df["total_revenue"] / df["quantity_sold"]).round(2)
    df["discount_amount"]    = (df["price"] * df["discount_percent"] / 100).round(2)
    df["has_discount"]       = df["discount_percent"] > 0
    df["effective_margin"]   = (1 - df["discount_percent"] / 100).round(4)

    # Vérification cohérence discounted_price
    computed_disc = (df["price"] * (1 - df["discount_percent"] / 100)).round(2)
    df["price_coherent"] = (abs(computed_disc - df["discounted_price"]) < 0.05)

    # Segmentation prix (quintiles)
    df["price_segment"] = pd.qcut(
        df["price"],
        q=5,
        labels=["Very Low", "Low", "Medium", "High", "Very High"],
        duplicates="drop",
    )

    # Segmentation discount
    df["discount_segment"] = pd.cut(
        df["discount_percent"],
        bins=[-1, 0, 10, 20, 30, 100],
        labels=["No discount", "1-10%", "11-20%", "21-30%", "30%+"],
    )

    # Segmentation rating
    df["rating_segment"] = pd.cut(
        df["rating"],
        bins=[0, 2, 3, 4, 5],
        labels=["Poor (0-2)", "Average (2-3)", "Good (3-4)", "Excellent (4-5)"],
        include_lowest=True,
    )

    # Segmentation volume
    df["volume_segment"] = pd.cut(
        df["quantity_sold"],
        bins=[0, 1, 2, 3, 5, 100],
        labels=["1 unit", "2 units", "3 units", "4-5 units", "6+ units"],
    )

    # Flag top revenue (top 20%)
    q80 = df["total_revenue"].quantile(0.80)
    df["is_top_revenue"] = df["total_revenue"] >= q80

    # Flag high rating
    df["is_high_rating"] = df["rating"] >= 4.0

    log.info("Enrichissement terminé")
    return df


def save_processed(df: pd.DataFrame) -> None:
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("processed_orders", conn, if_exists="replace", index=False)
    df.to_csv("data/processed/amazon_sales_processed.csv", index=False)
    log.info(f"{len(df):,} lignes → processed_orders + CSV")
    conn.close()


def run() -> pd.DataFrame:
    log.info("=== TRANSFORM START ===")
    df = load_raw()
    df = clean(df)
    df = enrich(df)
    save_processed(df)
    log.info("=== TRANSFORM DONE ===")
    return df


if __name__ == "__main__":
    run()