# 🛒 Amazon Sales Analytics — Analytics Engineering Project

> Pipeline ELT complet sur 50 000 commandes Amazon (2022–2023).  
> Architecture en couches (Staging → Intermediate → Marts) + Dashboard Power BI.

---

## 📋 Table des matières

- [Aperçu du projet](#aperçu-du-projet)
- [Dataset](#dataset)
- [Architecture ELT](#architecture-elt)
- [Structure du projet](#structure-du-projet)
- [Installation](#installation)
- [Utilisation](#utilisation)
- [Couches de données](#couches-de-données)
- [Data Marts](#data-marts)
- [Dashboard Power BI](#dashboard-power-bi)
- [Tests](#tests)
- [Insights clés](#insights-clés)

---

## Aperçu du projet

Ce projet adopte les pratiques d'un **Analytics Engineer** :

- Pipeline **ELT** (Extract → Load → Transform) vs ETL classique
- Architecture en **3 couches** : Raw → Staging → Intermediate → Marts
- **Data Marts** orientés besoins métier, prêts pour Power BI
- **Tests unitaires** sur la qualité des données
- **Rapport qualité** automatisé

**Stack :**
```
Python · Pandas · SQLite · Power BI · pytest
```

---

## Dataset

**Fichier :** `data/raw/amazon_sales_dataset.csv` — 50 000 lignes

| Colonne | Type | Description |
|---|---|---|
| `order_id` | int | Identifiant unique de la commande |
| `order_date` | date | Date de la commande |
| `product_id` | int | Identifiant produit |
| `product_category` | str | Catégorie (Electronics, Books, Fashion, Beauty, Sports, Home & Kitchen) |
| `price` | float | Prix original (USD) |
| `discount_percent` | float | Pourcentage de remise (0–100) |
| `quantity_sold` | int | Quantité commandée |
| `customer_region` | str | Région client (North America, Asia, Europe, Middle East) |
| `payment_method` | str | Mode de paiement |
| `rating` | float | Note produit (0–5) |
| `review_count` | int | Nombre d'avis |
| `discounted_price` | float | Prix après remise |
| `total_revenue` | float | Revenue total de la commande |

---

## Architecture ELT

```
┌───────────────────────────────────────────────────────────────────────┐
│                         PIPELINE ELT                                  │
│                                                                       │
│  ┌──────────────┐    ┌─────────────────┐    ┌──────────────────────┐ │
│  │   EXTRACT    │───▶│   LOAD (raw)    │───▶│     TRANSFORM        │ │
│  │              │    │                 │    │                      │ │
│  │ CSV → pandas │    │ raw_orders      │    │ Nettoyage            │ │
│  │ 50K lignes   │    │ (SQLite)        │    │ Typage / Dates       │ │
│  └──────────────┘    └─────────────────┘    │ Enrichissement       │ │
│                                             │ Feature Engineering  │ │
│                                             └──────────┬───────────┘ │
│                                                        │             │
│                    ┌───────────────────────────────────┘             │
│                    ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │                  LOAD (3 couches)                           │     │
│  │                                                             │     │
│  │  STAGING          INTERMEDIATE         MARTS                │     │
│  │  ─────────        ─────────────        ──────               │     │
│  │  stg_orders  ──▶  int_by_product  ──▶  mart_kpis           │     │
│  │                   int_daily_sales  ──▶  mart_category       │     │
│  │                                   ──▶  mart_region          │     │
│  │                                   ──▶  mart_payment         │     │
│  │                                   ──▶  mart_monthly_trend   │     │
│  │                                   ──▶  mart_quarterly       │     │
│  │                                   ──▶  mart_category_region │     │
│  │                                   ──▶  mart_discount_...    │     │
│  │                                   ──▶  mart_rating_...      │     │
│  │                                   ──▶  mart_price_segment   │     │
│  │                                   ──▶  mart_top_products    │     │
│  └─────────────────────────────────────────────────────────────┘     │
└───────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
              ┌────────────────────────────────────────┐
              │           POWER BI DASHBOARD           │
              │                                        │
              │  Page 1 : Executive Overview           │
              │  Page 2 : Produits & Catégories        │
              │  Page 3 : Tendances Temporelles        │
              │  Page 4 : Remises & Pricing            │
              │  Page 5 : Satisfaction & Paiements     │
              └────────────────────────────────────────┘
```

---

## Structure du projet

```
amazon_analytics/
│
├── data/
│   ├── raw/
│   │   └── amazon_sales_dataset.csv          ← source (50K lignes)
│   ├── processed/
│   │   └── amazon_sales_processed.csv        ← données nettoyées (généré)
│   ├── mart/
│   │   ├── mart_kpis.csv                     ← KPIs globaux
│   │   ├── mart_category.csv                 ← par catégorie
│   │   ├── mart_region.csv                   ← par région
│   │   ├── mart_payment.csv                  ← par paiement
│   │   ├── mart_monthly_trend.csv            ← tendance mensuelle
│   │   ├── mart_quarterly.csv                ← tendance trimestrielle
│   │   ├── mart_category_region.csv          ← catégorie × région
│   │   ├── mart_discount_analysis.csv        ← analyse remises
│   │   ├── mart_rating_analysis.csv          ← analyse satisfaction
│   │   ├── mart_price_segment.csv            ← segmentation prix
│   │   ├── mart_top_products.csv             ← top 100 produits
│   │   ├── stg_orders.csv                    ← staging
│   │   ├── int_orders_by_product.csv         ← intermediate
│   │   └── int_daily_sales.csv               ← intermediate
│   └── amazon_analytics.db                   ← SQLite (généré)
│
├── elt/
│   ├── extract/
│   │   └── extract.py                        ← CSV → raw_orders
│   ├── transform/
│   │   └── transform.py                      ← nettoyage + enrichissement
│   └── load/
│       └── load.py                           ← staging + intermediate + marts
│
├── models/
│   ├── staging/                              ← (pour extension dbt future)
│   ├── intermediate/
│   └── marts/
│
├── analysis/
│   ├── eda.py                                ← 9 graphiques EDA
│   ├── data_quality.py                       ← rapport qualité données
│   └── figures/                             ← PNG générés
│
├── powerbi/
│   └── POWERBI_GUIDE.md                      ← guide DAX + architecture
│
├── tests/
│   └── test_pipeline.py                      ← tests unitaires pytest
│
├── docs/                                     ← documentation additionnelle
├── logs/                                     ← logs pipeline
├── pipeline.py                               ← orchestrateur principal
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Installation

```bash
# 1. Cloner
git clone https://github.com/ton-username/amazon-sales-analytics.git
cd amazon-sales-analytics

# 2. Environnement virtuel
python -m venv venv
source venv/bin/activate      # Mac/Linux
# venv\Scripts\activate       # Windows

# 3. Dépendances
pip install -r requirements.txt
```

---

## Utilisation

### Lancer le pipeline ELT complet

```bash
python pipeline.py
```

**Output attendu :**
```
╔══════════════════════════════════════════════════╗
║   AMAZON SALES — ANALYTICS ENGINEER PIPELINE    ║
╚══════════════════════════════════════════════════╝
[EXTRACT]   50,000 lignes extraites
[TRANSFORM] Nettoyage + enrichissement terminé
[LOAD]      11 marts créés + SQLite + CSV
PIPELINE TERMINÉ EN ~Xs
```

### Lancer chaque étape séparément

```bash
python elt/extract/extract.py
python elt/transform/transform.py
python elt/load/load.py
```

### Analyse exploratoire (9 graphiques)

```bash
python analysis/eda.py
```

### Rapport qualité des données

```bash
python analysis/data_quality.py
```

---

## Couches de données

### RAW
Données brutes chargées telles quelles depuis le CSV. Aucune transformation.

### STAGING (`stg_orders`)
Table enrichie avec toutes les colonnes calculées :
colonnes temporelles, segmentations, flags, métriques dérivées.

### INTERMEDIATE
- `int_orders_by_product` — agrégation par produit
- `int_daily_sales` — agrégation journalière avec cumul

### MARTS
Tables finales agrégées, optimisées pour Power BI.

---

## Data Marts

| Mart | Description | Lignes |
|---|---|---|
| `mart_kpis` | KPIs globaux (revenue, orders, avg...) | 1 |
| `mart_category` | Revenue + stats par catégorie | 6 |
| `mart_region` | Revenue + stats par région | 4 |
| `mart_payment` | Revenue par méthode de paiement | 5 |
| `mart_monthly_trend` | Tendance mensuelle + MoM growth | ~24 |
| `mart_quarterly` | Tendance trimestrielle + QoQ | ~8 |
| `mart_category_region` | Croisement catégorie × région | ~24 |
| `mart_discount_analysis` | Impact remises sur revenue | 5 |
| `mart_rating_analysis` | Satisfaction par segment note | 4 |
| `mart_price_segment` | Revenue par segment de prix | 5 |
| `mart_top_products` | Top 100 produits par revenue | 100 |

---

## Dashboard Power BI

Voir [`powerbi/POWERBI_GUIDE.md`](powerbi/POWERBI_GUIDE.md) pour :
- Import des données step-by-step
- Modèle de données et relations
- Mesures DAX complètes
- Architecture des 5 pages
- Palette Amazon officielle

---

## Tests

```bash
# Tous les tests
pytest tests/ -v

# Avec rapport détaillé
pytest tests/ -v --tb=short
```

**Couverture :**
- Suppression des doublons
- Filtrage prix/revenue négatifs
- Validation rating et discount
- Création des colonnes temporelles
- Cohérence des segments calculés
- Qualité des données critiques

---

## Insights clés

Générés automatiquement par `analysis/eda.py` après exécution du pipeline :

- **Revenue total** sur 50 000 commandes
- **Catégorie dominante** par chiffre d'affaires
- **Région la plus active** par volume de commandes
- **Impact des remises** sur le volume vendu
- **Corrélation rating / revenue** par catégorie
- **Saisonnalité** 2022 vs 2023 par trimestre

---

## Roadmap

- [ ] Connexion dbt pour les modèles SQL
- [ ] Orchestration Airflow / Prefect
- [ ] Tests Great Expectations (qualité données)
- [ ] CI/CD GitHub Actions
- [ ] Export vers Google BigQuery

---

## Auteur

Projet réalisé par **Fouad MOUTAIRTOU**

Portfolio : https://portfolio-fouad.netlify.app/

Stack Utilisé : Python · Pandas · SQLite · Power BI · pytest

---
**Dataset utilisé:** 
*50 000 commandes · 6 catégories · 4 régions · 2 années (2022–2023)*