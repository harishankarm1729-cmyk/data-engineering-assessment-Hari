# data-engineering-assessment-Hari
PySpark Bronze/Silver/Gold pipeline for TTPD speeding ticket analysis

# TTPD Speeding Ticket Analysis
### Delaware North — Take-Home Technical Assessment

A PySpark-based data pipeline that ingests and analyzes speeding ticket data for the **Tiny Town Police Department (TTPD)** across 2020–2023. Built using a Bronze / Silver / Gold lakehouse architecture with Apache Spark and Delta Lake.

---

## Questions Answered

| # | Question | Answer |
|---|---|---|
| Q1 | Which police officer issued the most speeding tickets? | **Barbara Cervantes — 419 tickets** |
| Q2 | What 3 months had the most speeding tickets? | **Dec 2023 (1,258), Dec 2022 (824), Dec 2021 (803)** |
| Q3 | Who are the top 10 people who spent the most on tickets? | **Kimberly Mcdowell & Charles Dunn — $660 each** (see full list below) |

<details>
<summary>Full Top 10 Spenders</summary>

| Rank | Name | Total Paid |
|---|---|---|
| 1 | Kimberly Mcdowell | $660 |
| 2 | Charles Dunn | $660 |
| 3 | Ariel Smith | $630 |
| 4 | Pamela Young | $630 |
| 5 | Tracy Casey | $600 |
| 6 | Avery Smith | $600 |
| 7 | Carla Robinson | $570 |
| 8 | Adrian May | $570 |
| 9 | Kathryn Blair | $540 |
| 10 | Elizabeth Zuniga | $540 |

</details>

---

## Dataset

The `ttpd_data/` directory contains three entity types across multiple batch files:

| Entity | Format | Files | Records |
|---|---|---|---|
| People (citizens + officers) | CSV (pipe-delimited) | 52 | 7,123 |
| Automobiles | XML | 83 | 10,299 |
| Speeding Tickets | JSON | 36 | 16,123 |

> **Note:** `ttpd_data/` and `delta_tables/` are excluded from this repository via `.gitignore`. Place the unzipped `ttpd_data` folder in the root directory before running.

---

## Architecture — Bronze / Silver / Gold

```
ttpd_data/  (source files)
    │
    ▼
┌─────────────────────────────────┐
│  BRONZE  — Raw ingestion        │  CSV + XML + JSON → Delta tables, no changes
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│  SILVER  — Cleaning & typing    │  Nulls, timestamps, booleans, fee calc, officer flag
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│  GOLD    — Aggregations         │  Joins + group-bys that answer the 3 questions
└─────────────────────────────────┘
```

### Key Transformations per Layer

**Bronze**
- People CSVs merged into one Delta table via `spark.read.csv`
- Automobile XMLs parsed with Python's `ElementTree`, converted to Spark DataFrame
- Ticket JSONs read with `spark.read.json`, nested array exploded with `F.explode()`

**Silver**
- `profession` column uppercased so `"Police officer"` and `"Police Officer"` are treated identically
- `ticket_time` parsed from string to `TimestampType`; `ticket_year` and `ticket_month_str` (YYYY-MM) derived
- `ticket_fee` calculated using the assignment fee schedule:
  - Base only → **$30**
  - Base + school zone → **$60**
  - Base + work zone → **$60**
  - Base + school + work zone → **$120**
- `is_officer` boolean flag added to people table

**Gold**
- `gold.officer_ticket_counts` — tickets grouped by officer, joined to people where `is_officer = true`
- `gold.monthly_ticket_counts` — tickets grouped by `ticket_month_str`, ranked descending
- `gold.top_spenders` — three-table join: ticket → automobile → person, fees summed per person

---

## How to Run

### Option A — Google Colab (Recommended)

1. Open [Google Colab](https://colab.research.google.com)
2. Go to **File → Upload notebook** and upload `TTPD_Colab_Notebook.ipynb`
3. Upload `ttpd_data.zip` using the **Files panel** on the left sidebar
4. Run all cells **top to bottom**

> Cell 1 installs all dependencies automatically. A warning about `dataproc-spark-connect` may appear — ignore it, it does not affect the pipeline.

### Option B — Local Environment

**Prerequisites**
- Python 3.8+
- Java Runtime Environment — [download here](https://java.com/en/download/manual.jsp)

**Install dependencies**
```bash
pip install delta-spark==4.0.0
```

**Run**
```bash
# Place ttpd_data/ folder in the same directory as main.py, then:
python main.py
```

---

## Repository Structure

```
├── main.py                     # Complete pipeline — Bronze, Silver, Gold, answers
├── TTPD_Colab_Notebook.ipynb   # Google Colab notebook (same logic, cell-by-cell)
├── TTPD_Documentation.docx     # Full write-up: design, findings, production notes
├── README.md                   # This file
├── .gitignore
├── ttpd_data/                  # ← NOT committed. Place unzipped folder here before running.
└── delta_tables/               # ← NOT committed. Created at runtime by the pipeline.
```

---

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.8+ | Core language |
| Apache Spark (PySpark) | 4.0 | DataFrame processing |
| Delta Lake | 4.0.0 | ACID-compliant table storage |
| Java JRE | 8+ | Required by Spark |

---

## Bonus — Trend Analysis (Q2)

December is consistently the highest-volume month across all four years, suggesting a seasonal enforcement pattern or increased holiday traffic. Ticket volume also shows a clear year-over-year upward trend:

| Year | Total Tickets |
|---|---|
| 2020 | ~2,900 |
| 2021 | ~3,700 |
| 2022 | ~4,200 |
| 2023 | ~5,300 |


