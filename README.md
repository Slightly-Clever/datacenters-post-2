# Post 2: Building the Dataset

**Companion code and data for the blog post "Building the Dataset"**

*2,880 rows, 26 columns, and the data gaps that almost broke everything*

---

## What's in this package

This package contains the code and data behind Post 2 of the blog series on datacenter impacts on wholesale electricity prices. The post walks through how we built a zone×month panel dataset covering 30 electricity pricing zones (22 PJM + 8 ERCOT) from January 2019 through December 2026.

### Files

| File | Description |
|------|-------------|
| `build_panel.py` | Main panel construction script. Builds the 2,880-row scaffold, merges treatment indicators, demand, LMPs, gas prices, weather, and renewables. |
| `aggregate_pjm_lmp.py` | Aggregates hourly EIA PJM LMP data into monthly zone-level averages (peak/off-peak/all-hours). |
| `fetch_pjm_lmp_api.py` | Fetches the Jan 2019–Aug 2020 LMP gap from PJM's Data Miner 2 API. This is the script that filled the 20 missing months discussed in the post. |
| `data/analytical_panel.csv` | The complete output panel (2,880 rows × 28 columns). This is the dataset used in all subsequent analysis posts. |
| `data/dc_treatment_master.csv` | PJM B-9b datacenter load forecasts by zone and year (2025 and 2026 vintages). |
| `requirements.txt` | Python dependencies. |

### The panel (`analytical_panel.csv`)

Each row is one **zone-month** (e.g., DOM in July 2024). Key columns:

| Column | Description |
|--------|-------------|
| `iso` | Market operator: PJM or ERCOT |
| `zone` | Pricing zone (e.g., DOM, PECO, LZ_NORTH) |
| `year`, `month`, `year_month` | Time identifiers |
| `lmp_da_avg` | Day-ahead LMP, all-hours monthly average ($/MWh) |
| `lmp_da_peak` | Day-ahead LMP, peak hours only (7am–10pm weekdays) |
| `lmp_da_offpeak` | Day-ahead LMP, off-peak hours only |
| `lmp_congestion_avg` | Congestion component of LMP (PJM only, Sept 2020+) |
| `dc_zone` | Binary: 1 for datacenter-concentrated zones (DOM, PEPCO, AEP) |
| `post2022` | Binary: 1 for year ≥ 2023 |
| `dc_load_mw_forecast` | PJM B-9b datacenter load forecast (MW), 2025–2026 only |
| `gas_henry_hub` | Henry Hub natural gas spot price ($/MMBtu) |
| `cdd`, `hdd` | Cooling/heating degree days (weather controls) |
| `renewable_share` | Share of generation from wind + solar |
| `demand_mwh` | Total zonal demand (MWh), PJM zones only |

---

## How to use

### Option 1: Just explore the data

The pre-built panel is included — no API keys needed:

```python
import pandas as pd

panel = pd.read_csv('data/analytical_panel.csv')
print(panel.shape)          # (2880, 28)
print(panel.columns.tolist())

# PJM zones with LMP data
pjm = panel[(panel['iso'] == 'PJM') & panel['lmp_da_avg'].notna()]
print(f"PJM observations: {len(pjm)}")  # 1,617

# Compare DC zones vs control zones
dc = pjm[pjm['dc_zone'] == 1].groupby('year')['lmp_da_offpeak'].mean()
ctrl = pjm[pjm['dc_zone'] == 0].groupby('year')['lmp_da_offpeak'].mean()
print(pd.DataFrame({'DC zones': dc, 'Control zones': ctrl}))
```

### Option 2: Rebuild the panel from scratch

You'll need two free API keys:

1. **EIA API key** — sign up at https://api.eia.gov/signup
2. **PJM Data Miner 2 key** — register at https://dataminer2.pjm.com

```bash
pip install -r requirements.txt

# Step 1: Fetch EIA demand data (cached per-year, ~2 min/year)
python3 build_panel.py --eia-key YOUR_EIA_KEY --years 2019,2020,2021,2022,2023,2024,2025,2026

# Step 2: Fetch the Jan 2019–Aug 2020 LMP gap from PJM API
# (rate-limited to 6 requests/min — takes a few hours for the full gap)
PJM_API_KEY=YOUR_PJM_KEY python3 fetch_pjm_lmp_api.py --start 2019-01 --end 2020-08

# Step 3: Aggregate EIA hourly LMPs (Sept 2020+) into monthly
# (requires pjm_lmp_da_merged_2020_2025.csv from EIA — not included due to size)
python3 aggregate_pjm_lmp.py

# Step 4: Rebuild the panel with all data sources merged
python3 build_panel.py
```

Note: The EIA hourly LMP file (`pjm_lmp_da_merged_2020_2025.csv`, ~200MB) is not included in this package due to size. Download it from the [EIA Open Data portal](https://www.eia.gov/opendata/) (Electricity → Real-Time Operating Grid → Wholesale electricity prices).

---

## Data sources

| Source | What it provides | Access |
|--------|-----------------|--------|
| [EIA Open Data API](https://api.eia.gov/) | Hourly demand by PJM sub-BA; hourly LMPs (Sept 2020+); Henry Hub gas prices; renewable generation (EIA-930) | Free API key |
| [PJM Data Miner 2](https://dataminer2.pjm.com) | Hourly DA LMPs back to 2000 (fills the Jan 2019–Aug 2020 gap) | Free registration |
| [NOAA GSOM](https://www.ncei.noaa.gov/access/search/data-search/global-summary-of-the-month) | Monthly CDD/HDD by weather station (mapped to zones) | Free |
| [PJM Load Forecast (B-9b)](https://www.pjm.com/planning/services-requests/interconnection-queues) | Datacenter load forecasts by zone (2025, 2026 vintages) | Public PDF/CSV |

---

## Bugs discussed in the post

The `fetch_pjm_lmp_api.py` script includes fixes for two bugs encountered during data collection:

1. **Boundary spillover** (lines 202–209): The PJM API returns the first hour of the *next* month with each request. Without post-fetch filtering, neighboring months' data contaminated each other, causing phantom NaN values in peak-hour averages.

2. **December cross-year** (lines 168–188): Requesting December data with an end date in January of the next year causes a 400 Bad Request. The fix splits December into two sub-requests (Dec 1–15, Dec 16–31).

---

## Tools

This project was built with [Claude Code](https://claude.ai/claude-code), an AI coding assistant. Claude Code helped write the data pipeline scripts, debug the API issues described above, and run the economic analysis. The research design and interpretation are the author's.
