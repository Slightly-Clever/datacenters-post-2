"""
Build the master analytical panel for PJM/ERCOT datacenter study.
Pulls EIA hourly demand by sub-BA, aggregates to zone×month, merges with treatment.

Companion code for Blog Post 2: "Building the Dataset"

Usage:
  python3 build_panel.py --eia-key KEY              # fetch all uncached years
  python3 build_panel.py --eia-key KEY --years 2019,2020  # fetch specific years only
  python3 build_panel.py                             # build panel from cached data only

Cached EIA data is saved per-year in _eia_cache/ so the script can be re-run
without re-fetching. Each year takes ~2 minutes to fetch.

EIA API key: free at https://api.eia.gov/signup
"""
import pandas as pd
import requests
import time
import os
import argparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, '_eia_cache')

# --- PJM and ERCOT zone lists ---
pjm_zones = ['AE','AEP','APS','ATSI','BGE','COMED','DAYTON','DEOK',
             'DLCO','DOM','DPL','EKPC','JCPL','METED','OVEC','PECO',
             'PENLC','PEPCO','PL','PS','RECO','UGI']
ercot_zones = ['LZ_AEN','LZ_CPS','LZ_HOUSTON','LZ_LCRA',
               'LZ_NORTH','LZ_RAYBN','LZ_SOUTH','LZ_WEST']

# --- EIA sub-BA to PJM zone mapping ---
eia_to_pjm = {
    'AE':'AE', 'AEP':'AEP', 'AP':'APS', 'ATSI':'ATSI', 'BC':'BGE',
    'CE':'COMED', 'DAY':'DAYTON', 'DEOK':'DEOK', 'DOM':'DOM', 'DPL':'DPL',
    'DUQ':'DLCO', 'EKPC':'EKPC', 'JC':'JCPL', 'ME':'METED', 'PE':'PECO',
    'PEP':'PEPCO', 'PL':'PL', 'PN':'PENLC', 'PS':'PS', 'RECO':'RECO'
}

ALL_YEARS = list(range(2019, 2027))


def build_scaffold():
    print("Step 1: Building scaffold...")
    months = list(range(1, 13))
    rows = []
    for z in pjm_zones:
        for y in ALL_YEARS:
            for m in months:
                rows.append({'iso': 'PJM', 'zone': z, 'year': y, 'month': m})
    for z in ercot_zones:
        for y in ALL_YEARS:
            for m in months:
                rows.append({'iso': 'ERCOT', 'zone': z, 'year': y, 'month': m})
    scaffold = pd.DataFrame(rows)
    scaffold['year_month'] = (scaffold['year'].astype(str) + '-' +
                              scaffold['month'].apply(lambda x: f'{x:02d}'))

    # Treatment indicator columns (time-invariant binary zone classification)
    # dc_zone = 1 for DC-concentrated zones (Northern VA / DC Metro / OH corridor)
    # post2022 = 1 for year >= 2023 (ChatGPT-era datacenter boom started ~2022)
    # DiD spec: price[z,t] = α + β(dc_zone × post2022) + zone_FE + time_FE + controls
    DC_TREATMENT_ZONES = {'DOM', 'PEPCO', 'AEP'}
    scaffold['dc_zone']  = scaffold['zone'].isin(DC_TREATMENT_ZONES).astype(int)
    scaffold['post2022'] = (scaffold['year'] >= 2023).astype(int)

    print(f"  {scaffold.shape[0]} rows")
    print(f"  dc_zone=1 zones: {sorted(DC_TREATMENT_ZONES)}")
    return scaffold


def merge_treatment(scaffold):
    print("Step 2: Merging treatment data...")
    treat_path = os.path.join(BASE_DIR, 'data', 'dc_treatment_master.csv')
    if not os.path.exists(treat_path):
        print("  No treatment file found. Skipping B-9b forecast merge.")
        scaffold['dc_load_mw_forecast'] = 0.0
        return scaffold

    treat = pd.read_csv(treat_path)
    t25 = treat[(treat['forecast_vintage'] == '2025_forecast') &
                (treat['year'] == 2025)][['zone', 'year', 'dc_load_mw']].copy()
    t26 = treat[(treat['forecast_vintage'] == '2026_forecast') &
                (treat['year'] == 2026)][['zone', 'year', 'dc_load_mw']].copy()
    treat_sel = pd.concat([t25, t26], ignore_index=True)
    treat_sel = treat_sel.rename(columns={'dc_load_mw': 'dc_load_mw_forecast'})
    panel = scaffold.merge(treat_sel, on=['zone', 'year'], how='left')
    panel['dc_load_mw_forecast'] = panel['dc_load_mw_forecast'].fillna(0)
    print(f"  Non-zero treatment rows: {(panel['dc_load_mw_forecast'] > 0).sum()}")
    return panel


def fetch_eia_year(yr, api_key):
    """Fetch one year of hourly PJM sub-BA demand from EIA API."""
    cache_path = os.path.join(CACHE_DIR, f'eia_demand_{yr}.csv')
    if os.path.exists(cache_path):
        df = pd.read_csv(cache_path)
        print(f"  {yr}: cached ({len(df):,} rows)")
        return df

    rows = []
    offset = 0
    while True:
        url = (f'https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/'
               f'?api_key={api_key}&frequency=hourly&data[0]=value'
               f'&facets[parent][]=PJM'
               f'&start={yr}-01-01T00&end={yr}-12-31T23'
               f'&offset={offset}&length=5000')
        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            print(f"  {yr}: ERROR status {resp.status_code} at offset {offset}")
            if resp.status_code == 403:
                print(f"    API key may be invalid. Response: {resp.text[:200]}")
            return None
        data = resp.json().get('response', {}).get('data', [])
        if not data:
            break
        rows.extend(data)
        offset += 5000
        if len(data) < 5000:
            break
        time.sleep(0.3)

    if rows:
        df = pd.DataFrame(rows)
        os.makedirs(CACHE_DIR, exist_ok=True)
        df.to_csv(cache_path, index=False)
        print(f"  {yr}: fetched {len(df):,} rows -> {cache_path}")
        return df
    else:
        print(f"  {yr}: no data returned")
        return None


def fetch_eia_demand(api_key, years=None):
    """Fetch EIA demand for specified years, using cache when available."""
    if years is None:
        years = ALL_YEARS
    print(f"Step 3: EIA demand data for years {years[0]}-{years[-1]}...")

    # Quick API key validation
    test_url = (f'https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/'
                f'?api_key={api_key}&frequency=hourly&data[0]=value'
                f'&facets[parent][]=PJM&start=2024-01-01T00&end=2024-01-01T03'
                f'&offset=0&length=5')
    test_resp = requests.get(test_url, timeout=30)
    if test_resp.status_code != 200:
        print(f"  ERROR: API key invalid (status {test_resp.status_code})")
        return None

    frames = []
    for yr in years:
        df = fetch_eia_year(yr, api_key)
        if df is not None:
            frames.append(df)

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        print(f"  Total: {len(combined):,} rows across {len(frames)} years")
        return combined
    return None


def aggregate_demand(raw_demand):
    print("Step 4: Aggregating demand to zone × month...")
    df = raw_demand.copy()
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df['zone'] = df['subba'].map(eia_to_pjm)
    df = df.dropna(subset=['zone', 'value'])
    # Drop obvious EIA data errors (no sub-BA exceeds 50 GW)
    bad = df['value'] > 50_000
    if bad.any():
        print(f"  Dropping {bad.sum()} rows with value > 50,000 MW (data errors)")
    df = df[~bad]
    df['dt'] = pd.to_datetime(df['period'], format='mixed')
    df['year'] = df['dt'].dt.year
    df['month'] = df['dt'].dt.month
    agg = df.groupby(['zone', 'year', 'month']).agg(
        demand_mwh=('value', 'sum'),
        peak_demand_mw=('value', 'max')
    ).reset_index()
    print(f"  {agg.shape[0]} zone-month rows, {agg['zone'].nunique()} zones")
    return agg


def merge_lmp(panel):
    """Merge LMP price data from _cache/lmp_combined.csv."""
    print("Step 6: Merging LMP data...")
    lmp_path = os.path.join(BASE_DIR, '_cache', 'lmp_combined.csv')
    if not os.path.exists(lmp_path):
        print("  No LMP file found. Run aggregate_pjm_lmp.py or fetch_pjm_lmp_api.py first.")
        return panel

    lmp = pd.read_csv(lmp_path)
    lmp_cols = ['iso', 'zone', 'year', 'month',
                'lmp_da_avg', 'lmp_rt_avg', 'lmp_da_peak', 'lmp_da_offpeak',
                'lmp_congestion_avg']
    # Keep only columns that exist
    lmp_cols = [c for c in lmp_cols if c in lmp.columns]
    lmp = lmp[lmp_cols]

    panel = panel.merge(lmp, on=['iso', 'zone', 'year', 'month'], how='left')
    n_with = panel['lmp_da_avg'].notna().sum()
    print(f"  {n_with} rows with LMP data "
          f"({panel[panel['lmp_da_avg'].notna()]['iso'].value_counts().to_dict()})")
    return panel


def merge_gas_prices(panel):
    """Merge Henry Hub natural gas prices from _cache/controls_gas.csv."""
    print("Step 9: Merging gas prices...")
    gas_path = os.path.join(BASE_DIR, '_cache', 'controls_gas.csv')
    if not os.path.exists(gas_path):
        print("  No gas price file found.")
        return panel

    gas = pd.read_csv(gas_path)
    gas = gas[['year', 'month', 'gas_henry_hub']]

    panel = panel.merge(gas, on=['year', 'month'], how='left')
    n_with = panel['gas_henry_hub'].notna().sum()
    print(f"  {n_with} rows with gas price data "
          f"(${gas['gas_henry_hub'].min():.2f}-${gas['gas_henry_hub'].max():.2f}/MMBTU)")
    return panel


def merge_renewable_share(panel):
    """Merge renewable generation share from _cache/controls_renewables.csv."""
    print("Step 10: Merging renewable generation share...")
    ren_path = os.path.join(BASE_DIR, '_cache', 'controls_renewables.csv')
    if not os.path.exists(ren_path):
        print("  No renewables file found.")
        return panel

    ren = pd.read_csv(ren_path)
    ren = ren[['iso', 'year', 'month', 'renewable_share', 'solar_share',
               'wind_share', 'total_gen_gwh']]

    panel = panel.merge(ren, on=['iso', 'year', 'month'], how='left')
    n_with = panel['renewable_share'].notna().sum()
    print(f"  {n_with} rows with renewable share data")
    return panel


def merge_weather(panel):
    """Merge zone-level CDD/HDD weather controls from _cache/controls_weather.csv."""
    print("Step 11: Merging weather CDD/HDD...")
    weather_path = os.path.join(BASE_DIR, '_cache', 'controls_weather.csv')
    if not os.path.exists(weather_path):
        print("  No weather file found.")
        return panel
    weather = pd.read_csv(weather_path)
    weather = weather[['zone', 'year', 'month', 'cdd', 'hdd']]
    panel = panel.merge(weather, on=['zone', 'year', 'month'], how='left')
    n_with = panel['cdd'].notna().sum()
    print(f"  {n_with} rows with CDD/HDD data "
          f"({panel[panel['cdd'].notna()]['zone'].nunique()} zones)")
    return panel


def load_all_cached():
    """Load all cached yearly CSVs without hitting the API."""
    if not os.path.exists(CACHE_DIR):
        return None
    frames = []
    for yr in ALL_YEARS:
        path = os.path.join(CACHE_DIR, f'eia_demand_{yr}.csv')
        if os.path.exists(path):
            frames.append(pd.read_csv(path))
    if frames:
        return pd.concat(frames, ignore_index=True)
    return None


def main():
    parser = argparse.ArgumentParser(description='Build PJM/ERCOT analytical panel')
    parser.add_argument('--eia-key', type=str, default=None,
                        help='EIA API key (get one at https://api.eia.gov/signup)')
    parser.add_argument('--years', type=str, default=None,
                        help='Comma-separated years to fetch, e.g. 2019,2020,2021')
    args = parser.parse_args()

    # Step 1-2: Scaffold + treatment
    scaffold = build_scaffold()
    panel = merge_treatment(scaffold)

    # Step 3-5: EIA demand
    raw_demand = None
    if args.eia_key:
        years = None
        if args.years:
            years = [int(y.strip()) for y in args.years.split(',')]
        raw_demand = fetch_eia_demand(args.eia_key, years)
    else:
        # Try loading from cache even without API key
        print("Step 3: Loading cached EIA demand data...")
        raw_demand = load_all_cached()
        if raw_demand is not None:
            print(f"  Loaded {len(raw_demand):,} cached rows")
        else:
            print("  No cached data found. Run with --eia-key to fetch.")

    if raw_demand is not None and len(raw_demand) > 0:
        demand_agg = aggregate_demand(raw_demand)
        print("Step 5: Merging demand into panel...")
        panel = panel.merge(demand_agg, on=['zone', 'year', 'month'], how='left')
        n_with = panel['demand_mwh'].notna().sum()
        print(f"  {n_with} rows with demand data "
              f"({panel[panel['demand_mwh'].notna()]['zone'].nunique()} zones)")
    else:
        panel['demand_mwh'] = None
        panel['peak_demand_mw'] = None

    # Step 6: Merge LMP prices
    panel = merge_lmp(panel)

    # Step 9: Merge gas prices
    panel = merge_gas_prices(panel)

    # Step 10: Merge renewable generation share
    panel = merge_renewable_share(panel)

    # Step 11: Merge weather CDD/HDD
    panel = merge_weather(panel)

    # Save
    out_path = os.path.join(BASE_DIR, 'analytical_panel.csv')
    panel.to_csv(out_path, index=False)
    print(f"\nSaved: {out_path}")
    print(f"  Shape: {panel.shape}")
    print(f"  Columns: {panel.columns.tolist()}")

    # Summary
    print(f"\n--- Panel summary ---")
    print(f"  Shape: {panel.shape}")
    print(f"  Zones: {panel['zone'].nunique()} "
          f"({panel[panel['iso']=='PJM']['zone'].nunique()} PJM + "
          f"{panel[panel['iso']=='ERCOT']['zone'].nunique()} ERCOT)")
    print(f"  Year range: {panel['year'].min()}-{panel['year'].max()}")

    print(f"\n--- LMP ---")
    for col in ['lmp_da_avg', 'lmp_rt_avg', 'lmp_da_peak', 'lmp_da_offpeak', 'lmp_congestion_avg']:
        if col in panel.columns:
            n = panel[col].notna().sum()
            print(f"  {col}: {n} non-null rows")

    print(f"\n--- Control variables ---")
    if 'gas_henry_hub' in panel.columns:
        n = panel['gas_henry_hub'].notna().sum()
        print(f"  gas_henry_hub: {n}/{len(panel)} rows")
    if 'renewable_share' in panel.columns:
        n = panel['renewable_share'].notna().sum()
        print(f"  renewable_share: {n}/{len(panel)} rows")
    if 'cdd' in panel.columns:
        n = panel['cdd'].notna().sum()
        print(f"  cdd/hdd: {n}/{len(panel)} rows")

    print("\nDone!")


if __name__ == '__main__':
    main()
