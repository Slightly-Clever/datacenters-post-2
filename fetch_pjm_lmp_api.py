#!/usr/bin/env python3
"""
fetch_pjm_lmp_api.py
--------------------
Fetches PJM day-ahead hourly LMPs from the Data Miner 2 API (da_hrl_lmps feed)
for the gap period (Jan 2019 – Aug 2020) and aggregates to monthly zone-level
averages matching the existing _cache/lmp_combined.csv format.

This fills the LMP data gap: EIA's zonal LMP product for PJM only starts in
September 2020. The PJM API has data back to 2000.

Companion code for Blog Post 2: "Building the Dataset"

Usage:
    # Set API key via environment variable:
    PJM_API_KEY=your_key python3 fetch_pjm_lmp_api.py

    # Or with explicit key:
    python3 fetch_pjm_lmp_api.py --api-key YOUR_KEY

    # Fetch specific months (default: 2019-01 through 2020-08):
    python3 fetch_pjm_lmp_api.py --start 2019-01 --end 2020-08

PJM Data Miner 2 API key: free at https://dataminer2.pjm.com

Output:
    Updates _cache/lmp_combined.csv with new PJM rows for the gap period.
    Also saves raw hourly cache to _cache/pjm_api_lmp_hourly.csv.
"""

import os
import sys
import time
import argparse
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, '_cache')
OUTPUT = os.path.join(CACHE_DIR, 'lmp_combined.csv')
HOURLY_CACHE = os.path.join(CACHE_DIR, 'pjm_api_lmp_hourly.csv')

API_BASE = 'https://api.pjm.com/api/v1/da_hrl_lmps'

# PJM API zone names → panel zone codes
API_ZONE_TO_PANEL = {
    'AECO':    'AE',
    'AEP':     'AEP',
    'APS':     'APS',
    'ATSI':    'ATSI',
    'BGE':     'BGE',
    'COMED':   'COMED',
    'DAY':     'DAYTON',
    'DEOK':    'DEOK',
    'DOM':     'DOM',
    'DPL':     'DPL',
    'DUQ':     'DLCO',
    'EKPC':    'EKPC',
    'JCPL':    'JCPL',
    'METED':   'METED',
    'OVEC':    'OVEC',
    'PECO':    'PECO',
    'PENELEC': 'PENLC',
    'PEPCO':   'PEPCO',
    'PPL':     'PL',
    'PSEG':    'PS',
    'RECO':    'RECO',
}

# Zones to skip (system-wide aggregates, not individual zones)
SKIP_ZONES = {'PJM-RTO', 'MID-ATL/APS'}

# On-peak: hours 8-22 (7am-10pm Eastern, interval-beginning)
# Matches aggregate_pjm_lmp.py definition
PEAK_HOURS = set(range(8, 23))

# Rate limit: 6 requests/minute for non-members
REQUEST_DELAY = 11  # seconds between requests (conservative)


def _fetch_date_range(start_str: str, end_str: str, api_key: str) -> list:
    """Fetch all paginated rows for a single date range from PJM API."""
    headers = {'Ocp-Apim-Subscription-Key': api_key}
    all_rows = []
    page = 1
    page_size = 5000
    retries = 0

    while True:
        params = {
            'datetime_beginning_ept': f'{start_str}to{end_str}',
            'row_is_current': 'true',
            'type': 'ZONE',
            'rowCount': page_size,
            'startRow': (page - 1) * page_size + 1,
        }

        try:
            r = requests.get(API_BASE, headers=headers, params=params, timeout=120)
            r.raise_for_status()
            data = r.json()
        except requests.exceptions.HTTPError as e:
            print(f"  HTTP error: {e}")
            if r.status_code == 429:
                print("  Rate limited — waiting 60s...")
                time.sleep(60)
                continue
            if r.status_code == 400:
                if retries < 3:
                    retries += 1
                    wait = 30 * retries
                    print(f"  400 Bad Request — retry {retries}/3, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                else:
                    print(f"  400 Bad Request — exhausted 3 retries, skipping page {page}")
                    break
            raise
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if retries < 3:
                retries += 1
                wait = 30 * retries
                print(f"\n  Connection error — retry {retries}/3, waiting {wait}s...")
                time.sleep(wait)
                continue
            else:
                print(f"\n  Connection error — exhausted 3 retries, skipping page {page}")
                break
        except Exception as e:
            print(f"  Request failed: {e}")
            raise

        # Reset retry counter on success
        retries = 0

        items = data if isinstance(data, list) else data.get('items', data.get('value', []))

        if not items:
            break

        all_rows.extend(items)

        total_count = None
        if isinstance(data, dict):
            total_count = data.get('totalRows', data.get('totalCount', None))

        if len(items) < page_size:
            break  # Last page

        if total_count and len(all_rows) >= total_count:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return all_rows


def fetch_month(year: int, month: int, api_key: str) -> pd.DataFrame:
    """Fetch all hourly zone-level DA LMPs for one calendar month.

    For December, splits into two sub-requests (Dec 1-16, Dec 16-31) to avoid
    cross-year date ranges that cause HTTP 400 from the PJM API.

    Filters returned rows to only the requested month to prevent boundary
    spillover into adjacent months.
    """
    start_date = datetime(year, month, 1)

    if month == 12:
        # Split December into two sub-requests to avoid cross-year date range
        # (12/1/YYYY to 1/1/YYYY+1) which causes 400 Bad Request
        mid_date = datetime(year, 12, 16)
        end_date = datetime(year, 12, 31, 23, 59)

        start_str1 = start_date.strftime('%-m/%-d/%Y 00:00')
        end_str1 = mid_date.strftime('%-m/%-d/%Y 00:00')
        start_str2 = mid_date.strftime('%-m/%-d/%Y 00:00')
        end_str2 = end_date.strftime('%-m/%-d/%Y 23:59')

        print(f" (Dec split: 1-15 + 16-31)", end='', flush=True)
        rows1 = _fetch_date_range(start_str1, end_str1, api_key)
        if rows1:
            print(f" [{len(rows1)} rows first half]", end='', flush=True)
        time.sleep(REQUEST_DELAY)
        rows2 = _fetch_date_range(start_str2, end_str2, api_key)
        if rows2:
            print(f" [{len(rows2)} rows second half]", end='', flush=True)

        all_rows = rows1 + rows2
    else:
        end_date = datetime(year, month + 1, 1)
        start_str = start_date.strftime('%-m/%-d/%Y 00:00')
        end_str = end_date.strftime('%-m/%-d/%Y 00:00')
        all_rows = _fetch_date_range(start_str, end_str, api_key)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Filter rows to only the requested month to prevent boundary spillover
    # (e.g., midnight of the next month leaking in)
    if 'datetime_beginning_ept' in df.columns:
        df['_dt'] = pd.to_datetime(df['datetime_beginning_ept'])
        before = len(df)
        df = df[(df['_dt'].dt.year == year) & (df['_dt'].dt.month == month)].copy()
        dropped = before - len(df)
        if dropped > 0:
            print(f" [filtered {dropped} boundary rows]", end='', flush=True)
        df.drop(columns=['_dt'], inplace=True)

    return df


def aggregate_hourly_to_monthly(hourly: pd.DataFrame) -> pd.DataFrame:
    """Aggregate hourly LMP data to zone × year × month averages."""
    if hourly.empty:
        return pd.DataFrame()

    # Parse datetime
    hourly['dt'] = pd.to_datetime(hourly['datetime_beginning_ept'])
    hourly['year'] = hourly['dt'].dt.year
    hourly['month'] = hourly['dt'].dt.month
    hourly['hour'] = hourly['dt'].dt.hour + 1  # Convert 0-based to 1-based hour number

    # Map zone names
    hourly['panel_zone'] = hourly['pnode_name'].map(API_ZONE_TO_PANEL)

    # Filter: only zones we care about (skip system aggregates and unmapped)
    hourly = hourly[hourly['panel_zone'].notna()].copy()

    # Numeric columns
    for col in ['total_lmp_da', 'congestion_price_da']:
        hourly[col] = pd.to_numeric(hourly[col], errors='coerce')

    # Peak/off-peak flag
    hourly['is_peak'] = hourly['hour'].isin(PEAK_HOURS)

    # Aggregate
    grp_all = hourly.groupby(['panel_zone', 'year', 'month'])
    grp_peak = hourly[hourly['is_peak']].groupby(['panel_zone', 'year', 'month'])
    grp_offpeak = hourly[~hourly['is_peak']].groupby(['panel_zone', 'year', 'month'])

    agg = grp_all.agg(
        lmp_da_avg=('total_lmp_da', 'mean'),
        lmp_congestion_avg=('congestion_price_da', 'mean'),
    ).reset_index()

    peak_s = grp_peak['total_lmp_da'].mean().rename('lmp_da_peak').reset_index()
    offpeak_s = grp_offpeak['total_lmp_da'].mean().rename('lmp_da_offpeak').reset_index()

    agg = agg.merge(peak_s, on=['panel_zone', 'year', 'month'], how='left')
    agg = agg.merge(offpeak_s, on=['panel_zone', 'year', 'month'], how='left')

    agg.rename(columns={'panel_zone': 'zone'}, inplace=True)
    agg['iso'] = 'PJM'
    agg['lmp_rt_avg'] = np.nan  # RT not available from this feed

    return agg


def merge_into_combined(new_pjm: pd.DataFrame):
    """Merge new PJM rows into existing lmp_combined.csv."""
    COLS = ['iso', 'zone', 'year', 'month',
            'lmp_da_avg', 'lmp_da_peak', 'lmp_da_offpeak',
            'lmp_rt_avg', 'lmp_congestion_avg']

    if os.path.exists(OUTPUT):
        existing = pd.read_csv(OUTPUT)
        print(f"  Existing lmp_combined.csv: {len(existing)} rows")

        # Remove any existing PJM rows that overlap with new data
        new_ym = set(zip(new_pjm['year'], new_pjm['month']))
        overlap_mask = (
            (existing['iso'] == 'PJM') &
            existing.apply(lambda r: (r['year'], r['month']) in new_ym, axis=1)
        )
        n_overlap = overlap_mask.sum()
        if n_overlap > 0:
            print(f"  Removing {n_overlap} overlapping PJM rows")
            existing = existing[~overlap_mask].copy()
    else:
        existing = pd.DataFrame(columns=COLS)

    # Ensure columns match
    for col in COLS:
        if col not in new_pjm.columns:
            new_pjm[col] = np.nan
        if col not in existing.columns:
            existing[col] = np.nan

    combined = pd.concat([existing[COLS], new_pjm[COLS]], ignore_index=True)
    combined.sort_values(['iso', 'zone', 'year', 'month'], inplace=True)
    combined.reset_index(drop=True, inplace=True)

    combined.to_csv(OUTPUT, index=False)
    return combined


def main():
    parser = argparse.ArgumentParser(description='Fetch PJM DA LMPs from Data Miner 2 API')
    parser.add_argument('--api-key', default=os.environ.get('PJM_API_KEY', ''),
                        help='PJM Data Miner 2 API key (free at https://dataminer2.pjm.com)')
    parser.add_argument('--start', default='2019-01',
                        help='Start year-month (default: 2019-01)')
    parser.add_argument('--end', default='2020-08',
                        help='End year-month inclusive (default: 2020-08)')
    args = parser.parse_args()

    api_key = args.api_key
    if not api_key:
        print("ERROR: No API key provided.")
        print("  Set PJM_API_KEY env var or use --api-key")
        print("  Register free at https://dataminer2.pjm.com")
        sys.exit(1)

    os.makedirs(CACHE_DIR, exist_ok=True)

    # Parse date range
    start_y, start_m = map(int, args.start.split('-'))
    end_y, end_m = map(int, args.end.split('-'))

    # Build list of (year, month) to fetch
    months_to_fetch = []
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        months_to_fetch.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    print("=" * 60)
    print(f"Fetching PJM DA LMPs: {args.start} to {args.end}")
    print(f"  {len(months_to_fetch)} months to fetch")
    print(f"  API: {API_BASE}")
    print(f"  Delay between requests: {REQUEST_DELAY}s")
    print("=" * 60)

    # Check for cached hourly data
    all_hourly = []
    cached_months = set()

    if os.path.exists(HOURLY_CACHE):
        print(f"\nLoading cached hourly data from {os.path.basename(HOURLY_CACHE)}...")
        cached = pd.read_csv(HOURLY_CACHE, low_memory=False)
        cached['dt'] = pd.to_datetime(cached['datetime_beginning_ept'])
        cached['_year'] = cached['dt'].dt.year
        cached['_month'] = cached['dt'].dt.month

        for y, m in months_to_fetch:
            month_data = cached[(cached['_year'] == y) & (cached['_month'] == m)]
            if len(month_data) > 100:  # Reasonable threshold
                cached_months.add((y, m))
                all_hourly.append(month_data.drop(columns=['dt', '_year', '_month']))

        if cached_months:
            print(f"  Found {len(cached_months)} months already cached")

    # Fetch missing months
    months_needed = [(y, m) for y, m in months_to_fetch if (y, m) not in cached_months]

    if months_needed:
        print(f"\nFetching {len(months_needed)} months from API...")
        for i, (y, m) in enumerate(months_needed):
            print(f"\n  [{i+1}/{len(months_needed)}] Fetching {y}-{m:02d}...", end='', flush=True)
            df = fetch_month(y, m, api_key)

            if df.empty:
                print(f" 0 rows (empty)")
                continue

            # Filter to zone-level only
            if 'type' in df.columns:
                df = df[df['type'] == 'ZONE'].copy()

            # Filter out system aggregates
            if 'pnode_name' in df.columns:
                df = df[~df['pnode_name'].isin(SKIP_ZONES)].copy()

            print(f" {len(df)} rows", flush=True)
            all_hourly.append(df)

            # Incremental save
            print(f"    Saving cache ({sum(len(d) for d in all_hourly)} total rows)...", end='', flush=True)
            hourly_so_far = pd.concat(all_hourly, ignore_index=True)
            hourly_so_far.to_csv(HOURLY_CACHE, index=False)
            print(" done", flush=True)

            if i < len(months_needed) - 1:
                time.sleep(REQUEST_DELAY)

    if not all_hourly:
        print("\nNo data fetched. Check API key and date range.")
        sys.exit(1)

    # Combine all hourly data
    print("\n" + "=" * 60)
    print("Aggregating hourly → monthly...")
    hourly_all = pd.concat(all_hourly, ignore_index=True)

    # Save hourly cache
    hourly_all.to_csv(HOURLY_CACHE, index=False)
    print(f"  Saved {len(hourly_all)} hourly rows to {os.path.basename(HOURLY_CACHE)}")

    # Aggregate to monthly
    monthly = aggregate_hourly_to_monthly(hourly_all)
    print(f"  Aggregated to {len(monthly)} zone-months")
    print(f"  Zones: {sorted(monthly['zone'].unique())}")
    print(f"  Date range: {monthly['year'].min()}-{monthly['month'].min():02d} "
          f"to {monthly['year'].max()}-{monthly['month'].max():02d}")

    # Merge into lmp_combined.csv
    print("\n" + "=" * 60)
    print("Merging into lmp_combined.csv...")
    combined = merge_into_combined(monthly)

    # Summary
    pjm_rows = (combined['iso'] == 'PJM').sum()
    ercot_rows = (combined['iso'] == 'ERCOT').sum()
    pjm_data = combined[combined['iso'] == 'PJM']

    print(f"\nOutput: {OUTPUT}")
    print(f"  Total rows:    {len(combined)}")
    print(f"  PJM rows:      {pjm_rows}")
    print(f"  ERCOT rows:    {ercot_rows}")
    print(f"  PJM zones:     {pjm_data['zone'].nunique()}")

    print("\nDone.")


if __name__ == '__main__':
    main()
