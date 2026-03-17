#!/usr/bin/env python3
"""
aggregate_pjm_lmp.py

Aggregates hourly PJM day-ahead LMP data from the EIA merged file
into monthly zone-month averages. Combines with existing ERCOT LMP
data to produce _cache/lmp_combined.csv.

Companion code for Blog Post 2: "Building the Dataset"

Input:  pjm_lmp_da_merged_2020_2025.csv  (hourly, wide format from EIA)
        _cache/lmp_combined.csv           (existing ERCOT monthly data)
Output: _cache/lmp_combined.csv           (all zones, monthly)

Coverage:
  PJM:   21 zones, Sept 2020 through ~Jun 2025 (EIA start date)
  ERCOT: 8 zones, Jan 2019 onwards (pass-through from existing cache)
  UGI:   null (no EIA sub-BA mapping)

Run:
    python3 aggregate_pjm_lmp.py
"""

import os
import sys
import pandas as pd
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, '_cache')
LMP_FILE  = os.path.join(BASE_DIR, 'pjm_lmp_da_merged_2020_2025.csv')
OUTPUT    = os.path.join(CACHE_DIR, 'lmp_combined.csv')

# ---------------------------------------------------------------------------
# Zone name mapping: EIA full company name prefix → panel zone code
# Key = exact text before " LMP" / " (Congestion)" in EIA column headers
# None = skip (PJM Total is system-wide, not a zone)
# ---------------------------------------------------------------------------
EIA_TO_ZONE = {
    'Allegheny Power System':                  'APS',
    'American Electric Power Co., Inc':        'AEP',
    'American Transmission Systems, Inc':      'ATSI',
    'Atlantic Electric Company':               'AE',
    'Baltimore Gas and Electric Company':      'BGE',
    'ComEd':                                   'COMED',
    'Dayton Power and Light Company':          'DAYTON',
    'Delmarva Power and Light':                'DPL',
    'Dominion Energy':                         'DOM',
    'Duke Energy Ohio/Kentucky':               'DEOK',
    'Duquesne Light':                          'DLCO',
    'East Kentucky Power Coop':                'EKPC',
    'Jersey Central Power and Light Company':  'JCPL',
    'Metropolitan Edison Company':             'METED',
    'Ohio Valley Electric':                    'OVEC',
    'PECO Energy':                             'PECO',
    'PJM Total':                               None,    # system-wide aggregate; skip
    'PPL Electric Utilities':                  'PL',
    'Pennsylvania Electric':                   'PENLC',
    'Potomac Electric Power':                  'PEPCO',
    'Public Service Electric and Gas Company': 'PS',
    'Rockland Electric Company':               'RECO',
}

# On-peak definition: 7am–10pm Eastern (15 hours)
# Hour Number in EIA file is interval-beginning based:
#   Hour 1 = midnight–1am, Hour 8 = 7am–8am, Hour 22 = 9pm–10pm
# On-peak (7am–10pm) → Hours 8–22 inclusive
PEAK_HOURS = set(range(8, 23))


# ---------------------------------------------------------------------------
def aggregate_pjm() -> pd.DataFrame:
    """Read hourly EIA PJM LMP file; aggregate to zone × year × month."""
    if not os.path.exists(LMP_FILE):
        print(f"ERROR: Input file not found: {LMP_FILE}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {os.path.basename(LMP_FILE)} ...")
    df = pd.read_csv(LMP_FILE, low_memory=False)
    print(f"  {len(df):,} rows loaded ({df['Local Date'].min()} to {df['Local Date'].max()})")

    # Parse date
    df['year']     = pd.to_datetime(df['Local Date']).dt.year
    df['month']    = pd.to_datetime(df['Local Date']).dt.month
    df['is_peak']  = df['Hour Number'].isin(PEAK_HOURS)

    zone_frames = []
    for eia_name, zone_code in EIA_TO_ZONE.items():
        if zone_code is None:
            continue  # PJM Total — skip

        lmp_col  = f'{eia_name} LMP'
        cong_col = f'{eia_name} (Congestion)'

        if lmp_col not in df.columns:
            print(f"  WARNING: '{lmp_col}' not found — skipping {zone_code}")
            continue

        tmp = df[['year', 'month', 'is_peak', lmp_col, cong_col]].copy()
        tmp.rename(columns={lmp_col: 'lmp', cong_col: 'congestion'}, inplace=True)
        tmp['zone'] = zone_code
        zone_frames.append(tmp)

    print(f"  Built long frame for {len(zone_frames)} zones ...")
    long = pd.concat(zone_frames, ignore_index=True)

    # Aggregate: all-hours average
    grp_all     = long.groupby(['zone', 'year', 'month'])
    grp_peak    = long[long['is_peak']].groupby(['zone', 'year', 'month'])
    grp_offpeak = long[~long['is_peak']].groupby(['zone', 'year', 'month'])

    agg = grp_all.agg(
        lmp_da_avg        = ('lmp',        'mean'),
        lmp_congestion_avg = ('congestion', 'mean'),
    ).reset_index()

    peak_s    = grp_peak['lmp'].mean().rename('lmp_da_peak').reset_index()
    offpeak_s = grp_offpeak['lmp'].mean().rename('lmp_da_offpeak').reset_index()

    agg = agg.merge(peak_s,    on=['zone', 'year', 'month'], how='left')
    agg = agg.merge(offpeak_s, on=['zone', 'year', 'month'], how='left')

    agg['iso']       = 'PJM'
    agg['lmp_rt_avg'] = np.nan   # RT price not available in this EIA file

    print(f"  PJM aggregate: {len(agg)} zone-months, "
          f"{agg['zone'].nunique()} zones")
    return agg


# ---------------------------------------------------------------------------
def load_existing_ercot() -> pd.DataFrame:
    """Load existing ERCOT monthly LMP rows from current lmp_combined.csv."""
    if not os.path.exists(OUTPUT):
        print("  No existing lmp_combined.csv found — ERCOT rows will be absent.")
        return pd.DataFrame()

    df = pd.read_csv(OUTPUT)
    ercot = df[df['iso'] == 'ERCOT'].copy()
    print(f"  Loaded {len(ercot)} existing ERCOT zone-months from lmp_combined.csv")

    # Ensure lmp_congestion_avg exists (not available for ERCOT)
    if 'lmp_congestion_avg' not in ercot.columns:
        ercot['lmp_congestion_avg'] = np.nan

    return ercot


# ---------------------------------------------------------------------------
def main():
    os.makedirs(CACHE_DIR, exist_ok=True)

    print("=" * 60)
    print("Step 1: Aggregate PJM hourly LMP → monthly")
    print("=" * 60)
    pjm = aggregate_pjm()

    print()
    print("=" * 60)
    print("Step 2: Load existing ERCOT data")
    print("=" * 60)
    ercot = load_existing_ercot()

    # Canonical column order for output
    COLS = ['iso', 'zone', 'year', 'month',
            'lmp_da_avg', 'lmp_da_peak', 'lmp_da_offpeak',
            'lmp_rt_avg', 'lmp_congestion_avg']

    # Add any missing columns to each frame
    for col in COLS:
        if col not in pjm.columns:
            pjm[col] = np.nan
        if len(ercot) and col not in ercot.columns:
            ercot[col] = np.nan

    frames = [pjm[COLS]]
    if len(ercot):
        frames.append(ercot[COLS])

    print()
    print("=" * 60)
    print("Step 3: Combine and write output")
    print("=" * 60)
    combined = pd.concat(frames, ignore_index=True)
    combined.sort_values(['iso', 'zone', 'year', 'month'], inplace=True)
    combined.reset_index(drop=True, inplace=True)

    combined.to_csv(OUTPUT, index=False)

    # Summary
    pjm_rows   = (combined['iso'] == 'PJM').sum()
    ercot_rows = (combined['iso'] == 'ERCOT').sum()
    da_nonnull = combined['lmp_da_avg'].notna().sum()

    print(f"\nOutput: {OUTPUT}")
    print(f"  Total rows:    {len(combined)}")
    print(f"  PJM rows:      {pjm_rows}")
    print(f"  ERCOT rows:    {ercot_rows}")
    print(f"  lmp_da_avg:    {da_nonnull} non-null")


if __name__ == '__main__':
    main()
