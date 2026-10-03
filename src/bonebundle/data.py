"""Portable paths and strict as-of helpers for the exported research data."""
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parents[2]

def table_files(data_root,table,market_key,contract_start_ms):
    return sorted((Path(data_root)/'poly'/table/f'market_key={market_key}'/f'contract_start_ms={int(contract_start_ms)}').glob('session_id=*/*.parquet'))

def load_partition(data_root,table,market_key,contract_start_ms,columns=None):
    fs=table_files(data_root,table,market_key,contract_start_ms)
    if not fs:raise FileNotFoundError(f'Missing {table}/{market_key}/{contract_start_ms}; fetch group poly for this day/tenor')
    return pd.concat([pq.ParquetFile(f).read(columns=columns).to_pandas() for f in fs],ignore_index=True)

def strict_asof_indices(observed_ns,query_ns):
    """Both vectors are int64 nanoseconds; same-timestamp observations are excluded."""
    observed=np.asarray(observed_ns,dtype=np.int64)
    if np.any(observed[1:]<observed[:-1]):raise ValueError('Observations must be sorted by available/receive time')
    return np.searchsorted(observed,np.asarray(query_ns,dtype=np.int64),side='left')-1

def load_grid(data_root,market_key,contract_start_ms):
    f=Path(data_root)/'features/poly_grid_100ms'/f'market_key={market_key}'/f'contract_start_ms={int(contract_start_ms)}'/'grid.parquet'
    if not f.exists():raise FileNotFoundError('Fetch group features first')
    return pq.ParquetFile(f).read().to_pandas()
