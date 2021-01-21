# -*- coding: utf-8 -*-
"""
Created on Mon Oct  1 09:55:07 2018

@author: michaelek
"""
import numpy as np
import pandas as pd

###################################
### Parameters

freq_codes = ['D', 'W', 'M', 'A-JUN']


###################################
### Functions


def allo_ts_apply(row, from_date, to_date, freq, limit_col, remove_months=False):
    """
    Pandas apply function that converts the allocation data to a monthly time series.
    """

    crc_from_date = pd.Timestamp(row['from_date'])
    crc_to_date = pd.Timestamp(row['to_date'])
    start = pd.Timestamp(from_date)
    end = pd.Timestamp(to_date)

    if crc_from_date > start:
        start = crc_from_date
    if crc_to_date < end:
        end = crc_to_date

    end_date = (end - pd.DateOffset(hours=1) + pd.tseries.frequencies.to_offset(freq)).floor('D')
    dates1 = pd.date_range(start, end_date, freq=freq)
    if remove_months and ('A' not in freq):
        mon1 = np.arange(row['from_month'], 13)
        mon2 = np.arange(1, row['to_month'] + 1)
        in_mons = np.concatenate((mon1, mon2))
        dates1 = dates1[dates1.month.isin(in_mons)]

    if dates1.empty:
        return None

    if freq == 'D':
        val1 = 1
    elif freq == 'W':
        val1 = 7
    elif freq == 'M':
        val1 = dates1.daysinmonth.values
    else:
        val1 = dates1.dayofyear.values + 184

    s1 = pd.Series(val1, index=dates1, name='allo')

    if freq in ['A-JUN', 'D', 'W']:
        vol1 = s1.copy()
        vol1[:] = row[limit_col]
    elif 'M' in freq:
        # year1 = s1.resample('A-JUN').transform('sum')
        vol1 = s1/365 * row[limit_col]
    else:
        raise ValueError("freq must be either 'A-JUN', 'M', 'W', or 'D'")

    alt_dates = s1.values.copy()
    if len(s1) == 1:
        alt_dates[0] = s1[0] - (dates1[-1] - end).days - (s1[0] - (dates1[0] - start).days)
    else:
        start_diff = (dates1[0] - start).days + 1
        if start_diff < s1[0]:
            alt_dates[0] = start_diff
        end_diff = s1[-1] - (dates1[-1] - end).days
        if end_diff < s1[-1]:
            alt_dates[-1] = end_diff
    ratio_days = alt_dates/s1

    vols = ratio_days * vol1

    return vols


def allo_ts(permits, from_date, to_date, freq, limit_col, remove_months=False):
    """
    Combo function to completely create a time series from the allocation DataFrame. Source data must be from an instance of the Hydro db.

    Parameters
    ----------
    server : str
        The server where the Hydro db lays.
    from_date : str
        The start date for the time series.
    to_date: str
        The end date for the time series.
    freq : str
        Pandas frequency str. Must be 'D', 'W', 'M', or 'A-JUN'.
    restr_type : str
        The allocation rate/volume used as the values in the time series. Must be 'max rate', 'daily volume', or 'annual volume'.
    remove_months : bool
        Should the months that are defined in the consent only be returned?
    in_allo : bool
        Should the consumptive consents be returned?

    Returns
    -------
    Series
        indexed by crc, take_type, and allo_block
    """
    if freq not in freq_codes:
        raise ValueError('freq must be one of ' + str(freq_codes))

    permits2 = permits.set_index(['permit_id', 'hydro_feature']).copy()

    permits3 = permits2.apply(allo_ts_apply, axis=1, from_date=from_date, to_date=to_date, freq=freq, limit_col=limit_col, remove_months=remove_months)

    permits4 = permits3.stack()
    permits4.index.set_names(['permit_id', 'hydro_feature', 'date'], inplace=True)
    permits4.name = 'allo'

    return permits4





