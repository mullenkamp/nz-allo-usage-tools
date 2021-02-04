# -*- coding: utf-8 -*-
"""
Created on Sat Feb 16 09:50:42 2019

@author: michaelek
"""
import os
import numpy as np
import pandas as pd
import yaml
from allotools.data_io import get_permit_data, get_usage_data, allo_filter
from allotools.allocation_ts import allo_ts
import tethys_utils as tu
# from allotools.plot import plot_group as pg
# from allotools.plot import plot_stacked as ps
from datetime import datetime

# from matplotlib.pyplot import show

#########################################
### parameters

base_path = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_path, 'parameters.yml')) as param:
    param = yaml.safe_load(param)

pk = ['permit_id', 'wap', 'date']
dataset_types = ['allo', 'metered_allo',  'usage', 'usage_est']
allo_type_dict = {'D': 'max_daily_volume', 'W': 'max_daily_volume', 'M': 'max_annual_volume', 'A-JUN': 'max_annual_volume', 'A': 'max_annual_volume'}
# allo_mult_dict = {'D': 0.001*24*60*60, 'W': 0.001*24*60*60*7, 'M': 0.001*24*60*60*30, 'A-JUN': 0.001*24*60*60*365, 'A': 0.001*24*60*60*365}
temp_datasets = ['allo_ts', 'total_allo_ts', 'wap_allo_ts', 'usage_ts', 'metered_allo_ts']

#######################################
### Testing

# from_date = '2000-07-01'
# to_date = '2020-06-30'
#
# self = AlloUsage(from_date=from_date, to_date=to_date)
#
# results1 = self.get_ts(['allo', 'metered_allo', 'usage'], 'M', ['permit_id', 'wap'])
# results2 = self.get_ts(['usage'], 'D', ['wap'])
# results3 = self.get_ts(['allo', 'metered_allo', 'usage', 'usage_est'], 'M', ['permit_id', 'wap'])
# results3 = self.get_ts(['allo', 'metered_allo', 'usage', 'usage_est'], 'D', ['permit_id', 'wap'])

# wap_filter = {'wap': ['C44/0001']}
#
# self = AlloUsage(from_date=from_date, to_date=to_date, wap_filter=wap_filter)
#
# results1 = self.get_ts(['allo', 'metered_allo', 'usage'], 'M', ['permit_id', 'wap'])
# results2 = self.get_ts(['usage'], 'D', ['wap'])

# permit_filter = {'permit_id': ['200040']}
#
# self = AlloUsage(from_date=from_date, to_date=to_date, permit_filter=permit_filter)
#
# results1 = self.get_ts(['allo', 'metered_allo', 'usage', 'usage_est'], 'M', ['permit_id', 'wap'])
# results2 = self.get_ts(['allo', 'metered_allo', 'usage', 'usage_est'], 'D', ['permit_id', 'wap'])

########################################
### Core class


class AlloUsage(object):
    """
    Class to to process the allocation and usage data in NZ.

    Parameters
    ----------
    from_date : str or None
        The start date of the consent and the final time series. In the form of '2000-01-01'. None will return all consents and subsequently all dates.
    to_date : str or None
        The end date of the consent and the final time series. In the form of '2000-01-01'. None will return all consents and subsequently all dates.
    permit_filter : dict
        If permit_id_filter is a list, then it should represent the columns from the permit table that should be returned. If it's a dict, then the keys should be the column names and the values should be the filter on those columns.
    wap_filter : dict
        If wap_filter is a list, then it should represent the columns from the wap table that should be returned. If it's a dict, then the keys should be the column names and the values should be the filter on those columns.
    only_consumptive : bool
        Should only the consumptive takes be returned? Default True
    include_hydroelectric : bool
        Should hydro-electric takes be included? Default False

    Returns
    -------
    AlloUsage object
        with all of the base sites, allo, and allo_wap DataFrames

    """

    dataset_types = dataset_types
    # plot_group = pg
    # plot_stacked = ps

    _usage_remote = param['remote']['usage']
    _permit_remote = param['remote']['permit']

    ### Initial import and assignment function
    def __init__(self, from_date=None, to_date=None, permit_filter=None, wap_filter=None,  only_consumptive=True, include_hydroelectric=False):
        """

        Parameters
        ----------
        from_date : str or None
            The start date of the consent and the final time series. In the form of '2000-01-01'. None will return all consents and subsequently all dates.
        to_date : str or None
            The end date of the consent and the final time series. In the form of '2000-01-01'. None will return all consents and subsequently all dates.
        permit_filter : dict
            If permit_id_filter is a list, then it should represent the columns from the permit table that should be returned. If it's a dict, then the keys should be the column names and the values should be the filter on those columns.
        wap_filter : dict
            If wap_filter is a list, then it should represent the columns from the wap table that should be returned. If it's a dict, then the keys should be the column names and the values should be the filter on those columns.
        only_consumptive : bool
            Should only the consumptive takes be returned? Default True
        include_hydroelectric : bool
            Should hydro-electric takes be included? Default False

        Returns
        -------
        AlloUsage object
            with all of the base sites, allo, and allo_wap DataFrames

        """
        permits0 = get_permit_data(self._permit_remote['connection_config'], self._permit_remote['bucket'], self._permit_remote['permits_key'])

        waps, permits = allo_filter(permits0, from_date, to_date, permit_filter=permit_filter, wap_filter=wap_filter, only_consumptive=only_consumptive, include_hydroelectric=include_hydroelectric)

        if from_date is None:
            from_date1 = pd.Timestamp('1900-07-01')
        else:
            from_date1 = pd.Timestamp(from_date)
        if to_date is None:
            to_date1 = pd.Timestamp.now().floor('D')
        else:
            to_date1 = pd.Timestamp(to_date)

        setattr(self, 'waps', waps)
        setattr(self, 'permits', permits)
        # setattr(self, 'sd', sd)
        setattr(self, 'from_date', from_date1)
        setattr(self, 'to_date', to_date1)


    def _est_allo_ts(self):
        """

        """
        ### Run the allocation time series creation
        ### This has currently been hard-soded to only use the max rate. This should probably be changed once the permitting data gets fixed.
        limit_col = allo_type_dict[self.freq]
        # multiplier = allo_mult_dict[self.freq]
        # limit_col = 'max_rate'
        allo4 = allo_ts(self.permits, self.from_date, self.to_date, self.freq, limit_col).round()
        allo4.name = 'total_allo'

        # allo4 = (allo4 * multiplier).round()

        # if self.irr_season and ('A' not in self.freq):
        #     dates1 = allo4.index.levels[2]
        #     dates2 = dates1[dates1.month.isin([10, 11, 12, 1, 2, 3, 4])]
        #     allo4 = allo4.loc[(slice(None), slice(None), dates2)]

        setattr(self, 'total_allo_ts', allo4.reset_index())


    def _allo_wap_spit(self):
        """

        """
        allo6 = pd.merge(self.total_allo_ts, self.waps[['permit_id', 'wap', 'sd_ratio']], on=['permit_id'])
        # allo6 = pd.merge(allo5, self.sd, on=['permit_id', 'wap'], how='left')

        allo6['combo_wap_allo'] = allo6.groupby(['permit_id', 'hydro_feature', 'date'])['total_allo'].transform('sum')
        allo6['combo_wap_ratio'] = allo6['total_allo']/allo6['combo_wap_allo']

        # allo6['rate_wap_tot'] = allo6.groupby(['permit_id', 'hydro_feature', 'date'])['wap_max_rate'].transform('sum')
        # allo6['rate_wap_ratio'] = allo6['wap_max_rate']/allo6['rate_wap_tot']

        allo6['wap_allo'] = allo6['total_allo'] * allo6['combo_wap_ratio']

        # allo6.loc[allo6.rate_wap_ratio.notnull(), 'wap_allo'] = allo6.loc[allo6.rate_wap_ratio.notnull(), 'rate_wap_ratio'] * allo6.loc[allo6.rate_wap_ratio.notnull(), 'total_allo']

        # allo7 = allo6.drop(['combo_wap_allo', 'combo_wap_ratio', 'rate_wap_tot', 'rate_wap_ratio', 'wap_max_rate', 'total_allo'], axis=1).rename(columns={'wap_allo': 'total_allo'}).copy()

        allo7 = allo6.drop(['combo_wap_allo', 'combo_wap_ratio', 'total_allo'], axis=1).rename(columns={'wap_allo': 'total_allo'}).copy()

        allo7.loc[allo7.sd_ratio.isnull() & (allo7.hydro_feature == 'groundwater'), 'sd_ratio'] = 0
        allo7.loc[allo7.sd_ratio.isnull() & (allo7.hydro_feature == 'surface water'), 'sd_ratio'] = 1

        allo7['sw_allo'] = allo7['total_allo'] * allo7['sd_ratio']
        allo7['gw_allo'] = allo7['total_allo'] - allo7['sw_allo']

        allo8 = allo7.drop(['hydro_feature', 'sd_ratio'], axis=1).groupby(pk).mean()

        setattr(self, 'wap_allo_ts', allo8)


    def _get_allo_ts(self):
        """
        Function to create an allocation time series.

        """
        if not hasattr(self, 'total_allo_ts'):
            self._est_allo_ts()

        ### Convert to GW and SW allocation

        self._allo_wap_spit()


    def _process_usage(self):
        """

        """
        if not hasattr(self, 'wap_allo_ts'):
            self._get_allo_ts()
        allo1 = self.wap_allo_ts.copy().reset_index()

        waps = allo1.wap.unique().tolist()

        ## Get the ts data and aggregate
        if hasattr(self, 'usage_ts_daily'):
            tsdata1 = self.usage_ts_daily
        else:
            tsdata1, stns_waps = get_usage_data(self._usage_remote['connection_config'], self._usage_remote['bucket'], waps, self.from_date, self.to_date)
            tsdata1.rename(columns={'water_use': 'total_usage', 'time': 'date'}, inplace=True)

            tsdata1 = tsdata1[['wap', 'date', 'total_usage']].copy()

            ## filter - remove individual spikes and negative values
            tsdata1.loc[tsdata1['total_usage'] < 0, 'total_usage'] = 0

            def remove_spikes(x):
                val1 = bool(x[1] > (x[0] + x[2] + 2))
                if val1:
                    return (x[0] + x[2])/2
                else:
                    return x[1]

            tsdata1.iloc[1:-1, 2] = tsdata1['total_usage'].rolling(3, center=True).apply(remove_spikes, raw=True).iloc[1:-1]

            setattr(self, 'usage_ts_daily', tsdata1)

            ## Convert station data to DataFrame
            stns_waps1 = pd.DataFrame([{'wap': s['ref'], 'lon': s['geometry']['coordinates'][0], 'lat': s['geometry']['coordinates'][1]} for s in stns_waps])

            setattr(self, 'waps_only', stns_waps1)

        ### Aggregate
        tsdata2 = tu.grp_ts_agg(tsdata1, 'wap', 'date', self.freq, 'sum')

        setattr(self, 'usage_ts', tsdata2)


    def _usage_estimation(self, usage_allo_ratio=2, buffer_dis=40000, min_months=36):
        """

        """
        from gistools import vector

        ### Get the necessary data

        a1 = AlloUsage()
        a1.permits = self.permits.copy()
        a1.waps = self.waps.copy()
        a1.from_date = self.from_date
        a1.to_date = self.to_date

        allo_use1 = a1.get_ts(['allo', 'metered_allo', 'usage'], 'M', ['permit_id', 'wap'])

        permits = self.permits.copy()

        ### Create Wap locations
        waps1 = vector.xy_to_gpd('wap', 'lon', 'lat', self.waps.drop('permit_id', axis=1).drop_duplicates('wap'), 4326)
        waps2 = waps1.to_crs(2193)

        ### Determine which Waps need to be estimated
        allo_use_mis1 = allo_use1[allo_use1['total_metered_allo'] == 0].copy().reset_index()
        allo_use_with1 = allo_use1[allo_use1['total_metered_allo'] > 0].copy().reset_index()

        mis_waps1 = allo_use_mis1.groupby(['permit_id', 'wap'])['total_allo'].count().copy()
        with_waps1 = allo_use_with1.groupby(['permit_id', 'wap'])['total_allo'].count()
        with_waps2 = with_waps1[with_waps1 >= min_months]

        with_waps3 = pd.merge(with_waps2.reset_index()[['permit_id', 'wap']], permits[['permit_id', 'use_type']], on='permit_id')

        with_waps4 = pd.merge(waps2, with_waps3['wap'], on='wap')

        mis_waps2 = pd.merge(mis_waps1.reset_index(), permits[['permit_id', 'use_type']], on='permit_id')
        mis_waps3 = pd.merge(waps2, mis_waps2['wap'], on='wap')
        mis_waps3['geometry'] = mis_waps3['geometry'].buffer(buffer_dis)
        # mis_waps3.rename(columns={'wap': 'mis_wap'}, inplace=True)

        mis_waps4, poly1 = vector.pts_poly_join(with_waps4.rename(columns={'wap': 'good_wap'}), mis_waps3, 'wap')

        ## Calc ratios
        allo_use_with2 = pd.merge(allo_use_with1, permits[['permit_id', 'use_type']], on='permit_id')

        allo_use_with2['month'] = allo_use_with2['date'].dt.month
        allo_use_with2['usage_allo'] = allo_use_with2['total_usage']/allo_use_with2['total_allo']

        allo_use_ratio1 = allo_use_with2.groupby(['permit_id', 'wap', 'use_type', 'month'])['usage_allo'].mean().reset_index()

        allo_use_ratio2 = pd.merge(allo_use_ratio1.rename(columns={'wap': 'good_wap'}), mis_waps4[['good_wap', 'wap']], on='good_wap')

        ## Combine with the missing ones
        allo_use_mis2 = pd.merge(allo_use_mis1[['permit_id', 'wap', 'date']], permits[['permit_id', 'use_type']], on='permit_id')
        allo_use_mis2['month'] = allo_use_mis2['date'].dt.month

        allo_use_mis3 = pd.merge(allo_use_mis2, allo_use_ratio2[['use_type', 'month', 'usage_allo', 'wap']], on=['use_type', 'wap', 'month'])
        allo_use_mis4 = allo_use_mis3.groupby(['permit_id', 'wap', 'date'])['usage_allo'].mean().reset_index()

        allo_use_mis5 = pd.merge(allo_use_mis4, allo_use_mis1[['permit_id', 'wap', 'date', 'total_allo', 'sw_allo', 'gw_allo']], on=['permit_id', 'wap', 'date'])
        allo_use_mis5['total_usage_est'] = (allo_use_mis5['usage_allo'] * allo_use_mis5['total_allo']).round()
        allo_use_mis5['sw_usage_est'] = (allo_use_mis5['usage_allo'] * allo_use_mis5['sw_allo']).round()
        allo_use_mis5['gw_usage_est'] = allo_use_mis5['total_usage_est'] - allo_use_mis5['sw_usage_est']

        allo_use_mis6 = allo_use_mis5[['permit_id', 'wap', 'date', 'total_usage_est', 'sw_usage_est', 'gw_usage_est']].copy()

        ### Convert to daily if required
        if self.freq == 'D':
            days1 = allo_use_mis6.date.dt.daysinmonth
            days2 = pd.to_timedelta((days1/2).round().astype('int32'), unit='D')

            allo_use_mis6['total_usage_est'] = allo_use_mis6['total_usage_est'] / days1
            allo_use_mis6['sw_usage_est'] = allo_use_mis6['sw_usage_est'] / days1
            allo_use_mis6['gw_usage_est'] = allo_use_mis6['gw_usage_est'] / days1

            usage_rate0 = allo_use_mis6.copy()

            usage_rate0['date'] = usage_rate0['date'] - days2

            grp1 = allo_use_mis6.groupby(['permit_id', 'wap'])
            first1 = grp1.first()
            last1 = grp1.last()

            first1.loc[:, 'date'] = pd.to_datetime(first1.loc[:, 'date'].dt.strftime('%Y-%m') + '-01')

            usage_rate1 = pd.concat([first1, usage_rate0.set_index(['permit_id', 'wap']), last1], sort=True).reset_index().sort_values(['permit_id', 'wap', 'date'])

            usage_rate1.set_index('date', inplace=True)

            usage_daily_rate1 = usage_rate1.groupby(['permit_id', 'wap']).apply(lambda x: x.resample('D').interpolate(method='pchip')[['total_usage_est', 'sw_usage_est', 'gw_usage_est']]).round(2)

        else:
            usage_daily_rate1 = allo_use_mis6.set_index(['permit_id', 'wap', 'date'])

        setattr(self, 'usage_est', usage_daily_rate1)

        return usage_daily_rate1


    def _split_usage_ts(self, usage_allo_ratio=2):
        """

        """
        ### Get the usage data if it exists
        if not hasattr(self, 'usage_ts'):
            self._process_usage()
        tsdata2 = self.usage_ts.copy().reset_index()

        if not hasattr(self, 'allo_ts'):
            allo1 = self._get_allo_ts()
        allo1 = self.wap_allo_ts.copy().reset_index()

        allo1['combo_allo'] = allo1.groupby(['wap', 'date'])['total_allo'].transform('sum')
        allo1['combo_ratio'] = allo1['total_allo']/allo1['combo_allo']

        ### combine with consents info
        usage1 = pd.merge(allo1, tsdata2, on=['wap', 'date'])
        usage1['total_usage'] = usage1['total_usage'] * usage1['combo_ratio']

        ### Remove high outliers
        usage1.loc[usage1['total_usage'] > (usage1['total_allo'] * usage_allo_ratio), 'total_usage'] = np.nan

        ### Split the GW and SW components
        usage1['sw_ratio'] = usage1['sw_allo']/usage1['total_allo']

        usage1['sw_usage'] = usage1['sw_ratio'] * usage1['total_usage']
        usage1['gw_usage'] = usage1['total_usage'] - usage1['sw_usage']
        usage1.loc[usage1['gw_usage'] < 0, 'gw_usage'] = 0

        usage1.drop(['sw_allo', 'gw_allo', 'total_allo', 'combo_allo', 'combo_ratio', 'sw_ratio'], axis=1, inplace=True)

        usage2 = usage1.dropna().groupby(pk).mean()

        setattr(self, 'split_usage_ts', usage2)


    def _get_metered_allo_ts(self, proportion_allo=True):
        """

        """
        setattr(self, 'proportion_allo', proportion_allo)

        ### Get the allocation ts either total or metered
        if not hasattr(self, 'wap_allo_ts'):
            self._get_allo_ts()
        allo1 = self.wap_allo_ts.copy().reset_index()
        rename_dict = {'sw_allo': 'sw_metered_allo', 'gw_allo': 'gw_metered_allo', 'total_allo': 'total_metered_allo'}

        ### Combine the usage data to the allo data
        if not hasattr(self, 'split_usage_ts'):
            self._split_usage_ts()
        allo2 = pd.merge(self.split_usage_ts.reset_index()[pk], allo1, on=pk, how='right', indicator=True)

        ## Re-categorise
        allo2['_merge'] = allo2._merge.cat.rename_categories({'left_only': 2, 'right_only': 0, 'both': 1}).astype(int)

        if proportion_allo:
            allo2.loc[allo2._merge != 1, list(rename_dict.keys())] = 0
            allo3 = allo2.drop('_merge', axis=1).copy()
        else:
            allo2['usage_waps'] = allo2.groupby(['permit_id', 'date'])['_merge'].transform('sum')

            allo2.loc[allo2.usage_waps == 0, list(rename_dict.keys())] = 0
            allo3 = allo2.drop(['_merge', 'usage_waps'], axis=1).copy()

        allo3.rename(columns=rename_dict, inplace=True)
        allo4 = allo3.groupby(pk).mean()

        if 'total_metered_allo' in allo3:
            setattr(self, 'metered_allo_ts', allo4)
        else:
            setattr(self, 'metered_restr_allo_ts', allo4)


    def get_ts(self, datasets, freq, groupby, usage_allo_ratio=2, buffer_dis=40000, min_months=36):
        """
        Function to create a time series of allocation and usage.

        Parameters
        ----------
        datasets : list of str
            The dataset types to be returned. Must be one or more of {ds}.
        freq : str
            Pandas time frequency code for the time interval. Must be one of 'D', 'W', 'M', 'A', or 'A-JUN'.
        groupby : list of str
            The fields that should grouped by when returned. Can be any variety of fields including crc, take_type, allo_block, 'wap', CatchmentGroupName, etc. Date will always be included as part of the output group, so it doesn't need to be specified in the groupby.
        usage_allo_ratio : int or float
            The cut off ratio of usage/allocation. Any usage above this ratio will be removed from the results (subsequently reducing the metered allocation).

        Results
        -------
        DataFrame
            Indexed by the groupby (and date)
        """
        ### Add in date to groupby if it's not there
        if not 'date' in groupby:
            groupby.append('date')

        ### Check the dataset types
        if not np.in1d(datasets, self.dataset_types).all():
            raise ValueError('datasets must be a list that includes one or more of ' + str(self.dataset_types))

        ### Check new to old parameters and remove attributes if necessary
        if 'A' in freq:
            freq_agg = freq
            freq = 'M'
        else:
            freq_agg = freq

        if hasattr(self, 'freq'):
            # if (self.freq != freq) or (self.sd_days != sd_days) or (self.irr_season != irr_season):
            if (self.freq != freq):
                for d in temp_datasets:
                    if hasattr(self, d):
                        delattr(self, d)

        ### Assign pararameters
        setattr(self, 'freq', freq)
        # setattr(self, 'sd_days', sd_days)
        # setattr(self, 'irr_season', irr_season)

        ### Get the results and combine
        all1 = []

        if 'allo' in datasets:
            self._get_allo_ts()
            all1.append(self.wap_allo_ts)
        if 'metered_allo' in datasets:
            self._get_metered_allo_ts()
            all1.append(self.metered_allo_ts)
        if 'usage' in datasets:
            self._split_usage_ts(usage_allo_ratio)
            all1.append(self.split_usage_ts)
        if 'usage_est' in datasets:
            usage_est = self._usage_estimation(usage_allo_ratio, buffer_dis, min_months)
            all1.append(usage_est)

        if 'A' in freq_agg:
            all2 = tu.grp_ts_agg(pd.concat(all1, axis=1).reset_index(), ['permit_id', 'wap'], 'date', freq_agg, 'sum').reset_index()
        else:
            all2 = pd.concat(all1, axis=1).reset_index()

        if not np.in1d(groupby, pk).all():
            all2 = self._merge_extra(all2, groupby)

        all3 = all2.groupby(groupby).sum()
        all3.name = 'results'

        return all3


    def _merge_extra(self, data, cols):
        """

        """
        # sites_col = [c for c in cols if c in self.waps.columns]
        allo_col = [c for c in cols if c in self.permits.columns]

        data1 = data.copy()

        # if sites_col:
        #     all_sites_col = ['wap']
        #     all_sites_col.extend(sites_col)
        #     data1 = pd.merge(data1, self.waps.reset_index()[all_sites_col], on='wap')
        if allo_col:
            all_allo_col = ['permit_id']
            all_allo_col.extend(allo_col)
            data1 = pd.merge(data1, self.permits[all_allo_col], on=all_allo_col)

        data1.set_index(pk, inplace=True)

        return data1
