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
# from allotools.plot import plot_group as pg
# from allotools.plot import plot_stacked as ps
# import allotools.parameters as param
from datetime import datetime

#########################################
### parameters

base_path = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_path, 'parameters.yml')) as param:
    param = yaml.safe_load(param)


pk = ['permit_id', 'wap', 'date']
dataset_types = ['allo', 'metered_allo',  'usage']
allo_type_dict = {'D': 'max_daily_volume', 'W': 'max_daily_volume', 'M': 'max_annual_volume', 'A-JUN': 'max_annual_volume', 'A': 'max_annual_volume'}

#######################################
### Testing

# from_date = '2018-07-01'
# to_date = '2020-06-30'
#
# a1 = AlloUsage(from_date=from_date, to_date=to_date)
#
# results1 = a1.get_ts(['allo', 'metered_allo', 'usage'], 'M', ['permit_id', 'wap'])




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
    site_filter : dict
        A dict in the form of {str: [values]} to select specific values from a specific column in the ExternalSite table.
    crc_filter : dict
        A dict in the form of {str: [values]} to select specific values from a specific column in the CrcAllo table.
    crc_wap_filter : dict
        A dict in the form of {str: [values]} to select specific values from a specific column in the CrcWapAllo table.
    in_allo : bool
        Should only the consumptive takes be included?
    include_hydroelectric : bool
        Should hydroelectric takes be included?

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
    def __init__(self, from_date=None, to_date=None, site_filter=None, crc_filter=None, crc_wap_filter=None, only_consumptive=True, include_hydroelectric=False):
        """

        Parameters
        ----------
        from_date : str or None
            The start date of the consent and the final time series. In the form of '2000-01-01'. None will return all consents and subsequently all dates.
        to_date : str or None
            The end date of the consent and the final time series. In the form of '2000-01-01'. None will return all consents and subsequently all dates.
        site_filter : dict
            A dict in the form of {str: [values]} to select specific values from a specific column in the ExternalSite table.
        crc_filter : dict
            A dict in the form of {str: [values]} to select specific values from a specific column in the CrcAllo table.
        crc_wap_filter : dict
            A dict in the form of {str: [values]} to select specific values from a specific column in the CrcWapAllo table.
        in_allo : bool
            Should only the consumptive takes be included?
        include_hydroelectric : bool
            Should hydroelectric takes be included?

        Returns
        -------
        AlloUsage object
            with all of the base sites, allo, and allo_wap DataFrames

        """
        waps0, permits0, sd0 = get_permit_data(self._permit_remote['connection_config'], self._permit_remote['bucket'], self._permit_remote['waps_key'], self._permit_remote['permits_key'], self._permit_remote['sd_key'])

        waps, permits, sd = allo_filter(waps0, permits0, sd0, from_date, to_date)

        setattr(self, 'waps', waps)
        setattr(self, 'permits', permits)
        setattr(self, 'sd', sd)
        setattr(self, 'from_date', from_date)
        setattr(self, 'to_date', to_date)


    def _est_allo_ts(self):
        """

        """
        limit_col = allo_type_dict[self.freq]
        allo4 = allo_ts(self.permits, self.from_date, self.to_date, self.freq, limit_col)
        allo4.name = 'total_allo'

        # if self.irr_season and ('A' not in self.freq):
        #     dates1 = allo4.index.levels[2]
        #     dates2 = dates1[dates1.month.isin([10, 11, 12, 1, 2, 3, 4])]
        #     allo4 = allo4.loc[(slice(None), slice(None), dates2)]

        setattr(self, 'total_allo_ts', allo4.reset_index())


    def _allo_wap_spit(self):
        """

        """
        allo5 = pd.merge(self.total_allo_ts, self.waps[['permit_id', 'wap']], on=['permit_id'])
        allo6 = pd.merge(allo5, self.sd, on=['permit_id', 'wap'], how='left')

        allo6['combo_wap_allo'] = allo6.groupby(['permit_id', 'hydro_group', 'date'])['total_allo'].transform('sum')
        allo6['combo_wap_ratio'] = allo6['total_allo']/allo6['combo_wap_allo']

        allo6['rate_wap_tot'] = allo6.groupby(['permit_id', 'hydro_group', 'date'])['wap_max_rate'].transform('sum')
        allo6['rate_wap_ratio'] = allo6['wap_max_rate']/allo6['rate_wap_tot']

        allo6['wap_allo'] = allo6['total_allo'] * allo6['combo_wap_ratio']

        allo6.loc[allo6.rate_wap_ratio.notnull(), 'wap_allo'] = allo6.loc[allo6.rate_wap_ratio.notnull(), 'rate_wap_ratio'] * allo6.loc[allo6.rate_wap_ratio.notnull(), 'total_allo']

        allo7 = allo6.drop(['combo_wap_allo', 'combo_wap_ratio', 'rate_wap_tot', 'rate_wap_ratio', 'wap_max_rate', 'total_allo'], axis=1).rename(columns={'wap_allo': 'total_allo'}).copy()

        allo7.loc[allo7.sd_ratio.isnull() & (allo7.hydro_group == 'Groundwater'), 'sd_ratio'] = 0
        allo7.loc[allo7.sd_ratio.isnull() & (allo7.hydro_group == 'Surface Water'), 'sd_ratio'] = 1

        allo7['sw_allo'] = allo7['total_allo'] * allo7['sd_ratio']
        allo7['gw_allo'] = allo7['total_allo'] - allo7['sw_allo']

        allo8 = allo7.drop(['hydro_group', 'sd_ratio'], axis=1).groupby(pk).mean()

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
            tsdata1 = get_usage_data(self._usage_remote['connection_config'], self._usage_remote['bucket'], waps, self.from_date, self.to_date)
            tsdata1.rename(columns={'water_use': 'total_usage', 'time': 'date'}, inplace=True)

            ## filter - remove individual spikes and negative values
            tsdata1.loc[tsdata1['total_usage'] < 0, 'total_usage'] = 0

            def remove_spikes(x):
                val1 = bool(x[1] > (x[0] + x[2] + 2))
                if val1:
                    return (x[0] + x[2])/2
                else:
                    return x[1]

            tsdata1.iloc[1:-1, 1] = tsdata1['total_usage'].rolling(3, center=True).apply(remove_spikes, raw=True).iloc[1:-1]

            setattr(self, 'usage_ts_daily', tsdata1)

        ### Aggregate
        tsdata2 = tu.grp_ts_agg(tsdata1, 'wap', 'date', self.freq, 'sum')

        setattr(self, 'usage_ts', tsdata2)


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


    def get_ts(self, datasets, freq, groupby, usage_allo_ratio=2):
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
                for d in param.temp_datasets:
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

        if 'A' in freq_agg:
            all2 = util.grp_ts_agg(pd.concat(all1, axis=1).reset_index(), ['permit_id', 'wap'], 'date', freq_agg).sum().reset_index()
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
        sites_col = [c for c in cols if c in self.sites.columns]
        allo_col = [c for c in cols if c in self.allo.columns]

        data1 = data.copy()

        if sites_col:
            all_sites_col = ['wap']
            all_sites_col.extend(sites_col)
            data1 = pd.merge(data1, self.sites.reset_index()[all_sites_col], on='wap')
        if allo_col:
            all_allo_col = ['permit_id']
            all_allo_col.extend(allo_col)
            data1 = pd.merge(data1, self.allo.reset_index()[all_allo_col], on=all_allo_col)

        data1.set_index(pk, inplace=True)

        return data1

