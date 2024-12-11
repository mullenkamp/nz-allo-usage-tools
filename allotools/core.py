# -*- coding: utf-8 -*-
"""
Created on Sat Feb 16 09:50:42 2019

@author: michaelek
"""
import os
import numpy as np
import pandas as pd
# import yaml
# from data_io import get_permit_data, get_usage_data, allo_filter

# from allotools.plot import plot_group as pg
# from allotools.plot import plot_stacked as ps
# from datetime import datetime
from nz_stream_depletion import SD
# from tethys_data_models import permit
from gistools import vector
# from scipy.special import erfc
# import tethysts

from allotools.data_io import get_usage_data, allo_filter
# from data_io import get_usage_data, allo_filter

from allotools.allocation_ts import allo_ts
# from allocation_ts import allo_ts

from allotools.utils import grp_ts_agg
# from utils import grp_ts_agg


# from matplotlib.pyplot import show

#########################################
### parameters

# base_path = os.path.realpath(os.path.dirname(__file__))

# with open(os.path.join(base_path, 'parameters.yml')) as param:
#     param = yaml.safe_load(param)

pk = ['permit_id', 'wap', 'date']
dataset_types = ['allo', 'metered_allo',  'usage', 'usage_est', 'sd_rates']
allo_type_dict = {'D': 'max_daily_volume', 'W': 'max_daily_volume', 'M': 'max_annual_volume', 'A-JUN': 'max_annual_volume', 'A': 'max_annual_volume'}
# allo_mult_dict = {'D': 0.001*24*60*60, 'W': 0.001*24*60*60*7, 'M': 0.001*24*60*60*30, 'A-JUN': 0.001*24*60*60*365, 'A': 0.001*24*60*60*365}


#######################################
### Testing

# temp_datasets = ['allo_ts', 'total_allo_ts', 'wap_allo_ts', 'usage_ts', 'metered_allo_ts']

# permit_id = 'ATH-1970006239.00'
# wap = '4868c3083810c5036b95363d'

# permits_path = '/home/mike/git/HRC-flow-nat/data/permits.blt'
# usage_path = '/home/mike/git/HRC-flow-nat/data/abstraction_data_daily.blt'

# freq = 'D'
# proportion_allo=True

# from_date = '2004-07-01'
# to_date = '2024-06-30'

# datasets = ['allo', 'metered_allo', 'usage', 'usage_est']
# groupby = ['permit_id', 'wap']

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

# def get_usage_data(remote, waps, from_date=None, to_date=None, threads=30):
#     """

#     """
#     obj1 = tethysts.utils.get_object_s3(**remote)
#     wu1 = tethysts.utils.read_pkl_zstd(obj1, True)
#     wu1['ref'] = wu1['ref'].astype(str)
#     wu1.rename(columns={'ref': 'wap'}, inplace=True)

#     if isinstance(from_date, (str, pd.Timestamp)):
#         wu1 = wu1[wu1['time'] >= pd.Timestamp(from_date)].copy()
#     if isinstance(to_date, (str, pd.Timestamp)):
#         wu1 = wu1[wu1['time'] <= pd.Timestamp(to_date)].copy()

#     return wu1

########################################
### Core class


class AlloUsage(object):
    """
    Class to to process the allocation and usage data in NZ.

    Parameters
    ----------
    permits_path : str or pathlib.Path
        Path to booklet file structured according to the nzpermits package.
    usage_path : str or pathlib.Path
        Path to booklet file structured with the keys as wap/station id as pandas dataframes with the columns 'time' and {station_id}.
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
    use_type_mapping : dict
        Dict mapping of the detailed use types to more generic use types. This is used during the usage estimation process and can be mapped to most anything. The the fewer the use types the better.
    default_sd_ratio : float
        The default stream depletion ratio if no GW aquifer data is supplied AT ALL. 

    Returns
    -------
    AlloUsage object
        with all of the base sites, allo, and allo_wap DataFrames

    """
    dataset_types = dataset_types
    # plot_group = pg
    # plot_stacked = ps

    # _usage_remote = param['remote']['usage']
    # _permit_remote = param['remote']['permit']

    ### Initial import and assignment function
    def __init__(self, permits_path, usage_path, from_date=None, to_date=None, permit_filter=None, wap_filter=None, only_consumptive=True, include_hydroelectric=False, use_type_mapping={}, default_sd_ratio=0.35):
        """
        Parameters
        ----------
        permits_path : str or pathlib.Path
            Path to booklet file structured according to the nzpermits package/model.
        usage_path : str or pathlib.Path
            Path to booklet file structured with the keys as wap/station id as pandas dataframes with the columns 'time' and {station_id}.
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
        use_type_mapping : dict
            Dict mapping of the detailed use types to more generic use types. This is used during the usage estimation process and can be mapped to most anything. The the fewer the use types the better.
        default_sd_ratio : float
            The default stream depletion ratio if no GW aquifer data is supplied AT ALL. 

        Returns
        -------
        AlloUsage object
            with all of the base sites, allo, and allo_wap DataFrames

        """
        self.usage_path = usage_path
        self.default_sd_ratio = default_sd_ratio

        self.process_permits(permits_path, from_date, to_date, permit_filter, wap_filter, only_consumptive, include_hydroelectric, use_type_mapping)

        ## Recalculate the ratios
        # self._calc_sd_ratios()


    def process_permits(self, permits_path, from_date=None, to_date=None, permit_filter=None, wap_filter=None, only_consumptive=True, include_hydroelectric=False, use_type_mapping={}):
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
        # permits0 = get_permit_data(self._permit_remote)

        waps, permits = allo_filter(permits_path, from_date, to_date, permit_filter=permit_filter, wap_filter=wap_filter, only_consumptive=only_consumptive, include_hydroelectric=include_hydroelectric, use_type_mapping=use_type_mapping)

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
        setattr(self, 'from_date', from_date1)
        setattr(self, 'to_date', to_date1)

        ## Recalculate the ratios
        self._calc_sd_ratios()


    def _est_allo_ts(self, freq):
        """

        """
        ### Run the allocation time series creation
        limit_col = allo_type_dict[freq]
        allo4 = allo_ts(self.permits, self.from_date, self.to_date, freq, limit_col).round()
        allo4.name = 'total_allo'

        setattr(self, 'total_allo_ts', allo4.reset_index())


    # @staticmethod
    # def _prep_aquifer_data(series, all_params):
    #     """

    #     """
    #     v1 = series.dropna().to_dict()
    #     v2 = permit.AquiferProp(**{k: v for k, v in v1.items() if k in all_params}).dict(exclude_none=True)

    #     return v2


    def _calc_sd_ratios(self):
        """

        """
        if 'sep_distance' in self.waps.columns:
            waps1 = self.waps.dropna(subset=['sep_distance', 'pump_aq_trans', 'pump_aq_s', 'stream_depletion_ratio'], how='all').set_index(['permit_id', 'wap']).copy()

            sd = SD()

            all_params = set()

            _ = [all_params.update(p) for p in sd.all_methods.values()]

            sd_list = []

            for i, v in waps1.iterrows():

                if np.isnan(v['sep_distance']) or np.isnan(v['pump_aq_trans']) or np.isnan(v['pump_aq_s']):
                    if 'stream_depletion_ratio' in v:
                        d1 = list(i)
                        d1.extend([round(v['stream_depletion_ratio'], 3)])
                        sd_ratio2 = pd.DataFrame([d1], columns=['permit_id', 'wap', 'sd_ratio'])
                        sd_list.append(sd_ratio2)
                else:
                    # v2 = self._prep_aquifer_data(v, all_params)
                    v1 = v.dropna().to_dict()
                    v2 = {k: v for k, v in v1.items() if k in all_params}
                    n_days = int(v['n_days'])
                    method = v['method']

                    avail = sd.load_aquifer_data(**v2)

                    if method in avail:
                        sd_ratio1 = sd.calc_sd_ratio(n_days, method)
                    else:
                        sd_ratio1 = sd.calc_sd_ratio(n_days)

                    d1 = list(i)
                    d1.extend([round(sd_ratio1, 3)])

                    sd_ratio2 = pd.DataFrame([d1], columns=['permit_id', 'wap', 'sd_ratio'])
                    sd_list.append(sd_ratio2)

            sd_ratios = pd.concat(sd_list)

            waps2 = pd.merge(self.waps, sd_ratios, on=['permit_id', 'wap'], how='left')
        else:
            ## Alternative until we get data
            waps2 = self.waps.copy()
            waps2['sd_ratio'] = 1
            waps2.loc[waps2.permit_id.isin(self.permits.loc[self.permits.hydro_feature == 'groundwater', 'permit_id'].unique()), 'sd_ratio'] = self.default_sd_ratio

        setattr(self, 'waps', waps2)


    def _allo_wap_spit(self):
        """

        """
        allo6 = pd.merge(self.total_allo_ts, self.waps[['permit_id', 'wap', 'sd_ratio']], on=['permit_id'])
        # allo6 = pd.merge(allo5, self.sd, on=['permit_id', 'wap'], how='left')

        allo6['combo_wap_allo'] = allo6.groupby(['permit_id', 'hydro_feature', 'date'])['total_allo'].transform('sum')
        allo6['combo_wap_ratio'] = allo6['total_allo']/allo6['combo_wap_allo']

        allo6['wap_allo'] = allo6['total_allo'] * allo6['combo_wap_ratio']

        allo7 = allo6.drop(['combo_wap_allo', 'combo_wap_ratio', 'total_allo'], axis=1).rename(columns={'wap_allo': 'total_allo'}).copy()

        ## Calculate the stream depletion
        allo7.loc[allo7.sd_ratio.isnull() & (allo7.hydro_feature == 'groundwater'), 'sd_ratio'] = 0
        allo7.loc[allo7.sd_ratio.isnull() & (allo7.hydro_feature == 'surface water'), 'sd_ratio'] = 1

        allo7['sw_allo'] = allo7['total_allo'] * allo7['sd_ratio']
        allo7['gw_allo'] = allo7['total_allo']
        allo7.loc[allo7['hydro_feature'] == 'surface water', 'gw_allo'] = 0

        allo8 = allo7.drop(['hydro_feature', 'sd_ratio'], axis=1).groupby(pk).mean()

        setattr(self, 'wap_allo_ts', allo8)


    def _get_allo_ts(self, freq):
        """
        Function to create an allocation time series.

        """
        self._est_allo_ts(freq)

        ### Convert to GW and SW allocation
        self._allo_wap_spit()


    def _get_usage(self, freq):
        """

        """
        self._get_allo_ts(freq)
        allo1 = self.wap_allo_ts.copy().reset_index()

        waps = allo1.wap.unique().tolist()

        tsdata1 = get_usage_data(self.usage_path, waps, self.from_date, self.to_date)
        tsdata1.rename(columns={'water_use': 'total_usage', 'time': 'date'}, inplace=True)

        tsdata1 = tsdata1[['wap', 'date', 'total_usage']].sort_values(['wap', 'date']).copy()

        ## Create the data quality series
        qa = tsdata1.rename(columns={'total_usage': 'quality_code'}).copy()
        qa['quality_code'] = 0
        qa['quality_code'] = qa['quality_code'].astype('int16')
        qa = qa.set_index(['wap', 'date'])['quality_code'].copy()

        ## filter - remove negative values (spikes are too hard with only usage data)
        neg_bool = tsdata1['total_usage'] < 0
        qa.loc[neg_bool.values] = 1
        tsdata1.loc[neg_bool, 'total_usage'] = 0

        setattr(self, 'usage_ts_daily', tsdata1)
        setattr(self, 'usage_ts_daily_qa', qa)


    def _agg_usage(self, freq):
        """

        """
        if not hasattr(self, 'usage_ts_daily'):
            self._get_usage(freq)
        tsdata1 = self.usage_ts_daily

        ### Aggregate
        tsdata2 = grp_ts_agg(tsdata1, 'wap', 'date', freq, 'sum')

        setattr(self, 'usage_ts', tsdata2)


    def _usage_estimation(self, freq, buffer_dis=80000, min_months=36):
        """

        """
        ### Get the necessary data
        if freq in ('D', 'W', 'M'):
            allo_use1 = self.get_ts(['allo', 'metered_allo', 'usage'], 'M', ['permit_id', 'wap'])
        else:
            allo_use0 = self.get_ts(['allo', 'metered_allo', 'usage'], freq, ['permit_id', 'wap'])
            allo_use1 = allo_use0.reset_index().groupby(['permit_id', 'wap', pd.Grouper(key='date', freq='M')]).sum()

            del allo_use0

        permits = self.permits.copy()

        ### Create Wap locations
        waps1 = vector.xy_to_gpd('wap', 'lon', 'lat', self.waps.drop('permit_id', axis=1).drop_duplicates('wap'), 4326)
        waps2 = waps1.to_crs(2193)

        ### Get base data
        bool1 = allo_use1['total_metered_allo'] <  (allo_use1['total_allo']*0.5)
        allo_use_mis1 = allo_use1[bool1].copy().reset_index()
        allo_use_with1 = allo_use1[~bool1].copy().reset_index()

        ### Calc ratios
        allo_use_with2 = pd.merge(allo_use_with1, permits[['permit_id', 'use_type']], on='permit_id')

        allo_use_with2['month'] = allo_use_with2['date'].dt.month
        allo_use_with2['usage_allo'] = allo_use_with2['total_usage']/allo_use_with2['total_allo']

        allo_use_ratio1 = allo_use_with2.groupby(['permit_id', 'wap', 'use_type', 'month'])['usage_allo'].mean().reset_index()

        ### Assign ratios to consents/waps that already have data
        allo_use_mis1['month'] = allo_use_mis1['date'].dt.month

        allo_use_mis0 = pd.merge(allo_use_mis1[['permit_id', 'wap', 'month', 'date', 'total_allo', 'sw_allo', 'gw_allo']], allo_use_ratio1, on=['permit_id', 'wap', 'month']).drop(['month', 'use_type'], axis=1)

        allo_use_mis0['total_usage_est'] = (allo_use_mis0['usage_allo'] * allo_use_mis0['total_allo']).round()
        allo_use_mis0['sw_allo_usage_est'] = (allo_use_mis0['usage_allo'] * allo_use_mis0['sw_allo']).round()
        allo_use_mis0['gw_allo_usage_est'] = (allo_use_mis0['usage_allo'] * allo_use_mis0['gw_allo']).round()

        ### Determine which Waps need to be estimated
        mis_waps1 = allo_use_mis1.groupby(['permit_id', 'wap'])['total_allo'].count().copy()
        with_waps1 = allo_use_with1.groupby(['permit_id', 'wap'])['total_allo'].count()
        with_waps2 = with_waps1[with_waps1 >= min_months]

        with_waps3 = pd.merge(with_waps2.reset_index()[['permit_id', 'wap']], permits[['permit_id', 'use_type']], on='permit_id')

        with_waps4 = pd.merge(waps2, with_waps3['wap'], on='wap')

        mis_waps2 = pd.merge(mis_waps1.reset_index(), permits[['permit_id', 'use_type']], on='permit_id')
        mis_waps3 = pd.merge(waps2, mis_waps2['wap'], on='wap')
        mis_waps3['geometry'] = mis_waps3['geometry'].buffer(buffer_dis)

        mis_waps4, poly1 = vector.pts_poly_join(with_waps4.rename(columns={'wap': 'good_wap'}), mis_waps3, 'wap')

        allo_use_ratio2 = pd.merge(allo_use_ratio1.rename(columns={'wap': 'good_wap'}), mis_waps4[['good_wap', 'wap']], on='good_wap')

        ## Combine with the missing ones
        allo_use_mis2 = pd.merge(allo_use_mis1[['permit_id', 'wap', 'date']], permits[['permit_id', 'use_type']], on='permit_id')
        allo_use_mis2['month'] = allo_use_mis2['date'].dt.month

        allo_use_mis3 = pd.merge(allo_use_mis2, allo_use_ratio2[['use_type', 'month', 'usage_allo', 'wap']], on=['use_type', 'wap', 'month'])
        allo_use_mis4 = allo_use_mis3.groupby(['permit_id', 'wap', 'date'])['usage_allo'].mean().reset_index()

        allo_use_mis5 = pd.merge(allo_use_mis4, allo_use_mis1[['permit_id', 'wap', 'date', 'total_allo', 'sw_allo', 'gw_allo']], on=['permit_id', 'wap', 'date'])

        allo_use_mis5['total_usage_est'] = (allo_use_mis5['usage_allo'] * allo_use_mis5['total_allo']).round()
        allo_use_mis5['sw_allo_usage_est'] = (allo_use_mis5['usage_allo'] * allo_use_mis5['sw_allo']).round()
        allo_use_mis5['gw_allo_usage_est'] = (allo_use_mis5['usage_allo'] * allo_use_mis5['gw_allo']).round()

        allo_use_mis5b = allo_use_mis5[['permit_id', 'wap', 'date', 'total_usage_est', 'sw_allo_usage_est', 'gw_allo_usage_est']].copy()

        ## Combine with the eariler estimates
        allo_use_mis6 = pd.concat([allo_use_mis0.drop(['total_allo', 'sw_allo', 'gw_allo', 'usage_allo'], axis=1), allo_use_mis5b]).drop_duplicates(['permit_id', 'wap', 'date'], keep='first')

        ### Convert to daily if required
        if freq == 'D':
            days1 = allo_use_mis6.date.dt.daysinmonth
            days2 = pd.to_timedelta((days1/2).round().astype('int32'), unit='D')

            allo_use_mis6['total_usage_est'] = allo_use_mis6['total_usage_est'] / days1
            allo_use_mis6['sw_allo_usage_est'] = allo_use_mis6['sw_allo_usage_est'] / days1
            allo_use_mis6['gw_allo_usage_est'] = allo_use_mis6['gw_allo_usage_est'] / days1

            usage_rate0 = allo_use_mis6.copy()

            usage_rate0['date'] = usage_rate0['date'] - days2

            grp1 = allo_use_mis6.groupby(['permit_id', 'wap'])
            first1 = grp1.first()
            last1 = grp1.last()

            first1['date'] = pd.to_datetime(first1.loc[:, 'date'].dt.strftime('%Y-%m') + '-01')

            usage_rate1 = pd.concat([first1, usage_rate0.set_index(['permit_id', 'wap']), last1], sort=True).reset_index().sort_values(['permit_id', 'wap', 'date'])

            usage_rate1.set_index('date', inplace=True)

            usage_daily_rate1 = usage_rate1.groupby(['permit_id', 'wap']).apply(lambda x: x.resample('D').interpolate(method='pchip')[['total_usage_est', 'sw_allo_usage_est', 'gw_allo_usage_est']]).round(2)
        else:
            usage_daily_rate1 = allo_use_mis6.set_index(['permit_id', 'wap', 'date'])

        ## Put the actual usage back into the estimate
        act_use1 = self.get_ts(['usage'], freq, ['permit_id', 'wap'])

        combo1 = pd.concat([usage_daily_rate1, act_use1], axis=1).sort_index()
        combo1.loc[combo1['total_usage'].notnull(), 'total_usage_est'] = combo1.loc[combo1['total_usage'].notnull(), 'total_usage']
        combo1.loc[combo1['sw_allo_usage'].notnull(), 'sw_allo_usage_est'] = combo1.loc[combo1['sw_allo_usage'].notnull(), 'sw_allo_usage']
        combo1.loc[combo1['gw_allo_usage'].notnull(), 'gw_allo_usage_est'] = combo1.loc[combo1['gw_allo_usage'].notnull(), 'gw_allo_usage']
        combo1.drop(['total_usage', 'sw_allo_usage', 'gw_allo_usage'], axis=1, inplace=True)

        setattr(self, 'usage_est', combo1)

        return combo1


    def _calc_sd_rates(self, usage_allo_ratio=2, buffer_dis=80000, min_months=36):
        """

        """
        usage_est = self.get_ts(['usage_est'], 'D', ['permit_id', 'wap'], usage_allo_ratio=usage_allo_ratio, buffer_dis=buffer_dis, min_months=min_months)['total_usage_est']
        usage_est.name = 'sd_rate'

        ## SD groundwater takes
        sd_list = []
        if 'sep_distance' in self.waps.columns:
            usage_index = usage_est.index.droplevel(2).unique()
    
            waps1 = self.waps.dropna(subset=['sep_distance', 'pump_aq_trans', 'pump_aq_s']).set_index(['permit_id', 'wap']).copy()
    
            sd = SD()
    
            all_params = set()
    
            _ = [all_params.update(p) for p in sd.all_methods.values()]
    
            for i, v in waps1.iterrows():
                if i in usage_index:
                    use1 = usage_est.loc[i]
    
                    v2 = self._prep_aquifer_data(v, all_params)
                    # n_days = int(v['n_days'])
                    method = v['method']
    
                    avail = sd.load_aquifer_data(**v2)
    
                    if method in avail:
                        sd_rates1 = sd.calc_sd_extraction(use1, method)
                    else:
                        sd_rates1 = sd.calc_sd_extraction(use1)
    
                    sd_rates1.name = 'sd_rate'
    
                    sd_rates1 = sd_rates1.reset_index()
                    sd_rates1['permit_id'] = i[0]
                    sd_rates1['wap'] = i[1]
    
                    sd_list.append(sd_rates1)

        ## SW takes
        sw_permits = self.permits[self.permits.hydro_feature == 'surface water'].permit_id.unique()
        sw_permits_bool = usage_est.index.get_level_values(0).isin(sw_permits)

        sw_usage = usage_est.loc[sw_permits_bool].reset_index()

        sd_list.append(sw_usage)

        sd_rates2 = pd.concat(sd_list)

        sd_rates3 = sd_rates2.groupby(pk).mean()

        setattr(self, 'sd_rates_daily', sd_rates3)


    def _agg_sd_rates(self, freq, usage_allo_ratio=2, buffer_dis=40000, min_months=36):
        """

        """
        if not hasattr(self, 'sd_rates_daily'):
            self._calc_sd_rates(usage_allo_ratio, buffer_dis, min_months)
        tsdata1 = self.sd_rates_daily.reset_index()

        tsdata2 = grp_ts_agg(tsdata1, ['permit_id', 'wap'], 'date', freq, 'sum')

        setattr(self, 'sd_rates', tsdata2)

        return tsdata2


    def _split_usage_ts(self, freq, usage_allo_ratio=2):
        """

        """
        ### Get the usage data if it exists
        self._agg_usage(freq)
        tsdata2 = self.usage_ts.copy().reset_index()

        self._get_allo_ts(freq)
        allo1 = self.wap_allo_ts.copy().reset_index()

        allo1['combo_allo'] = allo1.groupby(['wap', 'date'])['total_allo'].transform('sum')
        allo1['combo_ratio'] = allo1['total_allo']/allo1['combo_allo']

        ### combine with consents info
        usage1 = pd.merge(allo1, tsdata2, on=['wap', 'date'])
        usage1['total_usage'] = usage1['total_usage'] * usage1['combo_ratio']

        ### Remove high outliers
        excess_usage_bool = usage1['total_usage'] > (usage1['total_allo'] * usage_allo_ratio)
        usage1.loc[excess_usage_bool, 'total_usage'] = np.nan
        qa_cols = pk.copy()
        qa_cols.append('total_usage')
        qa = usage1[qa_cols].set_index(pk)['total_usage'].copy()
        qa.loc[:] = 0
        qa = qa.astype('int16')
        qa.loc[excess_usage_bool.values] = 1

        ### Split the GW and SW components
        usage1['sw_ratio'] = usage1['sw_allo']/usage1['total_allo']
        usage1['gw_ratio'] = usage1['gw_allo']/usage1['total_allo']
        usage1['sw_allo_usage'] = usage1['sw_ratio'] * usage1['total_usage']
        usage1['gw_allo_usage'] = usage1['gw_ratio'] * usage1['total_usage']
        usage1.loc[usage1['gw_allo_usage'] < 0, 'gw_allo_usage'] = 0

        ### Remove other columns
        usage1.drop(['sw_allo', 'gw_allo', 'total_allo', 'combo_allo', 'combo_ratio', 'sw_ratio', 'gw_ratio'], axis=1, inplace=True)

        usage2 = usage1.dropna().groupby(pk).mean()

        setattr(self, 'split_usage_ts', usage2)
        setattr(self, 'split_usage_ts_qa', qa)


    def _get_metered_allo_ts(self, freq, proportion_allo=True):
        """

        """
        setattr(self, 'proportion_allo', proportion_allo)

        ### Get the allocation ts either total or metered
        self._get_allo_ts(freq)
        allo1 = self.wap_allo_ts.copy().reset_index()
        rename_dict = {'sw_allo': 'sw_metered_allo', 'gw_allo': 'gw_metered_allo', 'total_allo': 'total_metered_allo'}

        ### Combine the usage data to the allo data
        if not hasattr(self, 'split_usage_ts'):
            self._split_usage_ts(freq)
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

        ### Get the results and combine
        all1 = []

        if 'allo' in datasets:
            self._get_allo_ts(freq)
            all1.append(self.wap_allo_ts)
        if 'metered_allo' in datasets:
            self._get_metered_allo_ts(freq)
            all1.append(self.metered_allo_ts)
        if 'usage' in datasets:
            self._split_usage_ts(freq, usage_allo_ratio)
            all1.append(self.split_usage_ts)
        if 'usage_est' in datasets:
            usage_est = self._usage_estimation(freq, buffer_dis, min_months)
            all1.append(usage_est)
        if 'sd_rates' in datasets:
            sd_rates = self._agg_sd_rates(freq, usage_allo_ratio, buffer_dis, min_months)
            all1.append(sd_rates)

        all2 = pd.concat(all1, axis=1)

        if 'total_allo' in all2:
            all2 = all2[all2['total_allo'].notnull()].copy()

        if not np.in1d(groupby, pk).all():
            all2 = self._merge_extra(all2, groupby)

        all3 = all2.replace(np.nan, np.inf).groupby(groupby).sum().replace(np.inf, np.nan)
        all3.name = 'results'

        return all3


    def _merge_extra(self, data, cols):
        """

        """
        allo_col = [c for c in cols if c in self.permits.columns]

        data1 = data.copy()

        if allo_col:
            all_allo_col = ['permit_id']
            all_allo_col.extend(allo_col)
            data1 = pd.merge(data1.reset_index(), self.permits[all_allo_col], on=all_allo_col)

        data1.set_index(pk, inplace=True)

        return data1
