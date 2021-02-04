# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 16:40:35 2018

@author: michaelek
"""
import io
import os
import yaml
import pandas as pd
import tethys_utils as tu
from tethysts import Tethys
from tethysts import utils
# from multiprocessing.pool import ThreadPool

pd.options.display.max_columns = 10

#########################################
### parameters

base_path = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_path, 'parameters.yml')) as param:
    param = yaml.safe_load(param)

use_type_dict = param['input']['use_type_dict']

# use_type_dict = {'Dairying - Cows': 'irrigation', 'Water Supply - Rural': 'water_supply', 'Pasture Irrigation': 'irrigation', 'Crop Irrigation': 'irrigation', 'Stock Yard': 'stockwater', 'Water Supply - Town': 'water_supply', 'Quarrying': 'other', 'Recreational': 'other', 'Gravel extraction': 'other', 'Hydro-electric power generation': 'hydro_electric', 'Food Processing': 'other', 'Meat works': 'other', 'Tourism': 'other', 'Mining works': 'other', 'Industrial': 'other', 'Domestic': 'water_supply', 'Timber Processing incl Sawmills': 'other', 'Peat Harvesting/Processing': 'other', 'Milk and dairy industries': 'other', 'Gravel Wash': 'other', 'Carwash': 'other', 'Contaminated land earthworks': 'other', 'Dairying - Sheep': 'irrigation', 'Fertiliser Work': 'other', 'Fish Processing': 'other', 'Fisheries and wildlife habitat/control': 'other', 'Food Processing': 'other', 'Gravel Wash': 'other', 'Gravel extraction': 'other', 'Horticulture Irrigation': 'irrigation', 'Landfill and transfer stations': 'other', 'Manufacturing': 'other', 'River control': 'other', 'Slink Skins': 'other', 'Stockwater': 'stockwater', 'Transport': 'other', 'Truckwash': 'other'}

#########################################
### Functions


def get_permit_data(connection_config, bucket, permits_key):
    """

    """
    obj1 = utils.get_object_s3(permits_key, connection_config, bucket)
    permits = tu.read_json_zstd(obj1)

    return permits


def get_usage_data(connection_config, bucket, waps, from_date=None, to_date=None, threads=30):
    """

    """
    remote = [{'bucket': bucket, 'connection_config': connection_config}]
    t1 = Tethys(remote)

    stns_all = []

    ## Surface Water
    sw_ds = [ds for ds in t1.datasets if ds['feature'] == 'waterway'][0]
    sw_ds_id = sw_ds['dataset_id']
    sw_stns = t1.get_stations(sw_ds_id)
    sw_stns1 = [s for s in sw_stns if s['ref'] in waps]

    if sw_stns1:
        sw_waps = [s['station_id'] for s in sw_stns1]

        sw_data = t1.get_bulk_results(sw_ds_id, sw_waps, from_date=from_date, to_date=to_date, output='Dataset', threads=threads)

        sw_data_list = []
        for k, val in sw_data.items():
            val1 = val.squeeze('height').drop_vars('height')
            wap_id = str(val1['ref'].values)
            val2 = val1['water_use'].to_dataframe().reset_index()
            val2['wap'] = wap_id
            sw_data_list.append(val2)

        sw_data2 = pd.concat(sw_data_list)

        stns_all.extend(sw_stns1)

    else:
        sw_data2 = pd.DataFrame(columns=['time', 'wap', 'water_use'])

    ## Groundwater
    gw_ds = [ds for ds in t1.datasets if ds['feature'] == 'groundwater'][0]
    gw_ds_id = gw_ds['dataset_id']
    gw_stns = t1.get_stations(gw_ds_id)
    gw_stns1 = [s for s in gw_stns if s['ref'] in waps]

    if gw_stns1:
        gw_waps = [s['station_id'] for s in gw_stns1]

        gw_data = t1.get_bulk_results(gw_ds_id, gw_waps, from_date=from_date, to_date=to_date, output='Dataset', threads=30)

        gw_data_list = []
        for k, val in gw_data.items():
            val1 = val.squeeze('height').drop_vars('height')
            wap_id = str(val1['ref'].values)
            val2 = val1['water_use'].to_dataframe().reset_index()
            val2['wap'] = wap_id
            gw_data_list.append(val2)

        gw_data2 = pd.concat(gw_data_list)

        stns_all.extend(gw_stns1)

    else:
        gw_data2 = pd.DataFrame(columns=['time', 'wap', 'water_use'])

    all_data = pd.concat([sw_data2, gw_data2])
    all_data['time'] = all_data['time'] + pd.DateOffset(hours=12)

    return all_data, stns_all


def allo_filter(permits_dict, from_date=None, to_date=None, permit_filter=None, wap_filter=None, only_consumptive=True, include_hydroelectric=False):
    """
    Function to filter consents and WAPs in various ways.

    Parameters
    ----------
    server : str
        The server of the Hydro db.
    from_date : str
        The start date for the time series.
    to_date: str
        The end date for the time series.
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
    Three DataFrames
        Representing the filters on the ExternalSites, CrcAllo, and CrcWapAllo
    """
    ### Process the premits dict into the three dataframes
    waps0 = []
    permits0 = []

    for p in permits_dict:
        if p['excercised']:
            if p['activity']['activity_type'] == 'consumptive take water':
                p1 = {'permit_id': p['permit_id'], 'hydro_feature': p['activity']['hydro_feature'], 'permit_status': p['status'], 'use_type': p['activity']['primary_purpose'], 'max_rate': p['activity']['condition'][0]['limit'][0]['value'], 'from_date': p['commencement_date']}

                if 'effective_end_date' in p:
                    p1.update({'to_date': p['effective_end_date']})
                else:
                    p1.update({'to_date': p['expiry_date']})

                permits0.extend([p1])

                for s in p['activity']['station']:
                    w1 = {'permit_id': p['permit_id'], 'wap': s['ref'], 'lat': s['geometry']['coordinates'][1], 'lon': s['geometry']['coordinates'][0]}
                    if 'stream_depletion_ratio' in s:
                        w1.update({'sd_ratio': s['stream_depletion_ratio']})
                    waps0.extend([w1])

    waps = pd.DataFrame(waps0)
    permits = pd.DataFrame(permits0)
    permits['from_date'] = pd.to_datetime(permits['from_date'])
    permits['to_date'] = pd.to_datetime(permits['to_date'])

    permits = permits[permits['max_rate'] > 0].copy()
    permits['max_daily_volume'] = permits['max_rate'] *60*60*24*0.001
    permits['max_annual_volume'] = permits['max_rate'] *60*60*24*365*0.001

    ### Waps
    waps_cols = ['permit_id', 'wap', 'lon', 'lat', 'sd_ratio']
    waps1 = waps[waps_cols].copy()

    if isinstance(wap_filter, dict):
        waps_bool1 = [waps1[k].isin(v) for k, v in wap_filter.items()]
        waps_bool2 = pd.concat(waps_bool1, axis=1).prod(axis=1).astype(bool)
        waps1 = waps1[waps_bool2].copy()

    if isinstance(wap_filter, list):
        waps1 = waps1[waps1['wap'].isin(wap_filter)].copy()

    if isinstance(permit_filter, list):
        waps1 = waps1[waps1['permit_id'].isin(permit_filter)].copy()

    ### permits
    permit_cols = ['permit_id', 'hydro_feature', 'permit_status', 'from_date', 'to_date', 'use_type', 'max_rate', 'max_daily_volume', 'max_annual_volume']

    permits1 = permits[permit_cols].copy()

    permits1['use_type'] = permits1.use_type.replace(use_type_dict)

    if isinstance(permit_filter, dict):
        permit_bool1 = [permits1[k].isin(v) for k, v in permit_filter.items()]
        permit_bool2 = pd.concat(permit_bool1, axis=1).prod(axis=1).astype(bool)
        permits1 = permits1[permit_bool2].copy()

    bool1 = True

    # if only_consumptive:
    #     bool1 = permits.exercised & permits.consumptive
    if not include_hydroelectric:
        bool1 = bool1 & (permits1.use_type != 'hydro_electric')

    permits1 = permits1.loc[bool1].copy()

    permits2 = permits1[permits1.permit_id.isin(waps1.permit_id.unique())].copy()
    permits2['from_date'] = pd.to_datetime(permits2['from_date'])
    permits2['to_date'] = pd.to_datetime(permits2['to_date'])

    ## Remove consents without max rates
    permits2['max_rate'] = pd.to_numeric(permits2['max_rate'], errors='coerce')
    permits2 = permits2[permits2.max_rate.notnull()].copy()

    ## Remove consents without to/from dates or date ranges of less than a month
    permits2['from_date'] = pd.to_datetime(permits2['from_date'])
    permits2['to_date'] = pd.to_datetime(permits2['to_date'])
    permits2 = permits2[permits2['from_date'].notnull() & permits2['to_date'].notnull()]

    # Restrict dates
    if isinstance(from_date, str):
        start_time = pd.Timestamp(from_date)
        permits2 = permits2[(permits2['to_date'] - start_time).dt.days > 31]
    if isinstance(to_date, str):
        end_time = pd.Timestamp(to_date)
        permits2 = permits2[(end_time - permits2['from_date']).dt.days > 31]

    permits3 = permits2[(permits2['to_date'] - permits2['from_date']).dt.days > 31].copy()

    ## Calculate rates, daily and annual volumes (if they don't exist)
    # permits3.loc[permits3.max_rate.isnull(), 'max_rate'] = (permits3.loc[permits3.max_rate.isnull(), 'max_daily_volume'] * 1000/60/60/24).round()

    # permits3.loc[permits3.max_daily_volume.isnull(), 'max_daily_volume'] = (permits3.loc[permits3.max_daily_volume.isnull(), 'max_rate'] * 60*60*24/1000).round()

    # permits3.loc[permits3.max_annual_volume.isnull(), 'max_annual_volume'] = (permits3.loc[permits3.max_annual_volume.isnull(), 'max_daily_volume'] * 365).round()

    ## Index by permit_id and hydro_group - keep the largest limits
    limit_cols = ['max_rate', 'max_daily_volume', 'max_annual_volume']
    other_cols = list(permits3.columns[~(permits3.columns.isin(limit_cols) | permits3.columns.isin(['permit_id', 'hydro_feature']))])
    grp1 = permits3.groupby(['permit_id', 'hydro_feature'])
    other_df = grp1[other_cols].first()
    limits_df = grp1[limit_cols].max()

    permits4 = pd.concat([other_df, limits_df], axis=1).reset_index()

    ### sd
    # sd_cols = ['wap', 'sd_ratio']

    # sd1 = sd[sd_cols].copy()

    ### Filter
    # sd2 = sd1[(sd1.permit_id.isin(permits3.permit_id.unique())) & (sd1.wap.isin(waps1.wap.unique()))].copy()

    permits5 = permits4[permits4.permit_id.isin(waps1.permit_id.unique())].copy()
    waps2 = waps1[waps1.permit_id.isin(permits5.permit_id.unique())].copy()

    ### Index the DataFrames
    # permit_id_allo2.set_index(['permit_id', 'hydro_group'], inplace=True)
    # permit_id_wap2.set_index(['permit_id', 'hydro_group', 'wap'], inplace=True)
    # sites2.set_index('wap', inplace=True)

    return waps2, permits5




####################################################
### Testing

# remote = param['remote']
# #
# usage_conn_config = remote['usage']['connection_config']
# permit_conn_config = remote['permit']['connection_config']
# waps_key = remote['permit']['waps_key']
# permits_key = remote['permit']['permits_key']
# sd_key = remote['permit']['sd_key']
# permit_bucket = remote['permit']['bucket']
# usage_bucket = remote['usage']['bucket']
#
#
# waps, permits, sd = get_permit_data(permit_conn_config, permit_bucket, waps_key, permits_key, sd_key)
#
#
# waps, permits, sd = allo_filter(waps, permits, sd)


# waps = ['D45/0069', 'F45/0472', 'E45/0078', 'SW/0019', 'SW/0079']
#
#
# use_data = get_usage_data(connection_config, bucket, waps)
