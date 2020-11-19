# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 16:40:35 2018

@author: michaelek
"""
import io
import os
import pandas as pd
import tethys_utils as tu
from tethysts import Tethys
# from multiprocessing.pool import ThreadPool

pd.options.display.max_columns = 10

#########################################
### parameters

# base_path = os.path.realpath(os.path.dirname(__file__))
#
# with open(os.path.join(base_path, 'parameters.yml')) as param:
#     param = yaml.safe_load(param)

use_type_dict = {'Dairying - Cows': 'irrigation', 'Water Supply - Rural': 'water_supply', 'Pasture Irrigation': 'irrigation', 'Crop Irrigation': 'irrigation', 'Stock Yard': 'stockwater', 'Water Supply - Town': 'water_supply', 'Quarrying': 'other', 'Recreational': 'other', 'Gravel extraction': 'other', 'Hydro-electric power generation': 'hydro_electric', 'Food Processing': 'other', 'Meat works': 'other', 'Tourism': 'other', 'Mining works': 'other', 'Industrial': 'other', 'Domestic': 'water_supply', 'Timber Processing incl Sawmills': 'other', 'Peat Harvesting/Processing': 'other', 'Milk and dairy industries': 'other', 'Gravel Wash': 'other', 'Carwash': 'other', 'Contaminated land earthworks': 'other', 'Dairying - Sheep': 'irrigation', 'Fertiliser Work': 'other', 'Fish Processing': 'other', 'Fisheries and wildlife habitat/control': 'other', 'Food Processing': 'other', 'Gravel Wash': 'other', 'Gravel extraction': 'other', 'Horticulture Irrigation': 'irrigation', 'Landfill and transfer stations': 'other', 'Manufacturing': 'other', 'River control': 'other', 'Slink Skins': 'other', 'Stockwater': 'stockwater', 'Transport': 'other', 'Truckwash': 'other'}

#########################################
### Functions


def get_permit_data(connection_config, bucket, waps_key, permits_key, sd_key):
    """

    """
    dict1 = {'waps': waps_key, 'permits': permits_key, 'sd': sd_key}
    dict2 = dict1.copy()

    s3 = tu.s3_connection(connection_config)

    for k, key in dict1.items():
        resp = s3.get_object(Bucket=bucket, Key=key)
        obj1 = resp['Body'].read()
        s_io = io.StringIO(obj1.decode())
        csv1 = pd.read_csv(s_io)
        dict2[k] = csv1

    return dict2['waps'], dict2['permits'], dict2['sd']


def get_usage_data(connection_config, bucket, waps, from_date, to_date, threads=30):
    """

    """
    remote = [{'bucket': bucket, 'connection_config': connection_config}]
    t1 = Tethys(remote)

    ## Surface Water
    sw_ds = [ds for ds in t1.datasets if ds['feature'] == 'waterway'][0]
    sw_ds_id = sw_ds['dataset_id']
    sw_stns = t1.get_stations(sw_ds_id)
    sw_waps = [s['station_id'] for s in sw_stns if s['ref'] in waps]

    if sw_waps:
        sw_data = t1.bulk_time_series_results(sw_ds_id, sw_waps, from_date=from_date, to_date=to_date, output='Dataset', threads=threads)

        sw_data_list = []
        for k, val in sw_data.items():
            val1 = val.squeeze('height').drop_vars('height')
            wap_id = str(val1['ref'].values)
            val2 = val1['water_use'].to_dataframe().reset_index()
            val2['wap'] = wap_id
            sw_data_list.append(val2)

        sw_data2 = pd.concat(sw_data_list)

    else:
        sw_data2 = pd.DataFrame(columns=['time', 'wap', 'water_use'])

    ## Groundwater
    gw_ds = [ds for ds in t1.datasets if ds['feature'] == 'groundwater'][0]
    gw_ds_id = gw_ds['dataset_id']
    gw_stns = t1.get_stations(gw_ds_id)
    gw_waps = [s['station_id'] for s in gw_stns if s['ref'] in waps]

    if gw_waps:
        gw_data = t1.bulk_time_series_results(gw_ds_id, gw_waps, from_date=from_date, to_date=to_date, output='Dataset', threads=30)

        gw_data_list = []
        for k, val in gw_data.items():
            val1 = val.squeeze('height').drop_vars('height')
            wap_id = str(val1['ref'].values)
            val2 = val1['water_use'].to_dataframe().reset_index()
            val2['wap'] = wap_id
            gw_data_list.append(val2)

        gw_data2 = pd.concat(gw_data_list)

    else:
        gw_data2 = pd.DataFrame(columns=['time', 'wap', 'water_use'])

    all_data = pd.concat([sw_data2, gw_data2])
    all_data['time'] = all_data['time'] + pd.DateOffset(hours=12)

    return all_data










# def ts_filter(allo, from_date='1900-07-01', to_date='2020-06-30', in_allo=True):
#     """
#     Function to take an allo DataFrame and filter out the consents that cannot be converted to a time series due to missing data.
#     """
#     allo['to_date'] = pd.to_datetime(allo['to_date'], errors='coerce')
#     allo['from_date'] = pd.to_datetime(allo['from_date'], errors='coerce')
#
#     ### Remove consents without daily volumes (and consequently yearly volumes)
#     allo2 = allo1[allo1.daily_vol.notnull()]
#
#     ### Remove consents without to/from dates or date ranges of less than a month
#     allo3 = allo2[allo2['from_date'].notnull() & allo2['to_date'].notnull()]
#
#     ### Restrict dates
#     start_time = pd.Timestamp(from_date)
#     end_time = pd.Timestamp(to_date)
#
#     allo4 = allo3[(allo3['to_date'] - start_time).dt.days > 31]
#     allo5 = allo4[(end_time - allo4['from_date']).dt.days > 31]
#
#     allo5 = allo5[(allo5['to_date'] - allo5['from_date']).dt.days > 31]
#
#     ### Restrict by status_details
#     allo6 = allo5[allo5.permit_status.isin(param.status_codes)]
#
#     ### In allocation columns
#     if in_allo:
#         wap_allo = wap_allo[(wap_allo.hydro_group == 'Surface Water') & (wap_allo.in_sw_allo) | (wap_allo.hydro_group == 'Groundwater')]
#         allo6 = allo6[(allo6.hydro_group == 'Surface Water') | ((allo6.hydro_group == 'Groundwater') & (allo6.in_gw_allo))]
#         allo6 = allo6[(allo6.hydro_group == 'Groundwater') | allo6.permit_id.isin(wap_allo.permit_id.unique())]
#
#     ### Select the permit_id_waps
#     wap_allo2 = pd.merge(wap_allo, allo6[['permit_id', 'hydro_group']], on=['permit_id', 'hydro_group'], how='inner')
#     allo6 = pd.merge(allo6, wap_allo2[['permit_id', 'hydro_group']].drop_duplicates(), on=['permit_id', 'hydro_group'], how='inner')
#
#     ### Return
#     return allo6, wap_allo2


def allo_filter(waps, permits, sd, from_date='1900-01-01', to_date='2100-01-01', only_consumptive=True, include_hydroelectric=False):
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
    site_filter : list or dict
        If site_filter is a list, then it should represent the columns from the ExternalSite table that should be returned. If it's a dict, then the keys should be the column names and the values should be the filter on those columns.
    permit_id_filter : list or dict
        If permit_id_filter is a list, then it should represent the columns from the CrcAllo table that should be returned. If it's a dict, then the keys should be the column names and the values should be the filter on those columns.
    permit_id_wap_filter : list or dict
        If permit_id_wap_filter is a list, then it should represent the columns from the CrcWapAllo table that should be returned. If it's a dict, then the keys should be the column names and the values should be the filter on those columns.
    in_allo : bool
        Should only the consumptive takes be returned?

    Returns
    -------
    Three DataFrames
        Representing the filters on the ExternalSites, CrcAllo, and CrcWapAllo
    """

    ### Waps
    waps_cols = ['permit_id', 'wap', 'lon', 'lat']
    waps1 = waps[waps_cols].copy()

    ### permits
    permit_cols = ['permit_id', 'hydro_group', 'permit_status', 'from_date', 'to_date', 'use_type', 'max_rate', 'max_daily_volume', 'max_annual_volume']

    permits1 = permits[permit_cols].copy()

    permits1['use_type'] = permits1.use_type.replace(use_type_dict)

    bool1 = True

    if only_consumptive:
        bool1 = permits.exercised & permits.consumptive
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
    start_time = pd.Timestamp(from_date)
    end_time = pd.Timestamp(to_date)

    permits2 = permits2[(permits2['to_date'] - start_time).dt.days > 31]
    permits2 = permits2[(end_time - permits2['from_date']).dt.days > 31]
    permits3 = permits2[(permits2['to_date'] - permits2['from_date']).dt.days > 31].copy()

    ## Calculate rates, daily and annual volumes (if they don't exist)
    permits3.loc[permits3.max_rate.isnull(), 'max_rate'] = (permits3.loc[permits3.max_rate.isnull(), 'max_daily_volume'] * 1000/60/60/24).round()

    permits3.loc[permits3.max_daily_volume.isnull(), 'max_daily_volume'] = (permits3.loc[permits3.max_daily_volume.isnull(), 'max_rate'] * 60*60*24/1000).round()

    permits3.loc[permits3.max_annual_volume.isnull(), 'max_annual_volume'] = (permits3.loc[permits3.max_annual_volume.isnull(), 'max_daily_volume'] * 365).round()

    ## Index by permit_id and hydro_group - keep the largest take


    ### sd
    sd_cols = ['permit_id', 'wap', 'wap_max_rate', 'sd_ratio']

    sd1 = sd[sd_cols].copy()
    sd2 = sd1[(sd1.permit_id.isin(permits3.permit_id.unique())) & (sd1.wap.isin(waps1.wap.unique()))].copy()

    ### Index the DataFrames
    # permit_id_allo2.set_index(['permit_id', 'hydro_group'], inplace=True)
    # permit_id_wap2.set_index(['permit_id', 'hydro_group', 'wap'], inplace=True)
    # sites2.set_index('wap', inplace=True)

    return waps1, permits3, sd2




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














