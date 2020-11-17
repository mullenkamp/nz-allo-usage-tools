# -*- coding: utf-8 -*-
"""
Created on Tue Oct  2 13:17:03 2018

@author: michaelek
"""

#####################################
### Misc parameters for the various functions

server = 'edwprod01'
database = 'hydro'
allo_table = 'CrcAllo'
wap_allo_table = 'CrcWapAllo'
site_table = 'ExternalSite'
ts_summ_table = 'TSDataNumericDailySumm'
ts_table = 'TSDataNumericDaily'
lf_band_table = 'LowFlowRestrSiteBand'
lf_band_crc_table = 'LowFlowRestrSiteBandCrc'

dataset_dict = {9: 'Take Surface Water', 12: 'Take Groundwater'}

sd_dict = {7: 'sd1_7', 30: 'sd1_30', 150: 'sd1_150'}

status_codes = ['Terminated - Replaced', 'Issued - Active', 'Terminated - Surrendered', 'Terminated - Cancelled', 'Terminated - Expired', 'Terminated - Lapsed', 'Issued - s124 Continuance', 'Issued - Inactive']

use_type_dict = {'Aquaculture': 'irrigation', 'Dairy Shed (Washdown/Cooling)': 'stockwater', 'Intensive Farming - Dairy': 'irrigation', 'Intensive Farming - Other (Washdown/Stockwater/Cooling)': 'stockwater', 'Intensive Farming - Poultry': 'irrigation', 'Irrigation - Arable (Cropping)': 'irrigation', 'Irrigation - Industrial': 'irrigation', 'Irrigation - Mixed': 'irrigation', 'Irrigation - Pasture': 'irrigation', 'Irrigation Scheme': 'irrigation' , 'Viticulture': 'irrigation', 'Community Water Supply': 'water_supply', 'Domestic Use': 'water_supply', 'Construction': 'industrial', 'Construction - Dewatering': 'industrial', 'Cooling Water (non HVAC)': 'industrial', 'Dewatering': 'industrial', 'Gravel Extraction/Processing': 'industrial', 'HVAC': 'industrial', 'Industrial Use - Concrete Plant': 'industrial', 'Industrial Use - Food Products': 'industrial', 'Industrial Use - Other': 'industrial', 'Industrial Use - Water Bottling': 'industrial', 'Mining': 'industrial', 'Firefighting ': 'municipal', 'Firefighting': 'municipal', 'Flood Control': 'municipal', 'Landfills': 'municipal', 'Stormwater': 'municipal', 'Waste Water': 'municipal', 'Stockwater': 'stockwater', 'Snow Making': 'industrial', 'Augment Flow/Wetland': 'other', 'Fisheries/Wildlife Management': 'other', 'Other': 'other', 'Recreation/Sport': 'other', 'Research (incl testing)': 'other', 'Power Generation': 'hydroelectric', 'Drainage': 'municipal', 'Frost Protection': 'municipal'}

restr_type_dict = {'max rate': 'max_rate_crc', 'daily volume': 'daily_vol', 'annual volume': 'feav'}

allo_type_dict = {'D': 'daily_vol', 'W': 'daily_vol', 'M': 'feav', 'A-JUN': 'feav', 'A': 'feav'}

freq_codes = ['D', 'W', 'M', 'A-JUN']

dataset_types = ['allo', 'metered_allo', 'restr_allo', 'metered_restr_allo', 'usage']

pk = ['crc', 'take_type', 'allo_block', 'wap', 'date']

crc_wap_cols = set(['crc', 'take_type', 'allo_block', 'wap', 'max_rate_wap', 'in_sw_allo', 'sd1_7', 'sd1_30', 'sd1_150'])

crc_cols = set(['crc', 'take_type', 'allo_block', 'max_rate_crc', 'daily_vol', 'feav', 'crc_status', 'from_date', 'to_date', 'from_month', 'to_month', 'in_gw_allo', 'use_type'])

site_cols = set(['ExtSiteID', 'ExtSiteName', 'NZTMX', 'NZTMY', 'CatchmentName', 'CatchmentNumber', 'CatchmentGroupName', 'CatchmentGroupNumber', 'SwazName', 'SwazGroupName', 'SwazSubRegionalName', 'GwazName', 'CwmsName'])


temp_datasets = ['allo_ts', 'total_allo_ts', 'restr_allo_ts', 'lf_restr', 'usage_crc_ts', 'usage_ts', 'usage_crc_ts', 'metered_allo_ts', 'metered_restr_allo_ts']

#datasets = {'allo': ['total_allo', 'sw_allo', 'gw_allo'],














