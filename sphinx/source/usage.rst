How to use EcanAlloUsageTools
=============================

This section will describe how to use the EcanAlloUsageTools package. Nearly all result outputs are Pandas DataFrames.

Get time series data
--------------------
The most common use case is to extract a variety of time series data in the form of allocation, metered allocation, lowflow restricted allocation, lowflow restricted metered allocation, and usage datasets. All numeric results returned have the units of m^3.

First, you will need to know which of the above datasets you want.
The associated dataset codes are the following:
allocation = allo
metered allocation = metered_allo
lowflow restricted allocation = restr_allo
lowflow restricted metered allocation = metered_restr_allo
usage = usage

Please see :doc:`package_references` for all possible input parameters and filters.

Example:

.. code:: python

  import pandas as pd
  from allotools import AlloUsage

  pd.options.display.max_columns = 10

  # Parameters
  from_date = '2015-07-01'
  to_date = '2018-06-30'

  datasets = ['allo', 'restr_allo', 'metered_allo', 'metered_restr_allo', 'usage']
  freq = 'A-JUN'
  groupby = ['crc', 'wap', 'date']
  site_filter = {'CatchmentGroupName': ['Ashburton River']}

  export_path = r'E:\allousagetest'

  # Time series extraction
  a1 = AlloUsage(from_date, to_date, site_filter=site_filter)

  ts1 = a1.get_ts(datasets, freq, groupby, usage_allo_ratio=10).round()

  # Plotting
  a1.plot_group('A-JUN', val='total', group='crc', with_restr=True, export_path=export_path)

  a1.plot_stacked('A-JUN', val='total', export_path=export_path)
