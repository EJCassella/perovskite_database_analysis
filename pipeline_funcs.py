import pandas as pd
import numpy as np
import os
import logging
import regex as re
import sys

from utils import setup_logger


from cleaning_funcs import drop_stability_columns, drop_cols_with_50perc_missing_data, drop_reference_cols, drop_null_cells_modules, remove_metrics_outliers, reduce_cardinality_perovskite_deposition, merge_TS80m_data


logger = setup_logger()


def extract(database_file):
  try:
    dataframe = pd.read_csv(database_file, low_memory=False, index_col='Ref_ID', na_values=['Unknown'])
    logger.info('File read OK.')
    logger.info(f'Dataframe shape is {dataframe.shape}')
    return dataframe
  except:
    logger.error('File could not be read.')
    sys.exit()
  


def transform(dataframe):
  try:
    initial_shape = dataframe.shape
    dataframe = drop_stability_columns(dataframe)
    dataframe = drop_cols_with_50perc_missing_data(dataframe)
    dataframe = drop_reference_cols(dataframe)
    dataframe = drop_null_cells_modules(dataframe)
    dataframe = remove_metrics_outliers(dataframe, 'JV_default_PCE', 'JV_default_FF', 'JV_default_Jsc', 'JV_default_Voc')
    dataframe = reduce_cardinality_perovskite_deposition(dataframe)
    dataframe = merge_TS80m_data(dataframe, ts80_datafile='TS80m_datam_accessed_2025-01-22.csv')
    dataframe = remove_metrics_outliers(dataframe, 'TS80m')

    try:
      assert dataframe.shape[0] == initial_shape[0]
      logger.info(f'Dataframe entry count is still {dataframe.shape[0]} but now with {dataframe.shape[1]} columns.')
      logger.info(f'Removed {initial_shape[1] - dataframe.shape[1]} columns.')
    except:
      logger.warning(f'Entry count in dataframe has changed to {dataframe.shape[0]} from {initial_shape[0]} now with {dataframe.shape[1]} columns.')

    return dataframe
  except:
    logger.error('Dataframe not available for transformation.')
    sys.exit()



def load(cleaned_dataframe):
  try:
    cleaned_dataframe.to_csv('cleaned_perovskite_database_data.csv')
    logger.info('Cleaned data successfully saved.')
  except:
    logger.error('Cleaned data could not be saved.')