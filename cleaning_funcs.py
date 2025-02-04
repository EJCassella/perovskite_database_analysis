from utils import setup_logger

logger = setup_logger()

def drop_stability_columns(dataframe):
  """Drop the columns relating to device stability as we will use the TS80m data for stability.
  
  """
  dataframe = dataframe.loc[:, :'Stabilised_performance_link_raw_data']
  logger.info('Stability columns dropped successfully.')
  return dataframe


def drop_cols_with_50perc_missing_data(dataframe):
  """Drop columns where more than 50 percent of the data is missing. Handle the exception of module areas which are only empty due to class imbalance.
  
  """
  missing_50_perc = [col for col in dataframe.columns if dataframe[col].isna().sum() / dataframe.shape[0] > 0.5]
  
  #Keep the Module entries, as these are only missing due to the imbalance between cells and modules in the dataset
  try:
    missing_50_perc = [item for item in missing_50_perc if item not in ['Module_area_total', 'Module_area_effective']]
  except ValueError:
    pass

  dataframe = dataframe.drop(labels=missing_50_perc, axis=1)

  logger.info('Columns with more than 50 percent of data missing have been dropped successfully.')
  return dataframe


def drop_reference_cols(dataframe):
  """Drop columns that contain no useful information.
  
  """
  cols_to_drop = [
  'Ref_ID_temp',
  'Ref_name_of_person_entering_the_data',
  'Ref_data_entered_by_author',
  'Ref_lead_author',
  'Ref_part_of_initial_dataset',
  'Ref_original_filename_data_upload'
  ]   
  
  try:
    dataframe = dataframe.drop(labels=cols_to_drop, axis=1)
  except KeyError:
    pass

  logger.info('Unecessary reference columns dropped successfully.')

  return dataframe


