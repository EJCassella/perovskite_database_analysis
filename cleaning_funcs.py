from utils import setup_logger
import regex as re

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


def drop_null_cells_modules(dataframe):
  """Drop rows that we don't know if it's a cell or a module.
  
  """
  rows_to_drop = dataframe[dataframe['Module'].isnull()].index
  logger.info(f'There are {len(rows_to_drop)} rows to drop which do not contain a cell or module type')
  try:
    dataframe = dataframe.drop(index=rows_to_drop, axis=0)
  except KeyError:
    logger.info('There are no entries without cell types, continuing.')
  
  return dataframe


def remove_metrics_outliers(dataframe, whisker_size=1.5):
  """Remove outlying values from PCE, Jsc, Voc and FF using whisker_size * IQR.
  
  """
  cols_to_filter = ['JV_default_PCE', 'JV_default_FF', 'JV_default_Jsc', 'JV_default_Voc']


  def determine_iqr(dataframe, col):
    """Determine the IQR of a specified column in a dataframe
    
    """
    q1 = dataframe[col].quantile(0.25)
    q3 = dataframe[col].quantile(0.75)
    iqr = q3 - q1

    return q1, q3, iqr
  
  for col in cols_to_filter:
    q1, q3, iqr = determine_iqr(dataframe, col)
    filter = (dataframe[col] >= q1 - iqr*whisker_size) & (dataframe[col] <= q3 + iqr*whisker_size)
    dataframe = dataframe.loc[filter]
    logger.info(f'dataframe shape after removing {col} outliers: {dataframe.shape}')
  
  return dataframe



def reduce_cardinality_perovskite_deposition(dataframe):
  """Collapse the very large number of perovskite deposition methods into the most common techniques and collect the rest into an "Other" category. Add a new column so we retain the original information.
  
  
  """
  num_cats = dataframe['Perovskite_deposition_procedure'].nunique()
  logger.info(f'Current number of deposition categories {num_cats}.')

  #First, fill any NaN values with the most frequent
  most_freq = dataframe['Perovskite_deposition_procedure'].mode()[0]
  dataframe['Perovskite_deposition_procedure'] = dataframe['Perovskite_deposition_procedure'].fillna(most_freq)

  #Collapse the multistep processes into the first step and strip excess whitespace
  dataframe['Perovskite_deposition_procedure_grouped'] = [cat.replace('|', '>>').split('>>')[0].strip() if re.search('>>|\|', cat) else cat for cat in dataframe['Perovskite_deposition_procedure']]

  # Aggregate deposition methods and fix typos
  deposition_dict = {'Ultrasonic spray': 'Spray-coating',
 'Electrospraying': 'Spray-coating',
 'Air brush spray': 'Spray-coating',
 'Spray-pyrolys': 'Spray-coating',
 'spin-coatng': 'Spin-coating',
 'Hot-casting': 'Spin-coating',
 'Dipp-coating': 'Dip-coating',
 'Co-evaporation': 'Evaporation',
 'Flash evaporation': 'Evaporation',
 'Vacuum flash evaporation': 'Evaporation',
 'PVD': 'Evaporation',
 'CVD': 'Evaporation',
 'Closed space sublimation': 'Evaporation',
 'Pulsed laser deposition': 'Evaporation',
 'Sputtering': 'Evaporation',
  }
  dataframe['Perovskite_deposition_procedure_grouped'].replace(deposition_dict, inplace=True)

  #Get the top top 10 deposition procedures and collapse everything else into 'Other' 
  top_10_deposition = dataframe['Perovskite_deposition_procedure_grouped'].value_counts(normalize=True).nlargest(10).index

  dataframe['Perovskite_deposition_procedure_grouped'] = dataframe['Perovskite_deposition_procedure_grouped'].apply(lambda x: 'Other' if x not in top_10_deposition else x)

  new_cats = dataframe['Perovskite_deposition_procedure_grouped'].nunique()
  logger.info(f'Number of deposition categories after cleaning is {new_cats}')

  return dataframe



# merge ts80m with data
# handle anomalous TS80m

