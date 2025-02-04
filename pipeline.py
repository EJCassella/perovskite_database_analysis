from pipeline_funcs import extract, transform, load
from utils import setup_logger

if __name__ == '__main__':
  logger = setup_logger()
  df = extract('Perovskite_database_content_all_data accessed_2025-01-22.csv')
  cleaned_df = transform(df)
  load(cleaned_df)
  logger.info('Finished!')