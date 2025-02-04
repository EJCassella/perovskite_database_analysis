import logging
import sys

def setup_logger():
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)

  formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

  if logger.hasHandlers():
    logger.handlers.clear()
    
  console_handler = logging.StreamHandler(sys.stdout)
  console_handler.setLevel(logging.INFO)
  console_handler.setFormatter(formatter)
  
  logger.addHandler(console_handler)

  return logger