import logging
import os
import sys

LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logger = logging.getLogger('sentinel')
logger.setLevel(logging.DEBUG)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter(
    '%(asctime)s | %(levelname)s | %(message)s'
)
console_handler.setFormatter(console_formatter)

# File handler for all logs
file_handler = logging.FileHandler(os.path.join(LOG_DIR, 'app.log'))
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
file_handler.setFormatter(file_formatter)

# File handler for errors only
error_handler = logging.FileHandler(os.path.join(LOG_DIR, 'error.log'))
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter(
    '%(asctime)s | %(levelname)s | %(message)s'
)
error_handler.setFormatter(error_formatter)

# Adding handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)
logger.addHandler(error_handler)

# Silence py4j logger
logging.getLogger('py4j').setLevel(logging.ERROR)
