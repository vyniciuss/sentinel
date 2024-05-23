import logging
import os
import sys

from loguru import logger

LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)


logger.remove()

logger.add(
    sys.stdout,
    level='INFO',
    format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}',
    colorize=True,
)

logger.add(
    os.path.join(LOG_DIR, 'app_{time:YYYY-MM-DD}.log'),
    level='DEBUG',
    format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}',
    rotation='1 day',
    retention='7 days',
    compression='zip',
    serialize=False,
)

logger.add(
    os.path.join(LOG_DIR, 'error_{time:YYYY-MM-DD}.log'),
    level='ERROR',
    format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}',
    rotation='1 week',
    retention='1 month',
    compression='zip',
    serialize=False,
)

# Integração do Loguru com o logging padrão do Python
class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Obter o logger do Loguru e repassar as mensagens de log
        logger_opt = logger.opt(depth=6, exception=record.exc_info)
        logger_opt.log(record.levelname, record.getMessage())


# Redefinir configuração do logger padrão do Python
logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO)
logging.getLogger().handlers = [InterceptHandler()]

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.getLogger().addHandler(InterceptHandler())
