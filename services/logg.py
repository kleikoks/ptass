import logging

logger = logging.getLogger(__name__)

handler = logging.FileHandler('temp/log.log')

formatter = logging.Formatter('%(asctime)s %(message)s')

handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def get_logger():
    return logger