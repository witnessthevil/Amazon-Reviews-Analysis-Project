import logging

class Logger:
    def __init__(self):
        logging.basicConfig(
            format='%(asctime)s  - %(filename)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logger.txt'),
                logging.StreamHandler()
            ],
            level=logging.INFO
        )
    def get_logger(self,name):
        logger = logging.getLogger(name)
        return logger



