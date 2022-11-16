import multiprocessing
import time
from unicodedata import category
import requests 
import gzip 
from datetime import datetime
import json
from Logger import Logger
from multiprocessing import Pool
import psutil

logger = Logger().get_logger(__name__)

class Extractor:
    def __init__(self,category):

        self.category = category

# to interact with the file stroage with python in MAC m1, you need to have a python3 intel-64 version
    def extract_gzip(self,link:dict):
        
        try:
            for key,value in link.items():

                logger.info(f"Now extracting the {key}")

                with requests.get(value,stream=True) as res:

                    gzip_extracted = gzip.decompress(res.content)

                    with open(f'/Users/danie/new_project_pipeline_plus_recommanded_system/{key}.json','w') as f_in:

                        for i in gzip_extracted.split(b'\n'):

                            f_in.write(i.decode() +'\n')

            logger.info(f"Successfully downloaded {key} to local file system")

        except:
            with ValueError as e:
                logger.error(e)

if __name__ == "__main__":

    category = 'Video_Games'

    product_path =  [{f"product_{category}" : f'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_{category}_10.json.gz'}
    ,{f'metadata_{category}' : f'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_{category}.json.gz'}]

    logger.info("This is the extracting part")
    start_time = time.time()
    
    with Pool(2) as p:
        p.map(Extractor(category).extract_gzip,product_path)

    extracting_time = time.time() - start_time

    logger.info(f'Extract CPU usage {psutil.cpu_percent()}%')
    logger.info(f"Extract part has took {extracting_time}s")
    
    logger.info("You have sucessfully finished the extractor part, now move on to the transform part")

