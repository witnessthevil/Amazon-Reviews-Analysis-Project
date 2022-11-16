from functools import reduce
import time
from tokenize import String
from azure.storage.filedatalake import DataLakeServiceClient
import re
from multiprocessing import Pool
import os
import string
from Logger import Logger
import psutil
from pathlib import Path

logger = Logger().get_logger(__name__)

class Loader:
      def __init__(self,storage_account_name,password):
            self.service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=password)

      def upload_file_to_directory_bulk(self,file_name:string):

            logger.info(f"Now upload the {file_name} to azure data lake")

            try:
          
                  file_system_client = self.service_client.get_file_system_client(file_system="topics")

                  directory_client = file_system_client.get_directory_client("/")

                  target_file = re.search(r"(final)[^\s]+\/+.{10}",file_name).group(0).replace('/','_') + '.json'

                  file_client = directory_client.get_file_client(target_file)

                  local_file = open(file_name,'r')

                  file_contents = local_file.read()

                  file_client.upload_data(file_contents, overwrite=True)

                  logger.info(f"Successfully upload {file_name} to azure as {target_file}")
            
            except:

                  logger.warn(f"cannot upload {file_name} to azure")

            

#def finding_json(category):
#
#      path = os.getcwd() + '/' + category + '/'
#
#      all_file = map(lambda x: path + x, os.listdir(path))
#      
#      targeted_json = filter(lambda file: '.json' in file and 'crc' not in file,all_file)
#      
#      return list(targeted_json)
#
def finding_json2():

      targeted_path = list(Path(__file__).parents[2].glob("final*"))

      json_file = [list(i.glob("*.json")) for i in targeted_path]

      json_file_list = reduce(lambda x,y: x+y, json_file)

      return json_file_list


if __name__ == "__main__":    

      logger.info("now execute the load part")

      start_time = time.perf_counter()

      b = Loader(<storage_account_name>,<password>)

      real_dir = finding_json2()
      
      with Pool(len(real_dir)) as p:
            p.map(b.upload_file_to_directory_bulk,real_dir)

      load_time = time.perf_counter() - start_time

      logger.info(f'Extract CPU usage {psutil.cpu_percent()}%')
      logger.info(f"Extract part has took {load_time}s")
      
      logger.info("You have sucessfully finished the load part, etl complete")