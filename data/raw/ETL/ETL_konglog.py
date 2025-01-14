import requests
from datetime import datetime, timedelta
from tqdm import tqdm
import warnings
from typing import Dict, List, Any
import os
import json, csv

class ExtractKongLogs:
    def __init__(
        self, 
        url:str="https://116.101.122.180:5200/kong-access-*/_search",
        api_key:str=None,
        start_time:str="2024-12-14T00:00:00.000Z", 
        end_time:str="2024-12-15T00:00:00.000Z", 
        step:int=500,
        limit:int=5000
    ):
        warnings.filterwarnings("ignore")

        self.url = url
        self.__api_key = api_key
        self.start_time = start_time
        self.end_time = end_time
        self.step = step
        self.limit = limit
    
    @property    
    def __headers(self):
        return {
            "Authorization": f"ApiKey {self.__api_key}",
            "Content-Type": "application/json"
        }
        
    def __data(self, query_time):
        return {
            "from": 0,
            "size": self.limit,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": self.start_time,
                                    "lte": query_time,
                                    "format": "strict_date_optional_time"
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "@timestamp": {
                        "order": "asc"
                    }
                }
            ]
        }

    def process_response(self, response):
        # if response.status_code!=200:
        #     return False
        # else:
        # data = response.json()
        data = response
        length = data['hits']["total"]["value"]
        miss_count = self.limit-length
        logs = data['hits']["hits"]
        return logs, length, miss_count
        
    def get_log(self)->List[Dict[str, Any]]:
        LOGS = []
        
        start_dt = datetime.strptime(self.start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        end_dt = datetime.strptime(self.end_time, "%Y-%m-%dT%H:%M:%S.%fZ") 
        delta = end_dt - start_dt
        milliseconds = int(delta.total_seconds() * 1000)
        for _ in tqdm(range(0, milliseconds, self.step), desc="Extracting"):
            current_time = datetime.strptime(self.start_time, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(milliseconds=self.step)
            query_time = current_time.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z"
            try:
                # response = requests.get(self.url, headers=self.__headers, json=self.__data(query_time), verify=False, timeout=15)
                with open('D:\Job\Viettel\Intern\ETL\ETL\logs\kong-access-.json', 'r') as f:
                    response = json.load(f)
                if self.process_response(response):
                    log, length, miss_count = self.process_response(response)
                    if miss_count<0:
                        print(f"Missed {abs(miss_count)} lines from Kong-Logging")
                    LOGS+=log
            except Exception as e:
                print(f"An error occurred: {type(e).__name__} - {e}")
            
            self.start_time = current_time.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z"
        return LOGS
   
class Transform():
    def __init__(self, logs:List=None):
        self.logs = logs
        
    def get_info(self, log_line):
        return log_line["_source"]["message"]+"\n"
    
    def exact_log(self):
        LOGS_EXTRACTED = [self.get_info(log) for log in tqdm(self.logs, desc="Transforming")]
        return LOGS_EXTRACTED         

class Load:
    def __init__(self, logs:List, log_info:Dict, save_dir:str):
        self.logs = logs
        self.log_info = log_info
        self.save_dir = save_dir
        self.run()

    def log_name(self):
        start_time= self.log_info["start_time"].replace(":", "_").replace("T", "_").replace(".", "_").replace(':', '_')
        end_time = self.log_info["end_time"].replace(":", "_").replace("T", "_").replace(".", "_").replace(':', '_')

        return os.path.join(self.save_dir, "kong-logs-acesss.{}-{}".format(start_time, end_time))

    def run(self):     
        os.makedirs(self.log_name(), exist_ok=True)
        with open(os.path.join(self.log_name(), "kong-logs.txt"), 'w', encoding='utf-8') as f:
            for log in tqdm(self.logs, desc="Loading"):
                f.write(log)

        # Write logs to a CSV file
        with open(os.path.join(self.log_name(), "kong-logs.csv"), 'w', encoding='utf-8', newline='') as csvfile:
            # Define fieldnames (assuming logs only have a 'message' field)
            fieldnames = ['message']
            
            csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            csv_writer.writeheader()
            
            # Write each log to the CSV file
            for log in tqdm(self.logs, desc="Writing to CSV file"):
                # Ensure 'log' is formatted as a dictionary
                if isinstance(log, dict):
                    flattened_log = {'message': log.get('message', '')}
                else:
                    flattened_log = {'message': str(log)}
                
                csv_writer.writerow(flattened_log)
        
def run_etl(
    url:str="https://116.101.122.180:5200/kong-access-*/_search",
    api_key:str=None,
    start_time:str="2024-12-14T00:00:00.000Z", 
    end_time:str="2024-12-15T00:00:00.000Z", 
    step:int=1000,
    limit:int=5000,
    cut_off:int=4000 # unit is seconds
):
    
    start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    current_start = start_dt
    
    while current_start < end_dt:
        current_end = min(current_start + timedelta(seconds=cut_off), end_dt)
        new_start_time = (current_start.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z")
        new_end_time = (current_end.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z")
        
        print("Getting logs from {} to {}".format(new_start_time, new_end_time))
        
        info = {
            "url":url,
            "api_key":api_key,
            "start_time":new_start_time, 
            "end_time":new_end_time, 
            "step":step,
            "limit":limit
        }
        logs = ExtractKongLogs(
            **info
        ).get_log()
        logs = Transform(logs=logs).exact_log()
        Load(logs, info, save_dir=f"./logs/")
        current_start = datetime.strptime(new_start_time, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(seconds=cut_off)
    
if __name__ == "__main__":
    time_collect = [
        ["2024-12-22T00:00:00.000Z","2024-12-22T00:00:10.000Z"],
        # ["2024-12-22T00:00:00.000Z","2024-12-23T00:00:00.000Z"],
        # ["2024-12-23T00:00:00.000Z","2024-12-24T00:00:00.000Z"],
        # ["2024-12-24T00:00:00.000Z","2024-12-25T00:00:00.000Z"]
    ]
    for time in time_collect:
        run_etl(
            url="https://116.101.122.180:5200/kong-access-*/_search",
            api_key='',
            start_time=time[0], 
            end_time=time[1], 
            step=1000, # unit is mili seconds
            limit=6000,
            cut_off=5000 # unit is seconds
        )