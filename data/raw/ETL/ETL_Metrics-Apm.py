import requests
from datetime import datetime, timedelta
from tqdm import tqdm
import warnings
from typing import Dict, List, Any
import os
import json
import csv

class ExtractMetricApmLogs:
    def __init__(
        self, 
        url:str="https://116.101.122.180:5200/metric-apm-*/_search",
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
                            "query_string": {
                                "query": "cee25daa-3fd9-441b-af33-8211e3649f3e",
                                "default_operator": "AND"
                            }
                        },
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
        # if response.status_code != 200:
        #     return False
        # data = response.json()
        data = response
        length = data['hits']['total']['value']
        miss_count = self.limit - length
        logs = data['hits']['hits']
        return logs, length, miss_count
    
    def get_log(self)->List[Dict[str, Any]]:
        LOGS = []

        start_dt = datetime.strptime(self.start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        end_dt = datetime.strptime(self.end_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        delta = end_dt - start_dt
        miliseconds = int(delta.total_seconds() * 1000)

        for _ in tqdm(range(0, miliseconds, self.step), desc="Extracting"):
            current_time = datetime.strptime(self.start_time, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(milliseconds=self.step)
            query_time = current_time.isoformat(timespec="milliseconds").replace("+00:00","")+"Z"
            try:
                # response = requests.get(self.url, headers=self.__headers, json=self.__data(query_time), verify=False, timeout=15)
                with open('D:\Job\Viettel\Intern\ETL\example\logs\metrics-apm.json', 'r') as f:
                    response = json.load(f)

                if self.process_response(response):
                    logs, length, miss_count = self.process_response(response)
                    if miss_count < 0:
                        print(f"Missed {abs(miss_count)} lines from metric-apm-logging")
                    LOGS+=logs
            except Exception as e:
                print(f"An error occured: {type(e).__name__} - {e}")

            self.start_time = current_time.isoformat(timespec="milliseconds").replace("+00:00","")+"Z" 
        return LOGS 

class Transform():
    def __init__(self, logs:List=None):
        self.logs = logs

    def extract_system_resource_logs(self, entry):
        log_source = entry.get("_source", {})
        # trace_container_id = log_source.get("container", {}).get("id", {})
        # log_agent_id_status = log_source.get("event", {}).get("agent_id_status", {})
        # log_ingested = log_source.get("event", {}).get("ingested", {})
        # metrics = [key for key, value in log_source.get("processor", {}).items() if value == "metric"]
        request_data_hostname = log_source.get("host", {}).get("hostname", {})
        request_data_os_type = log_source.get("host", {}).get("os", {}).get("type", {})
        request_data_platform = log_source.get("host", {}).get("os", {}).get("platform", {})
        request_data_full = log_source.get("host", {}).get("os", {}).get("full", {})
        request_data_name = log_source.get("host", {}).get("name", {})
        request_data_architecture = log_source.get("host", {}).get("architecture", {})
        service_status = "online" if log_source.get("service", {}) else "offline"
        agent_name = log_source.get("agent", {}).get("name", {})
        agent_version = log_source.get("agent", {}).get("version", {})

        log_line = {
            # "trace_container_id": trace_container_id,
            # "log_agent_id_status": log_agent_id_status,
            # "log_ingested": log_ingested,
            # "metrics": metrics,
            "request_data_hostname": request_data_hostname,
            "request_data_os_type": request_data_os_type,
            "request_data_platform": request_data_platform,
            "request_data_full": request_data_full,
            "request_data_name": request_data_name,
            "request_data_architecture": request_data_architecture,
            "service_status": service_status,
            "agent_name": agent_name,
            "agent_version": agent_version
        }

        return log_line
    
    def exact_log(self):
        LOGS_EXTRACTED = [json.dumps(self.extract_system_resource_logs(log), indent=2) + '\n' for log in tqdm(self.logs, desc="Transforming")]
        return LOGS_EXTRACTED    
    
class Load():
    def __init__(self, logs: List, log_info: Dict, save_dir: str):
        self.logs = logs
        self.log_info = log_info
        self.save_dir = save_dir
        self.run()

    @property
    def log_name(self):
        start_time= self.log_info["start_time"].replace(":", "_").replace("T", "_").replace(".", "_")
        end_time = self.log_info["end_time"].replace(":", "_").replace("T", "_").replace(".", "_")
        return os.path.join(self.save_dir, f"metrics-apm-logs-{start_time}-{end_time}")
    
    def run(self):
        # print('log_file_path', os.path.dirname(self.log_name))

        # Write to txt file
        os.makedirs(self.log_name, exist_ok=True)
        with open(os.path.join(self.log_name, "metrics-apm-logs.txt"), 'w', encoding='utf-8') as f:
            for log in tqdm(self.logs, desc="Loading"):
                f.write(log)

        # Write logs to a CSV file
        with open(os.path.join(self.log_name, "metrics-apm-logs.csv"), 'w', encoding='utf-8', newline='') as csvfile:
            fieldnames = set()
            for log in self.logs:
                fieldnames.update(json.loads(log).keys())
            fieldnames = sorted(fieldnames)

            csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            csv_writer.writeheader()  

            for log in tqdm(self.logs, desc="Writing to CSV file"):
                flattened_log = {key: (json.dumps(value) if isinstance(value, (dict, list)) else value) for key, value in json.loads(log).items()}
                csv_writer.writerow(flattened_log)

def run_etl(
    url:str="https://116.101.122.180:5200/metric-apm-*/_search",
    api_key:str=None,
    start_time:str="2024-12-14T00:00:00.000Z",
    end_time:str="2024-12-15T00:00:00.000Z",
    step:int=1000,
    limit:int=5000,
    cut_off:int=2000 # unit is seconds
):
    start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    current_start = start_dt
    while current_start < end_dt:
        current_end = min(current_start + timedelta(seconds=cut_off), end_dt)

        new_start_time = current_start.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z"
        new_end_time = current_end.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z"

        print(f"Getting logs from {new_start_time} to {new_end_time}")

        info = {
            "url":url,
            "api_key":api_key,
            "start_time":new_start_time,
            "end_time":new_end_time,
            "step":step,
            "limit":limit
        }
        logs = ExtractMetricApmLogs(
            **info
        ).get_log()
        logs = Transform(logs=logs).exact_log()
        Load(logs, info, save_dir=f"./logs/")

        current_start = datetime.strptime(new_start_time, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(seconds=cut_off)

if __name__ == "__main__":

    time_collect = [
        ["2025-1-7T00:00:00.000Z","2025-1-7T00:00:10.000Z"],
        # ["2025-1-8T00:00:00.000Z","2025-1-9T00:00:00.000Z"]
    ]

    for time in time_collect:
        run_etl(
            url="https://116.101.122.180:5200/metric-apm-*/_search",
            api_key='',
            start_time=time[0],
            end_time=time[1],
            step=1000,
            limit=10000,
            cut_off=2000
        )