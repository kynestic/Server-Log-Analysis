import requests
from datetime import datetime, timedelta
from tqdm import tqdm
import warnings
from typing import Dict, List, Any
import os
import json, csv

class ExtractMetricBeatLogs:
    def __init__(
        self, 
        url:str="https://116.101.122.180:5200/metricbeat-*/_search",
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
                with open('logs\metricbeat-.json', 'r') as f:
                    response = json.load(f)
                
                if self.process_response(response):
                    log, length, miss_count = self.process_response(response)
                    if miss_count<0:
                        print(f"Missed {abs(miss_count)} lines from metricbeat-Logging")
                    LOGS+=log
            except Exception as e:
                print(f"An error occurred: {type(e).__name__} - {e}")
            
            self.start_time = current_time.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z"
        return LOGS
   
class Transform():
    def __init__(self, logs:List=None):
        self.logs = logs
        
    def extract_system_resource_logs(self, entry):
        
        log_source = entry.get("_source", {})
        timestamp = log_source.get("@timestamp", "N/A")
        host_name = log_source.get("host", {}).get("name", "N/A")
        metricset = log_source.get("metricset", {}).get("name", "N/A")
        env = log_source.get("env", "N/A")
        
        # Extract CPU
        if metricset == "cpu":
            cpu_info = log_source.get("system", {}).get("cpu", {})
            cpu_total = cpu_info.get("total", {}).get("pct", "N/A")
            cpu_user = cpu_info.get("user", {}).get("pct", "N/A")
            cpu_system = cpu_info.get("system", {}).get("pct", "N/A")
            cpu_iowait = cpu_info.get("iowait", {}).get("pct", "N/A")

            cpu_irq = cpu_info.get("irq", {}).get("pct", "N/A")
            cpu_softirq = cpu_info.get("softirq", {}).get("pct", "N/A")
            cpu_steal = cpu_info.get("steal", {}).get("pct", "N/A")
            cpu_nice = cpu_info.get("nice", {}).get("pct", "N/A")
            cpu_idle = cpu_info.get("idle", {}).get("pct", "N/A")
            timestamp = log_source.get("@timestamp")
            logline = {
                "cpu_total": cpu_total,
                "cpu_user": cpu_user,
                "cpu_system": cpu_system,
                "cpu_iowait": cpu_iowait,
                "cpu_irq ":cpu_irq,
                "cpu_softirq":cpu_softirq,
                "cpu_steal":cpu_steal,
                "cpu_nice":cpu_nice,
                "cpu_idle":cpu_idle
            }
            # logline = (
            #     f"[{timestamp}] ENV: {env} | HOST: {host_name} | CPU: "
            #     f"Total: {cpu_total:.4f}%, User: {cpu_user:.4f}%, System: {cpu_system:.4f}%, IOwait: {cpu_iowait:.4f}%"
            # )
            return logline
        
        # Extract RAM
        # elif metricset == "memory":
        #     memory_info = log_source.get("system", {}).get("memory", {})
        #     total_memory = memory_info.get("total", 0) / (1024**3)  #GB
        #     used_memory = memory_info.get("used", {}).get("pct", 0) * 100
        #     free_memory = memory_info.get("free", 0) / (1024**3)
        #     cached_memory = memory_info.get("cached", 0) / (1024**3)
        #     logline = {
        #         "timestamp": timestamp,
        #         "env": env,
        #         "host_name": host_name,
        #         "total_memory": total_memory,
        #         "used_memory": used_memory,
        #         "free_memory": free_memory,
        #         "cached_memory": cached_memory
        #     }
        #     # logline = (
        #     #     f"[{timestamp}] ENV: {env} | HOST: {host_name} | MEMORY: "
        #     #     f"Total: {total_memory:.2f} GB, Used: {used_memory:.2f}%, Free: {free_memory:.2f} GB, Cached: {cached_memory:.2f} GB"
        #     # )
        #     return logline
        
        # # Extract system load
        # elif metricset == "load":
            
        #     load_info = log_source.get("system", {}).get("load", {})
        #     cores = load_info.get("cores", "N/A")
        #     load_1 = load_info.get("1", "N/A")
        #     load_5 = load_info.get("5", "N/A")
        #     load_15 = load_info.get("15", "N/A")
        #     logline = {
        #         "timestamp": timestamp,
        #         "env": env,
        #         "host_name": host_name,
        #         "cores": cores,
        #         "load_1": load_1,
        #         "load_5": load_5,
        #         "load_15": load_15
        #     }
        #     # logline = (
        #     #     f"[{timestamp}] ENV: {env} | HOST: {host_name} | LOAD: "
        #     #     f"1min: {load_1:.2f}, 5min: {load_5:.2f}, 15min: {load_15:.2f}, Cores: {cores}"
        #     # )
        #     return logline
        
        # # Extract Networks
        # elif metricset == "network":
        #     network_info = log_source.get("system", {}).get("network", {})
        #     interface_name = network_info.get("name", "N/A")
        #     in_bytes = network_info.get("in", {}).get("bytes", 0) / (1024**2) #BYTES
        #     out_bytes = network_info.get("out", {}).get("bytes", 0) / (1024**2)
        #     logline = {
        #         "timestamp": timestamp,
        #         "env": env,
        #         "host_name": host_name,
        #         "interface_name": interface_name,
        #         "in_bytes": in_bytes,
        #         "out_bytes": out_bytes
        #     }
        #     # logline = (
        #     #     f"[{timestamp}] ENV: {env} | HOST: {host_name} | NETWORK ({interface_name}): "
        #     #     f"IN: {in_bytes:.2f} MB, OUT: {out_bytes:.2f} MB"
        #     # )
        #     return logline
        else:
            return {}

    
    def exact_log(self):
        LOGS_EXTRACTED = [json.dumps(self.extract_system_resource_logs(log), indent=2) + '\n' for log in tqdm(self.logs, desc="Transforming")]

        # LOGS_EXTRACTED = [self.extract_system_resource_logs(log)+"\n" for log in tqdm(self.logs, desc="Transforming")]
        return LOGS_EXTRACTED         

class Load:
    def __init__(self, logs:List, log_info:Dict, save_dir:str):
        self.logs = logs
        self.log_info = log_info
        self.save_dir = save_dir
        self.run()
    
    @property
    def log_name(self):
        start_time= self.log_info["start_time"].replace(":", "_").replace("T", "_").replace(".", "_")
        end_time = self.log_info["end_time"].replace(":", "_").replace("T", "_").replace(".", "_")
        return os.path.join(self.save_dir, f"metrics-beat-{start_time}-{end_time}")

    def run(self):     
         # Write to txt file
        os.makedirs(self.log_name, exist_ok=True)
        with open(os.path.join(self.log_name, "metrics-beat-logs.txt"), 'w', encoding='utf-8') as f:
            for log in tqdm(self.logs, desc="Loading"):
                f.write(log)

        # Write logs to a CSV file
        with open(os.path.join(self.log_name, "metrics-beat-logs.csv"), 'w', encoding='utf-8', newline='') as csvfile:
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
    url:str="https://116.101.122.180:5200/metricbeat-*/_search",
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
        
        new_start_time = current_start.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z"
        new_end_time = current_end.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z"
        
        print("Getting logs from {} to {}".format(new_start_time, new_end_time))
        
        info = {
            "url":url,
            "api_key":api_key,
            "start_time":new_start_time, 
            "end_time":new_end_time, 
            "step":step,
            "limit":limit
        }
        logs = ExtractMetricBeatLogs(
            **info
        ).get_log()
        logs = Transform(logs=logs).exact_log()
        Load(logs, info, save_dir=f"./logs/")
        
        current_start = datetime.strptime(new_start_time, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(seconds=cut_off)
        
    
if __name__ == "__main__":
    
    time_collect = [
        ["2025-01-02T00:00:00.000Z","2025-01-03T00:00:00.000Z"],
        # ["2024-12-23T00:00:00.000Z","2024-12-24T00:00:00.000Z"],
        # ["2024-12-24T00:00:00.000Z","2024-12-25T00:00:00.000Z"]
    ]
    for time in time_collect:
        run_etl(
            url="https://116.101.122.180:5200/metricbeat-*/_search",
            api_key='',
            start_time=time[0], 
            end_time=time[1], 
            step=1000, # unit is mili seconds
            limit=6000,
            cut_off=5000 # unit is seconds
        )