import json
import pandas as pd

class StagingDB:

    def __init__(self):
        pass
    
    def select(self, tablename: str, limit: int=None, offset: int=None):
        
        with open(f'/home/aawz/Documentos/ufsc/tcc/medicines-data/data_pipelines/dags/temp_files/2025-05-10{tablename}.json', 'r', encoding="utf8") as f:
            data = json.load(f)

        if offset:
            data = data[offset:]
        if limit:
            data = data[:limit]

        return data
