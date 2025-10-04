import json
import pandas as pd

class StagingDB:

    def __init__(self):
        pass
    
    def select(self, tablename: str, page: int = None, page_size: int = 1000) -> list[dict]:
        
        with open(f'/home/aawz/Documentos/ufsc/tcc/medicines-data/data_pipelines/dags/temp_files/2025-05-10{tablename}.json', 'r', encoding="utf8") as f:
            data = json.load(f)

        if page is not None and page_size is not None:
            offset = (page - 1) * page_size
            limit = page_size
        else:
            offset = None
            limit = None

        if offset:
            data = data[offset:]
        if limit:
            data = data[:limit]

        return data
