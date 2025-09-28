import json
import pandas as pd

class StagingDB:

    def __init__(self):
        pass
    
    def select(self, tablename: str):
        
        with open(f'C://Users/Marcelo/Desktop/Medicamentos/extracao-dados-medicamentos/data_pipelines/dags/temp_files/2025-05-10{tablename}.json', 'r', encoding="utf8") as f:
            data = json.load(f)

        return data
