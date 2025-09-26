import os
from core.models import Table, Record, Field
from core.file_manager import FileManager
from typing import List, Union, Any



class DatabaseManager:
    def __init__(self, table: Table, filename: str, index_class: Any):
        self.table = table
        self.filename = filename
        
        self.file_manager = FileManager(filename, table)
        self.index = index_class(filename)
        
        self.load_index_from_file()

    def load_index_from_file(self):
        if self.index.is_empty():
            if os.path.exists(self.file_manager.filename):
                
                idx = 0
                while True:
                    record = self.file_manager.read_record(idx)
                    
                    if record is None:
                        break
                    
                    if record.next == 0: 
                        self.index.insert(record.key, idx)
                    idx += 1
            self.file_manager.file_size = idx


    def add_record(self, record: Record):
        
        pos = self.file_manager.add_record(record)
        
        self.index.insert(record.key, pos) 
        print(f"Registro con llave '{record.key}' añadido en la posición {pos}.")


    def get_record(self, key: Any) -> Union[Record, None]:

        pos = self.index.search(key)
        if pos is not None:
            return self.file_manager.read_record(pos)
        return None


    def remove_record(self, key: Any) -> bool:
        pos = self.index.search(key)
        
        if pos is not None:
            if self.file_manager.remove_record(pos):
                self.index.delete(key)
                print(f"Registro con llave '{key}' eliminado.")
                return True
        return False
        
    def range_search(self, start_key: Any, end_key: Any) -> List[Record]:
        found_records = []
        positions = self.index.range_search(start_key, end_key)
        
        for _, pos in positions:
            record = self.file_manager.read_record(pos)

            if record and record.next == 0: 
                found_records.append(record)
        return found_records
        
    def get_all(self) -> List[Record]:
        return self.file_manager.get_all_records()
