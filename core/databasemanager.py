import os
from core.models import Table, Record, Field
from core.file_manager import FileManager
from bplus import BPlusTree
from typing import List, Union, Any



class DatabaseManager:
    def __init__(self, table: Table, filename: str, order: int = 4):
        self.table = table
        self.filename = filename
        
        # Crear nombres de archivos para datos e índice
        self.data_filename = filename
        self.index_filename = filename.replace('.dat', '.idx')
        
        # Inicializar FileManager para registros
        self.file_manager = FileManager(self.data_filename, table)
        
        # Inicializar B+ tree con persistencia
        self.index = BPlusTree(order=order, index_filename=self.index_filename)
        
        # Intentar cargar el índice desde archivo
        if not self.index.load_from_file():
            # Si no existe, construir el índice desde los registros existentes
            self.load_index_from_file()

    def load_index_from_file(self):
        """Construye el índice B+ desde los registros existentes en el archivo."""
        if self.index.is_empty():
            if os.path.exists(self.file_manager.filename):
                print("Construyendo índice B+ desde registros existentes...")
                idx = 0
                while True:
                    record = self.file_manager.read_record(idx)
                    
                    if record is None:
                        break
                    
                    if record.next == 0:  # Solo registros válidos (no eliminados)
                        self.index.insert(record.key, idx)
                    idx += 1
                self.file_manager.file_size = idx
                print(f"Índice construido con {self.index.traverse_leaves()} registros.")


    def add_record(self, record: Record):
        """Añade un nuevo registro tanto al archivo como al índice B+."""
        pos = self.file_manager.add_record(record)
        
        # El índice B+ se guarda automáticamente después de la inserción
        self.index.insert(record.key, pos) 
        print(f"Registro con llave '{record.key}' añadido en la posición {pos}.")


    def get_record(self, key: Any) -> Union[Record, None]:
        """Busca un registro por su clave usando el índice B+."""
        pos = self.index.search(key)
        if pos is not None:
            return self.file_manager.read_record(pos)
        return None

    def update_record(self, key: Any, new_values: List[Any]) -> bool:
        """Actualiza un registro existente."""
        pos = self.index.search(key)
        if pos is not None:
            # Crear nuevo registro con los valores actualizados
            new_record = Record(self.table, new_values, pos=pos)
            self.file_manager._write_record_at_pos(new_record, pos)
            print(f"Registro con llave '{key}' actualizado.")
            return True
        return False

    def remove_record(self, key: Any) -> bool:
        """Elimina un registro tanto del archivo como del índice B+."""
        pos = self.index.search(key)
        
        if pos is not None:
            if self.file_manager.remove_record(pos):
                # El índice B+ se guarda automáticamente después de la eliminación
                self.index.delete(key)
                print(f"Registro con llave '{key}' eliminado.")
                return True
        return False
        
    def range_search(self, start_key: Any, end_key: Any) -> List[Record]:
        """Busca todos los registros en un rango de claves."""
        found_records = []
        positions = self.index.range_search(start_key, end_key)
        
        for _, pos in positions:
            record = self.file_manager.read_record(pos)
            if record and record.next == 0:  # Solo registros válidos
                found_records.append(record)
        return found_records
        
    def get_all(self) -> List[Record]:
        """Obtiene todos los registros válidos."""
        return self.file_manager.get_all_records()
    
    def save_all(self):
        """Fuerza el guardado de todos los datos."""
        self.index.save_to_file()
        print("Datos guardados en memoria secundaria.")
    
    def get_index_info(self) -> dict:
        """Obtiene información sobre el estado del índice."""
        leaves = self.index.traverse_leaves()
        total_keys = sum(len(keys) for keys, _ in leaves)
        return {
            'total_keys': total_keys,
            'leaf_nodes': len(leaves),
            'order': self.index.order,
            'is_empty': self.index.is_empty()
        }
