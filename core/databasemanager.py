import os
from core.models import Table, Record, Field
from core.file_manager import FileManager
from bplus import BPlusTree
from isam import ISAMIndex
from typing import List, Union, Any


class DatabaseManager:
    def __init__(self, table: Table, filename: str, order: int = 4, index_type: str = 'bplus'):
        #index_type: 'bplus' (por defecto) o 'isam'
        
        self.table = table
        self.filename = filename

        # Crear nombres de archivos para datos e índice
        self.data_filename = filename
        self.index_filename = filename.replace('.dat', '.idx')

        # Inicializar FileManager para registros
        self.file_manager = FileManager(self.data_filename, table)

        # Inicializar índice según el tipo solicitado (mínimos cambios)
        if index_type == 'isam':
            # ISAMIndex acepta data_filename, index_filename y file_manager (opcional)
            self.index = ISAMIndex(self.data_filename, index_filename=self.index_filename, file_manager=self.file_manager)
        else:
            # Comportamiento por defecto: B+ Tree con persistencia
            self.index = BPlusTree(order=order, index_filename=self.index_filename)

        # Intentar cargar el índice desde archivo
        if not self.index.load_from_file():
            # Si no existe, construir el índice desde los registros existentes
            self.load_index_from_file()

    def load_index_from_file(self):
        """Construye el índice desde los registros existentes en el archivo."""
        if self.index.is_empty():
            if os.path.exists(self.file_manager.filename):
                print("Construyendo índice desde registros existentes...")
                idx = 0
                while True:
                    record = self.file_manager.read_record(idx)

                    if record is None:
                        break

                    if record.next == 0:  # Solo registros válidos (no eliminados)
                        self.index.insert(record.key, idx)
                    idx += 1
                self.file_manager.file_size = idx
                print(f"Índice construido con {len(self.index.traverse_leaves())} hojas/entradas.")

    def add_record(self, record: Record):
        """Añade un nuevo registro tanto al archivo como al índice."""
        pos = self.file_manager.add_record(record)

        # Insertar en el índice (ambas implementaciones exponen insert)
        self.index.insert(record.key, pos)
        print(f"Registro con llave '{record.key}' añadido en la posición {pos}.")

    def get_record(self, key: Any) -> Union[Record, None]:
        """Busca un registro por su clave usando el índice."""
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
        """Elimina un registro tanto del archivo como del índice."""
        pos = self.index.search(key)

        if pos is not None:
            if self.file_manager.remove_record(pos):
                # El índice se actualiza (ambas implementaciones tienen delete)
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
        # Ambos índices exponen save_to_file()
        self.index.save_to_file()
        print("Datos guardados en memoria secundaria.")
 
    def get_index_info(self) -> dict:
        """
        Obtiene información sobre el estado del índice.
        - Si el índice tiene traverse_leaves() (B+), se usa esa estructura.
        - Si es ISAM, se infiere desde atributos idx_l3 y overflow.
        """
        # Preferir traverse_leaves si existe (BPlusTree)
        if hasattr(self.index, 'traverse_leaves'):
            try:
                leaves = self.index.traverse_leaves()
                total_keys = sum(len(keys) for keys, _ in leaves)
                leaf_nodes = len(leaves)
                order = getattr(self.index, 'order', None)
                return {
                    'total_keys': total_keys,
                    'leaf_nodes': leaf_nodes,
                    'order': order,
                    'is_empty': self.index.is_empty()
                }
            except Exception:
                pass

        # Fallback para ISAMIndex (o índices sin traverse_leaves)
        idx_l3 = getattr(self.index, 'idx_l3', None)
        overflow = getattr(self.index, 'overflow', None)
        order = getattr(self.index, 'order', None)

        if idx_l3 is not None:
            total_keys = len(idx_l3)
            # contar posiciones extras en overflow como "keys" adicionales (si existen)
            if isinstance(overflow, dict):
                extra_positions = sum(len(v) for v in overflow.values())
            else:
                extra_positions = 0
            # leaf_nodes: calcular páginas lógicas según order (si está)
            if order and order > 0:
                leaf_nodes = (len(idx_l3) + order - 1) // order
            else:
                leaf_nodes = len(idx_l3)
            return {
                'total_keys': total_keys + extra_positions,
                'leaf_nodes': leaf_nodes,
                'order': order,
                'is_empty': self.index.is_empty()
            }

        # Último recurso: valores por defecto
        return {
            'total_keys': 0,
            'leaf_nodes': 0,
            'order': getattr(self.index, 'order', None),
            'is_empty': self.index.is_empty()
        }
