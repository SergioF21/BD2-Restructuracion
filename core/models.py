import struct
from typing import List, Any, Type, Union


class Field:
    def __init__(self, name: str, data_type: Type, size: int = 0):
        self.name = name
        self.data_type = data_type
        self.size = size 

    def __repr__(self) -> str:
        return f"Field(name={self.name}, type={self.data_type.__name__}, size={self.size})"
    
class Table:
    def __init__(self, name: str, fields: List[Field], key_field: str):
        self.name = name
        self.fields = fields
        self.key_field = key_field
        self.index = [f.name for f in fields].index(key_field)
        self.record_size = self._calculate_record_size()
        self.format_string = self._generate_format_string()

    def _calculate_record_size(self) -> int:
        # Usar struct.calcsize para obtener el tamaño real con padding
        format_string = self._generate_format_string()
        return struct.calcsize(format_string)
    
    def _generate_format_string(self) -> str:  ## lo necesitamos para que struct funcione (por ejemplo "i10s" para un int y un string de 10 caracteres)
        parseStruct = ""
        for field in self.fields:
            if field.data_type == int:
                parseStruct += "i"
            elif field.data_type == float:
                parseStruct += "f"
            elif field.data_type == str:
                parseStruct += f"{field.size}s"
            else:
                raise ValueError(f"Tipo de dato no soportado: {field.data_type}")
     
        return parseStruct + "i"  # para el next
    
class Record: 
    def __init__(self, table: Table, values: List[any], next: int = 0, pos: int = -1):
        self.table = table
        self.values = values
        self.next = next
        self.pos = pos

    @property
    def key(self) -> Any:
        return self.values[self.table.index]
    
    def pack(self) -> bytes:
        pack_values = []

        for i, fields in enumerate(self.table.fields):
            value = self.values[i]
            if fields.data_type == str:
                pack_values.append(value.encode('utf-8').ljust(fields.size, b'\x00'))
            else:
                pack_values.append(value)
        pack_values.append(self.next)
        return struct.pack(self.table.format_string, * pack_values)
    
    @staticmethod
    def unpack(table: Table, data: bytes) -> 'Record':
        unpacked_values = list(struct.unpack(table.format_string, data))
        values = []
        for i, field in enumerate(table.fields):
            value = unpacked_values[i]
            if field.data_type == str:
                values.append(value.decode('utf-8').rstrip('\x00'))
            else:
                values.append(value)
        next_ptr = unpacked_values[-1]
        record = Record(table, values, next=next_ptr)
        return record
    def __repr__(self) -> str:
        return f"Record(pos={self.pos}, values={self.values}, next={self.next}, pos={self.pos})"