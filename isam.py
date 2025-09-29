import struct

# Constantes de tamaño de bloque/índice
IDX_ENTRY_SIZE = struct.calcsize('ii')
IDX_PAGE_HEADER = struct.calcsize('i')
IDX_BLOCK_FACTOR = (4096 - IDX_PAGE_HEADER) // IDX_ENTRY_SIZE

class ISAMIndex:
    def __init__(self, data_filename: str):
        # Índices en memoria
        self.idx_l1 = []  # raiz, agrupa level 2
        self.idx_l2 = []  # medio, agrupa l3
        self.idx_l3 = []  # hojas, apuntan a páginas base

    # Utilidades
    @staticmethod
    def insert_pos(lista, key):
        # Busca donde insertar un elemento
        lo = 0
        hi = len(lista)
        while lo < hi:
            mid = (lo + hi) // 2
            if lista[mid][0] < key:
                lo = mid + 1
            else:
                hi = mid
        return lo

    @staticmethod
    def busqueda_binaria(lista, target):
        # Busca un elemento
        lo = 0
        hi = len(lista) - 1
        result = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            if lista[mid][0] <= target:
                result = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return result

    def recontruir2y1(self):
        #Reconstruye idx_l2 e idx_l1 desde idx_l3 (igual a tu build_index pero para mapping).
        self.idx_l2 = []
        self.idx_l1 = []
        if not self.idx_l3:
            return
        # L2: cada IDX_BLOCK_FACTOR entradas de idx_l3 forman una 'página' resumen
        for page_start in range(0, len(self.idx_l3), IDX_BLOCK_FACTOR):
            first_key = self.idx_l3[page_start][0]
            self.idx_l2.append((first_key, page_start))
        # L1: cada IDX_BLOCK_FACTOR páginas de idx_l2 forman un bloque
        for block_start in range(0, len(self.idx_l2), IDX_BLOCK_FACTOR):
            first_key = self.idx_l2[block_start][0]
            self.idx_l1.append((first_key, block_start))

    # Esperado

    def is_empty(self):
        return len(self.idx_l3) == 0

    def insert(self, key, pos):
        #Inserta (key,pos) manteniendo idx_l3 ordenada.
        # si hay entrada con exactamente (key,pos) ya, no duplicar
        idx = self.insert_pos(self.idx_l3, key)
        # insertar nuevo par justo en idx (mantenemos duplicados permitidos)
        self.idx_l3.insert(idx, (key, pos))
        self.recontruir2y1()

    def bulk_insert(self, pairs):
        """Reemplaza/crea idx_l3 con lista de(key,pos) (usa para carga masiva)."""
        # ordenar por clave
        self.idx_l3 = sorted(pairs, key=lambda x: x[0])
        self.recontruir2y1()

    def search(self, key):
        if not self.idx_l3:
            return None
        # búsqueda rápida directa en idx_l3
        i = self.insert_pos(self.idx_l3, key)
        if i < len(self.idx_l3) and self.idx_l3[i][0] == key:
            return self.idx_l3[i][1]
        if i > 0 and self.idx_l3[i-1][0] == key:
            return self.idx_l3[i-1][1]
        return None

    def delete(self, key):
        #Elimina la primera ocurrencia de key. Retorna True si eliminó.
        if not self.idx_l3:
            return False
        i = self.insert_pos(self.idx_l3, key)
        if i < len(self.idx_l3) and self.idx_l3[i][0] == key:
            self.idx_l3.pop(i)
            self.recontruir2y1()
            return True
        if i > 0 and self.idx_l3[i-1][0] == key:
            self.idx_l3.pop(i-1)
            self.recontruir2y1()
            return True
        return False

    def range_search(self, start_key, end_key):
        #Retorna lista de (key,pos) entre start_key y end_key inclusive.
        results = []
        if not self.idx_l3:
            return results
        # localizar inicio
        i = self.insert_pos(self.idx_l3, start_key)
        # si i>0 y anterior = start_key, retroceder para incluirlo
        if i > 0 and self.idx_l3[i-1][0] >= start_key:
            i = i-1
        # iterar hasta que key > end_key
        n = len(self.idx_l3)
        j = i
        while j < n and self.idx_l3[j][0] <= end_key:
            results.append(self.idx_l3[j])
            j += 1
        return results

    def update(self, key, pos):
        #Actualiza la primera ocurrencia encontrada; si no existe la inserta.
        if not self.idx_l3:
            self.idx_l3.append((key, pos))
            self.recontruir2y1()
            return True
        i = self.insert_pos(self.idx_l3, key)
        if i < len(self.idx_l3) and self.idx_l3[i][0] == key:
            self.idx_l3[i] = (key, pos)
            return True
        if i > 0 and self.idx_l3[i-1][0] == key:
            self.idx_l3[i-1] = (key, pos)
            return True
        # no encontrado -> insertar
        self.idx_l3.insert(i, (key, pos))
        self.recontruir2y1()
        return True

    #Probar
    def debug_print(self, max_show=10):
        print("ISAMIndex: entries:", len(self.idx_l3))
        print("L1:", self.idx_l1[:max_show])
        print("L2:", self.idx_l2[:max_show])
        print("L3 (primeras):", self.idx_l3[:max_show])
