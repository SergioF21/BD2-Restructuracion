class Bucket:
    # d = profundidad local, fb = Factor de Bloque
    def __init__(self, d, fb): # El factor de bloque máximo es 3
        self.d = d
        self.fb = fb
        self.records = [] # (key, value)
        self.next = None

    def isfull(self):
        return len(self.records) >= self.fb


class ExtendibleHashing:
    # D = profundidad global, 
    def __init__(self, bucketSize = 3):
        self.D = 2
        self.bucketSize = bucketSize

        bucket0 = Bucket(d = 1, fb = bucketSize)
        bucket1 = Bucket(d = 1, fb = bucketSize)

        # directorio de 2^D = 4 filas en el index file
        # [0,2] apuntan a bucket0, [1,3] apuntan a bucket1
        self.directory = [bucket0, bucket1, bucket0, bucket1] #punteros a los buckets


    def EH_hash(self, key):
        return hash(key) % (2 ** self.D)

    # split
    def split(self, pos): 
        old_bucket = self.directory[pos]
        old_bucket.d += 1 

        new_bucket = Bucket(d=old_bucket.d, fb = self.bucketSize)

        # desplazamos a la izquierda el 1 en binario (10 -> 100) -> 2^n
        m = (1 << old_bucket.d) # 2^d

        # actualizamos los punteros a los buckets del directorio
        for i in range(len(self.directory)):
            # & bit operator: 10 & 01 = 00
            # si i & (m >> 1) == 0 → el nuevo bit = 0 → sigue apuntando al bucket viejo
            # si i & (m >> 1) != 0 → el nuevo bit = 1 → apunta al bucket nuevo
            if self.directory[i] is old_bucket and (i & (m >> 1)):
                # cuando la posición de el directorio apunta al bucket que debe hacer split
                # y cuando (&) -> bit = 1, o sea se separa usando el bit extra 
                self.directory[i] = new_bucket

        # reinsertamos todos los registros
        records2reinsert = old_bucket.records[:]
        old_bucket.records = []
        for k,v in records2reinsert:
            self.insert(k,v)

    # Rehash
    def rehash(self):
        self.D += 1
        self.directory = self.directory * 2

        # Se reinsertan los buckets con chaining
        for i in range(len(self.directory)):
            bucket = self.directory[i]
            if bucket.next is not None:
               # Se extraen los registros del bucket con chaining 
                records2reinsert = bucket.next.records[:]
                bucket.next = None # cortamos el chaining
                for k,v in records2reinsert:
                    self.insert(k,v)
                


    # Inserción 
    def insert(self, key , value):
        pos = self.EH_hash(key)
        bucket = self.directory[pos]

        # Caso 1: Espacio en el bucker principal
        if not bucket.isfull():
            bucket.records.append((key,value))
            return
        
        #Caso 2: Bucket lleno
        # A) Se puede hacer split porque d < D
        if bucket.d < self.D:
            self.split(pos)
            # Dentro de split se vuelve a insertar los elementos que se encontraban en el bucket
            # entre los 2 vuckets
            # iterativo -> self.insert()

            self.insert(key,value)
            return
        
        # B) No se puede hacer split porque d = D, se hace chaining, máximo 1 bucket encadenado
        if bucket.next is None:
            bucket.next = Bucket(d = bucket.d, fb = self.bucketSize)
            bucket.next.records.append((key,value))
        else:
            # ya existe un bucket con chaining pero no está lleno
            if not bucket.next.isfull():
                bucket.next.records.append((key,value))

            # el bucket siguiente está lleno también -> rehashing
            else:
                self.rehash()
                self.insert(key,value)
    
    # Búsqueda
    def search(self, key):
        pos = self.EH_hash(key)
        bucket = self.directory[pos]

        while bucket:
            for k, v in bucket.records:
                if k == key:
                    return v  # devuelve el valor asociado
            bucket = bucket.next

        return None  # no encontrado
    
    # Búsqueda por rango, complejidad O(n), se recorre to', no necesario para hash
    def range_search(self, begin_key, end_key):
        resultados = []
        vistos = set() # como muchos índices apuntan al mismo bucket, esto hará que no se
        #recorran los buckets varias veces

        for bucket in self.directory:
            if id(bucket) in vistos:
                continue
            vistos.add(id(bucket)) # agregamos el bucket como visto

            for k, v in bucket.records:
                if begin_key <= k <= end_key:
                    resultados.append((k,v))

            # En caso existe bucket con chaining
            if bucket.next:
                for k, v in bucket.next.records:
                    if begin_key <= k <= end_key:
                        resultados.append((k,v))
        
        return resultados

    # Eliminar
    def delete(self, key):
        pos = self.EH_hash(key)
        bucket = self.directory[pos]

        # Buscamos en el bucket principal
        for i, (k, v) in enumerate(bucket.records):
            if k == key:
                bucket.records.pop(i)
                return f"{key} eliminado en el bucket principal"

        # Buscamos en el bucket con chaining
        if bucket.next is not None:
            for i, (k, v) in enumerate(bucket.next.records):  # <-- corregido aquí
                if k == key:
                    bucket.next.records.pop(i)
                    # Si el bucket queda vacío lo liberamos
                    if len(bucket.next.records) == 0:
                        bucket.next = None 
                        return f"{key} eliminado, liberando bucket encadenado"
                    return f"{key} eliminado en el bucket encadenado"

        return f"{key} no encontrado"

    
    # Nuevo: is_empty() — usado en load_index_from_file para saber si debe reconstruir el índice desde cero.
    def is_empty(self):
        # Recorremos buckets únicos (muchas entradas del directorio apuntan al mismo bucket)
        vistos = set()
        for bucket in self.directory:
            if id(bucket) in vistos:
                continue
            vistos.add(id(bucket))
            if len(bucket.records) > 0:
                return False
            if bucket.next and len(bucket.next.records) > 0:
                return False
        return True

    # Nuevo: update(key, pos) — actualiza la posición/valor asociado a una llave existente
    def update(self, key, pos):
        dir_pos = self.EH_hash(key)
        bucket = self.directory[dir_pos]

        # Buscamos en el bucket principal
        for i, (k,v) in enumerate(bucket.records):
            if k == key:
                bucket.records[i] = (k, pos)
                return f"{key} actualizado en el bucket principal"

        # Buscamos en el bucket con chaining
        if bucket.next is not None:
            for i, (k,v) in enumerate(bucket.next.records):
                if k == key:
                    bucket.next.records[i] = (k, pos)
                    return f"{key} actualizado en el bucket encadenado"

        return f"{key} no encontrado para actualizar"
    


