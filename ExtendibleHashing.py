


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
            self.add(k,v)

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
                    self.add(k,v)
                


    # Inserción 
    def add(self, key , value):
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
            # iterativo -> self.add()

            self.add(key,value)
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
                self.add(key,value)
    
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
    '''
    def rangeSearch(self, begin_key, end_key):
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
    '''
    # Eliminar
    def remove(self, key):
        pos = self.EH_hash(key)
        bucket = self.directory[pos]

        #Buscamos en el bucket principal
        for i, (k,v) in enumerate(bucket.records):
            if k == key:
                bucket.records.pop(i)
                return f"{key} eliminado en el bucket principal"

        #Buscamos en el bucket con chaining
        if bucket.next is not None:
            for i, (k,v) in enumerate(bucket.records):
                if k == key:
                    bucket.records.pop(i)
                    # Si el bucket queda vacío lo liberamos
                    if len(bucket.next.records) == 0:
                        bucket.next = None 
                        return f"{key} eliminado, liberando bucket encadenado"
                    return f"{key} eliminado en el bucket encadenado"
        
        return f"{key} no encontrado"
    

# probando funciones
''' 
if __name__ == "__main__":
    EH = ExtendibleHashing(bucketSize=3)

    # Insertar
    EH.add(1, "one")
    EH.add(2, "two")
    EH.add(3, "three")
    EH.add(4, "four")
    EH.add(5, "five")
    EH.add(6, "six")

    # Buscar
    print("Buscando 1:",EH.search(1))
    print("Buscando 10",EH.search(10))

    # Busqueda por rango
    #print("Busquedo por Rango [2,5]:", EH.rangeSearch(2, 5))

    #Eliminar
    print("Eliminar 3:",EH.remove(3))
    print("Buscar 3:",EH.search(3))

    print("Eliminar 99:",EH.remove(99))

    # Ver que buckets existen:
    # Ver qué hay en los buckets (debug)
    print("\nBuckets:")
    for i, b in enumerate(EH.directory):
        print(f"Dir {i} -> {[(k,v) for k,v in b.records]}",
              " | Next:", [(k,v) for k,v in b.next.records] if b.next else None)
        
'''

# Probando Split, Chaining y Rehash
if __name__ == "__main__":
    EH = ExtendibleHashing(bucketSize=3)

    print("Forzando split")
    EH.add(4, "A")
    EH.add(6, "B")
    EH.add(8, "C")
    EH.add(10, "D")

    print("\nForzando chaining")
    EH.add(1, "E")
    EH.add(5, "F")
    EH.add(9, "G")
    EH.add(13, "H")

    print("\nForzando rehash")
    EH.add(17, "I")
    EH.add(21, "J") 
    EH.add(25, "K") 

    print("\nEstado final de directorio y buckets")
    for i, b in enumerate(EH.directory):
        print(f"Dir {i} -> {[(k,v) for k,v in b.records]}",
              " | Next:", [(k,v) for k,v in b.next.records] if b.next else None)

