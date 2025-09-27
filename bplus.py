class BPlusTreeNode:
    def __init__(self, order, is_leaf=False):
        self.order = order
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []
        self.next = None  # Para enlazar hojas


class BPlusTree:
    def __init__(self, order=4):
        self.root = BPlusTreeNode(order, is_leaf=True)
        self.order = order

    # -------------------------------
    # BÚSQUEDA
    # -------------------------------
    def search(self, key, node=None):
        node = node or self.root
        if node.is_leaf:
            for i, item in enumerate(node.keys):
                if item == key:
                    return node.children[i]  # Lista de registros
            return None
        else:
            for i, item in enumerate(node.keys):
                if key < item:
                    return self.search(key, node.children[i])
            return self.search(key, node.children[-1])

    # -------------------------------
    # INSERCIÓN
    # -------------------------------
    def insert(self, key, record):
        root = self.root
        new_child = self._insert_recursive(root, key, record)
        if new_child:
            new_root = BPlusTreeNode(self.order, is_leaf=False)
            new_root.keys = [new_child[0]]
            new_root.children = [root, new_child[1]]
            self.root = new_root

    def _insert_recursive(self, node, key, record):
        if node.is_leaf:
            # ya existe la clave
            if key in node.keys:
                idx = node.keys.index(key)
                node.children[idx].append(record)
                return None
            # insertar nueva clave
            i = 0
            while i < len(node.keys) and node.keys[i] < key:
                i += 1
            node.keys.insert(i, key)
            node.children.insert(i, [record])
            if len(node.keys) > self.order:
                return self._split_leaf(node)
            return None
        else:
            # bajar recursivamente
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            new_child = self._insert_recursive(node.children[i], key, record)
            if new_child:
                new_key, new_node = new_child
                node.keys.insert(i, new_key)
                node.children.insert(i + 1, new_node)
                if len(node.keys) > self.order:
                    return self._split_internal(node)
            return None

    def _split_leaf(self, node):
        mid = len(node.keys) // 2
        new_node = BPlusTreeNode(self.order, is_leaf=True)
        new_node.keys = node.keys[mid:]
        new_node.children = node.children[mid:]
        node.keys = node.keys[:mid]
        node.children = node.children[:mid]

        new_node.next = node.next
        node.next = new_node

        return new_node.keys[0], new_node

    def _split_internal(self, node):
        mid = len(node.keys) // 2
        new_node = BPlusTreeNode(self.order, is_leaf=False)
        new_node.keys = node.keys[mid + 1:]
        new_node.children = node.children[mid + 1:]

        promoted_key = node.keys[mid]
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]

        return promoted_key, new_node

    # -------------------------------
    # ELIMINACIÓN
    # -------------------------------
    def delete(self, key, record=None):
        self._delete_recursive(self.root, key, record)
        # si la raíz se queda sin claves y no es hoja, se baja un nivel
        if not self.root.is_leaf and len(self.root.keys) == 0:
            self.root = self.root.children[0]

    def _delete_recursive(self, node, key, record=None):
        if node.is_leaf:
            if key in node.keys:
                idx = node.keys.index(key)
                if record is None:
                    node.children.pop(idx)
                    node.keys.pop(idx)
                else:
                    # eliminar solo un registro dentro de la lista
                    if record in node.children[idx]:
                        node.children[idx].remove(record)
                        if not node.children[idx]:  # lista vacía
                            node.children.pop(idx)
                            node.keys.pop(idx)
            return

        # nodo interno
        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1
        self._delete_recursive(node.children[i], key, record)

        # balancear si es necesario
        if len(node.children[i].keys) < (self.order + 1) // 2:
            self._rebalance(node, i)

    def _rebalance(self, parent, idx):
        child = parent.children[idx]
        if idx > 0:  # tiene hermano izquierdo
            left = parent.children[idx - 1]
            if len(left.keys) > (self.order + 1) // 2:
                # rotar desde la izquierda
                if child.is_leaf:
                    child.keys.insert(0, left.keys.pop(-1))
                    child.children.insert(0, left.children.pop(-1))
                    parent.keys[idx - 1] = child.keys[0]
                else:
                    child.keys.insert(0, parent.keys[idx - 1])
                    parent.keys[idx - 1] = left.keys.pop(-1)
                    child.children.insert(0, left.children.pop(-1))
                return
        if idx < len(parent.children) - 1:  # tiene hermano derecho
            right = parent.children[idx + 1]
            if len(right.keys) > (self.order + 1) // 2:
                # rotar desde la derecha
                if child.is_leaf:
                    child.keys.append(right.keys.pop(0))
                    child.children.append(right.children.pop(0))
                    parent.keys[idx] = right.keys[0]
                else:
                    child.keys.append(parent.keys[idx])
                    parent.keys[idx] = right.keys.pop(0)
                    child.children.append(right.children.pop(0))
                return

        # si no hay redistribución posible → merge
        if idx > 0:
            self._merge(parent, idx - 1)
        else:
            self._merge(parent, idx)

    def _merge(self, parent, idx):
        child = parent.children[idx]
        sibling = parent.children[idx + 1]

        if child.is_leaf:
            child.keys.extend(sibling.keys)
            child.children.extend(sibling.children)
            child.next = sibling.next
        else:
            child.keys.append(parent.keys[idx])
            child.keys.extend(sibling.keys)
            child.children.extend(sibling.children)

        parent.keys.pop(idx)
        parent.children.pop(idx + 1)

    # -------------------------------
    # UTILIDADES
    # -------------------------------
    def traverse_leaves(self):
        """Recorrido de todas las hojas encadenadas (para depuración)."""
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        result = []
        while node:
            result.append((node.keys, node.children))
            node = node.next
        return result

