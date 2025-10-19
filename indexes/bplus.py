import pickle
import os
import struct
from typing import List, Any, Optional

class BPlusTreeNode:
    def __init__(self, order, is_leaf=False):
        self.order = order
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []
        self.next = None  # Para enlazar hojas
        self.node_id = None  # ID único para persistencia


class BPlusTreePersistence:
    """Maneja la persistencia del árbol B+ en memoria secundaria."""
    
    def __init__(self, index_filename: str):
        self.index_filename = index_filename
        self.node_counter = 0
    
    def _generate_node_id(self) -> int:
        """Genera un ID único para cada nodo."""
        self.node_counter += 1
        return self.node_counter
    
    def _assign_node_ids(self, node: BPlusTreeNode):
        """Asigna IDs únicos a todos los nodos del árbol."""
        if node.node_id is None:
            node.node_id = self._generate_node_id()
        
        if not node.is_leaf:
            for child in node.children:
                if isinstance(child, BPlusTreeNode):
                    self._assign_node_ids(child)
    
    def save_tree(self, tree: 'BPlusTree'):
        """Guarda el árbol B+ completo en un archivo."""
        # Asignar IDs a todos los nodos
        self._assign_node_ids(tree.root)
        
        # Serializar el árbol
        tree_data = {
            'root': tree.root,
            'order': tree.order,
            'node_counter': self.node_counter
        }
        
        with open(self.index_filename, 'wb') as f:
            pickle.dump(tree_data, f)
    
    def load_tree(self) -> Optional['BPlusTree']:
        """Carga el árbol B+ desde un archivo."""
        if not os.path.exists(self.index_filename):
            return None
        
        try:
            with open(self.index_filename, 'rb') as f:
                tree_data = pickle.load(f)
            
            tree = BPlusTree(tree_data['order'])
            tree.root = tree_data['root']
            self.node_counter = tree_data.get('node_counter', 0)
            
            return tree
        except Exception as e:
            print(f"Error al cargar el árbol B+: {e}")
            return None


class BPlusTree:
    def __init__(self, order=4, index_filename: str = None):
        self.root = BPlusTreeNode(order, is_leaf=True)
        self.order = order
        self.persistence = BPlusTreePersistence(index_filename) if index_filename else None
        self._auto_save = True  # Guardar automáticamente después de cada operación

    def is_empty(self):
        """Check if the BPlus tree is empty."""
        return len(self.root.keys) == 0
    
    def load_from_file(self):
        """Carga el árbol desde el archivo de persistencia."""
        if self.persistence:
            loaded_tree = self.persistence.load_tree()
            if loaded_tree:
                self.root = loaded_tree.root
                self.order = loaded_tree.order
                if loaded_tree.persistence:
                    self.persistence.node_counter = loaded_tree.persistence.node_counter
                return True
        return False
    
    def save_to_file(self):
        """Guarda el árbol en el archivo de persistencia."""
        if self.persistence:
            self.persistence.save_tree(self)
    
    def _auto_save_if_enabled(self):
        """Guarda automáticamente si está habilitado."""
        if self._auto_save and self.persistence:
            self.save_to_file()

    # -------------------------------
    # BÚSQUEDA
    # -------------------------------
    def search(self, key, node=None):
        node = node or self.root
        if node.is_leaf:
            for i, item in enumerate(node.keys):
                if item == key:
                    return node.children[i]  # Return position
            return None
        else:
            for i, item in enumerate(node.keys):
                if key < item:
                    return self.search(key, node.children[i])
            return self.search(key, node.children[-1])

    def range_search(self, start, end):
        """Search for all keys in the range [start, end] and return list of (key, pos) tuples."""
        result = []
        if self.is_empty():
            return result
        
        # Find the first leaf node
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        
        # Traverse all leaf nodes
        while node:
            for i, key in enumerate(node.keys):
                if start <= key <= end:
                    result.append((key, node.children[i]))
                elif key > end:
                    break
            node = node.next
        
        return result

    # -------------------------------
    # INSERCIÓN
    # -------------------------------
    def insert(self, key, pos):
        root = self.root
        new_child = self._insert_recursive(root, key, pos)
        if new_child:
            new_root = BPlusTreeNode(self.order, is_leaf=False)
            new_root.keys = [new_child[0]]
            new_root.children = [root, new_child[1]]
            self.root = new_root
        
        # Guardar automáticamente después de la inserción
        self._auto_save_if_enabled()

    def _insert_recursive(self, node, key, pos):
        if node.is_leaf:
            # ya existe la clave
            if key in node.keys:
                idx = node.keys.index(key)
                node.children[idx] = pos  # Update position
                return None
            # insertar nueva clave
            i = 0
            while i < len(node.keys) and node.keys[i] < key:
                i += 1
            node.keys.insert(i, key)
            node.children.insert(i, pos)
            if len(node.keys) > self.order:
                return self._split_leaf(node)
            return None
        else:
            # bajar recursivamente
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            new_child = self._insert_recursive(node.children[i], key, pos)
            if new_child:
                new_key, new_node = new_child
                node.keys.insert(i, new_key)
                node.children.insert(i + 1, new_node)
                if len(node.keys) > self.order:
                    return self._split_internal(node)
            return None

    def update(self, key, pos):
        """Update the position for an existing key."""
        if self.search(key) is not None:
            self._update_recursive(self.root, key, pos)
            # Guardar automáticamente después de la actualización
            self._auto_save_if_enabled()
        else:
            # If key doesn't exist, insert it
            self.insert(key, pos)

    def _update_recursive(self, node, key, pos):
        if node.is_leaf:
            for i, item in enumerate(node.keys):
                if item == key:
                    node.children[i] = pos
                    return
        else:
            for i, item in enumerate(node.keys):
                if key < item:
                    self._update_recursive(node.children[i], key, pos)
                    return
            self._update_recursive(node.children[-1], key, pos)

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
    def delete(self, key):
        self._delete_recursive(self.root, key)
        # si la raíz se queda sin claves y no es hoja, se baja un nivel
        if not self.root.is_leaf and len(self.root.keys) == 0:
            self.root = self.root.children[0]
        
        # Guardar automáticamente después de la eliminación
        self._auto_save_if_enabled()

    def _delete_recursive(self, node, key):
        if node.is_leaf:
            if key in node.keys:
                idx = node.keys.index(key)
                node.children.pop(idx)
                node.keys.pop(idx)
            return

        # nodo interno
        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1
        self._delete_recursive(node.children[i], key)

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

