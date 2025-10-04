import struct

M = 4 # DEFAULT MAX CHILDREN PER NODE

class RTreeNode:
    def __init__(self, node_id):
        self.is_leaf = False
        self.node_id = node_id
        self.children = [] # List of child nodes pointers
        self.point = None # For leaf nodes, store point data
        self.bbox = (float('inf'), float('inf'), float('-inf'), float('-inf'))  # (minx, miny, maxx, maxy)
    
    def _point(self):
        return self.point

    def min_xy(self):
        return (self.bbox[0], self.bbox[1])
    
    def max_xy(self):
        return (self.bbox[2], self.bbox[3])

    def update_bbox(self):
        if self.is_leaf:
            if not self.children:
                self.bbox = (float('inf'), float('inf'), float('-inf'), float('-inf'))
                return
            minx = min(child[0] for child in self.children)
            miny = min(child[1] for child in self.children)
            maxx = max(child[2] for child in self.children)
            maxy = max(child[3] for child in self.children)
            self.bbox = (minx, miny, maxx, maxy)
        else:
            # Primero actualizar bbox de todos los hijos
            for child in self.children:
                child.update_bbox()
            if not self.children:
                self.bbox = (float('inf'), float('inf'), float('-inf'), float('-inf'))
                return
            # Luego calcular bbox que englobe todos los hijos
            minx = min(child.bbox[0] for child in self.children)
            miny = min(child.bbox[1] for child in self.children)
            maxx = max(child.bbox[2] for child in self.children)
            maxy = max(child.bbox[3] for child in self.children)
            self.bbox = (minx, miny, maxx, maxy)
        
    def area(self):
        minx, miny, maxx, maxy = self.bbox
        return (maxx - minx) * (maxy - miny)
    
    def enlarged_area(self, rect):
        minx, miny, maxx, maxy = self.bbox
        rminx, rminy, rmaxx, rmaxy = rect
        new_minx = min(minx, rminx)
        new_miny = min(miny, rminy)
        new_maxx = max(maxx, rmaxx)
        new_maxy = max(maxy, rmaxy)
        return (new_maxx - new_minx) * (new_maxy - new_miny) - self.area()
    
class RTree:
    def __init__(self, max_children=M):
        self.root = RTreeNode(0)
        self.root.is_leaf = True
        self.max_children = max_children
        self.node_count = 1  # To assign unique IDs to nodes

    def is_empty(self):
        return len(self.root.children) == 0

    def insert(self, rect, record):
        """Insert a record given its bounding rect and payload.

        rect: (minx, miny, maxx, maxy)
        record: any payload or identifier
        """
        if self.root.is_leaf and len(self.root.children) == 0:
            self.root.children.append((rect[0], rect[1], rect[2], rect[3], record))
            self.root.update_bbox()
        else:
            self.insert_by_rect(rect, record)

    def insert_by_rect(self, rect, record):
        leaf = self._choose_leaf(self.root, rect)
        leaf.children.append((rect[0], rect[1], rect[2], rect[3], record))
        leaf.update_bbox()
        if len(leaf.children) > self.max_children:
            self._split_node(leaf)
    
    def _choose_leaf(self, node, rect):
        if node.is_leaf:
            return node
        else:
            best_child = min(node.children, key=lambda child: child.enlarged_area(rect))
            return self._choose_leaf(best_child, rect)
    
    def _split_node(self, node):
        mid = len(node.children) // 2
        new_node = RTreeNode(self.node_count)
        self.node_count += 1
        new_node.is_leaf = node.is_leaf
        new_node.children = node.children[mid:]
        node.children = node.children[:mid]
        
        node.update_bbox()
        new_node.update_bbox()
        
        if node == self.root:
            new_root = RTreeNode(self.node_count)
            self.node_count += 1
            new_root.is_leaf = False
            new_root.children = [node, new_node]
            new_root.update_bbox()
            self.root = new_root
        else:
            parent = self._find_parent(self.root, node)
            parent.children.append(new_node)
            parent.update_bbox()
            if len(parent.children) > self.max_children:
                self._split_node(parent)

    def _find_parent(self, current, child):
        if current.is_leaf:
            return None
        for c in current.children:
            if c == child:
                return current
            if not c.is_leaf:  # Solo buscar recursivamente en nodos internos
                res = self._find_parent(c, child)
                if res:
                    return res
        return None

    def search(self, key):
        results = []
        self._search_recursive(self.root, key, results)
        return results
    
    def _search_recursive(self, node, key, results):
        if node.is_leaf:
            for child in node.children:
                if not (child[2] < key[0] or child[0] > key[2] or
                        child[3] < key[1] or child[1] > key[3]):
                    results.append(child[4])
        else:
            for child in node.children:
                if not (child.bbox[2] < key[0] or child.bbox[0] > key[2] or
                        child.bbox[3] < key[1] or child.bbox[1] > key[3]):
                    self._search_recursive(child, key, results)
    
    def range_search_radio(self, point, radius):
        # Range search within a circular area is not implemented in this basic R-Tree.
        results = []
        for child in self.root.children:
            if isinstance(child, RTreeNode):
                if (child.bbox[0] <= point[0] + radius and child.bbox[2] >= point[0] - radius and
                    child.bbox[1] <= point[1] + radius and child.bbox[3] >= point[1] - radius):
                    self._range_search_radio_recursive(child, point, radius, results)
        return results
    def _range_search_radio_recursive(self, node, point, radius, results):
        if node.is_leaf:
            for child in node.children:
                cx = (child[0] + child[2]) / 2
                cy = (child[1] + child[3]) / 2
                if (cx - point[0]) ** 2 + (cy - point[1]) ** 2 <= radius ** 2:
                    results.append(child[4])
        else:
            for child in node.children:
                if (child.bbox[0] <= point[0] + radius and child.bbox[2] >= point[0] - radius and
                    child.bbox[1] <= point[1] + radius and child.bbox[3] >= point[1] - radius):
                    self._range_search_radio_recursive(child, point, radius, results)

    def range_search_k(self, point, k):
        results = []
        for child in self.root.children:
            if isinstance(child, RTreeNode):
                self._range_search_k_recursive(child, point, results)
        return results[:k]

    def _range_search_k_recursive(self, node, point, results):
        if node.is_leaf:
            for child in node.children:
                cx = (child[0] + child[2]) / 2
                cy = (child[1] + child[3]) / 2
                dist = ((cx - point[0]) ** 2 + (cy - point[1]) ** 2) ** 0.5
                results.append((dist, child[4]))
            results.sort(key=lambda x: x[0])
        else:
            for child in node.children:
                self._range_search_k_recursive(child, point, results)

    def intersection_search(self, bbox):
        results = []
        self._intersection_search_recursive(self.root, bbox, results)
        return results
    
    def _intersection_search_recursive(self, node, bbox, results):
        if node.is_leaf:
            for child in node.children:
                if not (child[2] < bbox[0] or child[0] > bbox[2] or
                        child[3] < bbox[1] or child[1] > bbox[3]):
                    results.append(child[4])
        else:
            for child in node.children:
                if not (child.bbox[2] < bbox[0] or child.bbox[0] > bbox[2] or
                        child.bbox[3] < bbox[1] or child.bbox[1] > bbox[3]):
                    self._intersection_search_recursive(child, bbox, results)

    def delete(self, key):
        """Delete a record from the R-Tree"""
        deleted_nodes = []
        self._delete_recursive(self.root, key, deleted_nodes)
        
        # Reinsert orphaned entries from deleted nodes
        for orphaned_entries in deleted_nodes:
            for entry in orphaned_entries:
                if len(entry) == 5:  # Leaf entry: (minx, miny, maxx, maxy, record)
                    rect = (entry[0], entry[1], entry[2], entry[3])
                    self.insert(rect, entry[4])
                else:  # Internal node child
                    self._reinsert_subtree(entry)
        
        # If root has only one child and is not a leaf, make child the new root
        if not self.root.is_leaf and len(self.root.children) == 1:
            self.root = self.root.children[0]
    
    def _delete_recursive(self, node, record_id, deleted_nodes):
        """Recursively search and delete the record"""
        if node.is_leaf:
            # Remove matching records from leaf node
            original_count = len(node.children)
            node.children = [child for child in node.children if child[4] != record_id]
            
            if len(node.children) < original_count:
                node.update_bbox()
                # Check for underflow (less than m entries where m = max_children/2)
                min_entries = max(1, self.max_children // 2)
                if len(node.children) < min_entries and node != self.root:
                    # Node underflows - it will be deleted and entries reinserted
                    deleted_nodes.append(node.children[:])  # Save entries for reinsertion
                    node.children = []  # Mark node as deleted
                    return True
            return len(node.children) < original_count
        else:
            # Internal node - search in children
            nodes_to_remove = []
            for i, child in enumerate(node.children):
                if self._delete_recursive(child, record_id, deleted_nodes):
                    # Child was modified or deleted
                    if not child.children:  # Child node was deleted (empty)
                        nodes_to_remove.append(i)
            
            # Remove deleted child nodes
            for i in reversed(nodes_to_remove):
                node.children.pop(i)
            
            if node.children:  # If node still has children
                node.update_bbox()
                # Check for underflow in internal node
                min_entries = max(1, self.max_children // 2)
                if len(node.children) < min_entries and node != self.root:
                    # Save all child subtrees for reinsertion
                    deleted_nodes.append(node.children[:])
                    node.children = []  # Mark node as deleted
                    return True
            else:
                # Node has no children left
                return True
            
            return len(nodes_to_remove) > 0
    
    def _reinsert_subtree(self, subtree_root):
        """Reinsert a subtree that was orphaned during deletion"""
        if subtree_root.is_leaf:
            # Reinsert all entries in this leaf
            for entry in subtree_root.children:
                rect = (entry[0], entry[1], entry[2], entry[3])
                self.insert(rect, entry[4])
        else:
            # Recursively reinsert all subtrees
            for child in subtree_root.children:
                self._reinsert_subtree(child)

    
if __name__ == "__main__":
    # Quick self-tests placed here so the file is runnable
    def basic_test():
        print("=== Basic R-Tree test ===")
        t = RTree(max_children=4)
        t.insert((1, 1, 2, 2), "A")
        t.insert((2, 2, 3, 3), "B")
        t.insert((3, 3, 4, 4), "C")
        t.insert((5, 5, 6, 6), "D")
        t.insert((7, 7, 8, 8), "E")

        print("Search (1.5,1.5,2.5,2.5):", t.search((1.5, 1.5, 2.5, 2.5)))
        print("Search (6,6,7,7):", t.search((6, 6, 7, 7)))

        t.delete("B")
        print("After deleting B:")
        print("Search (1.5,1.5,2.5,2.5):", t.search((1.5, 1.5, 2.5, 2.5)))
        print("Search (2.5,2.5,3.5,3.5):", t.search((2.5, 2.5, 3.5, 3.5)))

    def underflow_test():
        print("\n=== Underflow test ===")
        t = RTree(max_children=2)
        # insert small grid
        recs = []
        for i in range(3):
            for j in range(3):
                r = (i, j, i + 0.4, j + 0.4)
                name = f"p_{i}_{j}"
                recs.append(name)
                t.insert(r, name)

        print("Total before deletions:", len(t.search((-1, -1, 10, 10))))
        # delete some
        for name in recs[:4]:
            t.delete(name)
        print("Total after deletions:", len(t.search((-1, -1, 10, 10))))

    def root_restructure_test():
        print("\n=== Root restructure test ===")
        t = RTree(max_children=2)
        t.insert((0, 0, 1, 1), "A")
        t.insert((10, 10, 11, 11), "B")
        t.insert((20, 20, 21, 21), "C")
        print("Before deletions - total:", len(t.search((-1, -1, 30, 30))))
        t.delete("A")
        t.delete("B")
        print("After deletions - total:", len(t.search((-1, -1, 30, 30))))

    basic_test()
    underflow_test()
    root_restructure_test()
