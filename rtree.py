import struct

M = 4 # DEFAULT MAX CHILDREN PER NODE

class RTreeNode:
    def __init__(self, node_id):
        self.is_leaf = False
        self.node_id = node_id
        self.children = []
        self.bbox = (float('inf'), float('inf'), float('-inf'), float('-inf'))  # (minx, miny, maxx, maxy)
    
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

    def insert(self, rect, record):
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

    def search(self, rect):
        results = []
        self._search_recursive(self.root, rect, results)
        return results

    def _search_recursive(self, node, rect, results):
        if node.is_leaf:
            for child in node.children:
                if not (child[2] < rect[0] or child[0] >
                        rect[2] or child[3] < rect[1] or child[1] > rect[3]):
                    results.append(child[4])
        else:
            for child in node.children:
                if not (child.bbox[2] < rect[0] or child.bbox[0] >
                        rect[2] or child.bbox[3] < rect[1] or child.bbox[1] > rect[3]):
                    self._search_recursive(child, rect, results)        
    
    def range_search_radio(self, point, radius):
        # Range search within a circular area is not implemented in this basic R-Tree.
        # This is a placeholder for potential future implementation.
        results = []
        pass

    def range_search_k(self, point, k):
        # k-nearest neighbors search is not implemented in this basic R-Tree.
        # This is a placeholder for potential future implementation.
        results = []
        pass

    def intersection_search(self, bbox):
        pass

    def delete(self, record_id):
        # Deletion in R-Trees is complex and often not implemented in basic versions.
        # This is a placeholder for potential future implementation.
        pass

    