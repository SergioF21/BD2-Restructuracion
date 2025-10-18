#!/usr/bin/env python3
"""
Script simple para ejecutar las pruebas del Sequential File
"""

import sys
import os

# Agregar el directorio del proyecto al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importar y ejecutar las pruebas
from test_sequential_file import main

if __name__ == "__main__":
    main()