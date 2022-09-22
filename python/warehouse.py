import os

def get_warehouse_top_level_dir():
    current_dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(current_dir, "..", "test_warehouse")

def ensure_warehouse():
    os.makedirs(get_warehouse_top_level_dir(), exist_ok=True)
