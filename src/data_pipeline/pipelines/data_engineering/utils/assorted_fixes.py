import pandas as pd
import re

def extract_years(value):
    # First, check if the value is purely numeric
    if value.isdigit():
        return int(value)
    match = re.search(r'(\d+)\s*years?', value)
    return int(match.group(1)) if match else None