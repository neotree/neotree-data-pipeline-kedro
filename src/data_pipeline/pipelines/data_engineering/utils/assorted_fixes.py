import pandas as pd
import re

def extract_years(value):
    if value is None:
        return None

    # Convert to string and strip whitespace
    val_str = str(value).strip()

    # First, check for a number (int or float) optionally followed by 'years'
    match = re.match(r'^(\d+(\.\d+)?)\s*(years?)?$', val_str, re.IGNORECASE)
    if match:
        return int(float(match.group(1)))  # handles both int and float strings

    # Second, try to extract number from phrases like '23 years', '23.5 years old', etc.
    match = re.search(r'(\d+(\.\d+)?)\s*years?', val_str, re.IGNORECASE)
    if match:
        return int(float(match.group(1)))

    return None
