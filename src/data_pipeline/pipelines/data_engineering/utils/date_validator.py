from dateutil.parser import parse

def is_date(value):
    """
    Return whether the string can be interpreted as a date.

    :param value: str, string to check for date
    """
    try:
        if(len(value.strip())<10): 
            return False
        else:
            return True

    except Exception as ex:
        return False

def is_date_formatable(value):
    """
    Return whether the string can be formatted into a date
    
    :param value: str, string to check for date
    """
    if pd.isna(value) or value is None or str(value).strip().lower() in ('none', 'null', ''):
        return False
    try:
        parse(str(value))
        return True
    except (ValueError, TypeError, OverflowError):
        return False
        