from dateutil.parser import parse

def is_date(value):
    """
    Return whether the string can be interpreted as a date.

    :param value: str, string to check for date
    """
    try: 
        parse(value)
        return True

    except ValueError:
        return False