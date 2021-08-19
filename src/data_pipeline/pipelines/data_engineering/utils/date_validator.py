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
    Return whether the string can formated into a date

    :param value: str, string to check for date
    """
        valid_dashed_date = str(value).count('-') >=2 and len(str(value))>10
        valid_slashed_date = str(value).count('/') >=2 and len(str(value))>10
        return valid_dashed_date or valid_slashed_date;
        