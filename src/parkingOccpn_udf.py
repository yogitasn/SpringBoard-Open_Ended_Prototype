from datetime import datetime, timedelta
from pyspark.sql.functions import udf
import re

# udf to remove ' and , in the column value
# regex_replace
commaRep = udf(lambda x: re.sub('[\'\s,]','', x))

# udf to remove '(' and ')' in the column value
braceRepl = udf(lambda x: re.sub('\(|\)','', x))


def format_minstoHHMMSS(x):
    """
    Function to convert the minutes to HH:MM:SS format
    """
    try:
        duration=timedelta(minutes=int(x))
        seconds = duration.total_seconds()
        minutes = seconds // 60
        hours = minutes // 60
        return "%02d:%02d:%02d" % (hours, minutes % 60, seconds % 60)
    except:
        None

# udf for entire dataframe
udf_format_minstoHHMMSS=udf(lambda x: format_minstoHHMMSS(x))