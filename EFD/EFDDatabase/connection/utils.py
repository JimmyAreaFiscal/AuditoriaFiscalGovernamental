import pandas as pd 
from datetime import datetime as dt 
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import os
import tracemalloc
import time
import numpy as np
import functools
import warnings 
import oracledb


# --------------------------------------------------------------------------------------------------------------------------------#
# ------------------------------------------------ Utils Functions  ------------------------------------------------ #


def oracle_manager(func):
    """ 
    Decorator to init oracledb
    
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            oracledb.init_oracle_client()
        except:
            raise
        
        warnings.filterwarnings('ignore')
        # Execute the function
        result = func(*args, **kwargs)
            
        return result
    return wrapper

 