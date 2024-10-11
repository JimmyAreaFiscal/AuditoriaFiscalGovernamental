# Third-party modules import
import pandas as pd 
import oracledb 
import warnings 
from datetime import datetime as dt
from datetime import timedelta
from dateutil.relativedelta import relativedelta


# User-defined modules import
from .utils import oracle_manager
from .usuario import dns_tns, pw, user

# Import built-in modules
import warnings 

@oracle_manager
def import_efd_9001(cabe_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros 9001    
    Args
    ----------------------
    cabe_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {
        'CONT_ID_9001': int,
        'CABE_ID_0000': int,
        'IND_MOV': str
    }

    assert len(cabe_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(cabe_id_0000)}"

    assert isinstance(cabe_id_0000, tuple), f"The cabe_id input isn't a tuple! Check the input! {type(cabe_id_0000)}"

    # SQL query to fetch 9001 data
    query = f""" 
                SELECT
                    CONT_ID_9001,
                    CABE_ID_0000,
                    CONT_TP_IND_MOV9001 AS "IND_MOV"
                FROM
                    EFD.TB_EFD_CONTROLE9001
                WHERE
                    CABE_ID_0000 in {cabe_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_9001 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_9001


@oracle_manager
def import_efd_9900(cont_id_9001) -> pd.DataFrame:
    """ 
    Importa os registros 9900    
    Args
    ----------------------
    cont_id_9001: tuple
        Chave estrangeira do registro 9001
    """

    # Creating SQL query
    columns = {
        'CONT_ID_9900': int,
        'CONT_ID_9001': int,
        'REG_BLC': str,
        'QTD_REG_BLC': float
    }

    assert len(cont_id_9001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(cont_id_9001)}"

    assert isinstance(cont_id_9001, tuple), f"The cont_id input isn't a tuple! Check the input! {type(cont_id_9001)}"

    # SQL query to fetch 9900 data
    query = f""" 
                SELECT
                    CONT_ID_9900,
                    CONT_ID_9001,
                    CONT_DS_REG_BLC9900 AS "REG_BLC",
                    CONT_QT_QTD_REG_BLC9900 AS "QTD_REG_BLC"
                FROM
                    EFD.TB_EFD_CONTROLE9900
                WHERE
                    CONT_ID_9001 in {cont_id_9001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_9900 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_9900


@oracle_manager
def import_efd_9990(cabe_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros 9990    
    Args
    ----------------------
    cabe_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {
        'CONT_ID_9990': int,
        'CABE_ID_0000': int,
        'QTD_LIN_9': float
    }

    assert len(cabe_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(cabe_id_0000)}"

    assert isinstance(cabe_id_0000, tuple), f"The cabe_id input isn't a tuple! Check the input! {type(cabe_id_0000)}"

    # SQL query to fetch 9990 data
    query = f""" 
                SELECT
                    CONT_ID_9990,
                    CABE_ID_0000,
                    CONT_QT_QTD_LIN_9 AS "QTD_LIN_9"
                FROM
                    EFD.TB_EFD_CONTROLE9990
                WHERE
                    CABE_ID_0000 in {cabe_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_9990 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_9990


@oracle_manager
def import_efd_9999() -> pd.DataFrame:
    """ 
    Importa os registros 9999    
    """

    # Creating SQL query
    columns = {
        'CONT_ID_9999': int,
        'QTD_LIN': float
    }

    # SQL query to fetch 9999 data
    query = f""" 
                SELECT
                    CONT_ID_9999,
                    CONT_QT_QTD_LIN9999 AS "QTD_LIN"
                FROM
                    EFD.TB_EFD_CONTROLE9999
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_9999 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_9999

