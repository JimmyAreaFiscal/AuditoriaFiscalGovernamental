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
def import_efd_G001(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros G001    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {         
                        'CRED_ID_001': int,
                        'CABE_ID_0000': int,
                        'IND_MOV': str
            }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # SQL query to fetch G001 data
    query = f""" 
                SELECT
                    CRED_ID_001,
                    CABE_ID_0000,
                    CRED_TP_IND_MOV AS "IND_MOV"
                FROM
                    EFD.TB_EFD_CREDITOG001
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_G001 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_G001


@oracle_manager
def import_efd_G110(efd_id_001) -> pd.DataFrame:
    """ 
    Importa os registros G110    
    Args
    ----------------------
    efd_id_001: tuple
        Chave estrangeira do registro G001
    """

    # Creating SQL query
    columns = {         
                        'CRED_ID_110': int,
                        'CRED_ID_001': int,
                        'DT_INI': 'datetime64[ns]',
                        'DT_FIN': 'datetime64[ns]',
                        'SALDO_IN_ICMS': float,
                        'SOM_PARC': float,
                        'VL_TRIB_EXP': float,
                        'VL_TOTAL': float,
                        'IND_PER_SAI': str,
                        'ICMS_APROP': float,
                        'SOM_ICMS_OC': float
            }

    assert len(efd_id_001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_001)}"

    assert isinstance(efd_id_001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_001)}"

    # SQL query to fetch G110 data
    query = f""" 
                SELECT
                    CRED_ID_110,
                    CRED_ID_001,
                    CRED_DT_INI125 AS "DT_INI",
                    CRED_DT_FIN125 AS "DT_FIN",
                    CRED_VL_SALDO_IN_ICMS125 AS "SALDO_IN_ICMS",
                    CRED_VL_SOM_PARC125 AS "SOM_PARC",
                    CRED_VL_TRIB_EXP125 AS "VL_TRIB_EXP",
                    CRED_VL_TOTAL125 AS "VL_TOTAL",
                    CRED_TP_IND_PER_SAI125 AS "IND_PER_SAI",
                    CRED_VL_ICMS_APROP125 AS "ICMS_APROP",
                    CRED_VL_SOM_ICMS_OC125 AS "SOM_ICMS_OC"
                FROM
                    EFD.TB_EFD_CREDITOG110
                WHERE
                    CRED_ID_001 in {efd_id_001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_G110 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_G110


@oracle_manager
def import_efd_G125(efd_id_110) -> pd.DataFrame:
    """ 
    Importa os registros G125    
    Args
    ----------------------
    efd_id_110: tuple
        Chave estrangeira do registro G110
    """

    # Creating SQL query
    columns = {         
                        'CRED_ID_125': int,
                        'CRED_ID_110': int,
                        'COD_IND_BEM': str,
                        'DT_MOV': 'datetime64[ns]',
                        'TIPO_MOV': str,
                        'VL_IMOB_ICMS_OP': float,
                        'VL_IMOB_ICMS_ST': float,
                        'VL_IMOB_ICMS_FRT': float,
                        'VL_IMOB_ICMS_DIF': float,
                        'NUM_PARC': int,
                        'VL_PARC_PASS': float
            }

    assert len(efd_id_110),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_110)}"

    assert isinstance(efd_id_110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_110)}"

    # SQL query to fetch G125 data
    query = f""" 
                SELECT
                    CRED_ID_125,
                    CRED_ID_110,
                    CRED_CD_COD_IND_BEM125 AS "COD_IND_BEM",
                    CRED_DT_MOV125 AS "DT_MOV",
                    CRED_TP_TIPO_MOV125 AS "TIPO_MOV",
                    CRED_VL_IMOB_ICMS_OP125 AS "VL_IMOB_ICMS_OP",
                    CRED_VL_IMOB_ICMS_ST125 AS "VL_IMOB_ICMS_ST",
                    CRED_VL_IMOB_ICMS_FRT125 AS "VL_IMOB_ICMS_FRT",
                    CRED_VL_IMOB_ICMS_DIF125 AS "VL_IMOB_ICMS_DIF",
                    CRED_NR_NUM_PARC125 AS "NUM_PARC",
                    CRED_VL_PARC_PASS125 AS "VL_PARC_PASS"
                FROM
                    EFD.TB_EFD_CREDITOG125
                WHERE
                    CRED_ID_110 in {efd_id_110}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_G125 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_G125


@oracle_manager
def import_efd_G126(efd_id_125) -> pd.DataFrame:
    """ 
    Importa os registros G126    
    Args
    ----------------------
    efd_id_125: tuple
        Chave estrangeira do registro G125
    """

    # Creating SQL query
    columns = {         
                        'CRED_ID_126': int,
                        'CRED_ID_125': int,
                        'DT_INI': 'datetime64[ns]',
                        'DT_FIN': 'datetime64[ns]',
                        'NUM_PARC': int,
                        'VL_PARC_PASS': float,
                        'VL_TRIB_OC': float,
                        'VL_TOTAL': float,
                        'IND_PER_SAI': str,
                        'VL_PARC_APROP': float
            }

    assert len(efd_id_125),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_125)}"

    assert isinstance(efd_id_125, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_125)}"

    # SQL query to fetch G126 data
    query = f""" 
                SELECT
                    CRED_ID_126,
                    CRED_ID_125,
                    CRED_DT_INI126 AS "DT_INI",
                    CRED_DT_FIN126 AS "DT_FIN",
                    CRED_NR_NUM_PARC126 AS "NUM_PARC",
                    CRED_VL_PARC_PASS126 AS "VL_PARC_PASS",
                    CRED_VL_TRIB_OC126 AS "VL_TRIB_OC",
                    CRED_VL_TOTAL126 AS "VL_TOTAL",
                    CRED_TP_IND_PER_SAI126 AS "IND_PER_SAI",
                    CRED_VL_PARC_APROP126 AS "VL_PARC_APROP"
                FROM
                    EFD.TB_EFD_CREDITOG126
                WHERE
                    CRED_ID_125 in {efd_id_125}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_G126 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_G126


@oracle_manager
def import_efd_G130(efd_id_125) -> pd.DataFrame:
    """ 
    Importa os registros G130    
    Args
    ----------------------
    efd_id_125: tuple
        Chave estrangeira do registro G125
    """

    # Creating SQL query
    columns = {         
                        'CRED_ID_130': int,
                        'CRED_ID_125': int,
                        'IND_EMIT': str,
                        'COD_PART': str,
                        'COD_MOD': str,
                        'SERIE': str,
                        'NUM_DOC': int,
                        'CHV_NFE_CTE': str,
                        'DT_DOC': 'datetime64[ns]',
                        'NUM_DA': str
            }

    assert len(efd_id_125),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_125)}"

    assert isinstance(efd_id_125, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_125)}"

    # SQL query to fetch G130 data
    query = f""" 
                SELECT
                    CRED_ID_130,
                    CRED_ID_125,
                    CRED_TP_IND_EMIT130 AS "IND_EMIT",
                    CRED_CD_COD_PART130 AS "COD_PART",
                    CRED_CD_COD_MOD130 AS "COD_MOD",
                    CRED_CD_SERIE130 AS "SERIE",
                    CRED_NR_DOC130 AS "NUM_DOC",
                    CRED_CD_CHV_NFE_CTE130 AS "CHV_NFE_CTE",
                    CRED_DT_DOC130 AS "DT_DOC",
                    CRED_NR_NUM_DA130 AS "NUM_DA"
                FROM
                    EFD.TB_EFD_CREDITOG130
                WHERE
                    CRED_ID_125 in {efd_id_125}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_G130 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_G130


@oracle_manager
def import_efd_G140(efd_id_130) -> pd.DataFrame:
    """ 
    Importa os registros G140    
    Args
    ----------------------
    efd_id_130: tuple
        Chave estrangeira do registro G130
    """

    # Creating SQL query
    columns = {         
                        'CRED_ID_140': int,
                        'CRED_ID_130': int,
                        'NR_NUM_ITEM': int,
                        'COD_ITEM': str,
                        'QTDE': float,
                        'UNID': str,
                        'VL_ICMS_OP_APLICADO': float,
                        'VL_ICMS_ST_APLICADO': float,
                        'VL_ICMS_FRT_APLICADO': float,
                        'VL_ICMS_DIF_APLICADO': float
            }

    assert len(efd_id_130),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_130)}"

    assert isinstance(efd_id_130, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_130)}"

    # SQL query to fetch G140 data
    query = f""" 
                SELECT
                    CRED_ID_140,
                    CRED_ID_130,
                    CRED_NR_NUM_ITEM140 AS "NR_NUM_ITEM",
                    CRED_CD_COD_ITEM140 AS "COD_ITEM",
                    CRED_QTDE140 AS "QTDE",
                    CRED_UNID140 AS "UNID",
                    CRED_VL_ICMS_OP_APLICADO140 AS "VL_ICMS_OP_APLICADO",
                    CRED_VL_ICMS_ST_APLICADO140 AS "VL_ICMS_ST_APLICADO",
                    CRED_VL_ICMS_FRT_APLICADO140 AS "VL_ICMS_FRT_APLICADO",
                    CRED_VL_ICMS_DIF_APLICADO140 AS "VL_ICMS_DIF_APLICADO"
                FROM
                    EFD.TB_EFD_CREDITOG140
                WHERE
                    CRED_ID_130 in {efd_id_130}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_G130 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_G130


@oracle_manager
def import_efd_G990(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros G990    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {         
                'CRED_ID_990': int,
                'CABE_ID_0000': int,
                'QTD_LIN_G': int
            }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # SQL query to fetch G990 data
    query = f""" 
                SELECT
                    CRED_ID_990,
                    CABE_ID_0000,
                    CRED_QT_QTD_LIN_G AS "QTD_LIN_G"
                FROM
                    EFD.TB_EFD_CREDITOG990
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_G990 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_G990
