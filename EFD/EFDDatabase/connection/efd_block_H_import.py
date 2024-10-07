# Third-party modules import
import pandas as pd 
import oracledb 

# User-defined modules import
from .utils import oracle_manager
from .usuario import dns_tns, pw, user



@oracle_manager
def import_efd_H001(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros H001    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {         
                'INVE_ID_001': int,
                'CABE_ID_0000': int,
                'IND_MOV': str
            }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # SQL query to fetch H001 data
    query = f""" 
                SELECT
                    INVE_ID_001,
                    CABE_ID_0000,
                    INVE_TP_IND_MOV001 AS "IND_MOV"
                FROM
                    EFD.TB_EFD_INVENTARIOH001
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_H001 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_H001


@oracle_manager
def import_efd_H005(efd_id_001) -> pd.DataFrame:
    """ 
    Importa os registros H005    
    Args
    ----------------------
    efd_id_001: tuple
        Chave estrangeira do registro H001
    """

    # Creating SQL query
    columns = {         
                        'INVE_ID_005': int,
                        'INVE_ID_001': int,
                        'DT_INV': 'datetime64[ns]',
                        'VL_INV': float,
                        'MOT_INV': str
            }

    assert len(efd_id_001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_001)}"

    assert isinstance(efd_id_001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_001)}"

    # SQL query to fetch H005 data
    query = f""" 
                SELECT
                    INVE_ID_005,
                    INVE_ID_001,
                    INVE_DT_INV005 AS "DT_INV",
                    INVE_VL_INV005 AS "VL_INV",
                    INVE_TP_MOT_INV005 AS "MOT_INV"
                FROM
                    EFD.TB_EFD_INVENTARIOH005
                WHERE
                    INVE_ID_001 in {efd_id_001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_H005 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_H005


@oracle_manager
def import_efd_H010(efd_id_005) -> pd.DataFrame:
    """ 
    Importa os registros H010    
    Args
    ----------------------
    efd_id_005: tuple
        Chave estrangeira do registro H005
    """

    # Creating SQL query
    columns = {         
                        'INVE_ID_010': int,
                        'INVE_ID_005': int,
                        'COD_ITEM': str,
                        'UNID': str,
                        'QTD': float,
                        'VL_UNIT': float,
                        'VL_ITEM': float,
                        'IND_PROP': str,
                        'COD_PART': str,
                        'TXT_COMPL': str,
                        'COD_CTA': str,
                        'VL_ITEM_IR': float
            }

    assert len(efd_id_005),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_005)}"

    assert isinstance(efd_id_005, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_005)}"

    # SQL query to fetch H010 data
    query = f""" 
                SELECT
                    INVE_ID_010,
                    INVE_ID_005,
                    INVE_CD_COD_ITEM010 AS "COD_ITEM",
                    INVE_MD_UNID010 AS "UNID",
                    INVE_QT_QTD010 AS "QTD",
                    INVE_VL_UNIT010 AS "VL_UNIT",
                    INVE_VL_ITEM010 AS "VL_ITEM",
                    INVE_TP_IND_PROP010 AS "IND_PROP",
                    INVE_CD_COD_PART010 AS "COD_PART",
                    INVE_DS_TXT_COMPL010 AS "TXT_COMPL",
                    INVE_CD_COD_CTA010 AS "COD_CTA",
                    INVE_VL_ITEM_IR010 AS "VL_ITEM_IR"
                FROM
                    EFD.TB_EFD_INVENTARIOH010
                WHERE
                    INVE_ID_005 in {efd_id_005}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_H010 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_H010


@oracle_manager
def import_efd_H020(efd_id_010) -> pd.DataFrame:
    """ 
    Importa os registros H020    
    Args
    ----------------------
    efd_id_010: tuple
        Chave estrangeira do registro H010
    """

    # Creating SQL query
    columns = {         
                        'INVE_ID_020': int,
                        'INVE_ID_010': int,
                        'CST_ICMS': str,
                        'BC_ICMS': float,
                        'VL_ICMS': float
            }

    assert len(efd_id_010),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_010)}"

    assert isinstance(efd_id_010, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_010)}"

    # SQL query to fetch H020 data
    query = f""" 
                SELECT
                    INVE_ID_020,
                    INVE_ID_010,
                    INVE_CD_CST_ICMS020 AS "CST_ICMS",
                    INVE_VL_BC_ICMS020 AS "BC_ICMS",
                    INVE_VL_ICMS020 AS "VL_ICMS"
                FROM
                    EFD.TB_EFD_INVENTARIOH020
                WHERE
                    INVE_ID_010 in {efd_id_010}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_H020 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_H020


@oracle_manager
def import_efd_H030(efd_id_010) -> pd.DataFrame:
    """ 
    Importa os registros H030    
    Args
    ----------------------
    efd_id_010: tuple
        Chave estrangeira do registro H010
    """

    # Creating SQL query
    columns = {         
                        'INVE_ID_030': int,
                        'INVE_ID_010': int,
                        'VL_ICMS_OP': float,
                        'VL_BC_ICMS_ST': float,
                        'VL_ICMS_ST': float,
                        'VL_FCP': float
            }

    assert len(efd_id_010),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_010)}"

    assert isinstance(efd_id_010, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_010)}"

    # SQL query to fetch H030 data
    query = f""" 
                SELECT
                    INVE_ID_030,
                    INVE_ID_010,
                    INVE_VL_ICMS_OP030 AS "VL_ICMS_OP",
                    INVE_VL_BC_ICMS_ST030 AS "VL_BC_ICMS_ST",
                    INVE_VL_ICMS_ST030 AS "VL_ICMS_ST",
                    INVE_VL_FCP030 AS "VL_FCP"
                FROM
                    EFD.TB_EFD_INVENTARIOH030
                WHERE
                    INVE_ID_010 in {efd_id_010}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_H030 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_H030


@oracle_manager
def import_efd_H990(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros H990    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {         
                'INVE_ID_990': int,
                'CABE_ID_0000': int,
                'QTD_LIN_H': int
            }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # SQL query to fetch H990 data
    query = f""" 
                SELECT
                    INVE_ID_990,
                    CABE_ID_0000,
                    INVE_QT_QTD_LIN_H AS "QTD_LIN_H"
                FROM
                    EFD.TB_EFD_INVENTARIOH990
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_H990 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_H990
