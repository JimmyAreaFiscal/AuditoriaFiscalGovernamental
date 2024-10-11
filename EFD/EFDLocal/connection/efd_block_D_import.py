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
def import_efd_D001(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros D001    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """


    # Creating SQL query
    columns = {         
                        'CABE_ID_0000': int,
                        'SERV_ID_001': int,
                        'IND_MOV': str

            }


    assert len(efd_id_0000),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    # assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                SELECT
                    CABE_ID_0000,
                    SERV_ID_001,
                    SERV_TP_IND_MOV AS "IND_MOV"



                FROM
                    EFD.TB_EFD_SERVICOD001
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
                    """

    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_D001 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_D001



@oracle_manager
def import_efd_D100(efd_id_D001) -> pd.DataFrame:
    """ 
    Importa os registros D100    
    Args
    ----------------------
    efd_id_D001: tuple
        Chave estrangeira do registro D001
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_100': int,
                        'SERV_ID_001': int,
                        'IND_OPER': str,
                        'IND_EMIT': str,
                        'COD_PART': str,
                        'COD_MOD': str,
                        'COD_SIT': str,
                        'SER': str,
                        'SUB': str,
                        'NUM_DOC': str,
                        'DT_DOC': 'datetime64[ns]',
                        'DT_A_P': 'datetime64[ns]',
                        'CHV_CTE': str,
                        'TP_CTE': str,
                        'CHV_CTE_REF': str,
                        'VL_DOC': float,
                        'VL_DESC': float,
                        'IND_FRT': str,
                        'VL_SERV': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'VL_NT': float,
                        'COD_INF': str,
                        'COD_CTA': str,
                        'COD_MUN_ORIG': str,
                        'COD_MUN_DEST': str
            }

    assert len(efd_id_D001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D001)}"

    # assert isinstance(efd_id_D001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D001)}"

    # SQL query to fetch D100 data
    query = f""" 
                SELECT
                    SERV_ID_100,
                    SERV_ID_001,
                    SERV_TP_IND_OPER100 AS "IND_OPER",
                    SERV_TP_IND_EMIT100 AS "IND_EMIT",
                    SERV_CD_COD_PART100 AS "COD_PART",
                    SERV_CD_COD_MOD100 AS "COD_MOD",
                    SERV_CD_COD_SIT100 AS "COD_SIT",
                    SERV_NR_SER100 AS "SER",
                    SERV_NR_SUB100 AS "SUB",
                    SERV_NR_NUM_DOC100 AS "NUM_DOC",
                    SERV_DT_DOC100 AS "DT_DOC",
                    SERV_DT_A_P100 AS "DT_A_P",
                    SERV_NR_CHV_CTE100 AS "CHV_CTE",
                    SERV_TP_CTE100 AS "TP_CTE",
                    SERV_NR_CHV_CTE_REF100 AS "CHV_CTE_REF",
                    SERV_VL_DOC100 AS "VL_DOC",
                    SERV_VL_DESC100 AS "VL_DESC",
                    SERV_TP_IND_FRT100 AS "IND_FRT",
                    SERV_VL_SERV100 AS "VL_SERV",
                    SERV_VL_BC_ICMS100 AS "VL_BC_ICMS",
                    SERV_VL_ICMS100 AS "VL_ICMS",
                    SERV_VL_NT100 AS "VL_NT",
                    SERV_CD_COD_INF100 AS "COD_INF",
                    SERV_CD_COD_CTA100 AS "COD_CTA",
                    SERV_CD_COD_MUN_ORIG AS "COD_MUN_ORIG",
                    SERV_CD_COD_MUN_DEST AS "COD_MUN_DEST"
                FROM
                    EFD.TB_EFD_SERVICOD100
                WHERE
                    SERV_ID_001 in {efd_id_D001}

                """ 
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_D100 = pd.read_sql_query(query, con = connection, dtype = columns)
    return efd_D100


@oracle_manager
def import_efd_D101(efd_id_D100) -> pd.DataFrame:
    """ 
    Importa os registros D101    
    Args
    ----------------------
    efd_id_D100: tuple
        Chave estrangeira do registro D100
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_101': int,
                        'SERV_ID_100': int,
                        'VL_FCP_UF_DEST': float,
                        'VL_ICMS_UF_DEST': float,
                        'VL_ICMS_UF_REM': float
            }

    assert len(efd_id_D100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D100)}"

    # assert isinstance(efd_id_D100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D100)}"

    # SQL query to fetch D101 data
    query = f""" 
                SELECT
                    SERV_ID_101,
                    SERV_ID_100,
                    SERV_VL_FCP_UF_DEST AS "VL_FCP_UF_DEST",
                    SERV_VL_ICMS_UF_DEST AS "VL_ICMS_UF_DEST",
                    SERV_VL_ICMS_UF_REM AS "VL_ICMS_UF_REM"
                FROM
                    EFD.TB_EFD_SERVICOD101
                WHERE
                    SERV_ID_100 in {efd_id_D100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D101 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D101


@oracle_manager
def import_efd_D110(efd_id_D100) -> pd.DataFrame:
    """ 
    Importa os registros D110    
    Args
    ----------------------
    efd_id_D100: tuple
        Chave estrangeira do registro D100
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_110': int,
                        'SERV_ID_100': int,
                        'NUM_ITEM': str,
                        'COD_ITEM': str,
                        'VL_SERV': float,
                        'VL_OUT': float
            }

    assert len(efd_id_D100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D100)}"

    # assert isinstance(efd_id_D100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D100)}"

    # SQL query to fetch D110 data
    query = f""" 
                SELECT
                    SERV_ID_110,
                    SERV_ID_100,
                    SERV_NR_NUM_ITEM AS "NUM_ITEM",
                    SERV_CD_COD_ITEM AS "COD_ITEM",
                    SERV_VL_SERV AS "VL_SERV",
                    SERV_VL_OUT AS "VL_OUT"
                FROM
                    EFD.TB_EFD_SERVICOD110
                WHERE
                    SERV_ID_100 in {efd_id_D100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D110 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D110

@oracle_manager
def import_efd_D190(efd_id_D100) -> pd.DataFrame:
    """ 
    Importa os registros D190    
    Args
    ----------------------
    efd_id_D100: tuple
        Chave estrangeira do registro D100
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_190': int,
                        'SERV_ID_100': int,
                        'CST_ICMS': str,
                        'CFOP': str,
                        'ALIQ_ICMS': float,
                        'VL_OPR': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'VL_RED_BC': float,
                        'COD_OBS': str
            }

    assert len(efd_id_D100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D100)}"

    # assert isinstance(efd_id_D100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D100)}"

    # SQL query to fetch D190 data
    query = f""" 
                SELECT
                    SERV_ID_190,
                    SERV_ID_100,
                    SERV_CD_CST_ICMS190 AS "CST_ICMS",
                    SERV_CD_CFOP190 AS "CFOP",
                    SERV_VL_ALIQ_ICMS190 AS "ALIQ_ICMS",
                    SERV_VL_OPR190 AS "VL_OPR",
                    SERV_VL_BC_ICMS190 AS "VL_BC_ICMS",
                    SERV_VL_ICMS190 AS "VL_ICMS",
                    SERV_VL_RED_BC190 AS "VL_RED_BC",
                    SERV_CD_COD_OBS190 AS "COD_OBS"
                FROM
                    EFD.TB_EFD_SERVICOD190
                WHERE
                    SERV_ID_100 in {efd_id_D100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D190 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D190


@oracle_manager
def import_efd_D195(efd_id_D100) -> pd.DataFrame:
    """ 
    Importa os registros D195    
    Args
    ----------------------
    efd_id_D100: tuple
        Chave estrangeira do registro D100
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_195': int,
                        'SERV_ID_100': int,
                        'COD_OBS': str,
                        'TXT_COMPL': str
            }

    assert len(efd_id_D100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D100)}"

    # assert isinstance(efd_id_D100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D100)}"

    # SQL query to fetch D195 data
    query = f""" 
                SELECT
                    SERV_ID_195,
                    SERV_ID_100,
                    SERV_CD_COD_OBS195 AS "COD_OBS",
                    SERV_DS_TXT_COMPL195 AS "TXT_COMPL"
                FROM
                    EFD.TB_EFD_SERVICOD195
                WHERE
                    SERV_ID_100 in {efd_id_D100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D195 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D195


@oracle_manager
def import_efd_D197(efd_id_D195) -> pd.DataFrame:
    """ 
    Importa os registros D197    
    Args
    ----------------------
    efd_id_D195: tuple
        Chave estrangeira do registro D195
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_197': int,
                        'SERV_ID_195': int,
                        'COD_AJ': str,
                        'DESCR_COMPL_AJ': str,
                        'COD_ITEM': str,
                        'VL_BC_ICMS': float,
                        'ALIQ_ICMS': float,
                        'VL_ICMS': float,
                        'VL_OUTROS': float
            }

    assert len(efd_id_D195),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D195)}"

    # assert isinstance(efd_id_D195, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D195)}"

    # SQL query to fetch D197 data
    query = f""" 
                SELECT
                    SERV_ID_197,
                    SERV_ID_195,
                    SERV_CD_COD_AJ197 AS "COD_AJ",
                    SERV_DS_DESCR_COMPL_AJ197 AS "DESCR_COMPL_AJ",
                    SERV_CD_COD_ITEM197 AS "COD_ITEM",
                    SERV_VL_BC_ICMS197 AS "VL_BC_ICMS",
                    SERV_VL_ALIQ_ICMS197 AS "ALIQ_ICMS",
                    SERV_VL_ICMS197 AS "VL_ICMS",
                    SERV_VL_OUTROS197 AS "VL_OUTROS"
                FROM
                    EFD.TB_EFD_SERVICOD197
                WHERE
                    SERV_ID_195 in {efd_id_D195}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D197 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D197


@oracle_manager
def import_efd_D500(efd_id_D001) -> pd.DataFrame:
    """ 
    Importa os registros D500    
    Args
    ----------------------
    efd_id_D001: tuple
        Chave estrangeira do registro D001
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_500': int,
                        'SERV_ID_001': int,
                        'IND_OPER': str,
                        'IND_EMIT': str,
                        'COD_PART': str,
                        'COD_MOD': str,
                        'COD_SIT': str,
                        'SER': str,
                        'SUB': str,
                        'NUM_DOC': str,
                        'DT_DOC': 'datetime64[ns]',
                        'DT_A_P': 'datetime64[ns]',
                        'VL_DOC': float,
                        'VL_DESC': float,
                        'VL_SERV_NT': float,
                        'VL_TERC': float,
                        'VL_DA': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'COD_INF': str,
                        'VL_PIS': float,
                        'VL_COFINS': float,
                        'COD_CTA': str,
                        'TP_ASSINANTE': str
            }

    assert len(efd_id_D001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D001)}"

    # assert isinstance(efd_id_D001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D001)}"

    # SQL query to fetch D500 data
    query = f""" 
                SELECT
                    SERV_ID_500,
                    SERV_ID_001,
                    SERV_TP_IND_OPER500 AS "IND_OPER",
                    SERV_TP_IND_EMIT500 AS "IND_EMIT",
                    SERV_CD_COD_PART500 AS "COD_PART",
                    SERV_CD_COD_MOD500 AS "COD_MOD",
                    SERV_CD_COD_SIT500 AS "COD_SIT",
                    SERV_CD_SER500 AS "SER",
                    SERV_NR_SUB500 AS "SUB",
                    SERV_NR_NUM_DOC500 AS "NUM_DOC",
                    SERV_DT_DOC500 AS "DT_DOC",
                    SERV_DT_A_P500 AS "DT_A_P",
                    SERV_VL_DOC500 AS "VL_DOC",
                    SERV_VL_DESC500 AS "VL_DESC",
                    SERV_VL_SERV_NT500 AS "VL_SERV_NT",
                    SERV_VL_TERC500 AS "VL_TERC",
                    SERV_VL_DA500 AS "VL_DA",
                    SERV_VL_BC_ICMS500 AS "VL_BC_ICMS",
                    SERV_VL_ICMS500 AS "VL_ICMS",
                    SERV_CD_COD_INF500 AS "COD_INF",
                    SERV_VL_PIS500 AS "VL_PIS",
                    SERV_VL_COFINS500 AS "VL_COFINS",
                    SERV_COD_CTA500 AS "COD_CTA",
                    SERV_TP_ASSINANTE500 AS "TP_ASSINANTE"
                FROM
                    EFD.TB_EFD_SERVICOD500
                WHERE
                    SERV_ID_001 in {efd_id_D001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D500 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D500


@oracle_manager
def import_efd_D510(efd_id_D500) -> pd.DataFrame:
    """ 
    Importa os registros D510    
    Args
    ----------------------
    efd_id_D500: tuple
        Chave estrangeira do registro D500
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_510': int,
                        'SERV_ID_500': int,
                        'NUM_ITEM': str,
                        'COD_ITEM': str,
                        'COD_CLASS': str,
                        'QTD': float,
                        'UNID': str,
                        'VL_ITEM': float,
                        'VL_DESC': float,
                        'CST_ICMS': str,
                        'CFOP': str,
                        'VL_BC_ICMS': float,
                        'ALIQ_ICMS': float,
                        'VL_ICMS': float,
                        'VL_BC_ICMS_UF': float,
                        'VL_ICMS_UF': float,
                        'IND_REC': str,
                        'COD_PART': str,
                        'VL_PIS': float,
                        'VL_COFINS': float,
                        'COD_CTA': str
            }

    assert len(efd_id_D500),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D500)}"

    # assert isinstance(efd_id_D500, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D500)}"

    # SQL query to fetch D510 data
    query = f""" 
                SELECT
                    SERV_ID_510,
                    SERV_ID_500,
                    SERV_NR_NUM_ITEM510 AS "NUM_ITEM",
                    SERV_CD_COD_ITEM510 AS "COD_ITEM",
                    SERV_CD_COD_CLASS510 AS "COD_CLASS",
                    SERV_QT_QTD510 AS "QTD",
                    SERV_NR_UNID510 AS "UNID",
                    SERV_VL_ITEM510 AS "VL_ITEM",
                    SERV_VL_DESC510 AS "VL_DESC",
                    SERV_CD_CST_ICMS510 AS "CST_ICMS",
                    SERV_CD_CFOP510 AS "CFOP",
                    SERV_VL_BC_ICMS510 AS "VL_BC_ICMS",
                    SERV_VL_ALIQ_ICMS510 AS "ALIQ_ICMS",
                    SERV_VL_ICMS510 AS "VL_ICMS",
                    SERV_VL_BC_ICMS_UF510 AS "VL_BC_ICMS_UF",
                    SERV_VL_ICMS_UF510 AS "VL_ICMS_UF",
                    SERV_TP_IND_REC510 AS "IND_REC",
                    SERV_CD_COD_PART510 AS "COD_PART",
                    SERV_VL_PIS510 AS "VL_PIS",
                    SERV_VL_COFINS510 AS "VL_COFINS",
                    SERV_CD_COD_CTA510 AS "COD_CTA"
                FROM
                    EFD.TB_EFD_SERVICOD510
                WHERE
                    SERV_ID_500 in {efd_id_D500}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D510 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D510


@oracle_manager
def import_efd_D530(efd_id_D500) -> pd.DataFrame:
    """ 
    Importa os registros D530    
    Args
    ----------------------
    efd_id_D500: tuple
        Chave estrangeira do registro D500
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_530': int,
                        'SERV_ID_500': int,
                        'IND_SERV': str,
                        'DT_INI_SERV': 'datetime64[ns]',
                        'DT_FIN_SERV': 'datetime64[ns]',
                        'PER_FISCAL': str,
                        'COD_AREA': str,
                        'NR_TERMINAL': str
            }

    assert len(efd_id_D500),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D500)}"

    # assert isinstance(efd_id_D500, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D500)}"

    # SQL query to fetch D530 data
    query = f""" 
                SELECT
                    SERV_ID_530,
                    SERV_ID_500,
                    SERV_TP_IND_SERV530 AS "IND_SERV",
                    SERV_DT_INI_SERV530 AS "DT_INI_SERV",
                    SERV_DT_FIN_SERV530 AS "DT_FIN_SERV",
                    SERV_NR_PER_FISCAL530 AS "PER_FISCAL",
                    SERV_CD_COD_AREA530 AS "COD_AREA",
                    SERV_NR_TERMINAL530 AS "NR_TERMINAL"
                FROM
                    EFD.TB_EFD_SERVICOD530
                WHERE
                    SERV_ID_500 in {efd_id_D500}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D530 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D530


@oracle_manager
def import_efd_D590(efd_id_D500) -> pd.DataFrame:
    """ 
    Importa os registros D590    
    Args
    ----------------------
    efd_id_D500: tuple
        Chave estrangeira do registro D500
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_590': int,
                        'SERV_ID_500': int,
                        'CST_ICMS': str,
                        'CFOP': str,
                        'ALIQ_ICMS': float,
                        'VL_OPR': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'VL_BC_ICMS_UF': float,
                        'VL_ICMS_UF': float,
                        'VL_RED_BC': float,
                        'COD_OBS': str
            }

    assert len(efd_id_D500),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D500)}"

    # assert isinstance(efd_id_D500, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D500)}"

    # SQL query to fetch D590 data
    query = f""" 
                SELECT
                    SERV_ID_590,
                    SERV_ID_500,
                    SERV_CD_CST_ICMS590 AS "CST_ICMS",
                    SERV_CD_CFOP590 AS "CFOP",
                    SERV_VL_ALIQ_ICMS590 AS "ALIQ_ICMS",
                    SERV_VL_OPR590 AS "VL_OPR",
                    SERV_VL_BC_ICMS590 AS "VL_BC_ICMS",
                    SERV_VL_ICMS590 AS "VL_ICMS",
                    SERV_VL_BC_ICMS_UF590 AS "VL_BC_ICMS_UF",
                    SERV_VL_ICMS_UF590 AS "VL_ICMS_UF",
                    SERV_VL_RED_BC590 AS "VL_RED_BC",
                    SERV_CD_COD_OBS590 AS "COD_OBS"
                FROM
                    EFD.TB_EFD_SERVICOD590
                WHERE
                    SERV_ID_500 in {efd_id_D500}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D590 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D590


@oracle_manager
def import_efd_D600(efd_id_D001) -> pd.DataFrame:
    """ 
    Importa os registros D600    
    Args
    ----------------------
    efd_id_D001: tuple
        Chave estrangeira do registro D001
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_600': int,
                        'SERV_ID_001': int,
                        'COD_MOD': str,
                        'COD_MUN': str,
                        'SER': str,
                        'SUB': str,
                        'COD_CONS': str,
                        'QT_CONS': float,
                        'DT_DOC': 'datetime64[ns]',
                        'VL_DOC': float,
                        'VL_DESC': float,
                        'VL_SERV': float,
                        'VL_SERV_NT': float,
                        'VL_TERC': float,
                        'VL_DA': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'VL_PIS': float,
                        'VL_COFINS': float
            }

    assert len(efd_id_D001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D001)}"

    # assert isinstance(efd_id_D001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D001)}"

    # SQL query to fetch D600 data
    query = f""" 
                SELECT
                    SERV_ID_600,
                    SERV_ID_001,
                    SERV_CD_COD_MOD600 AS "COD_MOD",
                    SERV_CD_COD_MUN600 AS "COD_MUN",
                    SERV_CD_SER600 AS "SER",
                    SERV_NR_SUB600 AS "SUB",
                    SERV_CD_COD_CONS600 AS "COD_CONS",
                    SERV_QT_QTD_CONS600 AS "QT_CONS",
                    SERV_DT_DOC600 AS "DT_DOC",
                    SERV_VL_DOC600 AS "VL_DOC",
                    SERV_VL_DESC600 AS "VL_DESC",
                    SERV_VL_SERV600 AS "VL_SERV",
                    SERV_VL_SERV_NT600 AS "VL_SERV_NT",
                    SERV_VL_TERC600 AS "VL_TERC",
                    SERV_VL_DA600 AS "VL_DA",
                    SERV_VL_BC_ICMS600 AS "VL_BC_ICMS",
                    SERV_VL_ICMS600 AS "VL_ICMS",
                    SERV_VL_PIS600 AS "VL_PIS",
                    SERV_VL_COFINS600 AS "VL_COFINS"
                FROM
                    EFD.TB_EFD_SERVICOD600
                WHERE
                    SERV_ID_001 in {efd_id_D001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D600 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D600


@oracle_manager
def import_efd_D610(efd_id_D600) -> pd.DataFrame:
    """ 
    Importa os registros D610    
    Args
    ----------------------
    efd_id_D600: tuple
        Chave estrangeira do registro D600
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_610': int,
                        'SERV_ID_600': int,
                        'COD_CLASS': str,
                        'COD_ITEM': str,
                        'QTD': float,
                        'UNID': str,
                        'VL_ITEM': float,
                        'VL_DESC': float,
                        'CST_ICMS': str,
                        'CFOP': str,
                        'ALIQ_ICMS': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'VL_BC_ICMS_UF': float,
                        'VL_ICMS_UF': float,
                        'VL_RED_BC': float,
                        'VL_PIS': float,
                        'VL_COFINS': float,
                        'COD_CTA': str
            }

    assert len(efd_id_D600),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D600)}"

    # assert isinstance(efd_id_D600, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D600)}"

    # SQL query to fetch D610 data
    query = f""" 
                SELECT
                    SERV_ID_610,
                    SERV_ID_600,
                    SERV_CD_COD_CLASS610 AS "COD_CLASS",
                    SERV_CD_COD_ITEM610 AS "COD_ITEM",
                    SERV_QT_QTD610 AS "QTD",
                    SERV_NR_UNID610 AS "UNID",
                    SERV_VL_ITEM610 AS "VL_ITEM",
                    SERV_VL_DESC610 AS "VL_DESC",
                    SERV_CD_CST_ICMS610 AS "CST_ICMS",
                    SERV_CD_CFOP610 AS "CFOP",
                    SERV_VL_ALIQ_ICMS610 AS "ALIQ_ICMS",
                    SERV_VL_BC_ICMS610 AS "VL_BC_ICMS",
                    SERV_VL_ICMS610 AS "VL_ICMS",
                    SERV_VL_BC_ICMS_UF610 AS "VL_BC_ICMS_UF",
                    SERV_VL_ICMS_UF610 AS "VL_ICMS_UF",
                    SERV_VL_RED_BC610 AS "VL_RED_BC",
                    SERV_VL_PIS610 AS "VL_PIS",
                    SERV_VL_COFINS610 AS "VL_COFINS",
                    SERV_CD_COD_CTA610 AS "COD_CTA"
                FROM
                    EFD.TB_EFD_SERVICOD610
                WHERE
                    SERV_ID_600 in {efd_id_D600}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D610 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D610



@oracle_manager
def import_efd_D690(efd_id_D600) -> pd.DataFrame:
    """ 
    Importa os registros D690    
    Args
    ----------------------
    efd_id_D600: tuple
        Chave estrangeira do registro D600
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_690': int,
                        'SERV_ID_600': int,
                        'CST_ICMS': str,
                        'CFOP': str,
                        'ALIQ_ICMS': float,
                        'VL_OPR': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'VL_BC_ICMS_UF': float,
                        'VL_ICMS_UF': float,
                        'VL_RED_BC': float,
                        'COD_OBS': str
            }

    assert len(efd_id_D600),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D600)}"

    # assert isinstance(efd_id_D600, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D600)}"

    # SQL query to fetch D690 data
    query = f""" 
                SELECT
                    SERV_ID_690,
                    SERV_ID_600,
                    SERV_CD_CST_ICMS690 AS "CST_ICMS",
                    SERV_CD_CFOP690 AS "CFOP",
                    SERV_VL_ALIQ_ICMS690 AS "ALIQ_ICMS",
                    SERV_VL_OPR690 AS "VL_OPR",
                    SERV_VL_BC_ICMS690 AS "VL_BC_ICMS",
                    SERV_VL_ICMS690 AS "VL_ICMS",
                    SERV_VL_BC_ICMS_UF690 AS "VL_BC_ICMS_UF",
                    SERV_VL_ICMS_UF690 AS "VL_ICMS_UF",
                    SERV_VL_RED_BC690 AS "VL_RED_BC",
                    SERV_CD_COD_OBS690 AS "COD_OBS"
                FROM
                    EFD.TB_EFD_SERVICOD690
                WHERE
                    SERV_ID_600 in {efd_id_D600}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D690 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D690


@oracle_manager
def import_efd_D695(efd_id_D001) -> pd.DataFrame:
    """ 
    Importa os registros D695    
    Args
    ----------------------
    efd_id_D001: tuple
        Chave estrangeira do registro D001
    """

    # Creating SQL query
    columns = {         
                'SERV_ID_695': int,
                'SERV_ID_001': int,
                'COD_MOD': str,
                'SER': str,
                'NR_ORD_INI': str,
                'NR_ORD_FIN': str,
                'DT_DOC_INI': 'datetime64[ns]',
                'DT_DOC_FIN': 'datetime64[ns]',
                'NOM_MEST': str,
                'CHV_COD_DIG': str
            }

    assert len(efd_id_D001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D001)}"

    # assert isinstance(efd_id_D001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D001)}"

    # SQL query to fetch D695 data
    query = f""" 
                SELECT
                    SERV_ID_695,
                    SERV_ID_001,
                    SERV_CD_COD_MOD695 AS "COD_MOD",
                    SERV_CD_SER695 AS "SER",
                    SERV_NR_NRO_ORD_INI695 AS "NR_ORD_INI",
                    SERV_NR_NRO_ORD_FIN695 AS "NR_ORD_FIN",
                    SERV_DT_DOC_INI695 AS "DT_DOC_INI",
                    SERV_DT_DOC_FIN695 AS "DT_DOC_FIN",
                    SERV_NM_NOM_MEST695 AS "NOM_MEST",
                    SERV_NR_CHV_COD_DIG695 AS "CHV_COD_DIG"
                FROM
                    EFD.TB_EFD_SERVICOD695
                WHERE
                    SERV_ID_001 in {efd_id_D001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D695 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D695


@oracle_manager
def import_efd_D696(efd_id_D695) -> pd.DataFrame:
    """ 
    Importa os registros D696    
    Args
    ----------------------
    efd_id_D695: tuple
        Chave estrangeira do registro D695
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_696': int,
                        'SERV_ID_695': int,
                        'CST_ICMS': str,
                        'CFOP': str,
                        'ALIQ_ICMS': float,
                        'VL_OPR': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'VL_BC_ICMS_UF': float,
                        'VL_ICMS_UF': float,
                        'VL_RED_BC': float,
                        'COD_OBS': str
            }

    assert len(efd_id_D695),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D695)}"

    # assert isinstance(efd_id_D695, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D695)}"

    # SQL query to fetch D696 data
    query = f""" 
                SELECT
                    SERV_ID_696,
                    SERV_ID_695,
                    SERV_CD_CST_ICMS696 AS "CST_ICMS",
                    SERV_CD_CFOP696 AS "CFOP",
                    SERV_VL_ALIQ_ICMS696 AS "ALIQ_ICMS",
                    SERV_VL_OPR696 AS "VL_OPR",
                    SERV_VL_BC_ICMS696 AS "VL_BC_ICMS",
                    SERV_VL_ICMS696 AS "VL_ICMS",
                    SERV_VL_BC_ICMS_UF696 AS "VL_BC_ICMS_UF",
                    SERV_VL_ICMS_UF696 AS "VL_ICMS_UF",
                    SERV_VL_RED_BC696 AS "VL_RED_BC",
                    SERV_CD_COD_OBS696 AS "COD_OBS"
                FROM
                    EFD.TB_EFD_SERVICOD696
                WHERE
                    SERV_ID_695 in {efd_id_D695}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D696 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D696


@oracle_manager
def import_efd_D697(efd_id_D696) -> pd.DataFrame:
    """ 
    Importa os registros D697    
    Args
    ----------------------
    efd_id_D696: tuple
        Chave estrangeira do registro D696
    """

    # Creating SQL query
    columns = {         
                'SERV_ID_697': int,
                'SERV_ID_696': int,
                'UF': str,
                'VL_BC_ICMS': float,
                'VL_ICMS': float
            }

    assert len(efd_id_D696),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D696)}"

    # assert isinstance(efd_id_D696, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D696)}"

    # SQL query to fetch D697 data
    query = f""" 
                SELECT
                    SERV_ID_697,
                    SERV_ID_696,
                    SERV_SG_UF_697 AS "UF",
                    SERV_VL_BC_ICMS697 AS "VL_BC_ICMS",
                    SERV_VL_ICMS697 AS "VL_ICMS"
                FROM
                    EFD.TB_EFD_SERVICOD697
                WHERE
                    SERV_ID_696 in {efd_id_D696}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D697 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D697



@oracle_manager
def import_efd_D700(efd_id_D001) -> pd.DataFrame:
    """ 
    Importa os registros D700    
    Args
    ----------------------
    efd_id_D001: tuple
        Chave estrangeira do registro D001
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_700': int,
                        'SERV_ID_001': int,
                        'IND_OPER': str,
                        'IND_EMIT': str,
                        'COD_PART': str,
                        'COD_MOD': str,
                        'COD_SIT': str,
                        'SER': str,
                        'NUM_DOC': str,
                        'DT_DOC': 'datetime64[ns]',
                        'DT_E_S': 'datetime64[ns]',
                        'VL_DOC': float,
                        'VL_DESC': float,
                        'VL_SERV': float,
                        'VL_SERV_NT': float,
                        'VL_TERC': float,
                        'VL_DA': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'COD_INF': str,
                        'VL_PIS': float,
                        'VL_COFINS': float,
                        'CHV_DOCe': str,
                        'FIN_DOCe': str,
                        'TIP_FAT': str,
                        'COD_MOD_DOC_REF': str,
                        'SER_DOC_REF': str,
                        'NUM_DOC_REF': str,
                        'MES_DOC_REF': str,
                        'COD_MUN_DEST': str
            }

    assert len(efd_id_D001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D001)}"

    # assert isinstance(efd_id_D001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D001)}"

    # SQL query to fetch D700 data
    query = f""" 
                SELECT
                    SERV_ID_700,
                    SERV_ID_001,
                    SERV_IND_OPER700 AS "IND_OPER",
                    SERV_IND_EMIT700 AS "IND_EMIT",
                    SERV_COD_PART700 AS "COD_PART",
                    SERV_COD_MOD700 AS "COD_MOD",
                    SERV_COD_SIT700 AS "COD_SIT",
                    SERV_SER700 AS "SER",
                    SERV_NUM_DOC700 AS "NUM_DOC",
                    SERV_DT_DOC700 AS "DT_DOC",
                    SERV_DT_E_S700 AS "DT_E_S",
                    SERV_VL_DOC700 AS "VL_DOC",
                    SERV_VL_DESC700 AS "VL_DESC",
                    SERV_VL_SERV700 AS "VL_SERV",
                    SERV_VL_SERV_NT700 AS "VL_SERV_NT",
                    SERV_VL_TERC700 AS "VL_TERC",
                    SERV_VL_DA700 AS "VL_DA",
                    SERV_VL_BC_ICMS700 AS "VL_BC_ICMS",
                    SERV_VL_ICMS700 AS "VL_ICMS",
                    SERV_COD_INF700 AS "COD_INF",
                    SERV_VL_PIS700 AS "VL_PIS",
                    SERV_VL_COFINS700 AS "VL_COFINS",
                    SERV_CHV_DOCe700 AS "CHV_DOCe",
                    SERV_FIN_DOCe700 AS "FIN_DOCe",
                    SERV_TIP_FAT700 AS "TIP_FAT",
                    SERV_COD_MOD_DOC_REF700 AS "COD_MOD_DOC_REF",
                    SERV_SER_DOC_REF700 AS "SER_DOC_REF",
                    SERV_NUM_DOC_REF700 AS "NUM_DOC_REF",
                    SERV_MES_DOC_REF700 AS "MES_DOC_REF",
                    SERV_COD_MUN_DEST700 AS "COD_MUN_DEST"
                FROM
                    EFD.TB_EFD_SERVICOD700
                WHERE
                    SERV_ID_001 in {efd_id_D001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D700 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D700


@oracle_manager
def import_efd_D730(efd_id_D700) -> pd.DataFrame:
    """ 
    Importa os registros D730    
    Args
    ----------------------
    efd_id_D700: tuple
        Chave estrangeira do registro D700
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_730': int,
                        'SERV_ID_700': int,
                        'CST_ICMS': str,
                        'CFOP': str,
                        'ALIQ_ICMS': float,
                        'VL_OPR': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'VL_RED_BC': float,
                        'COD_OBS': str
            }

    assert len(efd_id_D700),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D700)}"

    # assert isinstance(efd_id_D700, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D700)}"

    # SQL query to fetch D730 data
    query = f""" 
                SELECT
                    SERV_ID_730,
                    SERV_ID_700,
                    SERV_CST_ICMS730 AS "CST_ICMS",
                    SERV_CFOP730 AS "CFOP",
                    SERV_ALIQ_ICMS730 AS "ALIQ_ICMS",
                    SERV_VL_OPR730 AS "VL_OPR",
                    SERV_VL_BC_ICMS730 AS "VL_BC_ICMS",
                    SERV_VL_ICMS730 AS "VL_ICMS",
                    SERV_VL_RED_BC730 AS "VL_RED_BC",
                    SERV_COD_OBS730 AS "COD_OBS"
                FROM
                    EFD.TB_EFD_SERVICOD730
                WHERE
                    SERV_ID_700 in {efd_id_D700}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D730 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D730


@oracle_manager
def import_efd_D731(efd_id_D730) -> pd.DataFrame:
    """ 
    Importa os registros D731    
    Args
    ----------------------
    efd_id_D730: tuple
        Chave estrangeira do registro D730
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_731': int,
                        'SERV_ID_730': int,
                        'VL_FCP_OP': float
            }

    assert len(efd_id_D730),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D730)}"

    # assert isinstance(efd_id_D730, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D730)}"

    # SQL query to fetch D731 data
    query = f""" 
                SELECT
                    SERV_ID_731,
                    SERV_ID_730,
                    SERV_VL_FCP_OP731 AS "VL_FCP_OP"
                FROM
                    EFD.TB_EFD_SERVICOD731
                WHERE
                    SERV_ID_730 in {efd_id_D730}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D731 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D731


@oracle_manager
def import_efd_D735(efd_id_D700) -> pd.DataFrame:
    """ 
    Importa os registros D735    
    Args
    ----------------------
    efd_id_D700: tuple
        Chave estrangeira do registro D700
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_735': int,
                        'SERV_ID_700': int,
                        'COD_OBS': str,
                        'TXT_COMPL': str
            }

    assert len(efd_id_D700),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D700)}"

    # assert isinstance(efd_id_D700, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D700)}"

    # SQL query to fetch D735 data
    query = f""" 
                SELECT
                    SERV_ID_735,
                    SERV_ID_700,
                    SERV_COD_OBS735 AS "COD_OBS",
                    SERV_TXT_COMPL735 AS "TXT_COMPL"
                FROM
                    EFD.TB_EFD_SERVICOD735
                WHERE
                    SERV_ID_700 in {efd_id_D700}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D735 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D735


@oracle_manager
def import_efd_D737(efd_id_D735) -> pd.DataFrame:
    """ 
    Importa os registros D737    
    Args
    ----------------------
    efd_id_D735: tuple
        Chave estrangeira do registro D735
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_737': int,
                        'SERV_ID_735': int,
                        'COD_AJ': str,
                        'DESCR_COMPL_AJ': str,
                        'COD_ITEM': str,
                        'VL_BC_ICMS': float,
                        'ALIQ_ICMS': float,
                        'VL_ICMS': float,
                        'VL_OUTROS': float
            }

    assert len(efd_id_D735),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D735)}"

    # assert isinstance(efd_id_D735, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D735)}"

    # SQL query to fetch D737 data
    query = f""" 
                SELECT
                    SERV_ID_737,
                    SERV_ID_735,
                    SERV_COD_AJ737 AS "COD_AJ",
                    SERV_DESCR_COMPL_AJ737 AS "DESCR_COMPL_AJ",
                    SERV_COD_ITEM737 AS "COD_ITEM",
                    SERV_VL_BC_ICMS737 AS "VL_BC_ICMS",
                    SERV_VL_ALIQ_ICMS737 AS "ALIQ_ICMS",
                    SERV_VL_ICMS737 AS "VL_ICMS",
                    SERV_VL_OUTROS737 AS "VL_OUTROS"
                FROM
                    EFD.TB_EFD_SERVICOD737
                WHERE
                    SERV_ID_735 in {efd_id_D735}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D737 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D737



@oracle_manager
def import_efd_D750(efd_id_D001) -> pd.DataFrame:
    """ 
    Importa os registros D750    
    Args
    ----------------------
    efd_id_D001: tuple
        Chave estrangeira do registro D001
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_750': int,
                        'SERV_ID_001': int,
                        'COD_MOD': str,
                        'SER': str,
                        'DT_DOC': 'datetime64[ns]',
                        'QTD_CONS': float,
                        'IND_PREPAGO': str,
                        'VL_DOC': float,
                        'VL_SERV': float,
                        'VL_SERV_NT': float,
                        'VL_TERC': float,
                        'VL_DESC': float,
                        'VL_DA': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'VL_PIS': float,
                        'VL_COFINS': float
            }

    assert len(efd_id_D001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D001)}"

    # assert isinstance(efd_id_D001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D001)}"

    # SQL query to fetch D750 data
    query = f""" 
                SELECT
                    SERV_ID_750,
                    SERV_ID_001,
                    SERV_COD_MOD750 AS "COD_MOD",
                    SERV_SER750 AS "SER",
                    SERV_DT_DOC750 AS "DT_DOC",
                    SERV_QTD_CONS750 AS "QTD_CONS",
                    SERV_IND_PREPAGO750 AS "IND_PREPAGO",
                    SERV_VL_DOC750 AS "VL_DOC",
                    SERV_VL_SERV750 AS "VL_SERV",
                    SERV_VL_SERV_NT750 AS "VL_SERV_NT",
                    SERV_VL_TERC750 AS "VL_TERC",
                    SERV_VL_DESC750 AS "VL_DESC",
                    SERV_VL_DA750 AS "VL_DA",
                    SERV_VL_BC_ICMS750 AS "VL_BC_ICMS",
                    SERV_VL_ICMS750 AS "VL_ICMS",
                    SERV_VL_PIS750 AS "VL_PIS",
                    SERV_VL_COFINS750 AS "VL_COFINS"
                FROM
                    EFD.TB_EFD_SERVICOD750
                WHERE
                    SERV_ID_001 in {efd_id_D001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D750 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D750


@oracle_manager
def import_efd_D760(efd_id_D750) -> pd.DataFrame:
    """ 
    Importa os registros D760    
    Args
    ----------------------
    efd_id_D750: tuple
        Chave estrangeira do registro D750
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_760': int,
                        'SERV_ID_750': int,
                        'CST_ICMS': str,
                        'CFOP': str,
                        'ALIQ_ICMS': float,
                        'VL_OPR': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'VL_RED_BC': float,
                        'COD_OBS': str
            }

    assert len(efd_id_D750),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D750)}"

    # assert isinstance(efd_id_D750, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D750)}"

    # SQL query to fetch D760 data
    query = f""" 
                SELECT
                    SERV_ID_760,
                    SERV_ID_750,
                    SERV_CST_ICMS760 AS "CST_ICMS",
                    SERV_CFOP760 AS "CFOP",
                    SERV_ALIQ_ICMS760 AS "ALIQ_ICMS",
                    SERV_VL_OPR760 AS "VL_OPR",
                    SERV_VL_BC_ICMS760 AS "VL_BC_ICMS",
                    SERV_VL_ICMS760 AS "VL_ICMS",
                    SERV_VL_RED_BC760 AS "VL_RED_BC",
                    SERV_COD_OBS760 AS "COD_OBS"
                FROM
                    EFD.TB_EFD_SERVICOD760
                WHERE
                    SERV_ID_750 in {efd_id_D750}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D760 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D760


@oracle_manager
def import_efd_D761(efd_id_D760) -> pd.DataFrame:
    """ 
    Importa os registros D761    
    Args
    ----------------------
    efd_id_D760: tuple
        Chave estrangeira do registro D760
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_761': int,
                        'SERV_ID_760': int,
                        'VL_FCP_OP': float
            }

    assert len(efd_id_D760),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_D760)}"

    # assert isinstance(efd_id_D760, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_D760)}"

    # SQL query to fetch D761 data
    query = f""" 
                SELECT
                    SERV_ID_761,
                    SERV_ID_760,
                    SERV_VL_FCP_OP761 AS "VL_FCP_OP"
                FROM
                    EFD.TB_EFD_SERVICOD761
                WHERE
                    SERV_ID_760 in {efd_id_D760}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D761 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D761


@oracle_manager
def import_efd_D990(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros D990    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {         
                        'SERV_ID_990': int,
                        'CABE_ID_0000': int,
                        'QTD_LIN_D': int
            }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    # assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # SQL query to fetch D990 data
    query = f""" 
                SELECT
                    SERV_ID_990,
                    CABE_ID_0000,
                    SERV_QTD_LIN_D AS "QTD_LIN_D"
                FROM
                    EFD.TB_EFD_SERVICOD990
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_D990 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_D990
