# Third-party modules import
import pandas as pd 
import oracledb 

# User-defined modules import
from .utils import oracle_manager
from .usuario import dns_tns, pw, user

# Import built-in modules
import warnings 

@oracle_manager
def import_efd_E001(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros E001    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_001': int,
                        'CABE_ID_0000': int,
                        'IND_MOV': str
            }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # SQL query to fetch E001 data
    query = f""" 
                SELECT
                    APUR_ID_001,
                    CABE_ID_0000,
                    APUR_TP_IND_MOV AS "IND_MOV"
                FROM
                    EFD.TB_EFD_APURACAOE001
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E001 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E001


@oracle_manager
def import_efd_E100(efd_id_E001) -> pd.DataFrame:
    """ 
    Importa os registros E100    
    Args
    ----------------------
    efd_id_E001: tuple
        Chave estrangeira do registro E001
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_100': int,
                        'APUR_ID_001': int,
                        'DT_INI': 'datetime64[ns]',
                        'DT_FIN': 'datetime64[ns]'
            }

    assert len(efd_id_E001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E001)}"

    assert isinstance(efd_id_E001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E001)}"

    # SQL query to fetch E100 data
    query = f""" 
                SELECT
                    APUR_ID_100,
                    APUR_ID_001,
                    APUR_DT_INI AS "DT_INI",
                    APUR_DT_FIN AS "DT_FIN"
                FROM
                    EFD.TB_EFD_APURACAOE100
                WHERE
                    APUR_ID_001 in {efd_id_E001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E100 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E100


@oracle_manager
def import_efd_E110(efd_id_E100) -> pd.DataFrame:
    """ 
    Importa os registros E110    
    Args
    ----------------------
    efd_id_E100: tuple
        Chave estrangeira do registro E100
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_110': int,
                        'APUR_ID_100': int,
                        'VL_TOT_DEBITOS': float,
                        'VL_AJ_DEBITOS': float,
                        'VL_TOT_AJ_DEBITOS': float,
                        'VL_TOT_CREDITOS': float,
                        'VL_AJ_CREDITOS': float,
                        'VL_TOT_AJ_CREDITOS': float,
                        'VL_ESTORNOS_DEB': float,
                        'VL_ESTORNOS_CRED': float,
                        'VL_SLD_CREDOR_ANT': float,
                        'VL_SLD_APURADO': float,
                        'VL_TOT_DED': float,
                        'VL_ICMS_RECOLHER': float,
                        'VL_SLD_CREDOR_TRANSPORTAR': float,
                        'DEB_ESP': float
            }

    assert len(efd_id_E100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E100)}"

    assert isinstance(efd_id_E100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E100)}"

    # SQL query to fetch E110 data
    query = f""" 
                SELECT
                    APUR_ID_110,
                    APUR_ID_100,
                    APUR_VL_TOT_DEBITOS110 AS "VL_TOT_DEBITOS",
                    APUR_VL_AJ_DEBITOS110 AS "VL_AJ_DEBITOS",
                    APUR_VL_TOT_AJ_DEBITOS110 AS "VL_TOT_AJ_DEBITOS",
                    APUR_VL_TOT_CREDITOS110 AS "VL_TOT_CREDITOS",
                    APUR_VL_AJ_CREDITOS110 AS "VL_AJ_CREDITOS",
                    APUR_VL_TOT_AJ_CREDITOS110 AS "VL_TOT_AJ_CREDITOS",
                    APUR_VL_ESTORNOS_DEB110 AS "VL_ESTORNOS_DEB",
                    APUR_VL_ESTORNOS_CRED110 AS "VL_ESTORNOS_CRED",
                    APUR_VL_SLD_CREDOR_ANT110 AS "VL_SLD_CREDOR_ANT",
                    APUR_VL_SLD_APURADO110 AS "VL_SLD_APURADO",
                    APUR_VL_TOT_DED110 AS "VL_TOT_DED",
                    APUR_VL_ICMS_RECOLHER110 AS "VL_ICMS_RECOLHER",
                    APUR_VL_SLD_CRED_TRANSP110 AS "VL_SLD_CREDOR_TRANSPORTAR",
                    APUR_VL_DEB_ESP110 AS "DEB_ESP"
                FROM
                    EFD.TB_EFD_APURACAOE110
                WHERE
                    APUR_ID_100 in {efd_id_E100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E110 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E110


@oracle_manager
def import_efd_E111(efd_id_E110) -> pd.DataFrame:
    """ 
    Importa os registros E111    
    Args
    ----------------------
    efd_id_E110: tuple
        Chave estrangeira do registro E110
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_111': int,
                        'APUR_ID_110': int,
                        'COD_AJ_APUR': str,
                        'DESCR_COMPL_AJ': str,
                        'VL_AJ_APUR': float
            }

    assert len(efd_id_E110),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E110)}"

    assert isinstance(efd_id_E110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E110)}"

    # SQL query to fetch E111 data
    query = f""" 
                SELECT
                    APUR_ID_111,
                    APUR_ID_110,
                    APUR_CD_COD_AJ_APUR111 AS "COD_AJ_APUR",
                    APUR_DS_DESCR_COMPL_AJ111 AS "DESCR_COMPL_AJ",
                    APUR_VL_AJ_APUR111 AS "VL_AJ_APUR"
                FROM
                    EFD.TB_EFD_APURACAOE111
                WHERE
                    APUR_ID_110 in {efd_id_E110}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E111 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E111
   
@oracle_manager
def import_efd_E112(efd_id_E111) -> pd.DataFrame:
    """ 
    Importa os registros E112    
    Args
    ----------------------
    efd_id_E111: tuple
        Chave estrangeira do registro E111
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_112': int,
                        'APUR_ID_111': int,
                        'NR_NUM_DA': str,
                        'NR_NUM_PROC': str,
                        'TP_IND_PROC': str,
                        'DS_PROC': str,
                        'DS_TXT_COMPL': str
            }

    assert len(efd_id_E111),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E111)}"

    assert isinstance(efd_id_E111, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E111)}"

    # SQL query to fetch E112 data
    query = f""" 
                SELECT
                    APUR_ID_112,
                    APUR_ID_111,
                    APUR_NR_NUM_DA112 AS "NR_NUM_DA",
                    APUR_NR_NUM_PROC112 AS "NR_NUM_PROC",
                    APUR_TP_IND_PROC112 AS "TP_IND_PROC",
                    APUR_DS_PROC112 AS "DS_PROC",
                    APUR_DS_TXT_COMPL112 AS "DS_TXT_COMPL"
                FROM
                    EFD.TB_EFD_APURACAOE112
                WHERE
                    APUR_ID_111 in {efd_id_E111}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E112 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E112

@oracle_manager
def import_efd_E113(efd_id_E111) -> pd.DataFrame:
    """ 
    Importa os registros E113    
    Args
    ----------------------
    efd_id_E111: tuple
        Chave estrangeira do registro E111
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_113': int,
                        'APUR_ID_111': int,
                        'COD_PART': str,
                        'COD_MOD': str,
                        'SER': str,
                        'SUB': str,
                        'COD_MOD': str,
                        'DT_DOC': 'datetime64[ns]',
                        'CD_ITEM': str,
                        'VL_AJ_ITEM': float,
                        'CHV_DOCe': str
            }

    assert len(efd_id_E111),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E111)}"

    assert isinstance(efd_id_E111, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E111)}"

    # SQL query to fetch E113 data
    query = f""" 
                SELECT
                    APUR_ID_113,
                    APUR_ID_111,
                    APUR_CD_COD_PART113 AS "COD_PART",
                    APUR_CD_COD_MOD113 AS "COD_MOD",
                    APUR_NR_SER113 AS "SER",
                    APUR_CD_SUB113 AS "SUB",
                    APUR_NR_NUM_DOC113 AS "NUM_DOC",
                    APUR_DT_DOC113 AS "DT_DOC",
                    APUR_CD_COD_ITEM113 AS "CD_ITEM",
                    APUR_VL_AJ_ITEM113 AS "VL_AJ_ITEM",
                    APUR_CD_CHV_DOCE AS "CHV_DOCe"
                FROM
                    EFD.TB_EFD_APURACAOE113
                WHERE
                    APUR_ID_111 in {efd_id_E111}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E113 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E113



@oracle_manager
def import_efd_E115(efd_id_E110) -> pd.DataFrame:
    """ 
    Importa os registros E115    
    Args
    ----------------------
    efd_id_E110: tuple
        Chave estrangeira do registro E110
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_115': int,
                        'APUR_ID_110': int,
                        'COD_INF_ADIC': str,
                        'VL_INF_ADIC': float,
                        'DESCR_COMPL_AJ': str
            }

    assert len(efd_id_E110),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E110)}"

    assert isinstance(efd_id_E110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E110)}"

    # SQL query to fetch E115 data
    query = f""" 
                SELECT
                    APUR_ID_115,
                    APUR_ID_110,
                    APUR_CD_COD_INF_ADIC115 AS "COD_INF_ADIC",
                    APUR_VL_INF_ADIC115 AS "VL_INF_ADIC",
                    APUR_DS_DESCR_COMPL_AJ115 AS "DESCR_COMPL_AJ"
                FROM
                    EFD.TB_EFD_APURACAOE115
                WHERE
                    APUR_ID_110 in {efd_id_E110}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E115 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E115


@oracle_manager
def import_efd_E116(efd_id_E110) -> pd.DataFrame:
    """ 
    Importa os registros E116    
    Args
    ----------------------
    efd_id_E110: tuple
        Chave estrangeira do registro E110
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_116': int,
                        'APUR_ID_110': int,
                        'COD_OR': str,
                        'VL_OR': float,
                        'DT_VCTO': 'datetime64[ns]',
                        'COD_REC': str,
                        'NUM_PROC': str,
                        'IND_PROC': str,
                        'PROC': str,
                        'TXT_COMPL': str,
                        'MES_REF': str
            }

    assert len(efd_id_E110),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E110)}"

    assert isinstance(efd_id_E110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E110)}"

    # SQL query to fetch E116 data
    query = f""" 
                SELECT
                    APUR_ID_116,
                    APUR_ID_110,
                    APUR_CD_COD_OR116 AS "COD_OR",
                    APUR_VL_OR116 AS "VL_OR",
                    APUR_DT_VCTO116 AS "DT_VCTO",
                    APUR_CD_COD_REC116 AS "COD_REC",
                    APUR_NR_NUM_PROC116 AS "NUM_PROC",
                    APUR_TP_IND_PROC116 AS "IND_PROC",
                    APUR_DS_PROC116 AS "PROC",
                    APUR_DS_TXT_COMPL116 AS "TXT_COMPL",
                    APUR_CD_MES_REF116 AS "MES_REF"
                FROM
                    EFD.TB_EFD_APURACAOE116
                WHERE
                    APUR_ID_110 in {efd_id_E110}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E116 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E116



@oracle_manager
def import_efd_E200(efd_id_E001) -> pd.DataFrame:
    """ 
    Importa os registros E200    
    Args
    ----------------------
    efd_id_E001: tuple
        Chave estrangeira do registro E001
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_200': int,
                        'APUR_ID_001': int,
                        'UF': str,
                        'DT_INI': 'datetime64[ns]',
                        'DT_FIN': 'datetime64[ns]'
            }

    assert len(efd_id_E001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E001)}"

    assert isinstance(efd_id_E001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E001)}"

    # SQL query to fetch E200 data
    query = f""" 
                SELECT
                    APUR_ID_200,
                    APUR_ID_001,
                    APUR_UF200 AS "UF",
                    APUR_DT_INI200 AS "DT_INI",
                    APUR_DT_FIN200 AS "DT_FIN"
                FROM
                    EFD.TB_EFD_APURACAOE200
                WHERE
                    APUR_ID_001 in {efd_id_E001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E200 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E200


@oracle_manager
def import_efd_E210(efd_id_200) -> pd.DataFrame:
    """ 
    Importa os registros E210    
    Args
    ----------------------
    efd_id_200: tuple
        Chave estrangeira do registro E200
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_210': int,
                        'APUR_ID_200': int,
                        'IND_MOV_ST': str,
                        'VL_SLD_CRED_ANT_ST': float,
                        'VL_DEVOL_ST': float,
                        'VL_RESSARC_ST': float,
                        'VL_OUT_CRED_ST': float,
                        'VL_AJ_CREDITOS_ST': float,
                        'VL_RETENÇAO_ST': float,
                        'VL_OUT_DEB_ST': float,
                        'VL_AJ_DEBITOS_ST': float,
                        'VL_SLD_DEV_ANT_ST': float,
                        'VL_DEDUÇÕES_ST': float,
                        'VL_ICMS_RECOL_ST': float,
                        'DEB_ESP_ST': float,
                        'VL_SLD_CRED_ST_TRANSPORTAR': float
            }

    assert len(efd_id_200),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_200)}"

    assert isinstance(efd_id_200, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_200)}"

    # SQL query to fetch E210 data
    query = f""" 
                SELECT
                    APUR_ID_210,
                    APUR_ID_200,
                    APUR_TP_IND_MOV_ST210 AS "IND_MOV_ST",
                    APUR_VL_SLD_CRED_ANT_ST210 AS "VL_SLD_CRED_ANT_ST",
                    APUR_VL_DEVOL_ST210 AS "VL_DEVOL_ST",
                    APUR_VL_RESSARC_ST210 AS "VL_RESSARC_ST",
                    APUR_VL_OUT_CRED_ST210 AS "VL_OUT_CRED_ST",
                    APUR_VL_AJ_CREDITOS_ST210 AS "VL_AJ_CREDITOS_ST",
                    APUR_VL_RETENCAO_ST210 AS "VL_RETENÇAO_ST",
                    APUR_VL_OUT_DEB_ST210 AS "VL_OUT_DEB_ST",
                    APUR_VL_AJ_DEBITOS_ST210 AS "VL_AJ_DEBITOS_ST",
                    APUR_VL_SLD_DEV_ANT_ST210 AS "VL_SLD_DEV_ANT_ST",
                    APUR_VL_DEDUCOES_ST210 AS "VL_DEDUÇÕES_ST",
                    APUR_VL_ICMS_RECOL_ST210 AS "VL_ICMS_RECOL_ST",
                    APUR_VL_DEB_ESP_ST210 AS "DEB_ESP_ST",
                    APUR_VL_SLD_CRED_ST_TRAN210 AS "VL_SLD_CRED_ST_TRANSPORTAR"
                FROM
                    EFD.TB_EFD_APURACAOE210
                WHERE
                    APUR_ID_200 in {efd_id_200}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E210 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E210


@oracle_manager
def import_efd_E220(efd_id_210) -> pd.DataFrame:
    """ 
    Importa os registros E220    
    Args
    ----------------------
    efd_id_210: tuple
        Chave estrangeira do registro E210
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_220': int,
                        'APUR_ID_210': int,
                        'COD_AJ_APUR': str,
                        'DESCR_COMPL_AJ': str,
                        'VL_AJ_APUR': float
            }

    assert len(efd_id_210),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_210)}"

    assert isinstance(efd_id_210, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_210)}"

    # SQL query to fetch E220 data
    query = f""" 
                SELECT
                    APUR_ID_220,
                    APUR_ID_210,
                    APUR_CD_COD_AJ_APUR220 AS "COD_AJ_APUR",
                    APUR_DS_DESCR_COMPL_AJ220 AS "DESCR_COMPL_AJ",
                    APUR_VL_AJ_APUR220 AS "VL_AJ_APUR"
                FROM
                    EFD.TB_EFD_APURACAOE220
                WHERE
                    APUR_ID_210 in {efd_id_210}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E220 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E220


@oracle_manager
def import_efd_E230(efd_id_220) -> pd.DataFrame:
    """ 
    Importa os registros E230    
    Args
    ----------------------
    efd_id_220: tuple
        Chave estrangeira do registro E220
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_230': int,
                        'APUR_ID_220': int,
                        'NUM_DA': str,
                        'NUM_PROC': str,
                        'IND_PROC': str,
                        'PROC': str,
                        'TXT_COMPL': str
            }

    assert len(efd_id_220),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_220)}"

    assert isinstance(efd_id_220, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_220)}"

    # SQL query to fetch E230 data
    query = f""" 
                SELECT
                    APUR_ID_230,
                    APUR_ID_220,
                    APUR_NR_NUM_DA230 AS "NUM_DA",
                    APUR_NR_NUM_PROC230 AS "NUM_PROC",
                    APUR_TP_IND_PROC230 AS "IND_PROC",
                    APUR_DS_PROC230 AS "PROC",
                    APUR_DS_TXT_COMPL230 AS "TXT_COMPL"
                FROM
                    EFD.TB_EFD_APURACAOE230
                WHERE
                    APUR_ID_220 in {efd_id_220}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E230 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E230

@oracle_manager
def import_efd_E240(efd_id_220) -> pd.DataFrame:
    """ 
    Importa os registros E240    
    Args
    ----------------------
    efd_id_220: tuple
        Chave estrangeira do registro E220
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_240': int,
                        'APUR_ID_220': int,
                        'COD_PART': str,
                        'COD_MOD': str,
                        'SER': str,
                        'SUB': str,
                        'NUM_DOC': int,
                        'DT_DOC': 'datetime64[ns]',
                        'CD_ITEM': str,
                        'VL_AJ_ITEM': float,
                        'CHV_DOCe': str
            }

    assert len(efd_id_220),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_220)}"

    assert isinstance(efd_id_220, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_220)}"

    # SQL query to fetch E240 data
    query = f""" 
                SELECT
                    APUR_ID_240,
                    APUR_ID_220,
                    APUR_CD_COD_PART240 AS "COD_PART",
                    APUR_CD_COD_MOD240 AS "COD_MOD",
                    APUR_NR_SER240 AS "SER",
                    APUR_CD_SUB240 AS "SUB",
                    APUR_NR_NUM_DOC240 AS "NUM_DOC",
                    APUR_DT_DOC240 AS "DT_DOC",
                    APUR_CD_COD_ITEM240 AS "CD_ITEM",
                    APUR_VL_AJ_ITEM240 AS "VL_AJ_ITEM",
                    APUR_CD_CHV_DOCE240 AS "CHV_DOCe"
                FROM
                    EFD.TB_EFD_APURACAOE240
                WHERE
                    APUR_ID_220 in {efd_id_220}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E240 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E240



@oracle_manager
def import_efd_E250(efd_id_210) -> pd.DataFrame:
    """ 
    Importa os registros E250    
    Args
    ----------------------
    efd_id_210: tuple
        Chave estrangeira do registro E210
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_250': int,
                        'APUR_ID_210': int,
                        'COD_OR': str,
                        'VL_OR': float,
                        'DT_VCTO': 'datetime64[ns]',
                        'COD_REC': str,
                        'NUM_PROC': str,
                        'IND_PROC': str,
                        'PROC': str,
                        'TXT_COMPL': str,
                        'MES_REF': str
            }

    assert len(efd_id_210),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_210)}"

    assert isinstance(efd_id_210, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_210)}"

    # SQL query to fetch E250 data
    query = f""" 
                SELECT
                    APUR_ID_250,
                    APUR_ID_210,
                    APUR_CD_COD_OR250 AS "COD_OR",
                    APUR_VL_OR250 AS "VL_OR",
                    APUR_DT_VCTO250 AS "DT_VCTO",
                    APUR_CD_COD_REC250 AS "COD_REC",
                    APUR_NR_NUM_PROC250 AS "NUM_PROC",
                    APUR_TP_IND_PROC250 AS "IND_PROC",
                    APUR_DS_PROC250 AS "PROC",
                    APUR_DS_TXT_COMPL250 AS "TXT_COMPL",
                    APUR_CD_MES_REF250 AS "MES_REF"
                FROM
                    EFD.TB_EFD_APURACAOE250
                WHERE
                    APUR_ID_210 in {efd_id_210}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E250 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E250

@oracle_manager
def import_efd_E300(efd_id_001) -> pd.DataFrame:
    """ 
    Importa os registros E300    
    Args
    ----------------------
    efd_id_001: tuple
        Chave estrangeira do registro E001
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_300': int,
                        'APUR_ID_001': int,
                        'UF': str,
                        'DT_INI': 'datetime64[ns]',
                        'DT_FIN': 'datetime64[ns]'
            }

    assert len(efd_id_001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_001)}"

    assert isinstance(efd_id_001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_001)}"

    # SQL query to fetch E300 data
    query = f""" 
                SELECT
                    APUR_ID_300,
                    APUR_ID_001,
                    APUR_CD_UF300 AS "UF",
                    APUR_DT_INI300 AS "DT_INI",
                    APUR_DT_FIN300 AS "DT_FIN"
                FROM
                    EFD.TB_EFD_APURACAOE300
                WHERE
                    APUR_ID_001 in {efd_id_001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E300 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E300


@oracle_manager
def import_efd_E310(efd_id_300) -> pd.DataFrame:
    """ 
    Importa os registros E310    
    Args
    ----------------------
    efd_id_300: tuple
        Chave estrangeira do registro E300
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_310': int,
                        'APUR_ID_300': int,
                        'IND_MOV_DIFAL': str,
                        'VL_SLD_CRED_ANT_DIFAL': float,
                        'VL_TOT_DEBITOS_DIFAL': float,
                        'VL_OUT_DEB_DIFAL': float,
                        'VL_TOT_CREDITOS_DIFAL': float,
                        'VL_OUT_CRED_DIFAL': float,
                        'VL_SLD_DEV_ANT_DIFAL': float,
                        'VL_DEDUÇÕES_DIFAL': float,
                        'VL_RECOL_DIFAL': float,
                        'VL_SLD_CRED_TRANS_DIF': float,
                        'VL_DEB_ESP_DIFAL': float,
                        'VL_SLD_CRED_ANT_FCP': float,
                        'VL_TOT_DEB_FCP': float,
                        'VL_OUT_DEB_FCP': float,
                        'VL_TOT_CRED_FCP': float,
                        'VL_OUT_CRED_FCP': float,
                        'VL_SLD_DEV_ANT_FCP': float,
                        'VL_DEDUCOES_FCP': float,
                        'VL_RECOL_FCP': float,
                        'VL_SLD_CRED_TRANSPORTAR_FCP': float,
                        'DEB_ESP_FCP': float
            }

    assert len(efd_id_300),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_300)}"

    assert isinstance(efd_id_300, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_300)}"

    # SQL query to fetch E310 data
    query = f""" 
                SELECT
                    APUR_ID_310,
                    APUR_ID_300,
                    APUR_CD_IND_MOV_FCP_DIFAL AS "IND_MOV_DIFAL",
                    APUR_VL_SLDCREDANTDI AS "VL_SLD_CRED_ANT_DIFAL",
                    APUR_VL_TOT_DEB_DIFAL AS "VL_TOT_DEBITOS_DIFAL",
                    APUR_VL_OUT_DEB_DIFAL AS "VL_OUT_DEB_DIFAL",
                    APUR_VL_TOT_CRED_DIFAL AS "VL_TOT_CREDITOS_DIFAL",
                    APUR_VL_OUT_CRED_DIFAL AS "VL_OUT_CRED_DIFAL",
                    APUR_VL_SLDDEV_ANT_DIFAL AS "VL_SLD_DEV_ANT_DIFAL",
                    APUR_VL_DEDUCOES_DIFAL AS "VL_DEDUÇÕES_DIFAL",
                    APUR_VL_RECOL_DIFAL AS "VL_RECOL_DIFAL",
                    APUR_VL_SLDCREDTRANDIF AS "VL_SLD_CRED_TRANS_DIF",
                    APUR_DEB_ESP_DIFAL AS "VL_DEB_ESP_DIFAL",
                    APUR_VL_SLDCRED_ANT_FCP AS "VL_SLD_CRED_ANT_FCP",
                    APUR_VL_TOT_DEB_FCP AS "VL_TOT_DEB_FCP",
                    APUR_VL_OUT_DEB_FCP AS "VL_OUT_DEB_FCP",
                    APUR_VL_TOT_CRED_FCP AS "VL_TOT_CRED_FCP",
                    APUR_VL_OUT_CRED_FCP AS "VL_OUT_CRED_FCP",
                    APUR_VL_SLD_DEV_ANT_FCP AS "VL_SLD_DEV_ANT_FCP",
                    APUR_VL_DEDUCOES_FCP AS "VL_DEDUCOES_FCP",
                    APUR_VL_RECOL_FCP AS "VL_RECOL_FCP",
                    APUR_VL_SLD_CRED_TRAN_FCP AS "VL_SLD_CRED_TRANSPORTAR_FCP",
                    APUR_VL_DEB_ESP_FCP AS "DEB_ESP_FCP"
                FROM
                    EFD.TB_EFD_APURACAOE310
                WHERE
                    APUR_ID_300 in {efd_id_300}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E310 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E310


@oracle_manager
def import_efd_E311(efd_id_310) -> pd.DataFrame:
    """ 
    Importa os registros E311    
    Args
    ----------------------
    efd_id_310: tuple
        Chave estrangeira do registro E310
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_311': int,
                        'APUR_ID_310': int,
                        'COD_AJ_APUR': str,
                        'DESCR_COMPL_AJ': str,
                        'VL_AJ_APUR': float
            }

    assert len(efd_id_310),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_310)}"

    assert isinstance(efd_id_310, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_310)}"

    # SQL query to fetch E311 data
    query = f""" 
                SELECT
                    APUR_ID_311,
                    APUR_ID_310,
                    APUR_CD_AJ_APUR311 AS "COD_AJ_APUR",
                    APUR_DS_COMPL_AJ311 AS "DESCR_COMPL_AJ",
                    APUR_VL_AJ_APUR311 AS "VL_AJ_APUR"
                FROM
                    EFD.TB_EFD_APURACAOE311
                WHERE
                    APUR_ID_310 in {efd_id_310}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E311 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E311


@oracle_manager
def import_efd_E312(efd_id_311) -> pd.DataFrame:
    """ 
    Importa os registros E312    
    Args
    ----------------------
    efd_id_311: tuple
        Chave estrangeira do registro E311
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_312': int,
                        'APUR_ID_311': int,
                        'NUM_DA': str,
                        'NUM_PROC': str,
                        'IND_PROC': str,
                        'PROC': str,
                        'TXT_COMPL': str
            }

    assert len(efd_id_311),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_311)}"

    assert isinstance(efd_id_311, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_311)}"

    # SQL query to fetch E312 data
    query = f""" 
                SELECT
                    APUR_ID_312,
                    APUR_ID_311,
                    APUR_NR_DA AS "NUM_DA",
                    APUR_NR_PROC AS "NUM_PROC",
                    APUR_CD_IND_PROC AS "IND_PROC",
                    APUR_DS_PROC AS "PROC",
                    APUR_DS_COMPL AS "TXT_COMPL"
                FROM
                    EFD.TB_EFD_APURACAOE312
                WHERE
                    APUR_ID_311 in {efd_id_311}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E312 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E312


@oracle_manager
def import_efd_E313(efd_id_311) -> pd.DataFrame:
    """ 
    Importa os registros E313    
    Args
    ----------------------
    efd_id_311: tuple
        Chave estrangeira do registro E311
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_313': int,
                        'APUR_ID_311': int,
                        'COD_PART': str,
                        'COD_MOD': str,
                        'SER': str,
                        'SUB': str,
                        'NUM_DOC': int,
                        'DT_DOC': 'datetime64[ns]',
                        'COD_ITEM': str,
                        'VL_AJ_ITEM': float,
                        'CHV_DOCe': str
            }

    assert len(efd_id_311),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_311)}"

    assert isinstance(efd_id_311, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_311)}"

    # SQL query to fetch E313 data
    query = f""" 
                SELECT
                    APUR_ID_313,
                    APUR_ID_311,
                    APUR_CD_PART313 AS "COD_PART",
                    APUR_CD_MOD313 AS "COD_MOD",
                    APUR_NR_SER313 AS "SER",
                    APUR_NR_SUB313 AS "SUB",
                    APUR_NR_DOC313 AS "NUM_DOC",
                    APUR_CD_CHV_DOCE313 AS "CHV_DOCe",
                    APUR_DT_DOC313 AS "DT_DOC",
                    APUR_CD_ITEM313 AS "COD_ITEM",
                    APUR_VL_AJ_ITEM313 AS "VL_AJ_ITEM"
                FROM
                    EFD.TB_EFD_APURACAOE313
                WHERE
                    APUR_ID_311 in {efd_id_311}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E313 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E313


@oracle_manager
def import_efd_E316(efd_id_310) -> pd.DataFrame:
    """ 
    Importa os registros E316    
    Args
    ----------------------
    efd_id_310: tuple
        Chave estrangeira do registro E310
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_316': int,
                        'APUR_ID_310': int,
                        'COD_OR': str,
                        'VL_OR': float,
                        'DT_VCTO': 'datetime64[ns]',
                        'COD_REC': str,
                        'NUM_PROC': str,
                        'IND_PROC': str,
                        'PROC': str,
                        'TXT_COMPL': str,
                        'MES_REF': str
            }

    assert len(efd_id_310),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_310)}"

    assert isinstance(efd_id_310, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_310)}"

    # SQL query to fetch E316 data
    query = f""" 
                SELECT
                    APUR_ID_316,
                    APUR_ID_310,
                    APUR_CD_OR316 AS "COD_OR",
                    APUR_VL_OR316 AS "VL_OR",
                    APUR_DT_VCTO316 AS "DT_VCTO",
                    APUR_CD_REC316 AS "COD_REC",
                    APUR_NR_PROC316 AS "NUM_PROC",
                    APUR_CD_IND_PROC316 AS "IND_PROC",
                    APUR_DS_PROC316 AS "PROC",
                    APUR_DS_TXT_COMPL316 AS "TXT_COMPL",
                    APUR_MES_REF316 AS "MES_REF"
                FROM
                    EFD.TB_EFD_APURACAOE316
                WHERE
                    APUR_ID_310 in {efd_id_310}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E316 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E316


@oracle_manager
def import_efd_E500(efd_id_001) -> pd.DataFrame:
    """ 
    Importa os registros E500    
    Args
    ----------------------
    efd_id_001: tuple
        Chave estrangeira do registro E001
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_500': int,
                        'APUR_ID_001': int,
                        'IND_APUR': str,
                        'DT_INI': 'datetime64[ns]',
                        'DT_FIN': 'datetime64[ns]'
            }

    assert len(efd_id_001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_001)}"

    assert isinstance(efd_id_001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_001)}"

    # SQL query to fetch E500 data
    query = f""" 
                SELECT
                    APUR_ID_500,
                    APUR_ID_001,
                    APUR_TP_IND_APUR500 AS "IND_APUR",
                    APUR_DT_INI500 AS "DT_INI",
                    APUR_DT_FIN500 AS "DT_FIN"
                FROM
                    EFD.TB_EFD_APURACAOE500
                WHERE
                    APUR_ID_001 in {efd_id_001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E500 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E500


@oracle_manager
def import_efd_E510(efd_id_500) -> pd.DataFrame:
    """ 
    Importa os registros E510    
    Args
    ----------------------
    efd_id_500: tuple
        Chave estrangeira do registro E500
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_510': int,
                        'APUR_ID_500': int,
                        'CFOP': str,
                        'CST_IPI': str,
                        'VL_CONT_IPI': float,
                        'VL_BC_IPI': float,
                        'VL_IPI': float
            }

    assert len(efd_id_500),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_500)}"

    assert isinstance(efd_id_500, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_500)}"

    # SQL query to fetch E510 data
    query = f""" 
                SELECT
                    APUR_ID_510,
                    APUR_ID_500,
                    APUR_CD_CFOP510 AS "CFOP",
                    APUR_CD_CST_IPI510 AS "CST_IPI",
                    APUR_VL_CONT_IPI510 AS "VL_CONT_IPI",
                    APUR_VL_BC_IPI510 AS "VL_BC_IPI",
                    APUR_VL_IPI510 AS "VL_IPI"
                FROM
                    EFD.TB_EFD_APURACAOE510
                WHERE
                    APUR_ID_500 in {efd_id_500}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E510 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E510


@oracle_manager
def import_efd_E520(efd_id_500) -> pd.DataFrame:
    """ 
    Importa os registros E520    
    Args
    ----------------------
    efd_id_500: tuple
        Chave estrangeira do registro E500
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_520': int,
                        'APUR_ID_500': int,
                        'VL_SD_ANT_IPI': float,
                        'VL_DEB_IPI': float,
                        'VL_CRED_IPI': float,
                        'VL_OD_IPI': float,
                        'VL_OC_IPI': float,
                        'VL_SC_IPI': float,
                        'VL_SD_IPI': float
            }

    assert len(efd_id_500),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_500)}"

    assert isinstance(efd_id_500, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_500)}"

    # SQL query to fetch E520 data
    query = f""" 
                SELECT
                    APUR_ID_520,
                    APUR_ID_500,
                    APUR_VL_SD_ANT_IPI520 AS "VL_SD_ANT_IPI",
                    APUR_VL_DEB_IPI520 AS "VL_DEB_IPI",
                    APUR_VL_CRED_IPI520 AS "VL_CRED_IPI",
                    APUR_VL_OD_IPI520 AS "VL_OD_IPI",
                    APUR_VL_OC_IPI520 AS "VL_OC_IPI",
                    APUR_VL_SC_IPI520 AS "VL_SC_IPI",
                    APUR_VL_SD_IPI520 AS "VL_SD_IPI"
                FROM
                    EFD.TB_EFD_APURACAOE520
                WHERE
                    APUR_ID_500 in {efd_id_500}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E520 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E520


@oracle_manager
def import_efd_E530(efd_id_520) -> pd.DataFrame:
    """ 
    Importa os registros E530    
    Args
    ----------------------
    efd_id_520: tuple
        Chave estrangeira do registro E520
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_530': int,
                        'APUR_ID_520': int,
                        'IND_AJ': str,
                        'VL_AJ': float,
                        'COD_AJ': str,
                        'IND_DOC': str,
                        'NUM_DOC': str,
                        'DESCR_AJ': str
            }

    assert len(efd_id_520),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_520)}"

    assert isinstance(efd_id_520, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_520)}"

    # SQL query to fetch E530 data
    query = f""" 
                SELECT
                    APUR_ID_530,
                    APUR_ID_520,
                    APUR_TP_IND_AJ530 AS "IND_AJ",
                    APUR_VL_AJ530 AS "VL_AJ",
                    APUR_CD_COD_AJ530 AS "COD_AJ",
                    APUR_TP_IND_DOC530 AS "IND_DOC",
                    APUR_NR_NUM_DOC530 AS "NUM_DOC",
                    APUR_DS_DESCR_AJ530 AS "DESCR_AJ"
                FROM
                    EFD.TB_EFD_APURACAOE530
                WHERE
                    APUR_ID_520 in {efd_id_520}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E530 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E530


@oracle_manager
def import_efd_E531(efd_id_530) -> pd.DataFrame:
    """ 
    Importa os registros E531    
    Args
    ----------------------
    efd_id_530: tuple
        Chave estrangeira do registro E530
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_531': int,
                        'APUR_ID_530': int,
                        'COD_PART': str,
                        'COD_MOD': str,
                        'SER': str,
                        'SUB': str,
                        'NUM_DOC': int,
                        'DT_DOC': 'datetime64[ns]',
                        'COD_ITEM': str,
                        'VL_AJ_ITEM': float,
                        'CHV_NFE': str
            }

    assert len(efd_id_530),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_530)}"

    assert isinstance(efd_id_530, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_530)}"

    # SQL query to fetch E531 data
    query = f""" 
                SELECT
                    APUR_ID_531,
                    APUR_ID_530,
                    APUR_CD_PART531 AS "COD_PART",
                    APUR_CD_MOD531 AS "COD_MOD",
                    APUR_NR_SER531 AS "SER",
                    APUR_NR_SUB531 AS "SUB",
                    APUR_NR_DOC531 AS "NUM_DOC",
                    APUR_DT_DOC531 AS "DT_DOC",
                    APUR_CD_ITEM531 AS "COD_ITEM",
                    APUR_VL_AJ_ITEM531 AS "VL_AJ_ITEM",
                    APUR_CD_CHV_NFE531 AS "CHV_NFE"
                FROM
                    EFD.TB_EFD_APURACAOE531
                WHERE
                    APUR_ID_530 in {efd_id_530}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E531 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E531


@oracle_manager
def import_efd_E990(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros E990    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {         
                        'APUR_ID_990': int,
                        'CABE_ID_0000': int,
                        'QTD_LIN_E': int
            }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # SQL query to fetch E990 data
    query = f""" 
                SELECT
                    APUR_ID_990,
                    CABE_ID_0000,
                    APUR_QT_QTD_LIN_E AS "QTD_LIN_E"
                FROM
                    EFD.TB_EFD_APURACAOE990
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_E990 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_E990

