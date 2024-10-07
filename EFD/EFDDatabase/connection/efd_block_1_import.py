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
def import_efd_1001(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros 1001    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1001': int,
                        'CABE_ID_0000': int,
                        'IND_MOV': str
            }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # SQL query to fetch 1001 data
    query = f""" 
                SELECT
                    INFO_ID_1001,
                    CABE_ID_0000,
                    INFO_TP_IND_MOV1001 AS "IND_MOV"
                FROM
                    EFD.TB_EFD_INFORMACAO1001
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1001 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1001


@oracle_manager
def import_efd_1010(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1010    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1010': int,
                        'INFO_ID_1001': int,
                        'IND_EXP': str,
                        'IND_CCRF': str,
                        'IND_COMB': str,
                        'IND_USINA': str,
                        'IND_VA': str,
                        'IND_EE': str,
                        'IND_CART': str,
                        'IND_FORM': str,
                        'IND_AER': str,
                        'IND_GIAF1': str,
                        'IND_GIAF3': str,
                        'IND_GIAF4': str,
                        'IND_REST_RESSARC_COMPL_ICMS': str
            }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1010 data
    query = f""" 
                SELECT
                    INFO_ID_1010,
                    INFO_ID_1001,
                    INFO_TP_IND_EXP1010 AS "IND_EXP",
                    INFO_TP_IND_CCRF1010 AS "IND_CCRF",
                    INFO_TP_IND_COMB1010 AS "IND_COMB",
                    INFO_TP_IND_USINA1010 AS "IND_USINA",
                    INFO_TP_IND_VA1010 AS "IND_VA",
                    INFO_TP_IND_EE1010 AS "IND_EE",
                    INFO_TP_IND_CART1010 AS "IND_CART",
                    INFO_TP_IND_FORM1010 AS "IND_FORM",
                    INFO_TP_IND_AER1010 AS "IND_AER",
                    INFO_TP_IND_GIAF11010 AS "IND_GIAF1",
                    INFO_TP_IND_GIAF31010 AS "IND_GIAF3",
                    INFO_TP_IND_GIAF41010 AS "IND_GIAF4",
                    INFO_TP_REST_RES_COMPLICMS1010 AS "IND_REST_RESSARC_COMPL_ICMS"
                FROM
                    EFD.TB_EFD_INFORMACAO1010
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1010 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1010


@oracle_manager
def import_efd_1100(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1100    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1100': int,
                        'INFO_ID_1001': int,
                        'IND_DOC': str,
                        'NRO_DE': str,
                        'DT_DE': 'datetime64[ns]',
                        'NAT_EXP': str,
                        'NRO_RE': str,
                        'DT_RE': 'datetime64[ns]',
                        'CHC_EMB': str,
                        'DT_CHC': 'datetime64[ns]',
                        'DT_AVB': 'datetime64[ns]',
                        'TP_CHC': str,
                        'PAIS': str
            }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1100 data
    query = f""" 
                SELECT
                    INFO_ID_1100,
                    INFO_ID_1001,
                    INFO_TP_IND_DOC1100 AS "IND_DOC",
                    INFO_NR_NRO_DE1100 AS "NRO_DE",
                    INFO_DT_DE1100 AS "DT_DE",
                    INFO_TP_NAT_EXP1100 AS "NAT_EXP",
                    INFO_NR_NRO_RE1100 AS "NRO_RE",
                    INFO_DT_RE1100 AS "DT_RE",
                    INFO_NR_CHC_EMB1100 AS "CHC_EMB",
                    INFO_DT_CHC1100 AS "DT_CHC",
                    INFO_DT_AVB1100 AS "DT_AVB",
                    INFO_TP_CHC1100 AS "TP_CHC",
                    INFO_CD_PAIS1100 AS "PAIS"
                FROM
                    EFD.TB_EFD_INFORMACAO1100
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1100 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1100


@oracle_manager
def import_efd_1105(efd_id_1100) -> pd.DataFrame:
    """ 
    Importa os registros 1105    
    Args
    ----------------------
    efd_id_1100: tuple
        Chave estrangeira do registro 1100
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1105': int,
                        'INFO_ID_1100': int,
                        'COD_MOD': str,
                        'SER': str,
                        'NR_DOC': str,
                        'CHV_NFE': str,
                        'DT_DOC': 'datetime64[ns]',
                        'COD_ITEM': str
            }

    assert len(efd_id_1100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1100)}"

    assert isinstance(efd_id_1100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1100)}"

    # SQL query to fetch 1105 data
    query = f""" 
                SELECT
                    INFO_ID_1105,
                    INFO_ID_1100,
                    INFO_CD_COD_MOD1105 AS "COD_MOD",
                    INFO_CD_SERIE1105 AS "SER",
                    INFO_NR_NUM_DOC1105 AS "NR_DOC",
                    INFO_CD_CHV_NFE1105 AS "CHV_NFE",
                    INFO_DT_DOC1105 AS "DT_DOC",
                    INFO_CD_COD_ITEM1105 AS "COD_ITEM"
                FROM
                    EFD.TB_EFD_INFORMACAO1105
                WHERE
                    INFO_ID_1100 in {efd_id_1100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1105 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1105


@oracle_manager
def import_efd_1110(efd_id_1105) -> pd.DataFrame:
    """ 
    Importa os registros 1110    
    Args
    ----------------------
    efd_id_1105: tuple
        Chave estrangeira do registro 1105
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1110': int,
                        'INFO_ID_1105': int,
                        'COD_PART': str,
                        'COD_MOD': str,
                        'SER': str,
                        'NR_DOC': int,
                        'DT_DOC': 'datetime64[ns]',
                        'CHV_NFE': str,
                        'NR_MEMO': str,
                        'QTD': float,
                        'UNID': str
            }

    assert len(efd_id_1105),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1105)}"

    assert isinstance(efd_id_1105, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1105)}"

    # SQL query to fetch 1110 data
    query = f""" 
                SELECT
                    INFO_ID_1110 AS "INFO_ID_1110",
                    INFO_ID_1105 AS "INFO_ID_1105",
                    INFO_CD_COD_PART1110 AS "COD_PART",
                    INFO_CD_COD_MOD1110 AS "COD_MOD",
                    INFO_CD_SER1110 AS "SER",
                    INFO_NR_NUM_DOC1110 AS "NR_DOC",
                    INFO_DT_DOC1110 AS "DT_DOC",
                    INFO_CD_CHV_NFE1110 AS "CHV_NFE",
                    INFO_NR_MEMO1110 AS "NR_MEMO",
                    INFO_QT_QTD1110 AS "QTD",
                    INFO_TP_UNID1110 AS "UNID"
                FROM
                    EFD.TB_EFD_INFORMACAO1110
                WHERE
                    INFO_ID_1105 in {efd_id_1105}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1110 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1110


@oracle_manager
def import_efd_1200(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1200    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1200': int,
                        'INFO_ID_1001': int,
                        'COD_AJ_APUR': str,
                        'SLD_CRED': float,
                        'CRED_APR': float,
                        'CRED_RECEB': float,
                        'CRED_UTIL': float,
                        'SLD_CRED_FIM': float
            }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1200 data
    query = f""" 
                SELECT
                    INFO_ID_1200 AS "INFO_ID_1200",
                    INFO_ID_1001 AS "INFO_ID_1001",
                    INFO_CD_COD_AJ_APUR1200 AS "COD_AJ_APUR",
                    INFO_VL_SLD_CRED1200 AS "SLD_CRED",
                    INFO_VL_CRED_APR1200 AS "CRED_APR",
                    INFO_VL_CRED_RECEB1200 AS "CRED_RECEB",
                    INFO_VL_CRED_UTIL1200 AS "CRED_UTIL",
                    INFO_VL_SLD_CRED_FIM1200 AS "SLD_CRED_FIM"
                FROM
                    EFD.TB_EFD_INFORMACAO1200
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1200 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1200


@oracle_manager
def import_efd_1210(efd_id_1200) -> pd.DataFrame:
    """ 
    Importa os registros 1210    
    Args
    ----------------------
    efd_id_1200: tuple
        Chave estrangeira do registro 1200
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1210': int,
                        'INFO_ID_1200': int,
                        'TIPO_UTIL': str,
                        'NR_DOC': str,
                        'VL_CRED_UTIL': float,
                        'CHV_DOCe': str
            }

    assert len(efd_id_1200),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1200)}"

    assert isinstance(efd_id_1200, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1200)}"

    # SQL query to fetch 1210 data
    query = f""" 
                SELECT
                    INFO_ID_1210 AS "INFO_ID_1210",
                    INFO_ID_1200 AS "INFO_ID_1200",
                    INFO_TP_TIPO_UTIL1210 AS "TIPO_UTIL",
                    INFO_NR_DOC1210 AS "NR_DOC",
                    INFO_VL_CRED_UTIL1210 AS "VL_CRED_UTIL",
                    INFO_CD_CHV_DOCE1210 AS "CHV_DOCe"
                FROM
                    EFD.TB_EFD_INFORMACAO1210
                WHERE
                    INFO_ID_1200 in {efd_id_1200}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1210 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1210


@oracle_manager
def import_efd_1250(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1250    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1250': int,
                        'INFO_ID_1001': int,
                        'VL_CREDITO_ICMS_OP': float,
                        'VL_ICMS_ST_REST': float,
                        'VL_FCP_ST_REST': float,
                        'VL_ICMS_ST_COMPL': float,
                        'VL_FCP_ST_COMPL': float
            }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1250 data
    query = f""" 
                SELECT
                    INFO_ID_1250 AS "INFO_ID_1250",
                    INFO_ID_1001 AS "INFO_ID_1001",
                    INFO_VL_CREDITO_ICMS_OP1250 AS "VL_CREDITO_ICMS_OP",
                    INFO_VL_ICMS_ST_REST1250 AS "VL_ICMS_ST_REST",
                    INFO_VL_FCP_ST_REST1250 AS "VL_FCP_ST_REST",
                    INFO_VL_ICMS_ST_COMPL1250 AS "VL_ICMS_ST_COMPL",
                    INFO_VL_FCP_ST_COMPL1250 AS "VL_FCP_ST_COMPL"
                FROM
                    EFD.TB_EFD_INFORMACAO1250
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1250 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1250


@oracle_manager
def import_efd_1255(efd_id_1250) -> pd.DataFrame:
    """ 
    Importa os registros 1255    
    Args
    ----------------------
    efd_id_1250: tuple
        Chave estrangeira do registro 1250
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1255': int,
                        'INFO_ID_1250': int,
                        'COD_MOT_REST_COMPL': str,
                        'VL_CREDITO_ICMS_OP_MOT': float,
                        'VL_ICMS_ST_REST_MOT': float,
                        'VL_FCP_ST_REST_MOT': float,
                        'VL_ICMS_ST_COMPL_MOT': float,
                        'VL_FCP_ST_COMPL_MOT': float
            }

    assert len(efd_id_1250),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1250)}"

    assert isinstance(efd_id_1250, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1250)}"

    # SQL query to fetch 1255 data
    query = f""" 
                SELECT
                    INFO_ID_1255 AS "INFO_ID_1255",
                    INFO_ID_1250 AS "INFO_ID_1250",
                    INFO_COD_MOT_REST_COMPL1255 AS "COD_MOT_REST_COMPL",
                    INFO_VL_CRED_ICMS_OP_MOT1255 AS "VL_CREDITO_ICMS_OP_MOT",
                    INFO_VL_ICMS_ST_REST_MOT1255 AS "VL_ICMS_ST_REST_MOT",
                    INFO_VL_FCP_ST_REST_MOT1255 AS "VL_FCP_ST_REST_MOT",
                    INFO_VL_ICMS_ST_COMPL_MOT1255 AS "VL_ICMS_ST_COMPL_MOT",
                    INFO_VL_FCP_ST_COMPL_MOT1255 AS "VL_FCP_ST_COMPL_MOT"
                FROM
                    EFD.TB_EFD_INFORMACAO1255
                WHERE
                    INFO_ID_1250 in {efd_id_1250}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1255 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1255


@oracle_manager
def import_efd_1300(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1300    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1300': int,
                        'INFO_ID_1001': int,
                        'COD_ITEM': str,
                        'DT_FECH': 'datetime64[ns]',
                        'ESTQ_ABERT': float,
                        'VOL_ENTR': float,
                        'VOL_DISP': float,
                        'VOL_SAIDAS': float,
                        'ESTQ_ESCR': float,
                        'VAL_AJ_PERDA': float,
                        'VAL_AJ_GANHO': float,
                        'FECH_FISICO': float
            }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1300 data
    query = f""" 
                SELECT
                    INFO_ID_1300 AS "INFO_ID_1300",
                    INFO_ID_1001 AS "INFO_ID_1001",
                    INFO_CD_COD_ITEM1300 AS "COD_ITEM",
                    INFO_DT_FECH1300 AS "DT_FECH",
                    INFO_QT_ESTQ_ABERT1300 AS "ESTQ_ABERT",
                    INFO_QT_VOL_ENTR1300 AS "VOL_ENTR",
                    INFO_QT_VOL_DISP1300 AS "VOL_DISP",
                    INFO_QT_VOL_SAIDAS1300 AS "VOL_SAIDAS",
                    INFO_QT_ESTQ_ESCR1300 AS "ESTQ_ESCR",
                    INFO_VL_VAL_AJ_PERDA1300 AS "VAL_AJ_PERDA",
                    INFO_VL_VAL_AJ_GANHO1300 AS "VAL_AJ_GANHO",
                    INFO_QT_FECH_FISICO1300 AS "FECH_FISICO"
                FROM
                    EFD.TB_EFD_INFORMACAO1300
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1300 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1300


@oracle_manager
def import_efd_1310(efd_id_1300) -> pd.DataFrame:
    """ 
    Importa os registros 1310    
    Args
    ----------------------
    efd_id_1300: tuple
        Chave estrangeira do registro 1300
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1310': int,
                        'INFO_ID_1300': int,
                        'NUM_TANQUE': str,
                        'ESTQ_ABERT': float,
                        'VOL_ENTR': float,
                        'VOL_DISP': float,
                        'VOL_SAIDAS': float,
                        'ESTQ_ESCR': float,
                        'VAL_AJ_PERDA': float,
                        'VAL_AJ_GANHO': float,
                        'FECH_FISICO': float
            }

    assert len(efd_id_1300),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1300)}"

    assert isinstance(efd_id_1300, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1300)}"

    # SQL query to fetch 1310 data
    query = f""" 
                SELECT
                    INFO_ID_1310 AS "INFO_ID_1310",
                    INFO_ID_1300 AS "INFO_ID_1300",
                    INFO_NR_NUM_TANQUE1310 AS "NUM_TANQUE",
                    INFO_QT_ESTQ_ABERT1310 AS "ESTQ_ABERT",
                    INFO_QT_VOL_ENTR1310 AS "VOL_ENTR",
                    INFO_QT_VOL_DISP1310 AS "VOL_DISP",
                    INFO_QT_VOL_SAIDAS1310 AS "VOL_SAIDAS",
                    INFO_QT_ESTQ_ESCR1310 AS "ESTQ_ESCR",
                    INFO_VL_VAL_AJ_PERDA1310 AS "VAL_AJ_PERDA",
                    INFO_VL_VAL_AJ_GANHO1310 AS "VAL_AJ_GANHO",
                    INFO_QT_FECH_FISICO1310 AS "FECH_FISICO"
                FROM
                    EFD.TB_EFD_INFORMACAO1310
                WHERE
                    INFO_ID_1300 in {efd_id_1300}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1310 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1310


@oracle_manager
def import_efd_1320(efd_id_1310) -> pd.DataFrame:
    """ 
    Importa os registros 1320    
    Args
    ----------------------
    efd_id_1310: tuple
        Chave estrangeira do registro 1310
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1320': int,
                        'INFO_ID_1310': int,
                        'NUM_BICO': int,
                        'NR_INTERV': int,
                        'MOT_INTERV': str,
                        'NOM_INTERV': str,
                        'CNPJ_INTERV': str,
                        'CPF_INTERV': str,
                        'VAL_FECHA': float,
                        'VAL_ABERT': float,
                        'VOL_AFERI': float,
                        'VOL_VENDAS': float
            }

    assert len(efd_id_1310),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1310)}"

    assert isinstance(efd_id_1310, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1310)}"

    # SQL query to fetch 1320 data
    query = f""" 
                SELECT
                    INFO_ID_1320 AS "INFO_ID_1320",
                    INFO_ID_1310 AS "INFO_ID_1310",
                    INFO_NR_NUM_BICO1320 AS "NUM_BICO",
                    INFO_NR_INTERV1320 AS "NR_INTERV",
                    INFO_DS_MOT_INTERV1320 AS "MOT_INTERV",
                    INFO_NM_NOM_INTERV1320 AS "NOM_INTERV",
                    INFO_NR_CNPJ_INTERV1320 AS "CNPJ_INTERV",
                    INFO_NR_CPF_INTERV1320 AS "CPF_INTERV",
                    INFO_VL_VAL_FECHA1320 AS "VAL_FECHA",
                    INFO_VL_VAL_ABERT1320 AS "VAL_ABERT",
                    INFO_VL_VOL_AFERI1320 AS "VOL_AFERI",
                    INFO_VL_VOL_VENDAS AS "VOL_VENDAS"
                FROM
                    EFD.TB_EFD_INFORMACAO1320
                WHERE
                    INFO_ID_1310 in {efd_id_1310}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1320 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1320

@oracle_manager
def import_efd_1350(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1350    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1350': int,
                        'INFO_ID_1001': int,
                        'SERIE': str,
                        'FABRICANTE': str,
                        'MODELO': str,
                        'TIPO_MEDICAO': str
            }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1350 data
    query = f""" 
                SELECT
                    INFO_ID_1350 AS "INFO_ID_1350",
                    INFO_ID_1001 AS "INFO_ID_1001",
                    INFO_NR_SERIE1350 AS "SERIE",
                    INFO_NM_FABRICANTE1350 AS "FABRICANTE",
                    INFO_CD_MODELO1350 AS "MODELO",
                    INFO_TP_TIPO_MEDICAO1350 AS "TIPO_MEDICAO"
                FROM
                    EFD.TB_EFD_INFORMACAO1350
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1350 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1350



@oracle_manager
def import_efd_1360(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1360    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1360': int,
                        'INFO_ID_1001': int,
                        'NUM_LACRE': str,
                        'DT_APLICACAO': 'datetime64[ns]'
            }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1360 data
    query = f""" 
                SELECT
                    INFO_ID_1360 AS "INFO_ID_1360",
                    INFO_ID_1001 AS "INFO_ID_1001",
                    INFO_NR_NUM_LACRE1360 AS "NUM_LACRE",
                    INFO_DT_APLICACAO1360 AS "DT_APLICACAO"
                FROM
                    EFD.TB_EFD_INFORMACAO1360
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1360 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1360


@oracle_manager
def import_efd_1370(efd_id_1350) -> pd.DataFrame:
    """ 
    Importa os registros 1370    
    Args
    ----------------------
    efd_id_1350: tuple
        Chave estrangeira do registro 1350
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1370': int,
                        'INFO_ID_1350': int,
                        'NUM_BICO': str,
                        'COD_ITEM': str,
                        'NUM_TANQUE': str
            }

    assert len(efd_id_1350),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1350)}"

    assert isinstance(efd_id_1350, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1350)}"

    # SQL query to fetch 1370 data
    query = f""" 
                SELECT
                    INFO_ID_1370 AS "INFO_ID_1370",
                    INFO_ID_1350 AS "INFO_ID_1350",
                    INFO_NR_NUM_BICO1370 AS "NUM_BICO",
                    INFO_CD_COD_ITEM1370 AS "COD_ITEM",
                    INFO_NR_NUM_TANQUE1370 AS "NUM_TANQUE"
                FROM
                    EFD.TB_EFD_INFORMACAO1370
                WHERE
                    INFO_ID_1350 in {efd_id_1350}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1370 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1370


@oracle_manager
def import_efd_1390(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1390    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {         
                        'INFO_ID_1390': int,
                        'INFO_ID_1001': int,
                        'COD_PROD': str
            }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1390 data
    query = f""" 
                SELECT
                    INFO_ID_1390 AS "INFO_ID_1390",
                    INFO_ID_1001 AS "INFO_ID_1001",
                    INFO_CD_COD_PROD1390 AS "COD_PROD"
                FROM
                    EFD.TB_EFD_INFORMACAO1390
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1390 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1390



@oracle_manager
def import_efd_1391(efd_id_1390) -> pd.DataFrame:
    """ 
    Importa os registros 1391    
    Args
    ----------------------
    efd_id_1390: tuple
        Chave estrangeira do registro 1390
    """

    # Creating SQL query
    columns = {         
                'INFO_ID_1391': int,
                'INFO_ID_1390': int,
                'DT_REGISTRO': 'datetime64[ns]',
                'QTD_MOID': float,
                'ESTQ_INI': float,
                'QTD_PRODUZ': float,
                'ENT_ANID_HID': float,
                'OUTR_ENTR': float,
                'PERDA': float,
                'CONS': float,
                'SAI_ANI_HID': float,
                'SAÍDAS': float,
                'ESTQ_FIN': float,
                'ESTQ_INI_MEL': float,
                'PROD_DIA_MEL': float,
                'UTIL_MEL': float,
                'PROD_ALC_MEL': float,
                'OBS': str
            }

    assert len(efd_id_1390),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1390)}"

    assert isinstance(efd_id_1390, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1390)}"

    # SQL query to fetch 1391 data
    query = f""" 
                SELECT
                    INFO_ID_1391 AS "INFO_ID_1391",
                    INFO_ID_1390 AS "INFO_ID_1390",
                    INFO_DT_REGISTRO1391 AS "DT_REGISTRO",
                    INFO_QT_QTD_MOID1391 AS "QTD_MOID",
                    INFO_QT_ESTQ_INI1391 AS "ESTQ_INI",
                    INFO_QT_QTD_PRODUZ1391 AS "QTD_PRODUZ",
                    INFO_QT_ENT_ANID_HID1391 AS "ENT_ANID_HID",
                    INFO_QT_OUTR_ENTR1391 AS "OUTR_ENTR",
                    INFO_QT_PERDA1391 AS "PERDA",
                    INFO_QT_CONS1391 AS "CONS",
                    INFO_QT_SAI_ANI_HID1391 AS "SAI_ANI_HID",
                    INFO_QT_SAIDAS1391 AS "SAÍDAS",
                    INFO_QT_ESTQ_FIN1391 AS "ESTQ_FIN",
                    INFO_QT_ESTQ_INI_MEL1391 AS "ESTQ_INI_MEL",
                    INFO_QT_PROD_DIA_MEL1391 AS "PROD_DIA_MEL",
                    INFO_QT_UTIL_MEL1391 AS "UTIL_MEL",
                    INFO_QT_PROD_ALC_MEL1391 AS "PROD_ALC_MEL",
                    INFO_DS_OBS1391 AS "OBS"
                FROM
                    EFD.TB_EFD_INFORMACAO1391
                WHERE
                    INFO_ID_1390 in {efd_id_1390}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1391 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1391


@oracle_manager
def import_efd_1400(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1400    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {         
                'INFO_ID_1400': int,
                'INFO_ID_1001': int,
                'COD_ITEM_IPM': str,
                'MUN': int,
                'VALOR': float
            }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1400 data
    query = f""" 
                SELECT
                    INFO_ID_1400 AS "INFO_ID_1400",
                    INFO_ID_1001 AS "INFO_ID_1001",
                    INFO_CD_COD_ITEM1400 AS "COD_ITEM_IPM",
                    INFO_CD_MUN1400 AS "MUN",
                    INFO_VL_VALOR1400 AS "VALOR"
                FROM
                    EFD.TB_EFD_INFORMACAO1400
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1400 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1400

@oracle_manager
def import_efd_1500(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1500    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1500': int,
        'INFO_ID_1001': int,
        'IND_OPER': str,
        'IND_EMIT': str,
        'COD_PART': str,
        'COD_MOD': str,
        'COD_SIT': str,
        'SER': str,
        'SUB': str,
        'COD_CONS': str,
        'NUM_DOC': str,
        'DT_DOC': 'datetime64[ns]',
        'DT_E_S': 'datetime64[ns]',
        'VL_DOC': float,
        'VL_DESC': float,
        'VL_FORN': float,
        'VL_SERV_NT': float,
        'VL_TERC': float,
        'VL_DA': float,
        'VL_BC_ICMS': float,
        'VL_ICMS': float,
        'VL_BC_ICMS_ST': float,
        'VL_ICMS_ST': float,
        'COD_INF': str,
        'VL_PIS': float,
        'VL_COFINS': float,
        'TP_LIGACAO': str,
        'COD_GRUPO_TENSAO': str
    }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1500 data
    query = f""" 
                SELECT
                    INFO_ID_1500,
                    INFO_ID_1001,
                    INFO_TP_IND_OPER1500 AS "IND_OPER",
                    INFO_TP_IND_EMIT1500 AS "IND_EMIT",
                    INFO_CD_COD_PART1500 AS "COD_PART",
                    INFO_CD_COD_MOD1500 AS "COD_MOD",
                    INFO_CD_COD_SIT1500 AS "COD_SIT",
                    INFO_CD_SER1500 AS "SER",
                    INFO_NR_SUB1500 AS "SUB",
                    INFO_CD_COD_CONS1500 AS "COD_CONS",
                    INFO_NR_NUM_DOC1500 AS "NUM_DOC",
                    INFO_DT_DOC1500 AS "DT_DOC",
                    INFO_DT_E_S1500 AS "DT_E_S",
                    INFO_VL_DOC1500 AS "VL_DOC",
                    INFO_VL_DESC1500 AS "VL_DESC",
                    INFO_VL_FORN1500 AS "VL_FORN",
                    INFO_VL_SERV_NT1500 AS "VL_SERV_NT",
                    INFO_VL_TERC1500 AS "VL_TERC",
                    INFO_VL_DA1500 AS "VL_DA",
                    INFO_VL_BC_ICMS1500 AS "VL_BC_ICMS",
                    INFO_VL_ICMS1500 AS "VL_ICMS",
                    INFO_VL_BC_ICMS_ST1500 AS "VL_BC_ICMS_ST",
                    INFO_VL_ICMS_ST1500 AS "VL_ICMS_ST",
                    INFO_CD_COD_INF1500 AS "COD_INF",
                    INFO_VL_PIS1500 AS "VL_PIS",
                    INFO_VL_COFINS1500 AS "VL_COFINS",
                    INFO_TP_LIGACAO1500 AS "TP_LIGACAO",
                    INFO_CD_COD_GRUPO_TENSAO1500 AS "COD_GRUPO_TENSAO"
                FROM
                    EFD.TB_EFD_INFORMACAO1500
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1500 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1500


@oracle_manager
def import_efd_1510(efd_id_1500) -> pd.DataFrame:
    """ 
    Importa os registros 1510    
    Args
    ----------------------
    efd_id_1500: tuple
        Chave estrangeira do registro 1500
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1510': int,
        'INFO_ID_1500': int,
        'NUM_ITEM': int,
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
        'VL_BC_ICMS_ST': float,
        'ALIQ_ST': float,
        'VL_ICMS_ST': float,
        'IND_REC': str,
        'COD_PART': str,
        'VL_PIS': float,
        'VL_COFINS': float,
        'COD_CTA': str
    }

    assert len(efd_id_1500),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1500)}"

    assert isinstance(efd_id_1500, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1500)}"

    # SQL query to fetch 1510 data
    query = f""" 
                SELECT
                    INFO_ID_1510,
                    INFO_ID_1500,
                    INFO_NR_NUM_ITEM1510 AS "NUM_ITEM",
                    INFO_CD_COD_ITEM1510 AS "COD_ITEM",
                    INFO_CD_COD_CLASS1510 AS "COD_CLASS",
                    INFO_QT_QTD1510 AS "QTD",
                    INFO_TP_UNID1510 AS "UNID",
                    INFO_VL_ITEM1510 AS "VL_ITEM",
                    INFO_VL_DESC1510 AS "VL_DESC",
                    INFO_CD_CST_ICMS1510 AS "CST_ICMS",
                    INFO_CD_CFOP1510 AS "CFOP",
                    INFO_VL_BC_ICMS1510 AS "VL_BC_ICMS",
                    INFO_VL_ALIQ_ICMS1510 AS "ALIQ_ICMS",
                    INFO_VL_ICMS1510 AS "VL_ICMS",
                    INFO_VL_BC_ICMS_ST1510 AS "VL_BC_ICMS_ST",
                    INFO_VL_ALIQ_ST1510 AS "ALIQ_ST",
                    INFO_VL_ICMS_ST1510 AS "VL_ICMS_ST",
                    INFO_TP_IND_REC1510 AS "IND_REC",
                    INFO_CD_COD_PART1510 AS "COD_PART",
                    INFO_VL_PIS1510 AS "VL_PIS",
                    INFO_VL_COFINS1510 AS "VL_COFINS",
                    INFO_CD_COD_CTA1510 AS "COD_CTA"
                FROM
                    EFD.TB_EFD_INFORMACAO1510
                WHERE
                    INFO_ID_1500 in {efd_id_1500}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1510 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1510


@oracle_manager
def import_efd_1600(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1600    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1600': int,
        'INFO_ID_1001': int,
        'COD_PART': str,
        'VL_TOT_CREDITO': float,
        'VL_TOT_DEBITO': float
    }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1600 data
    query = f""" 
                SELECT
                    INFO_ID_1600,
                    INFO_ID_1001,
                    INFO_CD_COD_PART1600 AS "COD_PART",
                    INFO_VL_TOT_CREDITO1600 AS "VL_TOT_CREDITO",
                    INFO_VL_TOT_DEBITO1600 AS "VL_TOT_DEBITO"
                FROM
                    EFD.TB_EFD_INFORMACAO1600
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1600 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1600


@oracle_manager
def import_efd_1601(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1601    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1601': int,
        'INFO_ID_1001': int,
        'COD_PART_IP': str,
        'COD_PART_IT': str,
        'TOT_VS': float,
        'TOT_ISS': float,
        'TOT_OUTROS': float
    }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1601 data
    query = f""" 
                SELECT
                    INFO_ID_1601,
                    INFO_ID_1001,
                    INFO_COD_PART_IP1601 AS "COD_PART_IP",
                    INFO_COD_PART_IT1601 AS "COD_PART_IT",
                    INFO_TOT_VS1601 AS "TOT_VS",
                    INFO_TOT_ISS1601 AS "TOT_ISS",
                    INFO_TOT_OUTROS1601 AS "TOT_OUTROS"
                FROM
                    EFD.TB_EFD_INFORMACAO1601
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1601 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1601

####################################################################################################
@oracle_manager
def import_efd_1700(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1700    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1700': int,
        'INFO_ID_1001': int,
        'COD_DISP': str,
        'COD_MOD': str,
        'SER': str,
        'SUB': str,
        'NUM_DOC_INI': int,
        'NUM_DOC_FIN': int,
        'NUM_AUT': str
    }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1700 data
    query = f""" 
                SELECT
                    INFO_ID_1700,
                    INFO_ID_1001,
                    INFO_CD_COD_DISP1700 AS "COD_DISP",
                    INFO_CD_COD_MOD1700 AS "COD_MOD",
                    INFO_CD_SER1700 AS "SER",
                    INFO_NR_SUB1700 AS "SUB",
                    INFO_NR_NUM_DOC_INI1700 AS "NUM_DOC_INI",
                    INFO_NR_NUM_DOC_FIN1700 AS "NUM_DOC_FIN",
                    INFO_NR_NUM_AUT1700 AS "NUM_AUT"
                FROM
                    EFD.TB_EFD_INFORMACAO1700
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1700 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1700


@oracle_manager
def import_efd_1710(efd_id_1700) -> pd.DataFrame:
    """ 
    Importa os registros 1710    
    Args
    ----------------------
    efd_id_1700: tuple
        Chave estrangeira do registro 1700
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1710': int,
        'INFO_ID_1700': int,
        'NUM_DOC_INI': int,
        'NUM_DOC_FIN': int
    }

    assert len(efd_id_1700),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1700)}"

    assert isinstance(efd_id_1700, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1700)}"

    # SQL query to fetch 1710 data
    query = f""" 
                SELECT
                    INFO_ID_1710,
                    INFO_ID_1700,
                    INFO_NR_NUM_DOC_INI1710 AS "NUM_DOC_INI",
                    INFO_NR_NUM_DOC_FIN1710 AS "NUM_DOC_FIN"
                FROM
                    EFD.TB_EFD_INFORMACAO1710
                WHERE
                    INFO_ID_1700 in {efd_id_1700}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1710 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1710


@oracle_manager
def import_efd_1800(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1800    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1800': int,
        'INFO_ID_1001': int,
        'VL_CARGA': float,
        'VL_PASS': float,
        'VL_FAT': float,
        'IND_RAT': float,
        'VL_ICMS_ANT': float,
        'VL_BC_ICMS': float,
        'VL_ICMS_APUR': float,
        'VL_BC_ICMS_APUR': float,
        'VL_DIF': float
    }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1800 data
    query = f""" 
                SELECT
                    INFO_ID_1800,
                    INFO_ID_1001,
                    INFO_VL_CARGA1800 AS "VL_CARGA",
                    INFO_VL_PASS1800 AS "VL_PASS",
                    INFO_VL_FAT1800 AS "VL_FAT",
                    INFO_TP_IND_RAT1800 AS "IND_RAT",
                    INFO_VL_ICMS_ANT1800 AS "VL_ICMS_ANT",
                    INFO_VL_BC_ICMS1800 AS "VL_BC_ICMS",
                    INFO_VL_ICMS_APUR1800 AS "VL_ICMS_APUR",
                    INFO_VL_BC_ICMS_APUR1800 AS "VL_BC_ICMS_APUR",
                    INFO_VL_DIF1800 AS "VL_DIF"
                FROM
                    EFD.TB_EFD_INFORMACAO1800
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1800 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1800


@oracle_manager
def import_efd_1900(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1900    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1900': int,
        'INFO_ID_1001': int,
        'IND_APUR_ICMS': str,
        'DESCR_COMPL_OUT_APUR': str
    }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1900 data
    query = f""" 
                SELECT
                    INFO_ID_1900,
                    INFO_ID_1001,
                    INFO_TP_IND_APUR_ICMS1900 AS "IND_APUR_ICMS",
                    INFO_DESCR_COMPL_OUT_APUR1900 AS "DESCR_COMPL_OUT_APUR"
                FROM
                    EFD.TB_EFD_INFORMACAO1900
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1900 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1900


@oracle_manager
def import_efd_1910(efd_id_1900) -> pd.DataFrame:
    """ 
    Importa os registros 1910    
    Args
    ----------------------
    efd_id_1900: tuple
        Chave estrangeira do registro 1900
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1910': int,
        'INFO_ID_1900': int,
        'DT_INI': 'datetime64[ns]',
        'DT_FIN': 'datetime64[ns]'
    }

    assert len(efd_id_1900),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1900)}"

    assert isinstance(efd_id_1900, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1900)}"

    # SQL query to fetch 1910 data
    query = f""" 
                SELECT
                    INFO_ID_1910,
                    INFO_ID_1900,
                    INFO_DT_INI1900 AS "DT_INI",
                    INFO_DT_FIN1900 AS "DT_FIN"
                FROM
                    EFD.TB_EFD_INFORMACAO1910
                WHERE
                    INFO_ID_1900 in {efd_id_1900}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1910 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1910



@oracle_manager
def import_efd_1920(efd_id_1910) -> pd.DataFrame:
    """ 
    Importa os registros 1920    
    Args
    ----------------------
    efd_id_1910: tuple
        Chave estrangeira do registro 1910
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1920': int,
        'INFO_ID_1910': int,
        'VL_TOT_TRANSF_DEBITOS_OA': float,
        'VL_TOT_AJ_DEBITOS_OA': float,
        'VL_ESTORNOS_CRED_OA': float,
        'VL_TOT_TRANSF_CREDITOS_OA': float,
        'VL_TOT_AJ_CREDITOS_OA': float,
        'VL_ESTORNOS_DEB_OA': float,
        'VL_SLD_CREDOR_ANT_OA': float,
        'VL_SLD_APURADO_OA': float,
        'VL_TOT_DED': float,
        'VL_ICMS_RECOLHER_OA': float,
        'VL_SLD_CREDOR_TRANSP_OA': float,
        'DEB_ESP_OA': float
    }

    assert len(efd_id_1910),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1910)}"

    assert isinstance(efd_id_1910, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1910)}"

    # SQL query to fetch 1920 data
    query = f""" 
                SELECT
                    INFO_ID_1920,
                    INFO_ID_1910,
                    INFO_VL_TOT_TRANSF_DEBITOS_OA1 AS "VL_TOT_TRANSF_DEBITOS_OA",
                    INFO_VL_TOT_AJ_DEBITOS_OA1920 AS "VL_TOT_AJ_DEBITOS_OA",
                    INFO_VL_ESTORNOS_CRED_OA1920 AS "VL_ESTORNOS_CRED_OA",
                    INFO_VL_TOT_TRANSF_CREDITOS_OA AS "VL_TOT_TRANSF_CREDITOS_OA",
                    INFO_VL_TOT_AJ_CREDITOS_OA1920 AS "VL_TOT_AJ_CREDITOS_OA",
                    INFO_VL_ESTORNOS_DEB_OA1920 AS "VL_ESTORNOS_DEB_OA",
                    INFO_VL_SLD_CREDOR_ANT_OA1920 AS "VL_SLD_CREDOR_ANT_OA",
                    INFO_VL_SLD_APURADO_OA1920 AS "VL_SLD_APURADO_OA",
                    INFO_VL_TOT_DED1920 AS "VL_TOT_DED",
                    INFO_VL_ICMS_RECOLHER_OA1920 AS "VL_ICMS_RECOLHER_OA",
                    INFO_VL_SLD_CREDOR_TRANSP_OA19 AS "VL_SLD_CREDOR_TRANSP_OA",
                    INFO_DEB_ESP_OA1920 AS "DEB_ESP_OA"
                FROM
                    EFD.TB_EFD_INFORMACAO1920
                WHERE
                    INFO_ID_1910 in {efd_id_1910}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1920 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1920



@oracle_manager
def import_efd_1921(efd_id_1920) -> pd.DataFrame:
    """ 
    Importa os registros 1921    
    Args
    ----------------------
    efd_id_1920: tuple
        Chave estrangeira do registro 1920
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1921': int,
        'INFO_ID_1920': int,
        'COD_AJ_APUR': str,
        'DESCR_COMPL_AJ': str,
        'VL_AJ_APUR': float
    }

    assert len(efd_id_1920),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1920)}"

    assert isinstance(efd_id_1920, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1920)}"

    # SQL query to fetch 1921 data
    query = f""" 
                SELECT
                    INFO_ID_1921,
                    INFO_ID_1920,
                    INFO_CD_COD_AJ_APUR1920 AS "COD_AJ_APUR",
                    INFO_DS_DESCR_COMPL_AJ1920 AS "DESCR_COMPL_AJ",
                    INFO_VL_AJ_APUR1920 AS "VL_AJ_APUR"
                FROM
                    EFD.TB_EFD_INFORMACAO1921
                WHERE
                    INFO_ID_1920 in {efd_id_1920}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1921 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1921



@oracle_manager
def import_efd_1922(efd_id_1921) -> pd.DataFrame:
    """ 
    Importa os registros 1922    
    Args
    ----------------------
    efd_id_1921: tuple
        Chave estrangeira do registro 1921
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1922': int,
        'INFO_ID_1921': int,
        'NUM_DA': str,
        'NUM_PROC': str,
        'IND_PROC': str,
        'PROC': str,
        'TXT_COMPL': str
    }

    assert len(efd_id_1921),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1921)}"

    assert isinstance(efd_id_1921, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1921)}"

    # SQL query to fetch 1922 data
    query = f""" 
                SELECT
                    INFO_ID_1922,
                    INFO_ID_1921,
                    INFO_NR_NUM_DA1922 AS "NUM_DA",
                    INFO_NR_NUM_PROC1922 AS "NUM_PROC",
                    INFO_TP_IND_PROC1922 AS "IND_PROC",
                    INFO_DS_PROC1922 AS "PROC",
                    INFO_DS_TXT_COMPL1922 AS "TXT_COMPL"
                FROM
                    EFD.TB_EFD_INFORMACAO1922
                WHERE
                    INFO_ID_1921 in {efd_id_1921}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1922 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1922



@oracle_manager
def import_efd_1923(efd_id_1921) -> pd.DataFrame:
    """ 
    Importa os registros 1923    
    Args
    ----------------------
    efd_id_1921: tuple
        Chave estrangeira do registro 1921
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1923': int,
        'INFO_ID_1921': int,
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

    assert len(efd_id_1921),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1921)}"

    assert isinstance(efd_id_1921, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1921)}"

    # SQL query to fetch 1923 data
    query = f""" 
                SELECT
                    INFO_ID_1923,
                    INFO_ID_1921,
                    INFO_CD_COD_PART1923 AS "COD_PART",
                    INFO_CD_COD_MOD1923 AS "COD_MOD",
                    INFO_CD_SER1923 AS "SER",
                    INFO_NR_SUB1923 AS "SUB",
                    INFO_NR_NUM_DOC1923 AS "NUM_DOC",
                    INFO_DT_DOC1923 AS "DT_DOC",
                    INFO_CD_COD_ITEM1923 AS "COD_ITEM",
                    INFO_VL_AJ_ITEM1923 AS "VL_AJ_ITEM",
                    INFO_CD_CHV_DOCe1923 AS "CHV_DOCe"
                FROM
                    EFD.TB_EFD_INFORMACAO1923
                WHERE
                    INFO_ID_1921 in {efd_id_1921}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1923 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1923



@oracle_manager
def import_efd_1925(efd_id_1920) -> pd.DataFrame:
    """ 
    Importa os registros 1925    
    Args
    ----------------------
    efd_id_1920: tuple
        Chave estrangeira do registro 1920
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1925': int,
        'INFO_ID_1920': int,
        'COD_INF_ADIC': str,
        'VL_INF_ADIC': float,
        'DESCR_COMPL_AJ': str
    }

    assert len(efd_id_1920),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1920)}"

    assert isinstance(efd_id_1920, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1920)}"

    # SQL query to fetch 1925 data
    query = f""" 
                SELECT
                    INFO_ID_1925,
                    INFO_ID_1920,
                    INFO_CD_COD_INF_ADIC1925 AS "COD_INF_ADIC",
                    INFO_VL_INF_ADIC1925 AS "VL_INF_ADIC",
                    INFO_DS_DESCR_COMPL_AJ1925 AS "DESCR_COMPL_AJ"
                FROM
                    EFD.TB_EFD_INFORMACAO1925
                WHERE
                    INFO_ID_1920 in {efd_id_1920}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1925 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1925


@oracle_manager
def import_efd_1926(efd_id_1920) -> pd.DataFrame:
    """ 
    Importa os registros 1926    
    Args
    ----------------------
    efd_id_1920: tuple
        Chave estrangeira do registro 1920
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1926': int,
        'INFO_ID_1920': int,
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

    assert len(efd_id_1920),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1920)}"

    assert isinstance(efd_id_1920, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1920)}"

    # SQL query to fetch 1926 data
    query = f""" 
                SELECT
                    INFO_ID_1926,
                    INFO_ID_1920,
                    INFO_CD_COD_OR1926 AS "COD_OR",
                    INFO_VL_OR1926 AS "VL_OR",
                    INFO_DT_VCTO1926 AS "DT_VCTO",
                    INFO_CD_COD_REC1926 AS "COD_REC",
                    INFO_NR_NUM_PROC1926 AS "NUM_PROC",
                    INFO_TP_IND_PROC1926 AS "IND_PROC",
                    INFO_DS_PROC1926 AS "PROC",
                    INFO_DS_TXT_COMPL1926 AS "TXT_COMPL",
                    INFO_MES_REF1926 AS "MES_REF"
                FROM
                    EFD.TB_EFD_INFORMACAO1926
                WHERE
                    INFO_ID_1920 in {efd_id_1920}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1926 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1926




@oracle_manager
def import_efd_1960(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1960    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1960': int,
        'INFO_ID_1001': int,
        'CD_IND_AP': float,
        'G1_01': float,
        'G1_02': float,
        'G1_03': float,
        'G1_04': float,
        'G1_05': float,
        'G1_06': float,
        'G1_07': float,
        'G1_08': float,
        'G1_09': float,
        'G1_10': float,
        'G1_11': float
    }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1960 data
    query = f""" 
                SELECT
                    INFO_ID_1960,
                    INFO_ID_1001,
                    INFO_CD_IND_AP AS "CD_IND_AP",
                    INFO_VL_G1_01_1960 AS "G1_01",
                    INFO_VL_G1_02_1960 AS "G1_02",
                    INFO_VL_G1_03_1960 AS "G1_03",
                    INFO_VL_G1_04_1960 AS "G1_04",
                    INFO_VL_G1_05_1960 AS "G1_05",
                    INFO_VL_G1_06_1960 AS "G1_06",
                    INFO_VL_G1_07_1960 AS "G1_07",
                    INFO_VL_G1_08_1960 AS "G1_08",
                    INFO_VL_G1_09_1960 AS "G1_09",
                    INFO_VL_G1_10_1960 AS "G1_10",
                    INFO_VL_G1_11_1960 AS "G1_11"
                FROM
                    EFD.TB_EFD_INFORMACAO1960
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1960 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1960



@oracle_manager
def import_efd_1970(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1970    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1970': int,
        'INFO_ID_1001': int,
        'IND_AP': float,
        'G3_01': float,
        'G3_02': float,
        'G3_03': float,
        'G3_04': float,
        'G3_05': float,
        'G3_06': float,
        'G3_07': float,
        'G3_08': float,
        'G3_09': float,
        'G3_T': float
    }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1970 data
    query = f""" 
                SELECT
                    INFO_ID_1970,
                    INFO_ID_1001,
                    INFO_CD_IND_AP1970 AS "IND_AP",
                    INFO_NR_G3_01_1970 AS "G3_01",
                    INFO_NR_G3_02_1970 AS "G3_02",
                    INFO_NR_G3_03_1970 AS "G3_03",
                    INFO_NR_G3_04_1970 AS "G3_04",
                    INFO_NR_G3_05_1970 AS "G3_05",
                    INFO_NR_G3_06_1970 AS "G3_06",
                    INFO_NR_G3_07_1970 AS "G3_07",
                    INFO_NR_G3_T_1970 AS "G3_T",
                    INFO_NR_G3_08_1970 AS "G3_08",
                    INFO_NR_G3_09_1970 AS "G3_09"
                FROM
                    EFD.TB_EFD_INFORMACAO1970
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1970 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1970


@oracle_manager
def import_efd_1975(efd_id_1970) -> pd.DataFrame:
    """ 
    Importa os registros 1975    
    Args
    ----------------------
    efd_id_1970: tuple
        Chave estrangeira do registro 1970
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1975': int,
        'INFO_ID_1970': int,
        'ALIQ_IMP_BASE': float,
        'G3_10': float,
        'G3_11': float,
        'G3_12': float
    }

    assert len(efd_id_1970),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1970)}"

    assert isinstance(efd_id_1970, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1970)}"

    # SQL query to fetch 1975 data
    query = f""" 
                SELECT
                    INFO_ID_1975,
                    INFO_ID_1970,
                    INFO_VL_ALIQ_IMP_BASE_1975 AS "ALIQ_IMP_BASE",
                    INFO_VL_G3_10_1975 AS "G3_10",
                    INFO_VL_G3_11_1975 AS "G3_11",
                    INFO_VL_G3_12_1975 AS "G3_12"
                FROM
                    EFD.TB_EFD_INFORMACAO1975
                WHERE
                    INFO_ID_1970 in {efd_id_1970}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1975 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1975


@oracle_manager
def import_efd_1980(efd_id_1001) -> pd.DataFrame:
    """ 
    Importa os registros 1980    
    Args
    ----------------------
    efd_id_1001: tuple
        Chave estrangeira do registro 1001
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1980': int,
        'INFO_ID_1001': int,
        'IND_AP': float,
        'G4_01': float,
        'G4_02': float,
        'G4_03': float,
        'G4_04': float,
        'G4_05': float,
        'G4_06': float,
        'G4_07': float,
        'G4_08': float,
        'G4_09': float,
        'G4_10': float,
        'G4_11': float,
        'G4_12': float
    }

    assert len(efd_id_1001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_1001)}"

    assert isinstance(efd_id_1001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_1001)}"

    # SQL query to fetch 1980 data
    query = f""" 
                SELECT
                    INFO_ID_1980,
                    INFO_ID_1001,
                    INFO_CD_IND_AP_1980 AS "IND_AP",
                    INFO_VL_G4_01_1980 AS "G4_01",
                    INFO_VL_G4_02_1980 AS "G4_02",
                    INFO_VL_G4_03_1980 AS "G4_03",
                    INFO_VL_G4_04_1980 AS "G4_04",
                    INFO_VL_G4_05_1980 AS "G4_05",
                    INFO_VL_G4_06_1980 AS "G4_06",
                    INFO_VL_G4_07_1980 AS "G4_07",
                    INFO_VL_G4_08_1980 AS "G4_08",
                    INFO_VL_G4_09_1980 AS "G4_09",
                    INFO_VL_G4_10_1980 AS "G4_10",
                    INFO_VL_G4_11_1980 AS "G4_11",
                    INFO_VL_G4_12_1980 AS "G4_12"
                FROM
                    EFD.TB_EFD_INFORMACAO1980
                WHERE
                    INFO_ID_1001 in {efd_id_1001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1980 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1980


@oracle_manager
def import_efd_1990(cabe_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros 1990    
    Args
    ----------------------
    cabe_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {
        'INFO_ID_1990': int,
        'CABE_ID_0000': int,
        'QTD_LIN_1': float
    }

    assert len(cabe_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(cabe_id_0000)}"

    assert isinstance(cabe_id_0000, tuple), f"The cabe_id input isn't a tuple! Check the input! {type(cabe_id_0000)}"

    # SQL query to fetch 1990 data
    query = f""" 
                SELECT
                    INFO_ID_1990,
                    CABE_ID_0000,
                    INFO_QT_QTD_LIN_1 AS "QTD_LIN_1"
                FROM
                    EFD.TB_EFD_INFORMACAO1990
                WHERE
                    CABE_ID_0000 in {cabe_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_1990 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_1990


