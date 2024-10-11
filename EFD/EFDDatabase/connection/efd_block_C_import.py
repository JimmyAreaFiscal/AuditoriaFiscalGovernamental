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
def import_efd_C001(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros C001    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """


    # Creating SQL query
    columns = {         
                        'CABE_ID_0000': int,
                        'MERC_ID_001': int,
                        'IND_MOV': str

            }


    assert len(efd_id_0000),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                SELECT
                    CABE_ID_0000,
                    MERC_ID_001,
                    MERC_TP_IND_MOV AS "IND_MOV"



                FROM
                    EFD.TB_EFD_MERCADORIAC001
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
                    """

    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C001 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_C001

@oracle_manager
def import_efd_C100(efd_id_C001, nfe_only = False) -> pd.DataFrame:
    """ 
    Função para importar C100
    
    Args
    ----------------------
    efd_id_C001: tuple
        Chave estrangeira do C001

    nfe_only: boolean
        Quando ativado, busca apenas as NF-e de modelo 55. Padrão: False

    """
    # Creating SQL query
    columns = {         
                        'MERC_ID_001': int,
                        'MERC_ID_100': int,
                        'IND_OPER': str,
                        'IND_EMIT': str,
                        'COD_PART': str,
                        'COD_MOD': str,
                        'COD_SIT': str,
                        'SER': str,
                        'NUM_DOC': str,
                        'CHV_NFE': str,
                        'VL_DOC': float,
                        'IND_PGTO': str,
                        'VL_DESC': float,
                        'VL_ABAT_NT': float,
                        'VL_MERC': float,
                        'IND_FRT': str,
                        'VL_FRT': float,
                        'VL_SEG': float,
                        'VL_OUT_DA': float,
                        'VL_BC_ICMS': float,
                        'VL_ICMS': float,
                        'VL_BC_ICMS_ST': float,
                        'VL_ICMS_ST': float,
                        'VL_IPI': float,
                        'VL_PIS': float,
                        'VL_COFINS': float,
                        'VL_PIS_ST': float,
                        'VL_COFINS_ST': float
            }


    assert len(efd_id_C001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C001)}"

    assert isinstance(efd_id_C001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    
    query = f""" 
                SELECT
                    MERC_ID_001,
                    MERC_ID_100,
                    MERC_TP_IND_OPER AS "IND_OPER",
                    MERC_TP_IND_EMIT AS "IND_EMIT",
                    MERC_CD_COD_PART AS "COD_PART",
                    MERC_CD_COD_MOD AS "COD_MOD",
                    MERC_CD_COD_SIT AS "COD_SIT",
                    MERC_CD_SER AS "SER",
                    MERC_NR_NUM_DOC AS "NUM_DOC",
                    MERC_NR_CHV_NFE AS "CHV_NFE",
                    MERC_VL_DOC AS "VL_DOC",
                    MERC_IND_PGTO AS "IND_PGTO",
                    MERC_VL_DESC AS "VL_DESC",
                    MERC_VL_ABAT_NT AS "VL_ABAT_NT",
                    MERC_VL_MERC AS "VL_MERC",
                    MERC_TP_IND_FRT AS "IND_FRT",
                    MERC_VL_FRT AS "VL_FRT",
                    MERC_VL_SEG AS "VL_SEG",
                    MERC_VL_OUT_DA AS "VL_OUT_DA",
                    MERC_VL_BC_ICMS AS "VL_BC_ICMS",
                    MERC_VL_ICMS AS "VL_ICMS",
                    MERC_VL_BC_ICMS_ST AS "VL_BC_ICMS_ST",
                    MERC_VL_ICMS_ST AS "VL_ICMS_ST",
                    MERC_DT_DOC AS "DT_DOC",
                    MERC_DT_E_S AS "DT_E_S",
                    MERC_VL_IPI AS "VL_IPI", 
                    MERC_VL_PIS AS "VL_PIS",
                    MERC_VL_COFINS AS "VL_COFINS",
                    MERC_VL_PIS_ST AS "VL_PIS_ST",
                    MERC_VL_COFINS_ST AS "VL_COFINS_ST"
                FROM
                    EFD.TB_EFD_MERCADORIAC100
                WHERE
                    MERC_ID_001 in {efd_id_C001}
                    """
    if nfe_only:
        query += " AND MERC_CD_COD_MOD  = '55'"
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C100 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_C100

@oracle_manager
def import_efd_C101(efd_id_C100) -> pd.DataFrame:
    """ 
    Função para importar C101
    
    Args
    ----------------------
    efd_id_C100: tuple
        Chave estrangeira do registro C100

    """

    # Creating SQL query
    columns = {  
                'MERC_ID_100': int,     
                'MERC_ID_101': int,
                'VL_FCP_UF_DEST': float,
                'VL_ICMS_UF_DEST': float,
                'VL_ICMS_UF_REM': float,
                }


    assert len(efd_id_C100) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C100)}"

    assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        MERC_ID_100,
                        MERC_ID_101,
                        MERC_VL_FCP_UF_DEST AS "VL_FCP_UF_DEST", 
                        MERC_VL_ICMS_UF_DEST AS "VL_ICMS_UF_DEST",
                        MERC_VL_ICMS_UF_REM AS "VL_ICMS_UF_REM"

                    FROM
                        EFD.TB_EFD_MERCADORIAC101
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
                        
                        """
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C101 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_C101

@oracle_manager
def import_efd_C105(efd_id_C100) -> pd.DataFrame:
    """ 
    Função para importar C105
    
    Args
    ----------------------
    efd_id_C100: tuple
        Chave estrangeira do registro C100

    """

    # Creating SQL query
    columns = {  
                'MERC_ID_100': int,     
                'MERC_ID_105': int,
                'OPER': str,
                'UF': str
                }


    assert len(efd_id_C100) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C100)}"

    assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        MERC_ID_100,
                        MERC_ID_105,
                        MERC_TP_OPER105 AS "OPER", 
                        MERC_SG_UF105 AS "UF"

                    FROM
                        EFD.TB_EFD_MERCADORIAC105
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
                        
                        """
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C105 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_C105

@oracle_manager
def import_efd_C110(efd_id_C100) -> pd.DataFrame:
    """ 
    Function to import the register C110, having all extra informations from invoices declared.
    
    Args
    ----------------------
    efd_id_C100: tuple
        Refers to list of MERC_ID_100 from EFD to import.
        It shouldn't be given more than 1.000 ID to be searched for.
    
    Return
    ----------------------
    efd_C110 pandas.DataFrame
        Dataframe containing the register C112, with the following informations:
            1. MERC_CD_COD_INF
            2. MERC_DS_TXT_COMPL

    """

    # Creating SQL query
    columns = {  
                'MERC_ID_100': int,     
                'MERC_ID_110': int,
                'COD_INF': str,
                'TXT_COMPL': str
                }


    assert len(efd_id_C100) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C100)}"

    assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        MERC_ID_100,
                        MERC_ID_110,
                        MERC_CD_COD_INF AS "COD_INF", 
                        MERC_DS_TXT_COMPL AS "TXT_COMPL"

                    FROM
                        EFD.TB_EFD_MERCADORIAC110
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
                        
                        """
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C110 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_C110

@oracle_manager
def import_efd_C111(efd_id_C110) -> pd.DataFrame:
    """ 
    Função para importar C111
    
    Args
    ----------------------
    efd_id_C110: tuple
        Chave estrangeira do registro C110
    
    """

    # Creating SQL query
    columns = {    
                'MERC_ID_110': int,    
                'MERC_ID_111': int,
                'NUM_PROC': str,
                'IND_PROC': str
                }


    assert len(efd_id_C110) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C110)}"

    assert isinstance(efd_id_C110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C110)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        MERC_ID_110,
                        MERC_ID_111,
                        MERC_NR_NUM_PROC AS "NUM_PROC",
                        MERC_TP_IND_PROC AS "IND_PROC"


                    FROM
                        EFD.TB_EFD_MERCADORIAC111
                    WHERE
                        MERC_ID_110 in {efd_id_C110}
                        
                        """
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C111 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_C111

@oracle_manager
def import_efd_C112(efd_id_C110) -> pd.DataFrame:
    """ 
    Função para importar o registro C112
    
    Args
    ----------------------
    efd_id_C110: tuple
        Chave estrangeira do registro C110
    
    """

    # Creating SQL query
    columns = {    
                'MERC_ID_110': int,   
                'MERC_ID_112': int,
                 
                'COD_DA': str,
                'UF': str,
                'NUM_DA': str,
                'COD_AUT': str,
                'VL_DA': float
                }

    date_columns = ['DT_VCTO', 'DT_PGTO']

    assert len(efd_id_C110) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C110)}"

    assert isinstance(efd_id_C110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C110)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        MERC_ID_110,
                        MERC_ID_112,
                        MERC_TP_COD_DA112 AS "COD_DA", 
                        MERC_SG_UF112 AS "UF",
                        MERC_NR_NUM_DA112 AS "NUM_DA",
                        MERC_CD_COD_AUT112 AS "COD_AUT", 
                        MERC_VL_DA112 AS "VL_DA", 
                        MERC_DT_VCTO112 AS "DT_VCTO",
                        MERC_DT_PGTO112 AS "DT_PGTO"

                    FROM
                        EFD.TB_EFD_MERCADORIAC112
                    WHERE
                        MERC_ID_110 in {efd_id_C110}
                        
                        """
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C112 = pd.read_sql_query(query, con = connection, dtype = columns)

    for column in date_columns:
        efd_C112[column] = pd.to_datetime(efd_C112[column])

    return efd_C112

@oracle_manager
def import_efd_C113(efd_id_C110) -> pd.DataFrame:
    """ 
    Função para importar registro C113.
    
    Args
    ----------------------
    efd_id_C110: tuple
        Chave estrangeira do registro C110

    """

    # Creating SQL query
    columns = {    
                'MERC_ID_110': int,   
                'MERC_ID_113': int,
                'IND_OPER': str,
                'IND_EMIT': str,
                'COD_PART': str,
                'COD_MOD': str,
                'SER': str,
                'SUB': str,
                'NUM_DOC': str,
                'CHV_DOC': str
                }


    assert len(efd_id_C110) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C110)}"

    assert isinstance(efd_id_C110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C110)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        MERC_ID_110,
                        MERC_ID_113,
                        MERC_TP_IND_OPER113 AS "IND_OPER",
                        MERC_TP_IND_EMIT113 AS "IND_EMIT",
                        MERC_CD_COD_PART113 AS "COD_PART",
                        MERC_CD_COD_MOD113 AS "COD_MOD",
                        MERC_CD_SER113 AS "SER",
                        MERC_NR_SUB113 AS "SUB",
                        MERC_NR_NUM_DOC113 AS "NUM_DOC",
                        MERC_DT_DOC113 AS "DT_DOC",
                        MERC_CD_CHV_DOCE AS "CHV_DOC"

                    FROM
                        EFD.TB_EFD_MERCADORIAC113
                    WHERE
                        MERC_ID_110 in {efd_id_C110}
                        
                        """
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C113 = pd.read_sql_query(query, con = connection, dtype = columns)

    efd_C113['DT_DOC'] = pd.to_datetime(efd_C113['DT_DOC'])

    return efd_C113

@oracle_manager
def import_efd_C114(efd_id_C110) -> pd.DataFrame:
    """ 
    Função para importar registro C114.
    
    Args
    ----------------------
    efd_id_C110: tuple
        Chave estrangeira do registro C110

    """

    # Defining the columns with the expected data types
    columns = {    
                'MERC_ID_114': str,   
                'MERC_ID_110': str,
                'COD_MOD': str,
                'NR_ECF_FAB': str,
                'NR_ECF_CX': str,
                'NR_NUM_DOC': str
                }

    # Assert checks for input size and type
    assert len(efd_id_C110) <= 1000,\
        f"Limit exceeded! Check the input! Length should not be greater than 1,000. Actual length = {len(efd_id_C110)}"
    assert isinstance(efd_id_C110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C110)}"

    # Query to fetch the data from the database
    query = f""" 
                    SELECT
                        MERC_ID_114,
                        MERC_ID_110,
                        MERC_CD_COD_MOD114 AS "COD_MOD",
                        MERC_NR_ECF_FAB114 AS "NR_ECF_FAB",
                        MERC_NR_ECF_CX114 AS "NR_ECF_CX",
                        MERC_NR_NUM_DOC114 AS "NR_NUM_DOC",
                        MERC_DT_DOC114 AS "DT_DOC"
                    FROM
                        EFD.TB_EFD_MERCADORIAC114
                    WHERE
                        MERC_ID_110 in {efd_id_C110}
                        
                        """
    
    # Connecting to the Oracle database and executing the query
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C114 = pd.read_sql_query(query, con=connection, dtype=columns)

    # Converting the DT_DOC field to datetime format
    efd_C114['DT_DOC'] = pd.to_datetime(efd_C114['DT_DOC'])
    
    return efd_C114

@oracle_manager
def import_efd_C115(efd_id_C110) -> pd.DataFrame:
    """
    Função para importar registro C115.
    
    Args
    ----------------------
    efd_id_C110: tuple
        Chave estrangeira do registro C110
    """
    
    # Defining columns and expected data types
    columns = {    
                'MERC_ID_115': int,   
                'MERC_ID_110': int,
                'TP_IND_CARGA': str,
                'NR_CNPJ_COL': str,
                'NR_IE_COL': str,
                'NR_CPF_COL': str,
                'COD_MUN_COL': str,
                'NR_CNPJ_ENTG': str,
                'NR_IE_ENTG': str,
                'NR_CPF_ENTG': str,
                'COD_MUN_ENTG': str
                }

    assert len(efd_id_C110) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C110, tuple), f"Input is not a tuple: {type(efd_id_C110)}"
    
    query = f"""
                    SELECT
                        MERC_ID_115,
                        MERC_ID_110,
                        MERC_TP_IND_CARGA AS "TP_IND_CARGA",
                        MERC_NR_CNPJ_COL AS "NR_CNPJ_COL",
                        MERC_NR_IE_COL AS "NR_IE_COL",
                        MERC_NR_CPF_COL AS "NR_CPF_COL",
                        MERC_CD_COD_MUN_COL AS "COD_MUN_COL",
                        MERC_NR_CNPJ_ENTG AS "NR_CNPJ_ENTG",
                        MERC_NR_IE_ENTG AS "NR_IE_ENTG",
                        MERC_NR_CPF_ENTG AS "NR_CPF_ENTG",
                        MERC_CD_COD_MUN_ENTG AS "COD_MUN_ENTG"
                    FROM
                        EFD.TB_EFD_MERCADORIAC115
                    WHERE
                        MERC_ID_110 in {efd_id_C110}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C115 = pd.read_sql_query(query, con=connection, dtype=columns)
    
    return efd_C115

@oracle_manager
def import_efd_C116(efd_id_C110) -> pd.DataFrame:
    """
    Função para importar registro C116.
    
    Args
    ----------------------
    efd_id_C110: tuple
        Chave estrangeira do registro C110
    """
    
    # Defining columns and expected data types
    columns = {    
                'MERC_ID_116': str,   
                'MERC_ID_110': str,
                'COD_MOD': str,
                'NR_SAT': str,
                'NR_CHV_CFE': str,
                'NR_NUM_CFE': str,
                'DT_DOC': str
                }

    assert len(efd_id_C110) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C110, tuple), f"Input is not a tuple: {type(efd_id_C110)}"
    
    query = f"""
                    SELECT
                        MERC_ID_116,
                        MERC_ID_110,
                        MERC_CD_COD_MOD116 AS "COD_MOD",
                        MERC_NR_SAT116 AS "NR_SAT",
                        MERC_NR_CHV_CFE116 AS "NR_CHV_CFE",
                        MERC_NR_NUM_CFE116 AS "NR_NUM_CFE",
                        MERC_DT_DOC116 AS "DT_DOC"
                    FROM
                        EFD.TB_EFD_MERCADORIAC116
                    WHERE
                        MERC_ID_110 in {efd_id_C110}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C116 = pd.read_sql_query(query, con=connection, dtype=columns)

    efd_C116['DT_DOC'] = pd.to_datetime(efd_C116['DT_DOC'])
    
    return efd_C116

@oracle_manager
def import_efd_C120(efd_id_C100) -> pd.DataFrame:
    """
    Função para importar registro C120.
    
    Args
    ----------------------
    efd_id_C100: tuple
        Chave estrangeira do registro C100
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_120': int,   
                'MERC_ID_100': int,
                'COD_DOC_IMP': str,
                'NUM_DOC_IMP': str,
                'VL_PIS_IMP': float,
                'VL_COFINS_IMP': float,
                'NUM_ACDRAW': str
                }

    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"
    
    query = f"""
                    SELECT
                        MERC_ID_120,
                        MERC_ID_100,
                        MERC_CD_COD_DOC_IMP120 AS "COD_DOC_IMP",
                        MERC_NR_NUM_DOC__IMP120 AS "NUM_DOC_IMP",
                        MERC_VL_PIS_IMP120 AS "VL_PIS_IMP",
                        MERC_VL_COFINS_IMP120 AS "VL_COFINS_IMP",
                        MERC_NR_NUM_ACDRAW120 AS "NUM_ACDRAW"
                    FROM
                        EFD.TB_EFD_MERCADORIAC120
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C120 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C120

@oracle_manager
def import_efd_C130(efd_id_C100) -> pd.DataFrame:
    """
    Função para importar registro C130.
    
    Args
    ----------------------
    efd_id_C100: tuple
        Chave estrangeira do registro C100
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_130': int,   
                'MERC_ID_100': int,
                'VL_SERV_NT': float,
                'VL_BC_ISSQN': float,
                'VL_ISSQN': float,
                'VL_BC_IRRF': float,
                'VL_IRRF': float,
                'VL_BC_PREV': float,
                'VL_PREV': float
                }

    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"
    
    query = f"""
                    SELECT
                        MERC_ID_130,
                        MERC_ID_100,
                        MERC_VL_SERV_NT AS "VL_SERV_NT",
                        MERC_VL_BC_ISSQN AS "VL_BC_ISSQN",
                        MERC_VL_ISSQN AS "VL_ISSQN",
                        MERC_VL_BC_IRRF AS "VL_BC_IRRF",
                        MERC_VL_IRRF AS "VL_IRRF",
                        MERC_VL_BC_PREV AS "VL_BC_PREV",
                        MERC_VL_PREV AS "VL_PREV"
                    FROM
                        EFD.TB_EFD_MERCADORIAC130
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C130 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C130

@oracle_manager
def import_efd_C160(efd_id_C100) -> pd.DataFrame:
    """
    Função para importar registro C160.
    
    Args
    ----------------------
    efd_id_C100: tuple
        Chave estrangeira do registro C100
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_160': int,   
                'MERC_ID_100': int,
                'COD_PART': str,
                'VEIC_ID': str,
                'QTD_VOL': int,
                'PESO_BRT': float,
                'PESO_LIQ': float,
                'UF_ID': str
                }

    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"
    
    query = f"""
                    SELECT
                        MERC_ID_160,
                        MERC_ID_100,
                        MERC_CD_COD_PART160 AS "COD_PART",
                        MERC_DS_VEIC_ID160 AS "VEIC_ID",
                        MERC_QT_QTD_VOL160 AS "QTD_VOL",
                        MERC_MD_PESO_BRT160 AS "PESO_BRT",
                        MERC_MD_PESO_LIQ160 AS "PESO_LIQ",
                        MERC_SG_UF_ID160 AS "UF_ID"
                    FROM
                        EFD.TB_EFD_MERCADORIAC160
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C160 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C160

@oracle_manager
def import_efd_C165(efd_id_C100) -> pd.DataFrame:
    """
    Função para importar registro C165.
    
    Args
    ----------------------
    efd_id_C100: tuple
        Chave estrangeira do registro C100
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_165': int,   
                'MERC_ID_100': int,
                'COD_PART': str,
                'VEIC_ID': str,
                'COD_AUT': str,
                'NR_PASSE': str,
                'HR_HORA': str,
                'NR_TEMPER': str,
                'QTD_VOL': int,
                'PESO_BRT': float,
                'PESO_LIQ': float,
                'CPF': str,
                'NOM_MOT': str,
                'UF_ID': str
                }

    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"
    
    query = f"""
                    SELECT
                        MERC_ID_165,
                        MERC_ID_100,
                        MERC_CD_COD_PART165 AS "COD_PART",
                        MERC_DS_VEIC_ID165 AS "VEIC_ID",
                        MERC_CD_COD_AUT165 AS "COD_AUT",
                        MERC_NR_PASSE165 AS "NR_PASSE",
                        MERC_HR_HORA165 AS "HR_HORA",
                        MERC_NR_TEMPER165 AS "NR_TEMPER",
                        MERC_QT_QTD_VOL165 AS "QTD_VOL",
                        MERC_MD_PESO_BRT165 AS "PESO_BRT",
                        MERC_MD_PESO_LIQ165 AS "PESO_LIQ",
                        MERC_NR_CPF165 AS "CPF",
                        MERC_NM_NOM_MOT165 AS "NOM_MOT",
                        MERC_SG_UF_ID165 AS "UF_ID"
                    FROM
                        EFD.TB_EFD_MERCADORIAC165
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C165 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C165

@oracle_manager
def import_efd_C170(efd_id_C100) -> pd.DataFrame:
    """
    Função para importar registro C170.
    
    Args
    ----------------------
    efd_id_C100: tuple
        Chave estrangeira do registro C100
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_170': int,   
                'MERC_ID_100': int,
                'NUM_ITEM': str,
                'COD_ITEM': str,
                'DESCR_COMPL': str,
                'QTD': float,
                'UNID': str,
                'VL_ITEM': float,
                'VL_DESC': float,
                'IND_MOV': str,
                'CST_ICMS': str,
                'CFOP': str,
                'COD_NAT': str,
                'VL_BC_ICMS': float,
                'ALIQ_ICMS': float,
                'VL_ICMS': float,
                'VL_BC_ICMS_ST': float,
                'ALIQ_ST': float,
                'VL_ICMS_ST': float,
                'IND_APUR': str,

                'CST_IPI': str,
                'COD_ENQ': str,
                'VL_BC_IPI': float,


                'ALIQ_IPI': float,
                'VL_IPI': float,


                'CST_PIS': str,


                'VL_BC_PIS': float,
                
                'ALIQ_PIS': float,
                'QUANT_BC_PIS': float,
                'ALIQ_PIS_R': float, 


                'VL_PIS': float,
                'CST_COFINS': str,
                'VL_BC_COFINS': float,
                'ALIQ_COFINS': float,
                'QUANT_BC_COFINS': float,
                'ALIQ_COFINS_R': float, 

                'VL_COFINS': float,
                'COD_CTA': str,
                'VL_ABAT_NT': float
                }

    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"
    
    query = f"""
                    SELECT
                        MERC_ID_170,
                        MERC_ID_100,
                        MERC_NR_NUM_ITEM170 AS "NUM_ITEM",
                        MERC_CD_COD_ITEM170 AS "COD_ITEM",
                        MERC_DS_DESCR_COMPL170 AS "DESCR_COMPL",
                        MERC_QT_QTD170 AS "QTD",
                        MERC_IN_UNID170 AS "UNID",
                        MERC_VL_ITEM170 AS "VL_ITEM",
                        MERC_VL_DESC170 AS "VL_DESC",
                        MERC_TP_IND_MOV170 AS "IND_MOV",
                        MERC_CD_CST_ICMS170 AS "CST_ICMS",
                        MERC_CD_CFOP170 AS "CFOP",
                        MERC_CD_COD_NAT170 AS "COD_NAT",
                        MERC_VL_BC_ICMS170 AS "VL_BC_ICMS",
                        MERC_VL_ALIQ_ICMS170 AS "ALIQ_ICMS",
                        MERC_VL_ICMS170 AS "VL_ICMS",
                        MERC_VL_BC_ICMS_ST170 AS "VL_BC_ICMS_ST",
                        MERC_VL_ALIQ_ST170 AS "ALIQ_ST",
                        MERC_VL_ICMS_ST170 AS "VL_ICMS_ST",
                        MERC_TP_IND_APUR170 AS "IND_APUR",
                        MERC_CD_CST_IPI170 AS "CST_IPI",
                        MERC_CD_COD_ENQ170 AS "COD_ENQ",
                        MERC_VL_BC_IPI170 AS "VL_BC_IPI",
                        MERC_VL_ALIQ_IPI170 AS "ALIQ_IPI",
                        MERC_VL_IPI170 AS "VL_IPI",
                        MERC_CD_CST_PIS170 AS "CST_PIS",
                        MERC_VL_BC_PIS170 AS "VL_BC_PIS",
                        MERC_PC_ALIQ_PIS170 AS "ALIQ_PIS",
                        MERC_QT_QUANT_BC_PIS170 AS "QUANT_BC_PIS",
                        MERC_VL_ALIQ_PIS170 AS "ALIQ_PIS_R",
                        MERC_VL_PIS170 AS "VL_PIS",

                        MERC_CD_CST_COFINS170 AS "CST_COFINS",
                        MERC_VL_BC_COFINS170 AS "VL_BC_COFINS",
                        MERC_PC_ALIQ_COFINS170 AS "ALIQ_COFINS",
                        MERC_QT_QUANT_BC_COFINS170 AS "QUANT_BC_COFINS",
                        MERC_VL_ALIQ_COFINS170 AS "ALIQ_COFINS_R",
                        MERC_VL_COFINS170 AS "VL_COFINS",
                        MERC_CD_COD_CTA170 AS "COD_CTA",
                        MERC_VL_ABAT_NT170 AS "VL_ABAT_NT"
                    FROM
                        EFD.TB_EFD_MERCADORIAC170
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C170 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C170

@oracle_manager
def import_efd_C171(efd_id_C170) -> pd.DataFrame:
    """ 
    Função para extrair C171
    

    Args
    ----------------------
    efd_id_C170: tuple
        Chave estrangeira do registro C170



    """
    # Creating SQL query
    columns = {    
                    'MERC_ID_170': int,   
                    'MERC_ID_171': int,
                    'NUM_TANQUE': str,
                    'QTDE': float,
                }


    assert len(efd_id_C170) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C170)}"

    assert isinstance(efd_id_C170, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C170)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        
                        MERC_ID_170,
                        MERC_ID_171,
                        MERC_NR_NUM_TANQUE AS NUM_TANQUE,
                        MERC_QT_QTDE AS QTDE


                    FROM
                        EFD.TB_EFD_MERCADORIAC171
                    WHERE
                        MERC_ID_170 in {efd_id_C170}
  
                        """
   
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C171 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_C171

@oracle_manager
def import_efd_C172(efd_id_C170) -> pd.DataFrame:
    """
    Função para importar registro C172.
    
    Args
    ----------------------
    efd_id_C170: tuple
        Chave estrangeira do registro C170
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_172': int,   
                'MERC_ID_170': int,
                'VL_BC_ISSQN': float,
                'VL_ALIQ_ISSQN': float,
                'VL_ISSQN': float
                }

    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"
    
    query = f"""
                    SELECT
                        MERC_ID_172,
                        MERC_ID_170,
                        MERC_VL_BC_ISSQN172 AS "VL_BC_ISSQN",
                        MERC_VL_ALIQ_ISSQN172 AS "VL_ALIQ_ISSQN",
                        MERC_VL_ISSQN172 AS "VL_ISSQN"
                    FROM
                        EFD.TB_EFD_MERCADORIAC172
                    WHERE
                        MERC_ID_170 in {efd_id_C170}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C172 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C172

@oracle_manager
def import_efd_C173(efd_id_C170) -> pd.DataFrame:
    """ 
    Function to import the register C173, to control meds.
    
    Args
    ----------------------
    efd_id_C170: tuple
        Refers to list of MERC_ID_170 from EFD to import.
        It shouldn't be given more than 1.000 ID to be searched for.
    
            7. VL_TAB_MAX
    """

    # Creating SQL query
    columns = {    
                    'MERC_ID_170': int,   
                    'MERC_ID_173': int,
                    'LOTE_MED': str,
                    'QTD_ITEM': int, 
                    'IND_MED': str,
                    'TP_PROD': str,
                    'VL_TAB_MAX': float
                }

    date_columns = ['DT_FAB', 'DT_VAL']
    assert len(efd_id_C170) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C170)}"

    assert isinstance(efd_id_C170, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C170)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        MERC_ID_170,
                        MERC_ID_173,
                        MERC_NR_LOTE_MED AS "LOTE_MED",
                        MERC_QT_QTD_ITEM AS "QTD_ITEM",
                        MERC_DT_FAB AS "DT_FAB",
                        MERC_DT_VAL AS "DT_VAL",
                        MERC_TP_IND_MED AS "IND_MED",
                        MERC_TP_PROD AS "TP_PROD",
                        MERC_VL_TAB_MAX AS "VL_TAB_MAX"

                    FROM
                        EFD.TB_EFD_MERCADORIAC173
                    WHERE
                        MERC_ID_170 in {efd_id_C170}
  
                        """
   
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C173 = pd.read_sql_query(query, con = connection, dtype = columns)
    for column in date_columns:
        efd_C173[column] = pd.to_datetime(efd_C173[column])

    return efd_C173

@oracle_manager
def import_efd_C174(efd_id_C170) -> pd.DataFrame:
    """ 
    Function to import the register C174, to control guns.
    

    Args
    ----------------------
    efd_id_C170: tuple
        Refers to list of MERC_ID_170 from EFD to import.
        It shouldn't be given more than 1.000 ID to be searched for.
  

    """

    # Creating SQL query
    columns = {    
                    'MERC_ID_170': int,   
                    'MERC_ID_174': int,
                    'IND_ARM': str,
                    'NUM_ARM': str,
                    'DESCR_COMPL': object
                }


    assert len(efd_id_C170) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C170)}"

    assert isinstance(efd_id_C170, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C170)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                       
                        MERC_ID_170,
                        MERC_ID_174,
                        MERC_TP_IND_ARM174 AS "IND_ARM",
                        MERC_NR_NUM_ARM174 AS "NUM_ARM",
                        MERC_DS_DESCR_COMPL174 AS "DESCR_COMPL"


                    FROM
                        EFD.TB_EFD_MERCADORIAC174
                    WHERE
                        MERC_ID_170 in {efd_id_C170}
  
                        """
   
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C174 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_C174

@oracle_manager
def import_efd_C175(efd_id_C170) -> pd.DataFrame:
    """ 
    Function to import the register C175, to control vehicles commerce.
    

    Args
    ----------------------
    efd_id_C170: tuple
        Refers to list of MERC_ID_170 from EFD to import.
        It shouldn't be given more than 1.000 ID to be searched for.
    

    """

    # Creating SQL query
    columns = {    
                    'MERC_ID_170': int,   
                    'MERC_ID_175': int,
                    'IND_VEIC_OPER': str,
                    'CNPJ': str,
                    'UF': str,
                    'CHASSI_VEIC': object
                }

    assert len(efd_id_C170) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C170)}"

    assert isinstance(efd_id_C170, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C170)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT     
                        MERC_ID_170,
                        MERC_ID_175,
                        MERC_TP_IND_VEIC_OPER175 AS "IND_VEIC_OPER",
                        MERC_NR_CNPJ175 AS "CNPJ",
                        MERC_SG_UF175 AS "UF",
                        MERC_CD_CHASSI_VEIC175 AS "CHASSI_VEIC"

                    FROM
                        EFD.TB_EFD_MERCADORIAC175
                    WHERE
                        MERC_ID_170 in {efd_id_C170}
  
                        """
   
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C175 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_C175

@oracle_manager
def import_efd_C176(efd_id_C170) -> pd.DataFrame:
    """
    Função para importar registro C176.
    
    Args
    ----------------------
    efd_id_C170: tuple
        Chave estrangeira do registro C170
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_176': int,   
                'MERC_ID_170': int,
                'COD_MOD_ULT_E': str,
                'NUM_DOC_ULT_E': str,
                'NR_SER_ULT_E': str,
                'DT_ULT_E': str,
                'COD_PART_ULT_E': str,
                'QUANT_ULT_E': float,
                'VL_UNIT_BC_ST': float,
                'CHAVE_NFE_ULT_E': str,
                'NUM_ITEM_ULT_E': str,
                'VL_UNIT_BC_ICMS_ULT_E': float,
                'VL_UNIT_LIMBICICMS_ULT_E': float,
                'VL_UNIT_ICMS_ULT_E': float,
                'ALIQ_ST_ULT_E': float,
                'VL_UNIT_RES': float,
                'COD_RESP_RET': str,
                'COD_MOT_RES': str,
                'CHAVE_NFE_RET': str,
                'COD_PART_NFE_RET': str,
                'NR_SER_RET': str,
                'NUM_NFE_RET': str,
                'NUM_ITEM_NFE_RET': str,
                'COD_DA': str,
                'NUM_DA': str,
                'VL_UNIT_RES_FCP_ST': float
                }

    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"
    
    query = f"""
                    SELECT
                        MERC_ID_176,
                        MERC_ID_170,
                        MERC_CD_COD_MOD_ULT_E AS "COD_MOD_ULT_E",
                        MERC_NR_NUM_DOC_ULT_E AS "NUM_DOC_ULT_E",
                        MERC_NR_SER_ULT_E AS "NR_SER_ULT_E",
                        MERC_DT_ULT_E AS "DT_ULT_E",
                        MERC_CD_COD_PART_ULT_E AS "COD_PART_ULT_E",
                        MERC_QT_QUANT_ULT_E AS "QUANT_ULT_E",
                        MERC_VL_UNIT_BC_ST AS "VL_UNIT_BC_ST",
                        MERC_CD_CHAVE_NFE_ULT_E AS "CHAVE_NFE_ULT_E",
                        MERC_NR_NUM_ITEM_ULT_E AS "NUM_ITEM_ULT_E",
                        MERC_VL_UNIT_BC_ICMS_ULT_E AS "VL_UNIT_BC_ICMS_ULT_E",
                        MERC_VL_UNIT_LIMBCICMS_ULT_E AS "VL_UNIT_LIMBICICMS_ULT_E",
                        MERC_VL_UNIT_ICMS_ULT_E AS "VL_UNIT_ICMS_ULT_E",
                        MERC_VL_ALIQ_ST_ULT_E AS "ALIQ_ST_ULT_E",
                        MERC_VL_UNIT_RES AS "VL_UNIT_RES",
                        MERC_CD_COD_RESP_RET AS "COD_RESP_RET",
                        MERC_CD_COD_MOT_RES AS "COD_MOT_RES",
                        MERC_CD_CHAVE_NFE_RET AS "CHAVE_NFE_RET",
                        MERC_CD_COD_PART_NFE_RET AS "COD_PART_NFE_RET",
                        MERC_CD_SER_NFE_RET AS "NR_SER_RET",
                        MERC_NR_NUM_NFE_RET AS "NUM_NFE_RET",
                        MERC_NR_ITEM_NFE_RET AS "NUM_ITEM_NFE_RET",
                        MERC_CD_COD_DA AS "COD_DA",
                        MERC_NR_NUM_DA AS "NUM_DA",
                        MERC_VL_UNIT_RES_FCP_ST AS "VL_UNIT_RES_FCP_ST"
                    FROM
                        EFD.TB_EFD_MERCADORIAC176
                    WHERE
                        MERC_ID_170 in {efd_id_C170}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C176 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C176

@oracle_manager
def import_efd_C177(efd_id_C170) -> pd.DataFrame:
    """
    Função para importar registro C177.
    
    Args
    ----------------------
    efd_id_C170: tuple
        Chave estrangeira do registro C170
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_177': int,   
                'MERC_ID_170': int,
                'COD_SELO_IPI': str,
                'QT_SELO_IPI': int,
                'COD_INF_ITEM': str
                }

    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"
    
    query = f"""
                    SELECT
                        MERC_ID_177,
                        MERC_ID_170,
                        MERC_CD_COD_SELO_IPI AS "COD_SELO_IPI",
                        MERC_QT_QT_SELO_IPI AS "QT_SELO_IPI",
                        MERC_CD_COD_INF_ITEM AS "COD_INF_ITEM"
                    FROM
                        EFD.TB_EFD_MERCADORIAC177
                    WHERE
                        MERC_ID_170 in {efd_id_C170}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C177 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C177

@oracle_manager
def import_efd_C178(efd_id_C170) -> pd.DataFrame:
    """
    Função para importar registro C178.
    
    Args
    ----------------------
    efd_id_C170: tuple
        Chave estrangeira do registro C170
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_178': int,   
                'MERC_ID_170': int,
                'CL_ENQ': str,
                'VL_UNID': float,
                'QUANT_PAD': float
                }

    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"
    
    query = f"""
                    SELECT
                        MERC_ID_178,
                        MERC_ID_170,
                        MERC_CD_CL_ENQ AS "CL_ENQ",
                        MERC_VL_UNID AS "VL_UNID",
                        MERC_QT_QUANT_PAD AS "QUANT_PAD"
                    FROM
                        EFD.TB_EFD_MERCADORIAC178
                    WHERE
                        MERC_ID_170 in {efd_id_C170}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C178 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C178

@oracle_manager
def import_efd_C179(efd_id_C170) -> pd.DataFrame:
    """
    Função para importar registro C179.
    
    Args
    ----------------------
    efd_id_C170: tuple
        Chave estrangeira do registro C170
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_179': int,   
                'MERC_ID_170': int,
                'VL_BC_ST_ORIG_DEST': float,
                'VL_ICMS_ST_REP': float,
                'VL_ICMS_ST_COMPL': float,
                'VL_BC_RET': float,
                'VL_ICMS_RET': float
                }

    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"
    
    query = f"""
                    SELECT
                        MERC_ID_179,
                        MERC_ID_170,
                        MERC_VL_BC_ST_ORIG_DEST AS "VL_BC_ST_ORIG_DEST",
                        MERC_VL_ICMS_ST_REP AS "VL_ICMS_ST_REP",
                        MERC_VL_ICMS_ST_COMPL AS "VL_ICMS_ST_COMPL",
                        MERC_VL_BC_RET AS "VL_BC_RET",
                        MERC_VL_ICMS_RET AS "VL_ICMS_RET"
                    FROM
                        EFD.TB_EFD_MERCADORIAC179
                    WHERE
                        MERC_ID_170 in {efd_id_C170}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C179 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C179

@oracle_manager
def import_efd_C180(efd_id_C170) -> pd.DataFrame:
    """
    Função para importar registro C180.
    
    Args
    ----------------------
    efd_id_C170: tuple
        Chave estrangeira do registro C170
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_180': int,   
                'MERC_ID_170': int,
                'COD_RESP_RET': str,
                'QUANT_CONV': float,
                'UNID': str,
                'VL_UN_CONV': float,
                'VL_ICMS_OP_CONV': float,
                'VL_BC_ICMS_ST_CONV': float,
                'VL_ICMS_ST_CONV': float,
                'VL_FCP_ST_CONV': float,
                'COD_DA': str,
                'NUM_DA': str
                }

    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"
    
    query = f"""
                    SELECT
                        MERC_ID_180,
                        MERC_ID_170,
                        MERC_COD_RESP_RET180 AS "COD_RESP_RET",
                        MERC_QUANT_CONV180 AS "QUANT_CONV",
                        MERC_UNID180 AS "UNID",
                        MERC_VL_UN_CONV180 AS "VL_UN_CONV",
                        MERC_VL_UN_ICMS_OP_CONV180 AS "VL_ICMS_OP_CONV",
                        MERC_VL_UN_BC_ICMS_ST_CONV180 AS "VL_BC_ICMS_ST_CONV",
                        MERC_VL_UN_ICMS_ST_CONV180 AS "VL_ICMS_ST_CONV",
                        MERC_VL_UN_FCP_ST_CONV180 AS "VL_FCP_ST_CONV",
                        MERC_COD_DA180 AS "COD_DA",
                        NUM_DA180 AS "NUM_DA"
                    FROM
                        EFD.TB_EFD_MERCADORIAC180
                    WHERE
                        MERC_ID_170 in {efd_id_C170}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C180 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C180

@oracle_manager
def import_efd_C181(efd_id_C170) -> pd.DataFrame:
    """
    Função para importar registro C181.
    
    Args
    ----------------------
    efd_id_C170: tuple
        Chave estrangeira do registro C170
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_181': int,   
                'MERC_ID_170': int,
                'COD_MOT_REST_COMPL': str,
                'QUANT_CONV': float,
                'UNID': str,
                'COD_MOD_SAIDA': str,
                'SERIE_SAIDA': str,
                'ECF_FAB_SAIDA': str,
                'NUM_DOC_SAIDA': str,
                'CHV_DFE_SAIDA': str,
                'DT_DOC_SAIDA': str,
                'NUM_ITEM_SAIDA': str,
                'VL_UNIT_CONV_SAIDA': float,
                'VL_UNIT_ICMS_OP_ESTOQUE_CONV_SAIDA': float,
                'VL_UNIT_ICMS_ST_ESTOQUE_CONV_SAIDA': float,
                'VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV_SAIDA': float,
                'VL_UNIT_ICMS_NA_OPERACAO_CONV_SAIDA': float,
                'VL_UNIT_ICMS_OP_CONV_SAIDA': float,

                'VL_UNIT_ICMS_ST_CONV_SAIDA': float, 

                'VL_UNIT_FCP_ST_CONV_SAIDA': float,

                'VL_UNIT_ICMS_ST_CONV_COMPL': float,

                'VL_UNIT_FCP_ST_CONV_COMPL': float
                }

    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"
    
    query = f"""
                    SELECT
                        MERC_ID_181,
                        MERC_ID_170,
                        MERC_COD_MOT_REST_COMPL181 AS "COD_MOT_REST_COMPL",
                        MERC_QUANT_CONV181 AS "QUANT_CONV",
                        MERC_UNID181 AS "UNID",
                        MERC_COD_MOD_SAIDA181 AS "COD_MOD_SAIDA",
                        MERC_SERIE_SAIDA181 AS "SERIE_SAIDA",
                        MERC_ECF_FAB_SAIDA181 AS "ECF_FAB_SAIDA",
                        MERC_NUM_DOC_SAIDA181 AS "NUM_DOC_SAIDA",
                        MERC_CHV_DFE_SAIDA181 AS "CHV_DFE_SAIDA",
                        MERC_DT_DOC_SAIDA181 AS "DT_DOC_SAIDA",
                        MERC_NUM_ITEM_SAIDA181 AS "NUM_ITEM_SAIDA",
                        MERC_VL_UN_CONV_SAIDA181 AS "VL_UNIT_CONV_SAIDA",
                        MERC_VL_UN_ICMS_OP_EST_SAI181 AS "VL_UNIT_ICMS_OP_ESTOQUE_CONV_SAIDA",
                        MERC_VL_UN_ICMS_ST_EST_SAI181 AS "VL_UNIT_ICMS_ST_ESTOQUE_CONV_SAIDA",
                        MERC_VL_UN_FCP_ICMS_ST_SAI181 AS "VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV_SAIDA",
                        MERC_VL_UN_ICMS_OPER_SAI181 AS "VL_UNIT_ICMS_NA_OPERACAO_CONV_SAIDA",
                        MERC_VL_UN_ICMS_OP_SAI181 AS "VL_UNIT_ICMS_OP_CONV_SAIDA",
                        MERC_VL_UN_ICMS_ST_REST181 AS "VL_UNIT_ICMS_ST_CONV_SAIDA",
                        MERC_VL_UN_FCP_ST_REST181 AS "VL_UNIT_FCP_ST_CONV_SAIDA",
                        MERC_VL_UN_ICMS_ST_COMPL181 AS "VL_UNIT_ICMS_ST_CONV_COMPL",
                        MERC_VL_UN_FCP_ST_COMPL181 AS "VL_UNIT_FCP_ST_CONV_COMPL"
                    FROM
                        EFD.TB_EFD_MERCADORIAC181
                    WHERE
                        MERC_ID_170 in {efd_id_C170}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C181 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C181

@oracle_manager
def import_efd_C185(efd_id_C100) -> pd.DataFrame:
    """
    Função para importar registro C185.
    
    Args
    ----------------------
    efd_id_C100: tuple
        Chave estrangeira do registro C100
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_185': int,   
                'MERC_ID_100': int,
                'NUM_ITEM': str,
                'COD_ITEM': str,
                'CST_ICMS': str,
                'CFOP': str,
                'COD_MOT_REST_COMPL': str,
                'QUANT_CONV': float,
                'UNID': str,
                'VL_UN_CONV': float,
                'VL_UNIT_ICMS_NA_OPERACAO_CONV': float,
                'VL_UNIT_ICMS_OP_CONV': float,
                'VL_UNIT_ICMS_OP_ESTOQUE_CONV': float, 
                'VL_UNIT_ICMS_ST_ESTOQUE_CONV': float, 
                'VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV': float, 
                'VL_UNIT_ICMS_ST_CONV_REST': float, 
                'VL_UNIT_FCP_ST_CONV_REST': float, 
                'VL_UNIT_ICMS_ST_CONV_COMPL': float, 
                'VL_UNIT_FCP_ST_CONV_COMPL': float
                }

    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"
    
    query = f"""
                    SELECT
                        MERC_ID_185,
                        MERC_ID_100,
                        MERC_NUM_ITEM185 AS "NUM_ITEM",
                        MERC_COD_ITEM185 AS "COD_ITEM",
                        MERC_CST_ICMS185 AS "CST_ICMS",
                        MERC_CFOP185 AS "CFOP",
                        MERC_COD_MOT_REST_COMPL185 AS "COD_MOT_REST_COMPL",
                        MERC_QUANT_CONV185 AS "QUANT_CONV",
                        MERC_UNID185 AS "UNID",
                        MERC_VL_UN_CONV185 AS "VL_UN_CONV",
                        MERC_VL_UN_ICMS_OPER_CONV185 AS "VL_UNIT_ICMS_NA_OPERACAO_CONV",
                        MERC_VL_UN_ICMS_OP_CONV185 AS "VL_UNIT_ICMS_OP_CONV",
                        MERC_VL_UN_ICMS_OP_EST_CONV185 AS "VL_UNIT_ICMS_OP_ESTOQUE_CONV",
                        MERC_VL_UN_ICMS_ST_EST_CONV185 AS "VL_UNIT_ICMS_ST_ESTOQUE_CONV",
                        MERC_VL_UN_FCP_ICMSST_EST_C185 AS "VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV",
                        MERC_VL_UN_ICMS_ST_CON_REST185 AS "VL_UNIT_ICMS_ST_CONV_REST",
                        MERC_VL_UN_FCP_ST_CONV_REST185 AS "VL_UNIT_FCP_ST_CONV_REST",
                        MERC_VL_UN_ICMS_ST_CONV_COM185 AS "VL_UNIT_ICMS_ST_CONV_COMPL",
                        MERC_VL_UN_FCP_ST_CONV_COM185 AS "VL_UNIT_FCP_ST_CONV_COMPL"
                    FROM
                        EFD.TB_EFD_MERCADORIAC185
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C185 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C185

@oracle_manager
def import_efd_C186(efd_id_C100) -> pd.DataFrame:
    """
    Função para importar registro C186.
    
    Args
    ----------------------
    efd_id_C100: tuple
        Chave estrangeira do registro C100
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_186': int,   
                'MERC_ID_100': int,
                'NUM_ITEM': str,
                'COD_ITEM': str,
                'CST_ICMS': str,
                'CFOP': str,
                'COD_MOT_REST_COMPL': str,
                'QUANT_CONV': float,
                'UNID': str,
                'COD_MOD_ENTRADA': str,
                'SERIE_ENTRADA': str,
                'NUM_DOC_ENTRADA': str,
                'CHV_DFE_ENTRADA': str,
                'DT_DOC_ENTRADA': str,
                'NUM_ITEM_ENTRADA': str,
                'VL_UNIT_CONV_ENTRADA': float,
                'VL_UNIT_ICMS_OP_CONV_ENTRADA': float,
                'VL_UNIT_BC_ICMS_ST_CONV_ENTRADA': float,
                'VL_UNIT_ICMS_ST_CONV_ENTRADA': float,
                'VL_UNIT_FCP_ST_CONV_ENTRADA': float
                }

    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"
    
    query = f"""
                    SELECT
                        MERC_ID_186,
                        MERC_ID_100,
                        MERC_NUM_ITEM186 AS "NUM_ITEM",
                        MERC_COD_ITEM186 AS "COD_ITEM",
                        MERC_CST_ICMS186 AS "CST_ICMS",
                        MERC_CFOP186 AS "CFOP",
                        MERC_COD_MOT_REST_COMPL186 AS "COD_MOT_REST_COMPL",
                        MERC_QUANT_CONV186 AS "QUANT_CONV",
                        MERC_UNID186 AS "UNID",
                        MERC_COD_MOD_ENTRADA186 AS "COD_MOD_ENTRADA",
                        MERC_SERIE_ENTRADA186 AS "SERIE_ENTRADA",
                        MERC_NUM_DOC_ENTRADA186 AS "NUM_DOC_ENTRADA",
                        MERC_CHV_DFE_ENTRADA186 AS "CHV_DFE_ENTRADA",
                        MERC_DT_DOC_ENTRADA186 AS "DT_DOC_ENTRADA",
                        MERC_NUM_ITEM_ENTRADA186 AS "NUM_ITEM_ENTRADA",
                        MERC_VL_UN_CONV_ENTRADA186 AS "VL_UNIT_CONV_ENTRADA",
                        MERC_VL_UN_ICMS_OP_CONV_ENT186 AS "VL_UNIT_ICMS_OP_CONV_ENTRADA",
                        MERC_VL_UN_BCICMSSTCONV_ENT186 AS "VL_UNIT_BC_ICMS_ST_CONV_ENTRADA",
                        MERC_VL_UN_ICMS_ST_CONV_ENT186 AS "VL_UNIT_ICMS_ST_CONV_ENTRADA",
                        MERC_VL_UN_FCP_ST_CONV_ENT186 AS "VL_UNIT_FCP_ST_CONV_ENTRADA"
                    FROM
                        EFD.TB_EFD_MERCADORIAC186
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C186 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C186

@oracle_manager
def import_efd_C190(efd_id_C100) -> pd.DataFrame:
    """
    Função para importar registro C190.
    
    Args
    ----------------------
    efd_id_C100: tuple
        Chave estrangeira do registro C100
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_190': int,   
                'MERC_ID_100': int,
                'CST_ICMS': str,
                'CFOP': str,
                'ALIQ_ICMS': float,
                'VL_OPR': float,
                'VL_BC_ICMS': float,
                'VL_ICMS': float,
                'VL_BC_ICMS_ST': float,
                'VL_ICMS_ST': float,
                'VL_RED_BC': float,
                'VL_IPI': float,
                'COD_OBS': str
                }

    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"
    
    query = f"""
                    SELECT
                        MERC_ID_190,
                        MERC_ID_100,
                        MERC_CD_CST_ICMS190 AS "CST_ICMS",
                        MERC_CD_CFOP190 AS "CFOP",
                        MERC_VL_ALIQ_ICMS190 AS "ALIQ_ICMS",
                        MERC_VL_OPR190 AS "VL_OPR",
                        MERC_VL_BC_ICMS190 AS "VL_BC_ICMS",
                        MERC_VL_ICMS190 AS "VL_ICMS",
                        MERC_VL_BC_ICMS_ST190 AS "VL_BC_ICMS_ST",
                        MERC_VL_ICMS_ST190 AS "VL_ICMS_ST",
                        MERC_VL_RED_BC190 AS "VL_RED_BC",
                        MERC_VL_IPI190 AS "VL_IPI",
                        MERC_CD_COD_OBS190 AS "COD_OBS"
                    FROM
                        EFD.TB_EFD_MERCADORIAC190
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C190 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C190

@oracle_manager
def import_efd_C191(efd_id_C190) -> pd.DataFrame:
    """
    Função para importar registro C191.
    
    Args
    ----------------------
    efd_id_C190: tuple
        Chave estrangeira do registro C190
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_191': int,   
                'MERC_ID_190': int,
                'VL_FCP_OP': float,
                'VL_FCP_ST': float,
                'VL_FCP_RET': float
                }

    assert len(efd_id_C190) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C190, tuple), f"Input is not a tuple: {type(efd_id_C190)}"
    
    query = f"""
                    SELECT
                        MERC_ID_191,
                        MERC_ID_190,
                        MERC_VL_FCP_OP AS "VL_FCP_OP",
                        MERC_VL_FCP_ST AS "VL_FCP_ST",
                        MERC_VL_FCP_RET AS "VL_FCP_RET"
                    FROM
                        EFD.TB_EFD_MERCADORIAC191
                    WHERE
                        MERC_ID_190 in {efd_id_C190}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C191 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C191

@oracle_manager
def import_efd_C195(efd_id_C100) -> pd.DataFrame:
    """ 
    Function to import the register C195, with fiscal observations from invoices declared.
    
    Args
    ----------------------
    efd_id_C100: tuple
        Refers to list of MERC_ID_100 from EFD to import.
        It shouldn't be given more than 1.000 ID to be searched for.

    """

    # Creating SQL query
    columns = {    
                'MERC_ID_100': int,   
                'MERC_ID_195': int,
                'COD_OBS': str,
                'TXT_COMPL': str
            }


    assert len(efd_id_C100) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C100)}"

    assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        MERC_ID_100,
                        MERC_ID_195,
                        MERC_CD_COD_OBS195 AS "COD_OBS",
                        MERC_DS_TXT_COMPL195 AS "TXT_COMPL"

                    FROM
                        EFD.TB_EFD_MERCADORIAC195
                    WHERE
                        MERC_ID_100 in {efd_id_C100}
  
                        """
   
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C195 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_C195

@oracle_manager
def import_efd_C197(efd_id_C195) -> pd.DataFrame:
    """ 
    Function to import the register C197, with fiscal adjustments from invoices declared.
    
    Args
    ----------------------
    efd_id_C195: tuple
        Refers to list of MERC_ID_195 from EFD to import.
        It shouldn't be given more than 1.000 ID to be searched for.

    """

    # Creating SQL query
    columns = {    
                'MERC_ID_197': int,   
                'MERC_ID_195': int,
                'COD_AJ': str,
                'DESCR_COMPL_AJ': str,
                'COD_ITEM': str,
                'VL_BC_ICMS': float,
                'ALIQ_ICMS': float,
                'VL_ICMS': float,
                'VL_OUTROS': float
            }


    assert len(efd_id_C195) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_C195)}"

    assert isinstance(efd_id_C195, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C195)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        MERC_ID_195,
                        MERC_ID_197,
                        MERC_CD_COD_AJ197 AS COD_AJ,
                        MERC_DS_DESCR_COMPL_AJ197 AS DESCR_COMPL_AJ,
                        MERC_CD_COD_ITEM197 AS COD_ITEM,
                        MERC_VL_BC_ICMS197 AS VL_BC_ICMS,
                        MERC_VL_ALIQ_ICMS197 AS ALIQ_ICMS,
                        MERC_VL_ICMS197 AS VL_ICMS,
                        MERC_VL_OUTROS197 AS VL_OUTROS

                    FROM
                        EFD.TB_EFD_MERCADORIAC197
                    WHERE
                        MERC_ID_195 in {efd_id_C195}
  
                        """
   
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_C197 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_C197

@oracle_manager
def import_efd_C500(efd_id_C001) -> pd.DataFrame:
    """
    Função para importar registro C500.
    
    Args
    ----------------------
    efd_id_C001: tuple
        Chave estrangeira do registro C001
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_500': int,   
                'MERC_ID_001': int,
                'TP_IND_OPER': str,
                'TP_IND_EMIT': str,
                'COD_PART': str,
                'COD_MOD': str,
                'COD_SIT': str,
                'SER': str,
                'SUB': str,
                'COD_CONS': str,
                'NUM_DOC': str,
                'DT_DOC': str,
                'DT_E': str,
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
                'COD_GRUPO_TENSAO': str,
                'CHV_DOCe': str,
                'FIN_DOCe': str,
                'CHV_DOCe_REF': str,
                'IND_DEST': str, 
                'COD_MUN_DEST': str,
                'COD_CTA': str,
                'COD_MOD_DOC_REF': str,
                'HASH_DOC_REF': str,
                'SER_DOC_REF': str,
                'NUM_DOC_REF': str,
                'MES_DOC_REF': str,
                'ENER_INJET': float,
                'OUTRAS_DED': float
                }

    assert len(efd_id_C001) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C001, tuple), f"Input is not a tuple: {type(efd_id_C001)}"
    
    query = f"""
                    SELECT
                        MERC_ID_500,
                        MERC_ID_001,
                        MERC_TP_IND_OPER500 AS "TP_IND_OPER",
                        MERC_TP_IND_EMIT500 AS "TP_IND_EMIT",
                        MERC_CD_COD_PART500 AS "COD_PART",
                        MERC_CD_COD_MOD500 AS "COD_MOD",
                        MERC_CD_COD_SIT500 AS "COD_SIT",
                        MERC_NR_SER500 AS "SER",
                        MERC_NR_SUB500 AS "SUB",
                        MERC_COD_CONS500 AS "COD_CONS",

                        MERC_NR_NUM_DOC500 AS "NUM_DOC",
                        MERC_DT_DOC500 AS "DT_DOC",
                        MERC_DT_E_S500 AS "DT_E",
                        MERC_VL_DOC500 AS "VL_DOC",
                        MERC_VL_DESC500 AS "VL_DESC",
                        MERC_VL_FORN500 AS "VL_FORN",
                        MERC_VL_SERV_NT500 AS "VL_SERV_NT",
                        MERC_VL_TERC500 AS "VL_TERC",
                        MERC_VL_DA500 AS "VL_DA",
                        MERC_VL_BC_ICMS500 AS "VL_BC_ICMS",
                        MERC_VL_ICMS500 AS "VL_ICMS",
                        MERC_VL_BC_ICMS_ST500 AS "VL_BC_ICMS_ST",
                        MERC_VL_ICMS_ST500 AS "VL_ICMS_ST",
                        MERC_CD_COD_INF500 AS "COD_INF",
                        MERC_VL_PIS500 AS "VL_PIS",
                        MERC_VL_COFINS500 AS "VL_COFINS",
                        MERC_TP_LIGACAO500 AS "TP_LIGACAO",
                        MERC_CD_COD_GRUPO_TENSAO500 AS "COD_GRUPO_TENSAO",
                        MERC_ID_CHV_DOCE500 AS "CHV_DOCe",
                        MERC_FIN_DOCE500 AS "FIN_DOCe",
                        MERC_ID_CHV_DOCE_REF500 AS "CHV_DOCe_REF",
                        MERC_IND_DEST500 AS "IND_DEST", 
                        MERC_CD_COD_MUN_DEST500 AS "COD_MUN_DEST",

                        MERC_CD_COD_CTA500 AS "COD_CTA",
                        MERC_CD_COD_MOD_DOC_REF500 AS "COD_MOD_DOC_REF",
                        MERC_CD_HASH_DOC_REF500 AS "HASH_DOC_REF",
                        MERC_SER_DOC_REF500 AS "SER_DOC_REF",
                        MERC_NR_NUM_DOC_REF500 AS "NUM_DOC_REF",
                        MERC_MES_DOC_REF500 AS "MES_DOC_REF",
                        MERC_ENER_INJET500 AS "ENER_INJET",
                        MERC_OUTRAS_DED500 AS "OUTRAS_DED"
                        
                    FROM
                        EFD.TB_EFD_MERCADORIAC500
                    WHERE
                        MERC_ID_001 in {efd_id_C001}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C500 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C500

@oracle_manager
def import_efd_C510(efd_id_C500) -> pd.DataFrame:
    """
    Função para importar registro C510.
    
    Args
    ----------------------
    efd_id_C500: tuple
        Chave estrangeira do registro C500
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_510': int,   
                'MERC_ID_500': int,
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
                'VL_BC_ICMS_ST': float,
                'ALIQ_ST': float,
                'VL_ICMS_ST': float,
                'IND_REC': str,
                'COD_PART': str,
                'VL_PIS': float,
                'VL_COFINS': float,
                'COD_CTA': str
                }

    assert len(efd_id_C500) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C500, tuple), f"Input is not a tuple: {type(efd_id_C500)}"
    
    query = f"""
                    SELECT
                        MERC_ID_510,
                        MERC_ID_500,
                        MERC_NR_NUM_ITEM510 AS "NUM_ITEM",
                        MERC_CD_COD_ITEM510 AS "COD_ITEM",
                        MERC_CD_COD_CLASS510 AS "COD_CLASS",
                        MERC_QT_QTD510 AS "QTD",
                        MERC_NR_UNID510 AS "UNID",
                        MERC_VL_ITEM510 AS "VL_ITEM",
                        MERC_VL_DESC510 AS "VL_DESC",
                        MERC_CD_CST_ICMS510 AS "CST_ICMS",
                        MERC_CD_CFOP510 AS "CFOP",
                        MERC_VL_BC_ICMS510 AS "VL_BC_ICMS",
                        MERC_VL_ALIQ_ICMS510 AS "ALIQ_ICMS",
                        MERC_VL_ICMS510 AS "VL_ICMS",
                        MERC_VL_BC_ICMS_ST510 AS "VL_BC_ICMS_ST",
                        MERC_VL_ALIQ_ST510 AS "ALIQ_ST",
                        MERC_VL_ICMS_ST510 AS "VL_ICMS_ST",
                        MERC_TP_IND_REC510 AS "IND_REC",
                        MERC_CD_COD_PART510 AS "COD_PART",
                        MERC_VL_PIS510 AS "VL_PIS",
                        MERC_VL_COFINS510 AS "VL_COFINS",
                        MERC_CD_COD_CTA510 AS "COD_CTA"
                    FROM
                        EFD.TB_EFD_MERCADORIAC510
                    WHERE
                        MERC_ID_500 in {efd_id_C500}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C510 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C510

@oracle_manager
def import_efd_C590(efd_id_C500) -> pd.DataFrame:
    """
    Função para importar registro C590.
    
    Args
    ----------------------
    efd_id_C500: tuple
        Chave estrangeira do registro C500
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_590': int,   
                'MERC_ID_500': int,
                'CST_ICMS': str,
                'CFOP': str,
                'ALIQ_ICMS': float,
                'VL_OPR': float,
                'VL_BC_ICMS': float,
                'VL_ICMS': float,
                'VL_BC_ICMS_ST': float,
                'VL_ICMS_ST': float,
                'VL_RED_BC': float,
                'COD_OBS': str
                }

    assert len(efd_id_C500) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C500, tuple), f"Input is not a tuple: {type(efd_id_C500)}"
    
    query = f"""
                    SELECT
                        MERC_ID_590,
                        MERC_ID_500,
                        MERC_CD_CST_ICMS590 AS "CST_ICMS",
                        MERC_CD_CFOP590 AS "CFOP",
                        MERC_VL_ALIQ_ICMS590 AS "ALIQ_ICMS",
                        MERC_VL_OPR590 AS "VL_OPR",
                        MERC_VL_BC_ICMS590 AS "VL_BC_ICMS",
                        MERC_VL_ICMS590 AS "VL_ICMS",
                        MERC_VL_BC_ICMS_ST590 AS "VL_BC_ICMS_ST",
                        MERC_VL_ICMS_ST590 AS "VL_ICMS_ST",
                        MERC_VL_RED_BC590 AS "VL_RED_BC",
                        MERC_CD_COD_OBS590 AS "COD_OBS"
                    FROM
                        EFD.TB_EFD_MERCADORIAC590
                    WHERE
                        MERC_ID_500 in {efd_id_C500}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C590 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C590

@oracle_manager
def import_efd_C591(efd_id_C590) -> pd.DataFrame:
    """
    Função para importar registro C591.
    
    Args
    ----------------------
    efd_id_C590: tuple
        Chave estrangeira do registro C590
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_591': int,   
                'MERC_ID_590': int,
                'VL_FCP_OP': float,
                'VL_FCP_ST': float
                }

    assert len(efd_id_C590) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C590, tuple), f"Input is not a tuple: {type(efd_id_C590)}"
    
    query = f"""
                    SELECT
                        MERC_ID_591,
                        MERC_ID_590,
                        MERC_VL_FCP_OP591 AS "VL_FCP_OP",
                        MERC_VL_FCP_ST591 AS "VL_FCP_ST"
                    FROM
                        EFD.TB_EFD_MERCADORIAC591
                    WHERE
                        MERC_ID_590 in {efd_id_C590}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C591 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C591

@oracle_manager
def import_efd_C595(efd_id_C591) -> pd.DataFrame:
    """
    Função para importar registro C595.
    
    Args
    ----------------------
    efd_id_C591: tuple
        Chave estrangeira do registro C591
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_595': int,   
                'MERC_ID_591': int,
                'COD_OBS': str,
                'TXT_COMPL': str
                }

    assert len(efd_id_C591) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C591, tuple), f"Input is not a tuple: {type(efd_id_C591)}"
    
    query = f"""
                    SELECT
                        MERC_ID_595,
                        MERC_ID_591,
                        MERC_COD_OBS595 AS "COD_OBS",
                        MERC_TXT_COMPL595 AS "TXT_COMPL"
                    FROM
                        EFD.TB_EFD_MERCADORIAC595
                    WHERE
                        MERC_ID_591 in {efd_id_C591}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C595 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C595

@oracle_manager
def import_efd_C597(efd_id_C595) -> pd.DataFrame:
    """
    Função para importar registro C597.
    
    Args
    ----------------------
    efd_id_C595: tuple
        Chave estrangeira do registro C595
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_597': int,   
                'MERC_ID_595': int,
                'COD_AJ': str,
                'DESCR_COMPL_AJ': str,
                'COD_ITEM': str,
                'VL_BC_ICMS': float,
                'ALIQ_ICMS': float,
                'VL_ICMS': float,
                'VL_OUTROS': float
                }

    assert len(efd_id_C595) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C595, tuple), f"Input is not a tuple: {type(efd_id_C595)}"
    
    query = f"""
                    SELECT
                        MERC_ID_597,
                        MERC_ID_595,
                        MERC_COD_AJ597 AS "COD_AJ",
                        MERC_DESCR_COMPL_AJ597 AS "DESCR_COMPL_AJ",
                        MERC_COD_ITEM597 AS "COD_ITEM",
                        MERC_VL_BC_ICMS597 AS "VL_BC_ICMS",
                        MERC_ALIQ_ICMS597 AS "ALIQ_ICMS",
                        MERC_VL_ICMS597 AS "VL_ICMS",
                        MERC_VL_OUTROS597 AS "VL_OUTROS"
                    FROM
                        EFD.TB_EFD_MERCADORIAC597
                    WHERE
                        MERC_ID_595 in {efd_id_C595}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C597 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C597

@oracle_manager
def import_efd_C600(efd_id_C001) -> pd.DataFrame:
    """
    Função para importar registro C600.
    
    Args
    ----------------------
    efd_id_C001: tuple
        Chave estrangeira do registro C001
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_600': int,   
                'MERC_ID_001': int,
                'COD_MOD': str,
                'COD_MUN': str,
                'SER': str,
                'SUB': str,
                'COD_CONS': str,
                'QTD_CONS': float,
                'QTD_CANC': float,
                'DT_DOC': str,
                'VL_DOC': float,
                'VL_DESC': float,
                'CONS': float,
                'VL_FORN': float,
                'VL_SERV_NT': float,
                'VL_TERC': float,
                'VL_DA': float,
                'VL_BC_ICMS': float,
                'VL_ICMS': float,
                'VL_BC_ICMS_ST': float,
                'VL_ICMS_ST': float,
                'VL_PIS': float,
                'VL_COFINS': float
                }

    assert len(efd_id_C001) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C001, tuple), f"Input is not a tuple: {type(efd_id_C001)}"
    
    query = f"""
                    SELECT
                        MERC_ID_600,
                        MERC_ID_001,
                        MERC_CD_COD_MOD600 AS "COD_MOD",
                        MERC_CD_COD_MUN600 AS "COD_MUN",
                        MERC_NR_SER600 AS "SER",
                        MERC_NR_SUB600 AS "SUB",
                        MERC_CD_COD_CONS600 AS "COD_CONS",
                        MERC_QT_QTD_CONS600 AS "QTD_CONS",
                        MERC_QT_QTD_CANC600 AS "QTD_CANC",
                        MERC_DT_DOC600 AS "DT_DOC",
                        MERC_VL_DOC600 AS "VL_DOC",
                        MERC_VL_DESC600 AS "VL_DESC",
                        MERC_NR_CONS600 AS "CONS",
                        MERC_VL_FORN600 AS "VL_FORN",
                        MERC_VL_SERV_NT600 AS "VL_SERV_NT",
                        MERC_VL_TERC600 AS "VL_TERC",
                        MERC_VL_DA600 AS "VL_DA",
                        MERC_VL_BC_ICMS600 AS "VL_BC_ICMS",
                        MERC_VL_ICMS600 AS "VL_ICMS",
                        MERC_VL_BC_ICMS_ST600 AS "VL_BC_ICMS_ST",
                        MERC_VL_ICMS_ST600 AS "VL_ICMS_ST",
                        MERC_VL_PIS600 AS "VL_PIS",
                        MERC_VL_COFINS600 AS "VL_COFINS"
                    FROM
                        EFD.TB_EFD_MERCADORIAC600
                    WHERE
                        MERC_ID_001 in {efd_id_C001}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C600 = pd.read_sql_query(query, con=connection, dtype=columns)

    efd_C600['DT_DOC'] = pd.to_datetime(efd_C600['DT_DOC'])
    return efd_C600

@oracle_manager
def import_efd_C601(efd_id_C600) -> pd.DataFrame:
    """
    Função para importar registro C601.
    
    Args
    ----------------------
    efd_id_C600: tuple
        Chave estrangeira do registro C600
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_601': int,   
                'MERC_ID_600': int,
                'NUM_DOC_CANC': str
                }

    assert len(efd_id_C600) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C600, tuple), f"Input is not a tuple: {type(efd_id_C600)}"
    
    query = f"""
                    SELECT
                        MERC_ID_601,
                        MERC_ID_600,
                        MERC_NR_NUM_DOC_CANC601 AS "NUM_DOC_CANC"
                    FROM
                        EFD.TB_EFD_MERCADORIAC601
                    WHERE
                        MERC_ID_600 in {efd_id_C600}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C601 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C601

@oracle_manager
def import_efd_C610(efd_id_C600) -> pd.DataFrame:
    """
    Função para importar registro C610.
    
    Args
    ----------------------
    efd_id_C600: tuple
        Chave estrangeira do registro C600
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_610': int,   
                'MERC_ID_600': int,
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
                'VL_BC_ICMS_ST': float,
                'VL_ICMS_ST': float,
                'VL_PIS': float,
                'VL_COFINS': float,
                'COD_CTA': str
                }

    assert len(efd_id_C600) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C600, tuple), f"Input is not a tuple: {type(efd_id_C600)}"
    
    query = f"""
                    SELECT
                        MERC_ID_610,
                        MERC_ID_600,
                        MERC_CD_COD_CLASS610 AS "COD_CLASS",
                        MERC_CD_COD_ITEM610 AS "COD_ITEM",
                        MERC_QT_QTD610 AS "QTD",
                        MERC_NR_UNID610 AS "UNID",
                        MERC_VL_ITEM610 AS "VL_ITEM",
                        MERC_VL_DESC610 AS "VL_DESC",
                        MERC_CD_CST_ICMS610 AS "CST_ICMS",
                        MERC_CD_CFOP610 AS "CFOP",
                        MERC_VL_ALIQ_ICMS610 AS "ALIQ_ICMS",
                        MERC_VL_BC_ICMS610 AS "VL_BC_ICMS",
                        MERC_VL_ICMS610 AS "VL_ICMS",
                        MERC_VL_BC_ICMS_ST610 AS "VL_BC_ICMS_ST",
                        MERC_VL_ICMS_ST610 AS "VL_ICMS_ST",
                        MERC_VL_PIS610 AS "VL_PIS",
                        MERC_VL_COFINS610 AS "VL_COFINS",
                        MERC_COD_CTA610 AS "COD_CTA"
                    FROM
                        EFD.TB_EFD_MERCADORIAC610
                    WHERE
                        MERC_ID_600 in {efd_id_C600}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C610 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C610

@oracle_manager
def import_efd_C690(efd_id_C600) -> pd.DataFrame:
    """
    Função para importar registro C690.
    
    Args
    ----------------------
    efd_id_C600: tuple
        Chave estrangeira do registro C600
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_690': int,   
                'MERC_ID_600': int,
                'CST_ICMS': str,
                'CFOP': str,
                'ALIQ_ICMS': float,
                'VL_OPR': float,
                'VL_BC_ICMS': float,
                'VL_ICMS': float,
                'VL_BC_ICMS_ST': float,
                'VL_ICMS_ST': float,
                'VL_RED_BC': float,
                'COD_OBS': str
                }

    assert len(efd_id_C600) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C600, tuple), f"Input is not a tuple: {type(efd_id_C600)}"
    
    query = f"""
                    SELECT
                        MERC_ID_690,
                        MERC_ID_600,
                        MERC_CD_CST_ICMS690 AS "CST_ICMS",
                        MERC_CD_CFOP690 AS "CFOP",
                        MERC_VL_ALIQ_ICMS690 AS "ALIQ_ICMS",
                        MERC_VL_OPR690 AS "VL_OPR",
                        MERC_VL_BC_ICMS690 AS "VL_BC_ICMS",
                        MERC_VL_ICMS690 AS "VL_ICMS",
                        MERC_VL_BC_ICMS_ST690 AS "VL_BC_ICMS_ST",
                        MERC_VL_ICMS_ST690 AS "VL_ICMS_ST",
                        MERC_VL_RED_BC690 AS "VL_RED_BC",
                        MERC_CD_COD_OBS690 AS "COD_OBS"
                    FROM
                        EFD.TB_EFD_MERCADORIAC690
                    WHERE
                        MERC_ID_600 in {efd_id_C600}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C690 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C690

@oracle_manager
def import_efd_C700(efd_id_C001) -> pd.DataFrame:
    """
    Função para importar registro C700.
    
    Args
    ----------------------
    efd_id_C001: tuple
        Chave estrangeira do registro C001
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_700': int,   
                'MERC_ID_001': int,
                'COD_MOD': str,
                'SER': str,
                'NRO_ORD_INI': str,
                'NRO_ORD_FIN': str,
                'DT_DOC_INI': str,
                'DT_DOC_FIN': str,
                'NOM_MEST': str,
                'CHV_COD_DIG': str
                }

    date_columns = ['DT_DOC_INI', 'DT_DOC_FIN']
    assert len(efd_id_C001) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C001, tuple), f"Input is not a tuple: {type(efd_id_C001)}"
    
    query = f"""
                    SELECT
                        MERC_ID_700,
                        MERC_ID_001,
                        MERC_CD_COD_MOD700 AS "COD_MOD",
                        MERC_NR_SER700 AS "SER",
                        MERC_NR_NRO_ORD_INI700 AS "NRO_ORD_INI",
                        MERC_NRO_ORD_FIN700 AS "NRO_ORD_FIN",
                        MERC_DT_DOC_INI700 AS "DT_DOC_INI",
                        MERC_DT_DOC_FIN700 AS "DT_DOC_FIN",
                        MERC_NM_NOM_MEST700 AS "NOM_MEST",
                        MERC_DS_CHV_COD_DIG700 AS "CHV_COD_DIG"
                    FROM
                        EFD.TB_EFD_MERCADORIAC700
                    WHERE
                        MERC_ID_001 in {efd_id_C001}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C700 = pd.read_sql_query(query, con=connection, dtype=columns)

    for column in date_columns:
        efd_C700[column] = pd.to_datetime(efd_C700[column])

    return efd_C700

@oracle_manager
def import_efd_C790(efd_id_C700) -> pd.DataFrame:
    """
    Função para importar registro C790.
    
    Args
    ----------------------
    efd_id_C700: tuple
        Chave estrangeira do registro C700
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_790': int,   
                'MERC_ID_700': int,
                'CST_ICMS': str,
                'CFOP': str,
                'ALIQ_ICMS': float,
                'VL_OPR': float,
                'VL_BC_ICMS': float,
                'VL_ICMS': float,
                'VL_BC_ICMS_ST': float,
                'VL_ICMS_ST': float,
                'VL_RED_BC': float,
                'COD_OBS': str
                }

    assert len(efd_id_C700) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C700, tuple), f"Input is not a tuple: {type(efd_id_C700)}"
    
    query = f"""
                    SELECT
                        MERC_ID_790,
                        MERC_ID_700,
                        MERC_CD_CST_ICMS790 AS "CST_ICMS",
                        MERC_CD_CFOP790 AS "CFOP",
                        MERC_VL_ALIQ_ICMS790 AS "ALIQ_ICMS",
                        MERC_VL_OPR790 AS "VL_OPR",
                        MERC_VL_BC_ICMS790 AS "VL_BC_ICMS",
                        MERC_VL_ICMS790 AS "VL_ICMS",
                        MERC_VL_BC_ICMS_ST790 AS "VL_BC_ICMS_ST",
                        MERC_VL_ICMS_ST790 AS "VL_ICMS_ST",
                        MERC_VL_RED_BC790 AS "VL_RED_BC",
                        MERC_COD_OBS790 AS "COD_OBS"
                    FROM
                        EFD.TB_EFD_MERCADORIAC790
                    WHERE
                        MERC_ID_700 in {efd_id_C700}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C790 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C790

@oracle_manager
def import_efd_C791(efd_id_C790) -> pd.DataFrame:
    """
    Função para importar registro C791.
    
    Args
    ----------------------
    efd_id_C790: tuple
        Chave estrangeira do registro C790
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_791': int,   
                'MERC_ID_790': int,
                'SG_UF': str,
                'VL_BC_ICMS_ST': float,
                'VL_ICMS_ST': float
                }

    assert len(efd_id_C790) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_C790, tuple), f"Input is not a tuple: {type(efd_id_C790)}"
    
    query = f"""
                    SELECT
                        MERC_ID_791,
                        MERC_ID_790,
                        MERC_SG_UF791 AS "SG_UF",
                        MERC_VL_BC_ICMS_ST791 AS "VL_BC_ICMS_ST",
                        MERC_VL_ICMS_ST791 AS "VL_ICMS_ST"
                    FROM
                        EFD.TB_EFD_MERCADORIAC791
                    WHERE
                        MERC_ID_790 in {efd_id_C790}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C791 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C791


@oracle_manager
def import_efd_C990(efd_id_0000) -> pd.DataFrame:
    """
    Função para importar registro C990.
    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """
    
    # Defining columns with aliases from the practical guide
    columns = {    
                'MERC_ID_990': int,   
                'CABE_ID_0000': int,
                'QTD_LIN_C': int
                }

    assert len(efd_id_0000) <= 1000, "Limit exceeded!"
    assert isinstance(efd_id_0000, tuple), f"Input is not a tuple: {type(efd_id_0000)}"
    
    query = f"""
                    SELECT
                        MERC_ID_990,
                        CABE_ID_0000,
                        MERC_QT_QTD_LIN_C AS "QTD_LIN_C"
                    FROM
                        EFD.TB_EFD_MERCADORIAC990
                    WHERE
                        CABE_ID_0000 in {efd_id_0000}
            """
    
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_C990 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_C990





