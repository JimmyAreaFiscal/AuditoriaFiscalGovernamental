# Third-party modules import
import pandas as pd 
import oracledb 
from datetime import datetime as dt
from datetime import timedelta
from dateutil.relativedelta import relativedelta

# Import user-defined packages
from .utils import oracle_manager

from .usuario import dns_tns, pw, user

# Import built-in modules
import warnings 


# Disabling all warnings
warnings.filterwarnings('ignore')


@oracle_manager
def import_efd_title(mes_ref: str, cnpj:str, finalidade:int) -> pd.DataFrame:
   
    # Creating SQL query
    efd_column = {'CABE_ID_0000': int,
                   'CABE_ID_0001': int,
                   'MERC_ID_001': int, 
                   'SERV_ID_001': int,
                   'APUR_ID_001': int,
                   'CRED_ID_001': int,
                   'INVE_ID_001': int,
                   'PROD_ID_K001': int,
                   'CONT_ID_9001': int,
                   'INFO_ID_1001': int,
                   
                   'cnpj': str,
                   'COD_FIN': str,
                   "MES_REF": str
                   }
    
    

    query = f""" 
                SELECT
                    COALESCE(O.CABE_ID_0000, 0) AS "CABE_ID_0000",
                    COALESCE(O1.CABE_ID_0001, 0) AS "CABE_ID_0001",
                    COALESCE(C.MERC_ID_001, 0) AS "MERC_ID_001", 
                    COALESCE(D.SERV_ID_001, 0) AS "SERV_ID_001",
                    COALESCE(E.APUR_ID_001, 0) AS "APUR_ID_001",
                    COALESCE(G.CRED_ID_001, 0) AS "CRED_ID_001",
                    COALESCE(H.INVE_ID_001, 0) AS "INVE_ID_001",
                    COALESCE(K.PROD_ID_K001, 0) AS "PROD_ID_K001",
                    COALESCE(INFO1.INFO_ID_1001, 0) AS "INFO_ID_1001",
                    COALESCE(INFO9.CONT_ID_9001, 0) AS "CONT_ID_9001",

                    O.CABE_NR_CNPJ AS "cnpj", 
                    O.CABE_CD_IE, 
                    O.CABE_CD_FIN AS "COD_FIN",
                    TO_CHAR(O.CABE_DT_INI, 'MM/YYYY') AS "MES_REF"
                    
                    
                FROM
                    EFD.TB_EFD_CABECALHO0000 O
                        INNER JOIN EFD.TB_EFD TB ON O.EFD_ID = TB.EFD_ID
                        LEFT JOIN EFD.TB_EFD_CABECALHO0001 O1 ON (O1.CABE_ID_0000 = O.CABE_ID_0000)
                        LEFT JOIN EFD.TB_EFD_MERCADORIAC001 C ON (O.CABE_ID_0000 = C.CABE_ID_0000)
                        LEFT JOIN EFD.TB_EFD_SERVICOD001 D ON (O.CABE_ID_0000 = D.CABE_ID_0000)
                        LEFT JOIN EFD.TB_EFD_APURACAOE001 E ON (O.CABE_ID_0000 = E.CABE_ID_0000)
                        LEFT JOIN EFD.TB_EFD_CREDITOG001 G ON (O.CABE_ID_0000 = G.CABE_ID_0000)
                        LEFT JOIN EFD.TB_EFD_INVENTARIOH001 H ON (O.CABE_ID_0000 = H.CABE_ID_0000)
                        LEFT JOIN EFD.TB_EFD_PRODUCAOK001 K ON (O.CABE_ID_0000 = K.CABE_ID_0000)
                        LEFT JOIN EFD.TB_EFD_INFORMACAO1001 INFO1 ON (O.CABE_ID_0000 = INFO1.CABE_ID_0000)
                        LEFT JOIN EFD.TB_EFD_CONTROLE9001 INFO9 ON (O.CABE_ID_0000 = INFO9.CABE_ID_0000)
                        
                        
                WHERE
                    TO_CHAR(O.CABE_DT_INI, 'MM/YYYY') = '{mes_ref}' AND O.CABE_NR_CNPJ = '{cnpj}'
                    AND O.CABE_CD_FIN = '{finalidade}'
                    """
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_title = pd.read_sql_query(query, con = connection, dtype = efd_column)

    return efd_title


@oracle_manager
def import_efd_0000(efd_id_0000) -> pd.DataFrame:
    """ 
    Function to import the register 0000, having all enterprise basic informations.
    

    Args
    ----------------------
    efd_id_0000: tuple
        Refers to list of CABE_ID_0000 from EFD to import.
        It shouldn't be given more than 1.000 ID to be searched for.
   

    """


    # Creating SQL query
    columns = { 
                        'CABE_ID_0000': int,
                        'NOME': str,
                        'CNPJ': str,
                        'CPF': str,
                        'UF': str,
                        'IE': str,
                        'COD_MUN': str,
                        'IM': str,
                        'SUFRAMA': str,
                        'IND_PERFIL': str,
                        'IND_ATIV': str,
                        'COD_FIN': str,
                        'COD_VER': str
                        }


    assert len(efd_id_0000),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0000, 
                        CABE_DT_INI AS "DT_INI",
                        CABE_DT_FIN AS "DT_FIN",
                        CABE_NM_NOME AS "NOME",
                        CABE_NR_CNPJ AS "CNPJ",
                        CABE_NR_CPF AS "CPF",
                        CABE_SG_UF AS "UF",
                        CABE_CD_IE AS "IE",
                        CABE_CD_MUNICIPIO AS "COD_MUN",
                        CABE_NR_IM AS "IM",
                        CABE_NR_SUFRAMA AS "SUFRAMA",
                        CABE_TP_IND_PERFIL AS "IND_PERFIL",
                        CABE_TP_IND_ATIV AS "IND_ATIV",
                        CABE_CD_FIN AS "COD_FIN",
                        CABE_CD_VER AS "COD_VER"

                    FROM
                        EFD.TB_EFD_CABECALHO0000
                    WHERE
                        CABE_ID_0000 in {efd_id_0000}
                        """

    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0000 = pd.read_sql_query(query, con = connection, dtype = columns)
    
    # Assuring that dates are datetimes
    efd_0000['DT_INI'] = pd.to_datetime(efd_0000['DT_INI'])
    efd_0000['DT_FIN'] = pd.to_datetime(efd_0000['DT_FIN'])
    return efd_0000


@oracle_manager
def import_efd_0001(efd_id_0000) -> pd.DataFrame:
    """ 
    Função para importar o registro 0001
    
    Args
    ----------------------
    efd_id_0000: tuple
        Refere-se às chaves estrangeiras do registro 0000. Deve-se entregar uma tupla!
    
    """


    # Creating SQL query
    columns = { 
                        'CABE_ID_0000': int,
                        'CABE_ID_0001': int,
                        'IND_MOV': str
                        }


    assert len(efd_id_0000),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0000, 
                        CABE_ID_0001, 
                        CABE_TP_IND_MOV AS "IND_MOV"


                    FROM
                        EFD.TB_EFD_CABECALHO0001
                    WHERE
                        CABE_ID_0000 in {efd_id_0000}
                        """

    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0001 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0001


@oracle_manager
def import_efd_0002(efd_id_0001: tuple) -> pd.DataFrame:
    """ 
    Função para importar o registro 0002
    
    Args
    ----------------------
    efd_id_0001: tuple
        Refere-se às chaves estrangeiras do registro 0001. Deve-se entregar uma tupla!
    
    """


    # Creating SQL query
    columns = { 
                        'CABE_ID_0002': int,
                        'CABE_ID_0001': int,
                        'CLAS_ESTAB_IND': str
                        }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0001, 
                        CABE_ID_0002, 
                        CABE_CLAS_ESTAB_IND0002 AS "CLAS_ESTAB_IND"


                    FROM
                        EFD.TB_EFD_CABECALHO0002
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0002 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0002


@oracle_manager
def import_efd_0005(efd_id_0001) -> pd.DataFrame:
    """ 
    Função para importar o registro 0005.
    

    Args
    ----------------------
    efd_id_0001: tuple
        Refere-se às chaves estrangeiras do registro 0001. Deve-se entregar uma tupla!

    """


    # Creating SQL query
    columns = { 
                        'CABE_ID_0001': int,
                        'CABE_ID_0005': int,
                        'FANTASIA':str,
                        'CEP': str,
                        'END': str,
                        'NUM': str,
                        'COMPL': str,
                        'BAIRRO': str,
                        'FONE': str,
                        'FAX': str,
                        'EMAIL': str
                        }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0001,
                        CABE_ID_0005, 
                        CABE_NM_FANTASIA AS "FANTASIA", 
                        CABE_NR_CEP AS "CEP",
                        CABE_DS_END AS "END",
                        CABE_NR_NUM AS "NUM",
                        CABE_DS_COMPL AS "COMPL",
                        CABE_NM_BAIRRO AS "BAIRRO",
                        CABE_NR_FONE AS "FONE",
                        CABE_NR_FAX AS "FAX",
                        CABE_DS_EMAIL AS "EMAIL"


                    FROM
                        EFD.TB_EFD_CABECALHO0005
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0005 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0005


@oracle_manager
def import_efd_0015(efd_id_0001) -> pd.DataFrame:
    """ 
    Função para importar o registro 0015.
    

    Args
    ----------------------
    efd_id_0001: tuple
        Refere-se às chaves estrangeiras do registro 0001. Deve-se entregar uma tupla!

    """
    # Creating SQL query
    columns = { 
                        'CABE_ID_0001': int,
                        'CABE_ID_0015': int,
                        'UF_ST':str,
                        'IE_ST': str
                        }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0001, 
                        CABE_ID_0015,
                        CABE_SG_UF_ST AS "UF_ST", 
                        CABE_NR_IE_ST AS "IE_ST"


                    FROM
                        EFD.TB_EFD_CABECALHO0015
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0015 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0015


@oracle_manager
def import_efd_0100(efd_id_0001) -> pd.DataFrame:
    """ 
    Function to import the register 0100, having accountants informations for each EFD / enterprise.
    

    Args
    ----------------------
    efd_id_0001: tuple
        Refers to list of CABE_ID_0001 from EFD to import.
        It shouldn't be given more than 1.000 ID to be searched for.
    
    Return
    ----------------------
    efd_0100 pandas.DataFrame
        Dataframe containing the register 0100, with the following informations:
            1. NOME
            2. CPF
            3. CRC
            4. CNPJ
            5. CEP
            6. END
            7. NUM
            8. COMPL
            9. BAIRRO
            10. FONE
            11. FAX
            12. EMAIL
            13. COD_MUN

    """
    # Creating SQL query
    columns = { 
                        'CABE_ID_0001': int,
                        'CABE_ID_0100': int,
                        'NOME':str,
                        'CPF': str,
                        'CRC': str,
                        'CNPJ': str,
                        'CEP': str,
                        'END': str,
                        'NUM': str,
                        'COMPL': str,
                        'BAIRRO': str,
                        'FONE': str,
                        'FAX': str,
                        'EMAIL': str,
                        'COD_MUN': str
                        }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0001,
                        CABE_ID_0100,
                        CABE_NM_NOME100 AS "NOME",
                        CABE_NR_CPF100 AS "CPF",
                        CABE_NR_CR100 AS "CRC",
                        CABE_NR_CNPJ100 AS "CNPJ",
                        CABE_CD_CEP100 AS "CEP",
                        CABE_DS_END100 AS "END",
                        CABE_NR_NUM100 AS "NUM",
                        CABE_DS_COMPL100 AS "COMPL",
                        CABE_NM_BAIRRO100 AS "BAIRRO",
                        CABE_NR_FONE100 AS "FONE",
                        CABE_NR_FAX100 AS "FAX",
                        CABE_DS_EMAIL100 AS "EMAIL",
                        CABE_CD_COD_MUNC100 AS "COD_MUN"

                    FROM
                        EFD.TB_EFD_CABECALHO0100
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0100 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0100


@oracle_manager
def import_efd_0150(efd_id_0001) -> pd.DataFrame:
    """ 
    Function to import the register 0150, having enterprise partners information declared.
    

    Args
    ----------------------
    efd_id_0001: tuple
        Refers to list of CABE_ID_0001 from EFD to import.
        It shouldn't be given more than 1.000 ID to be searched for.
    
    Return
    ----------------------
    efd_0150 pandas.DataFrame
        Dataframe containing the register 0005, with the following informations:
            1. 'COD_PART',
            2. 'NOME',
            3. 'COD_PAIS',
            4. 'CNPJ',
            5. 'IE',
            6. 'COD_MUN',
            7. 'SUFRAMA',
            8. 'END',
            9. 'NUM',
            10. 'COMPL',
            11. 'BAIRRO'

    """


    # Creating SQL query
    columns = {         
                        'CABE_ID_0001': int,
                        'CABE_ID_0150': int,
                        'COD_PART':str,
                        'NOME': str,
                        'COD_PAIS': str,
                        'CNPJ': str,
                        'CPF': str,
                        'IE': str,
                        'COD_MUN': str,
                        'SUFRAMA': str,
                        'END': str,
                        'NUM': str,
                        'COMPL': str,
                        'BAIRRO': str
                    }


    assert len(efd_id_0001) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0150,
                        CABE_ID_0001, 
                        CABE_CD_PART150 AS "COD_PART", 
                        CABE_NM_NOME150 AS "NOME",
                        CABE_CD_PAIS150 AS "COD_PAIS",
                        CABE_NR_CNPJ150 AS "CNPJ",
                        CABE_NR_CPF150 AS "CPF",
                        CABE_NR_IE150 AS "IE",
                        CABE_CD_MUNICIPIO150 AS "COD_MUN",
                        CABE_NR_SUFRAMA150 AS "SUFRAMA",
                        CABE_DS_END150 AS "END",
                        CABE_NR_NUM150 AS "NUM",
                        CABE_DS_COMPL150 AS "COMPL",
                        CABE_NM_BAIRRO150 AS "BAIRRO"


                    FROM
                        EFD.TB_EFD_CABECALHO0150
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0150 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0150


@oracle_manager
def import_efd_0175(efd_id_0150) -> pd.DataFrame:
    """ 
    Função para importar o registro 0175.
    
    Args
    ----------------------
    efd_id_0150: tuple
        Refere-se às chaves estrangeiras oriundas do registro 0150 a ser pesquisado
    
    """
    # Creating SQL query
    columns = {         
                        'CABE_ID_0150': int,
                        'CABE_ID_0175': int,
                        #'DT_ALT':str,
                        'NR_CAMPO': str,
                        'CONT_ANT': str,
                    }

    assert len(efd_id_0150) <= 1000,\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0150)}"

    assert isinstance(efd_id_0150, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0150)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0150,
                        CABE_ID_0175, 
                        CABE_DT_ALT AS "DT_ALT", 
                        CABE_NR_CAMPO AS "NR_CAMPO",
                        CABE_DS_CONT_ANT AS "CONT_ANT"


                    FROM
                        EFD.TB_EFD_CABECALHO0175
                    WHERE
                        CABE_ID_0150 in {efd_id_0150}
                        """

    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0175 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0175


@oracle_manager
def import_efd_0190(efd_id_0001) -> pd.DataFrame:
    """ 
    Função para importar registro 0190
    
    Args
    ----------------------
    efd_id_0001: tuple
        Chave estrangeira do registro 0001
    

    """


    # Creating SQL query
    columns = { 
                'CABE_ID_0001': int,
                'CABE_ID_0190': int,
                'UNID':str,
                'DESCR': str
              }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0001,
                        CABE_ID_0190,

                        CABE_CD_UNID AS "UNID", 
                        CABE_DS_DESCR AS "DESCR"


                    FROM
                        EFD.TB_EFD_CABECALHO0190
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0190 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0190


@oracle_manager
def import_efd_0200(efd_id_0001) -> pd.DataFrame:
    """ 
    Função para importar registro 0200
    
    Args
    ----------------------
    efd_id_0001: tuple
        Chave estrangeira do registro 0001
    
    """


    # Creating SQL query
    columns = {
                'CABE_ID_0001': int,
                'CABE_ID_0200': int,
                'COD_ITEM':str,
                'DESCR_ITEM': str,
                'COD_BARRA': str,
                'COD_ANT_ITEM': str,
                'UNID_INV': str,
                'TIPO_ITEM': str,
                'COD_NCM': str,
                'EX_IPI': str,
                'COD_GEN': str,
                'COD_LST': str,
                'ALIQ_ICMS': float,
                'CEST': str
              }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0200,
                        CABE_ID_0001,
                        CABE_CD_ITEM AS "COD_ITEM",
                        CABE_DS_ITEM AS "DESCR_ITEM",
                        CABE_CD_BARRA AS "COD_BARRA",
                        CABE_CD_ANT_ITEM AS "COD_ANT_ITEM",
                        CABE_MD_INV AS "UNID_INV",
                        CABE_TP_ITEM AS "TIPO_ITEM",
                        CABE_CD_NCM AS "COD_NCM",
                        CABE_CD_EX_IPI AS "EX_IPI",
                        CABE_CD_GEN AS "COD_GEN",
                        CABE_CD_LST AS "COD_LST",
                        CABE_PC_ALIQ_ICMS AS "ALIQ_ICMS",
                        CABE_CD_CEST AS "CEST"

                    FROM
                        EFD.TB_EFD_CABECALHO0200
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0200 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0200


@oracle_manager
def import_efd_0205(efd_id_0200) -> pd.DataFrame:
    """ 
    Função para importar registro 0205
    
    Args
    ----------------------
    efd_id_0200: tuple
        Chave estrangeira do registro 0200
    
    """


    # Creating SQL query
    columns = {
                'CABE_ID_0200': int,
                'CABE_ID_0205': int,
                'DESCR_ANT_ITEM': str,
                'COD_ANT_ITEM': str
              }
    
    date_columns = ['DT_INI', 'DT_FIM']


    assert len(efd_id_0200),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0200)}"

    assert isinstance(efd_id_0200, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0200)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0200,
                        CABE_ID_0205,
                        CABE_DS_ANT_ITEM205 AS "DESCR_ANT_ITEM",
                        CABE_DT_INI205 AS "DT_INI",
                        CABE_DT_FIM205 AS "DT_FIM",
                        CABE_CD_ANT_ITEM205 AS "COD_ANT_ITEM"

                    FROM
                        EFD.TB_EFD_CABECALHO0205
                    WHERE
                        CABE_ID_0200 in {efd_id_0200}
                        """
    
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0205 = pd.read_sql_query(query, con = connection, dtype = columns)

    # Assuring that DT_INI and DT_FIM are datetime
    for column in date_columns:
        efd_0205[column] = pd.to_datetime(efd_0205[column])

    return efd_0205


@oracle_manager
def import_efd_0206(efd_id_0200) -> pd.DataFrame:
    """ 
    Função para importar registro 0206
    
    Args
    ----------------------
    efd_id_0200: tuple
        Chave estrangeira do registro 0200
    
    """


    # Creating SQL query
    columns = {
                'CABE_ID_0200': int,
                'CABE_ID_0206': int,
                'COD_COMB': str
              }
    

    assert len(efd_id_0200),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0200)}"

    assert isinstance(efd_id_0200, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0200)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0200,
                        CABE_ID_0206,
                        CABE_CD_COMB AS "COD_COMB"

                    FROM
                        EFD.TB_EFD_CABECALHO0206
                    WHERE
                        CABE_ID_0200 in {efd_id_0200}
                        """
    
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0206 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0206

@oracle_manager
def import_efd_0210(efd_id_0200) -> pd.DataFrame:
    """ 
    Função para importar registro 0210
    
    Args
    ----------------------
    efd_id_0200: tuple
        Chave estrangeira do registro 0200
    
    """


    # Creating SQL query
    columns = {
                'CABE_ID_0200': int,
                'CABE_ID_0210': int,
                'COD_ITEM_COMP': str,
                'QTD_COMP': float,
                'PERDA': float
              }
    


    assert len(efd_id_0200),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0200)}"

    assert isinstance(efd_id_0200, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0200)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0200,
                        CABE_ID_0210,
                        CABE_CD_ITEM_COMP AS "COD_ITEM_COMP",
                        CABE_QT_COMP AS "QTD_COMP",
                        CABE_NR_PERDA AS "PERDA"

                    FROM
                        EFD.TB_EFD_CABECALHO0210
                    WHERE
                        CABE_ID_0200 in {efd_id_0200}
                        """
    
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0210 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0210


@oracle_manager
def import_efd_0220(efd_id_0200) -> pd.DataFrame:
    """ 
    Função para importar o registro 0220
    

    Args
    ----------------------
    efd_id_0200: tuple
        Chave estrangeira do registro 0200
    
    Return
    ----------------------
    efd_0220 pandas.DataFrame
        Dataframe containing the register 0220, with the following informations:
            1. UNID_CONV
            2. FAT_CONV
    """

    # Creating SQL query
    columns = {
                'CABE_ID_0220': int,
                'CABE_ID_0200': int,
                'UNID_CONV': str,
                'FAT_CONV': float,
                'COD_BARRA': str
              }


    assert len(efd_id_0200),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0200)}"

    assert isinstance(efd_id_0200, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0200)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0200,
                        CABE_ID_0220,
                        CABE_UNID_CONV AS "UNID_CONV",
                        CABE_NR_FAT_CONV AS "FAT_CONV",
                        CABE_CD_COD_BARRA AS "COD_BARRA"
                    FROM
                        EFD.TB_EFD_CABECALHO0220
                    WHERE
                        CABE_ID_0200 in {efd_id_0200}
                        """

    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0220 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0220


@oracle_manager
def import_efd_0221(efd_id_0200) -> pd.DataFrame:
    """ 
    Função para importar o registro 0221
    
    Args
    ----------------------
    efd_id_0200: tuple
        Chave estrangeira do registro 0200
  
    """

    # Creating SQL query
    columns = {
                'CABE_ID_0221': int,
                'CABE_ID_0200': int,
                'COD_ITEM_ATOMICO': str,
                'QTD_CONTIDA': float
              }


    assert len(efd_id_0200),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0200)}"

    assert isinstance(efd_id_0200, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0200)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0200,
                        CABE_ID_0221,
                        CABE_COD_ITEM_ATOMICO0221 AS "COD_ITEM_ATOMICO",
                        CABE_QTD_CONTIDA0221 AS "QTD_CONTIDA"
                    FROM
                        EFD.TB_EFD_CABECALHO0221
                    WHERE
                        CABE_ID_0200 in {efd_id_0200}
                        """

    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0221 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0221


@oracle_manager
def import_efd_0300(efd_id_0001) -> pd.DataFrame:
    """ 
    Função para importar registro 0300.
    
    Args
    ----------------------
    efd_id_0001: tuple
        Chave estrangeira do registro 0001
    
    """

    # Creating SQL query
    columns = {
                    'CABE_ID_0001': int,
                    'CABE_ID_0300': int,
                    'COD_IND_BEM': str,
                    'IDENT_MERC': str,
                    'DESCR_ITEM': str,
                    'COD_PRNC': str,
                    'COD_CTA': str,
                    'NR_PARC': int
                }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0001,
                        CABE_ID_0300,
                        CABE_CD_IND_BEM300 AS "COD_IND_BEM",
                        CABE_LG_IDENT_MERC300 AS "IDENT_MERC",
                        CABE_DS_ITEM300 AS "DESCR_ITEM",
                        CABE_CD_PRNC300 AS "COD_PRNC",
                        CABE_CD_CTA300 AS "COD_CTA",
                        CABE_NR_PARC300 AS "NR_PARC"

                    FROM
                        EFD.TB_EFD_CABECALHO0300
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """
    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0300 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0300


@oracle_manager
def import_efd_0305(efd_id_0300: tuple) -> pd.DataFrame:
    """ 
    Função para importar registro 0305.
    

    Args
    ----------------------
    efd_id_0300: tuple
        Chave estrangeira do registro 0300
    

    """

    # Creating SQL query
    columns = {
                    'CABE_ID_0300': int,
                    'CABE_ID_0305':int,
                    'COD_CCUS': str,
                    'FUNC': str,
                    'VIDA_UTIL': int
                }


    assert len(efd_id_0300),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0300)}"

    assert isinstance(efd_id_0300, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0300)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        
                        CABE_ID_0300,
                        CABE_ID_0305,
                        CABE_CD_CCUS AS "COD_CCUS",
                        CABE_DS_FUNC AS "FUNC",
                        CABE_NR_VIDA_UTIL AS "VIDA_UTIL"

                    FROM
                        EFD.TB_EFD_CABECALHO0305
                    WHERE
                        CABE_ID_0300 in {efd_id_0300}
                        """

    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0305 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0305


@oracle_manager
def import_efd_0400(efd_id_0001) -> pd.DataFrame:
    """ 
    Função para importar o registro 0400
    
    Args
    ----------------------
    efd_id_0001: tuple
        Chave estrangeira do registro 0001
    

    """


    # Creating SQL query
    columns = {
                    'CABE_ID_0400': int,
                    'CABE_ID_0001': int,
                    'COD_NAT': object,
                    'DESCR_NAT': object
                }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0001,
                        CABE_ID_0400,
                        CABE_CD_NAT AS "COD_NAT",
                        CABE_DS_NAT AS "DESCR_NAT"
                    FROM
                        EFD.TB_EFD_CABECALHO0400
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0400 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0400


@oracle_manager
def import_efd_0450(efd_id_0001: tuple) -> pd.DataFrame:
    """ 
    Função para importar registro 0450
    
    Args
    ----------------------
    efd_id_0001: tuple
        Chave estrangeira do registro 0001

    """


    # Creating SQL query
    columns = {
                    'CABE_ID_0450': int,
                    'CABE_ID_0001': int,
                    'COD_INF': object,
                    'TXT': object
                }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0001,
                        CABE_ID_0450,
                        
                        CABE_CD_INF AS "COD_INF",
                        CABE_DS_TXT AS "TXT"

                    FROM
                        EFD.TB_EFD_CABECALHO0450
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0450 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0450


@oracle_manager
def import_efd_0460(efd_id_0001) -> pd.DataFrame:
    """ 
    Função para importar registro 0460
    

    Args
    ----------------------
    efd_id_0001: tuple
        Chave estrangeira do registro 0001
    
    """


    # Creating SQL query
    columns = {
                'CABE_ID_0001': int,
                'CABE_ID_0460': int,
                'COD_OBS': str,
                'TXT': str
              }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0001,
                        CABE_ID_0460,
                        CABE_CD_OBS460 AS "COD_OBS",
                        CABE_DS_TEXT460 AS "TXT"

                    FROM
                        EFD.TB_EFD_CABECALHO0460
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0460 = pd.read_sql_query(query, con = connection, dtype = columns)

    return efd_0460




@oracle_manager
def import_efd_0500(efd_id_0001: tuple) -> pd.DataFrame:
    """ 
    Função para importar registro 0500
    
    Args
    ----------------------
    efd_id_0001: tuple
        Chave estrangeira do registro 0001
    
    """


    # Creating SQL query
    columns = {
                    'CABE_ID_0001':object,
                    'CABE_ID_0500':object,
                    'COD_NAT_CC': object,
                    'IND_CTA': object,
                    'NIVEL': int,
                    'COD_CTA': object,
                    'NOME_CTA': object
                }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0001,
                        CABE_ID_0500,
                        CABE_DT_ALT500 AS "DT_ALT",
                        CABE_CD_NAT_CC500 AS "COD_NAT_CC",
                        CABE_LG_IND_CTA500 AS "IND_CTA",
                        CABE_NR_NIVEL500 AS "NIVEL",
                        CABE_CD_CTA500 AS "COD_CTA",
                        CABE_NM_CTA500 AS "NOME_CTA"


                    FROM
                        EFD.TB_EFD_CABECALHO0500
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0500 = pd.read_sql_query(query, con = connection, dtype = columns)

    # Assuring DT_ALT is datetime
    efd_0500['DT_ALT'] = pd.to_datetime(efd_0500['DT_ALT'])

    return efd_0500




@oracle_manager
def import_efd_0600(efd_id_0001: tuple) -> pd.DataFrame:
    """ 
    Função para importar registro 0600
    
    Args
    ----------------------
    efd_id_0001: tuple
        Chave estrangeira do registro 0001
    
    """


    # Creating SQL query
    columns = {
                    'CABE_ID_0001': int,
                    'CABE_ID_0600':int,
                    'COD_CCUS': str,
                    'CCUS': str
                }


    assert len(efd_id_0001),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0001)}"

    assert isinstance(efd_id_0001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0001)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0001,
                        CABE_ID_0600,
                        CABE_DT_ALT600 AS "DT_ALT",
                        CABE_CD_CCUS600 AS "COD_CCUS",
                        CABE_NM_CCUS600 AS "CCUS"

                    FROM
                        EFD.TB_EFD_CABECALHO0600
                    WHERE
                        CABE_ID_0001 in {efd_id_0001}
                        """

    
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0600 = pd.read_sql_query(query, con = connection, dtype = columns)

    # Assuring DT_ALT is datetime
    efd_0600['DT_ALT'] = pd.to_datetime(efd_0600['DT_ALT'])

    return efd_0600


@oracle_manager
def import_efd_0990(efd_id_0000: tuple) -> pd.DataFrame:
    """ 
    Função para importar registro 0990
    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    
    """
    # Creating SQL query
    columns = { 
                'CABE_ID_0000': int,
                'CABE_ID_0990': int,
                'QTD_LIN_0': int,
                }


    assert len(efd_id_0000),\
        f"Limit has been exceded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # It uses initial date as the reference date (in format MM/YYYY)
    query = f""" 
                    SELECT
                        CABE_ID_0000, 
                        CABE_ID_0990, 
                        CABE_QT_LIN_0 AS "QTD_LIN_0"

                    FROM
                        EFD.TB_EFD_CABECALHO0990
                    WHERE
                        CABE_ID_0000 in {efd_id_0000}
                        """

    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        efd_0990 = pd.read_sql_query(query, con = connection, dtype = columns)
    
    return efd_0990
