# Third-party modules import
import pandas as pd 
import oracledb 

# User-defined modules import
from .utils import oracle_manager
from .usuario import dns_tns, pw, user


@oracle_manager
def import_efd_K001(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros K001    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {         
                'PROD_ID_K001': int,
                'CABE_ID_0000': int,
                'IND_MOV': str
            }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # SQL query to fetch K001 data
    query = f""" 
                SELECT
                    PROD_ID_K001,
                    CABE_ID_0000,
                    PROD_CD_IND_MOV AS "IND_MOV"
                FROM
                    EFD.TB_EFD_PRODUCAOK001
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K001 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K001


@oracle_manager
def import_efd_K010(efd_id_001) -> pd.DataFrame:
    """ 
    Importa os registros K010    
    Args
    ----------------------
    efd_id_001: tuple
        Chave estrangeira do registro K001
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K001': int,
                        'PROD_ID_K001': int,
                        'IND_TP_LEIAUTE': str
            }

    assert len(efd_id_001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_001)}"

    assert isinstance(efd_id_001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_001)}"

    # SQL query to fetch K010 data
    query = f""" 
                SELECT
                    PROD_ID_010 AS "PROD_ID_K010",
                    PROD_ID_001 AS "PROD_ID_K001",
                    PROD_IND_TP_LEIAUTE010 AS "IND_TP_LEIAUTE"
                FROM
                    EFD.TB_EFD_PRODUCAOK010
                WHERE
                    PROD_ID_001 in {efd_id_001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K010 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K010


@oracle_manager
def import_efd_K100(efd_id_K001) -> pd.DataFrame:
    """ 
    Importa os registros K100    
    Args
    ----------------------
    efd_id_K001: tuple
        Chave estrangeira do registro K001
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K100': int,
                        'PROD_ID_K001': int,
                        'DT_INI': 'datetime64[ns]',
                        'DT_FIN': 'datetime64[ns]'
            }

    assert len(efd_id_K001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K001)}"

    assert isinstance(efd_id_K001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K001)}"

    # SQL query to fetch K100 data
    query = f""" 
                SELECT
                    PROD_ID_K100,
                    PROD_ID_K001,
                    PROD_DT_INIK100 AS "DT_INI",
                    PROD_DT_FINK100 AS "DT_FIN"
                FROM
                    EFD.TB_EFD_PRODUCAOK100
                WHERE
                    PROD_ID_K001 in {efd_id_K001}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K100 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K100



@oracle_manager
def import_efd_K200(efd_id_K100) -> pd.DataFrame:
    """ 
    Importa os registros K200    
    Args
    ----------------------
    efd_id_K100: tuple
        Chave estrangeira do registro K100
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K200': int,
                        'PROD_ID_K100': int,
                        'DT_EST': 'datetime64[ns]',
                        'COD_ITEM': str,
                        'QTD': float,
                        'IND_EST': str,
                        'COD_PART': str
            }

    assert len(efd_id_K100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K100)}"

    assert isinstance(efd_id_K100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K100)}"

    # SQL query to fetch K200 data
    query = f""" 
                SELECT
                    PROD_ID_K200,
                    PROD_ID_K100,
                    PROD_DT_ESTK200 AS "DT_EST",
                    PROD_CD_ITEMK200 AS "COD_ITEM",
                    PROD_VL_QTDK200 AS "QTD",
                    PROD_CD_IND_ESTK200 AS "IND_EST",
                    PROD_CD_PARTK200 AS "COD_PART"
                FROM
                    EFD.TB_EFD_PRODUCAOK200
                WHERE
                    PROD_ID_K100 in {efd_id_K100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K200 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K200


@oracle_manager
def import_efd_K210(efd_id_K100) -> pd.DataFrame:
    """ 
    Importa os registros K210    
    Args
    ----------------------
    efd_id_K100: tuple
        Chave estrangeira do registro K100
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K210': int,
                        'PROD_ID_K100': int,
                        'DT_INI_OS': 'datetime64[ns]',
                        'DT_FIN_OS': 'datetime64[ns]',
                        'COD_DOC_OS': str,
                        'COD_ITEM_ORI': str,
                        'QTD_ORI': float
            }

    assert len(efd_id_K100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K100)}"

    assert isinstance(efd_id_K100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K100)}"

    # SQL query to fetch K210 data
    query = f""" 
                SELECT
                    PROD_ID_K210,
                    PROD_ID_K100,
                    PROD_DT_INI_OS210 AS "DT_INI_OS",
                    PROD_DT_FIN_OS210 AS "DT_FIN_OS",
                    PROD_CD_DOC_OS210 AS "COD_DOC_OS",
                    PROD_CD_ITEM_ORI210 AS "COD_ITEM_ORI",
                    PROD_QT_ORI210 AS "QTD_ORI"
                FROM
                    EFD.TB_EFD_PRODUCAOK210
                WHERE
                    PROD_ID_K100 in {efd_id_K100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K210 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K210


@oracle_manager
def import_efd_K215(efd_id_K210) -> pd.DataFrame:
    """ 
    Importa os registros K215    
    Args
    ----------------------
    efd_id_K210: tuple
        Chave estrangeira do registro K210
    """

    # Creating SQL query
    columns = {         
                'PROD_ID_K215': int,
                'PROD_ID_K210': int,
                'COD_ITEM_DES': str,
                'QTD_DES': float
            }

    assert len(efd_id_K210),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K210)}"

    assert isinstance(efd_id_K210, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K210)}"

    # SQL query to fetch K215 data
    query = f""" 
                SELECT
                    PROD_ID_K215,
                    PROD_ID_K210,
                    PROD_CD_ITEM_DES215 AS "COD_ITEM_DES",
                    PROD_QT_DES215 AS "QTD_DES"
                FROM
                    EFD.TB_EFD_PRODUCAOK215
                WHERE
                    PROD_ID_K210 in {efd_id_K210}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K215 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K215


@oracle_manager
def import_efd_K220(efd_id_K100) -> pd.DataFrame:
    """ 
    Importa os registros K220    
    Args
    ----------------------
    efd_id_K100: tuple
        Chave estrangeira do registro K100
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K220': int,
                        'PROD_ID_K100': int,
                        'DT_MOV': 'datetime64[ns]',
                        'COD_ITEM_ORI': str,
                        'COD_ITEM_DEST': str,
                        'QTD_ORI': float,
                        'QTD_DEST': float
            }

    assert len(efd_id_K100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K100)}"

    assert isinstance(efd_id_K100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K100)}"

    # SQL query to fetch K220 data
    query = f""" 
                SELECT
                    PROD_ID_K220,
                    PROD_ID_K100,
                    PROD_DT_MOV220 AS "DT_MOV",
                    PROD_CD_ITEM_ORI220 AS "COD_ITEM_ORI",
                    PROD_CD_ITEM_DEST220 AS "COD_ITEM_DEST",
                    PROD_QT_ORI220 AS "QTD_ORI",
                    PROD_QT_DEST220 AS "QTD_DEST"
                FROM
                    EFD.TB_EFD_PRODUCAOK220
                WHERE
                    PROD_ID_K100 in {efd_id_K100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K220 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K220


@oracle_manager
def import_efd_K230(efd_id_K100) -> pd.DataFrame:
    """ 
    Importa os registros K230    
    Args
    ----------------------
    efd_id_K100: tuple
        Chave estrangeira do registro K100
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K230': int,
                        'PROD_ID_K100': int,
                        'DT_INI_OP': 'datetime64[ns]',
                        'DT_FIN_OP': 'datetime64[ns]',
                        'COD_DOC_OP': str,
                        'COD_ITEM': str,
                        'QTD_ENC': float
            }

    assert len(efd_id_K100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K100)}"

    assert isinstance(efd_id_K100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K100)}"

    # SQL query to fetch K230 data
    query = f""" 
                SELECT
                    PROD_ID_K230,
                    PROD_ID_K100,
                    PROD_DT_INI_OP230 AS "DT_INI_OP",
                    PROD_DT_FIN_OP230 AS "DT_FIN_OP",
                    PROD_CD_DOC_OP230 AS "COD_DOC_OP",
                    PROD_CD_ITEM230 AS "COD_ITEM",
                    PROD_QT_ENC230 AS "QTD_ENC"
                FROM
                    EFD.TB_EFD_PRODUCAOK230
                WHERE
                    PROD_ID_K100 in {efd_id_K100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K230 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K230


@oracle_manager
def import_efd_K235(efd_id_K230) -> pd.DataFrame:
    """ 
    Importa os registros K235    
    Args
    ----------------------
    efd_id_K230: tuple
        Chave estrangeira do registro K230
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K235': int,
                        'PROD_ID_K230': int,
                        'DT_SAÍDA': 'datetime64[ns]',
                        'COD_ITEM': str,
                        'QTD': float,
                        'COD_INS_SUBST': str
            }

    assert len(efd_id_K230),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K230)}"

    assert isinstance(efd_id_K230, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K230)}"

    # SQL query to fetch K235 data
    query = f""" 
                SELECT
                    PROD_ID_K235,
                    PROD_ID_K230,
                    PROD_DT_SAIDA235 AS "DT_SAÍDA",
                    PROD_CD_ITEM235 AS "COD_ITEM",
                    PROD_QT235 AS "QTD",
                    PROD_CD_INS_SUBST235 AS "COD_INS_SUBST"
                FROM
                    EFD.TB_EFD_PRODUCAOK235
                WHERE
                    PROD_ID_K230 in {efd_id_K230}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K235 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K235




@oracle_manager
def import_efd_K250(efd_id_K100) -> pd.DataFrame:
    """ 
    Importa os registros K250    
    Args
    ----------------------
    efd_id_K100: tuple
        Chave estrangeira do registro K100
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K250': int,
                        'PROD_ID_K100': int,
                        'DT_PROD': str,
                        'COD_ITEM': str,
                        'QTD': float
            }

    assert len(efd_id_K100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K100)}"

    assert isinstance(efd_id_K100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K100)}"

    # SQL query to fetch K250 data
    query = f""" 
                SELECT
                    PROD_ID_K250,
                    PROD_ID_K100,
                    PROD_DT_PROD250 AS "DT_PROD",
                    PROD_CD_ITEM250 AS "COD_ITEM",
                    PROD_QT250 AS "QTD"
                FROM
                    EFD.TB_EFD_PRODUCAOK250
                WHERE
                    PROD_ID_K100 in {efd_id_K100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K250 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K250


@oracle_manager
def import_efd_K255(efd_id_K250) -> pd.DataFrame:
    """ 
    Importa os registros K255    
    Args
    ----------------------
    efd_id_K250: tuple
        Chave estrangeira do registro K250
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K255': int,
                        'PROD_ID_K250': int,
                        'DT_CONS': 'datetime64[ns]',
                        'COD_ITEM': str,
                        'QTD': float,
                        'COD_INS_SUBST': str
            }

    assert len(efd_id_K250),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K250)}"

    assert isinstance(efd_id_K250, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K250)}"

    # SQL query to fetch K255 data
    query = f""" 
                SELECT
                    PROD_ID_K255,
                    PROD_ID_K250,
                    PROD_DT_CONS255 AS "DT_CONS",
                    PROD_CD_ITEM255 AS "COD_ITEM",
                    PROD_QT255 AS "QTD",
                    PROD_CD_INS_SUBST255 AS "COD_INS_SUBST"
                FROM
                    EFD.TB_EFD_PRODUCAOK255
                WHERE
                    PROD_ID_K250 in {efd_id_K250}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K255 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K255


@oracle_manager
def import_efd_K260(efd_id_K100) -> pd.DataFrame:
    """ 
    Importa os registros K260    
    Args
    ----------------------
    efd_id_K100: tuple
        Chave estrangeira do registro K100
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K260': int,
                        'PROD_ID_K100': int,
                        'COD_OP_OS': str,
                        'COD_ITEM': str,
                        'DT_SAÍDA': 'datetime64[ns]',
                        'QTD_SAÍDA': float,
                        'DT_RET': 'datetime64[ns]',
                        'QTD_RET': float
            }

    assert len(efd_id_K100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K100)}"

    assert isinstance(efd_id_K100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K100)}"

    # SQL query to fetch K260 data
    query = f""" 
                SELECT
                    PROD_ID_K260,
                    PROD_ID_K100,
                    PROD_CD_OP_OS260 AS "COD_OP_OS",
                    PROD_CD_ITEM260 AS "COD_ITEM",
                    PROD_DT_SAIDA260 AS "DT_SAÍDA",
                    PROD_QT_SAIDA260 AS "QTD_SAÍDA",
                    PROD_DT_RET260 AS "DT_RET",
                    PROD_QT_RET260 AS "QTD_RET"
                FROM
                    EFD.TB_EFD_PRODUCAOK260
                WHERE
                    PROD_ID_K100 in {efd_id_K100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K260 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K260



@oracle_manager
def import_efd_K265(efd_id_K260) -> pd.DataFrame:
    """ 
    Importa os registros K265    
    Args
    ----------------------
    efd_id_K260: tuple
        Chave estrangeira do registro K260
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K265': int,
                        'PROD_ID_K260': int,
                        'COD_ITEM': str,
                        'QTD_CONS': float,
                        'QTD_RET': float
            }

    assert len(efd_id_K260),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K260)}"

    assert isinstance(efd_id_K260, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K260)}"

    # SQL query to fetch K265 data
    query = f""" 
                SELECT
                    PROD_ID_K265,
                    PROD_ID_K260,
                    PROD_CD_ITEM265 AS "COD_ITEM",
                    PROD_QT_CONS265 AS "QTD_CONS",
                    PROD_QT_RET265 AS "QTD_RET"
                FROM
                    EFD.TB_EFD_PRODUCAOK265
                WHERE
                    PROD_ID_K260 in {efd_id_K260}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K265 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K265


@oracle_manager
def import_efd_K270(efd_id_K100) -> pd.DataFrame:
    """ 
    Importa os registros K270    
    Args
    ----------------------
    efd_id_K100: tuple
        Chave estrangeira do registro K100
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K270': int,
                        'PROD_ID_K100': int,
                        'DT_INI_AP': 'datetime64[ns]',
                        'DT_FIN_AP': 'datetime64[ns]',
                        'COD_OP_OS': str,
                        'COD_ITEM': str,
                        'QTD_COR_POS': float,
                        'QTD_COR_NEG': float,
                        'ORIGEM': str
            }

    assert len(efd_id_K100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K100)}"

    assert isinstance(efd_id_K100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K100)}"

    # SQL query to fetch K270 data
    query = f""" 
                SELECT
                    PROD_ID_K270,
                    PROD_ID_K100,
                    PROD_DT_INI_AP270 AS "DT_INI_AP",
                    PROD_DT_FIN_AP270 AS "DT_FIN_AP",
                    PROD_CD_OP_OS270 AS "COD_OP_OS",
                    PROD_CD_ITEM270 AS "COD_ITEM",
                    PROD_QT_COR_POS270 AS "QTD_COR_POS",
                    PROD_QT_COR_NEG270 AS "QTD_COR_NEG",
                    PROD_CD_ORIGEM270 AS "ORIGEM"
                FROM
                    EFD.TB_EFD_PRODUCAOK270
                WHERE
                    PROD_ID_K100 in {efd_id_K100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K270 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K270


@oracle_manager
def import_efd_K275(efd_id_K270) -> pd.DataFrame:
    """ 
    Importa os registros K275    
    Args
    ----------------------
    efd_id_K270: tuple
        Chave estrangeira do registro K270
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K275': int,
                        'PROD_ID_K270': int,
                        'COD_ITEM': str,
                        'QTD_COR_POS': float,
                        'QTD_COR_NEG': float,
                        'COD_INS_SUBST': str
            }

    assert len(efd_id_K270),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K270)}"

    assert isinstance(efd_id_K270, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K270)}"

    # SQL query to fetch K275 data
    query = f""" 
                SELECT
                    PROD_ID_K275,
                    PROD_ID_K270,
                    PROD_CD_ITEM275 AS "COD_ITEM",
                    PROD_QT_COR_POS275 AS "QTD_COR_POS",
                    PROD_QT_COR_NEG275 AS "QTD_COR_NEG",
                    PROD_CD_INS_SUBST275 AS "COD_INS_SUBST"
                FROM
                    EFD.TB_EFD_PRODUCAOK275
                WHERE
                    PROD_ID_K270 in {efd_id_K270}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K275 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K275






@oracle_manager
def import_efd_K280(efd_id_K100) -> pd.DataFrame:
    """ 
    Importa os registros K280    
    Args
    ----------------------
    efd_id_K100: tuple
        Chave estrangeira do registro K100
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K280': int,
                        'PROD_ID_K100': int,
                        'DT_EST': 'datetime64[ns]',
                        'COD_ITEM': str,
                        'QTD_COR_POS': float,
                        'QTD_COR_NEG': float,
                        'IND_EST': str,
                        'COD_PART': str
            }

    assert len(efd_id_K100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K100)}"

    assert isinstance(efd_id_K100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K100)}"

    # SQL query to fetch K280 data
    query = f""" 
                SELECT
                    PROD_ID_K280,
                    PROD_ID_K100,
                    PROD_DT_EST280 AS "DT_EST",
                    PROD_CD_ITEM280 AS "COD_ITEM",
                    PROD_QT_COR_POS280 AS "QTD_COR_POS",
                    PROD_QT_COR_NEG280 AS "QTD_COR_NEG",
                    PROD_CD_IND_EST280 AS "IND_EST",
                    PROD_CD_PART280 AS "COD_PART"
                FROM
                    EFD.TB_EFD_PRODUCAOK280
                WHERE
                    PROD_ID_K100 in {efd_id_K100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K280 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K280


@oracle_manager
def import_efd_K290(efd_id_K100) -> pd.DataFrame:
    """ 
    Importa os registros K290    
    Args
    ----------------------
    efd_id_K100: tuple
        Chave estrangeira do registro K100
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K290': int,
                        'PROD_ID_K100': int,
                        'DT_INI_OP': 'datetime64[ns]',
                        'DT_FIN_OP': 'datetime64[ns]',
                        'COD_DOC_OP': str
            }

    assert len(efd_id_K100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K100)}"

    assert isinstance(efd_id_K100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K100)}"

    # SQL query to fetch K290 data
    query = f""" 
                SELECT
                    PROD_ID_K290,
                    PROD_ID_K100,
                    PROD_DT_INI_OP290 AS "DT_INI_OP",
                    PROD_DT_FIN_OP290 AS "DT_FIN_OP",
                    PROD_CD_DOC_OP290 AS "COD_DOC_OP"
                FROM
                    EFD.TB_EFD_PRODUCAOK290
                WHERE
                    PROD_ID_K100 in {efd_id_K100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K290 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K290



@oracle_manager
def import_efd_K291(efd_id_K290) -> pd.DataFrame:
    """ 
    Importa os registros K291    
    Args
    ----------------------
    efd_id_K290: tuple
        Chave estrangeira do registro K290
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K291': int,
                        'PROD_ID_K290': int,
                        'COD_ITEM': str,
                        'QTD': float
            }

    assert len(efd_id_K290),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K290)}"

    assert isinstance(efd_id_K290, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K290)}"

    # SQL query to fetch K291 data
    query = f""" 
                SELECT
                    PROD_ID_K291,
                    PROD_ID_K290,
                    PROD_CD_ITEM291 AS "COD_ITEM",
                    PROD_QT291 AS "QTD"
                FROM
                    EFD.TB_EFD_PRODUCAOK291
                WHERE
                    PROD_ID_K290 in {efd_id_K290}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K291 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K291


@oracle_manager
def import_efd_K292(efd_id_K290) -> pd.DataFrame:
    """ 
    Importa os registros K292    
    Args
    ----------------------
    efd_id_K290: tuple
        Chave estrangeira do registro K290
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K292': int,
                        'PROD_ID_K290': int,
                        'COD_ITEM': str,
                        'QTD': float
            }

    assert len(efd_id_K290),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K290)}"

    assert isinstance(efd_id_K290, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K290)}"

    # SQL query to fetch K292 data
    query = f""" 
                SELECT
                    PROD_ID_K292,
                    PROD_ID_K290,
                    PROD_CD_ITEM292 AS "COD_ITEM",
                    PROD_QT292 AS "QTD"
                FROM
                    EFD.TB_EFD_PRODUCAOK292
                WHERE
                    PROD_ID_K290 in {efd_id_K290}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K292 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K292


@oracle_manager
def import_efd_K300(efd_id_K100) -> pd.DataFrame:
    """ 
    Importa os registros K300    
    Args
    ----------------------
    efd_id_K100: tuple
        Chave estrangeira do registro K100
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K300': int,
                        'PROD_ID_K100': int,
                        'DT_PROD': 'datetime64[ns]'
            }

    assert len(efd_id_K100),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K100)}"

    assert isinstance(efd_id_K100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K100)}"

    # SQL query to fetch K300 data
    query = f""" 
                SELECT
                    PROD_ID_K300,
                    PROD_ID_K100,
                    PROD_DT_PROD300 AS "DT_PROD"
                FROM
                    EFD.TB_EFD_PRODUCAOK300
                WHERE
                    PROD_ID_K100 in {efd_id_K100}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K300 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K300


@oracle_manager
def import_efd_K301(efd_id_K300) -> pd.DataFrame:
    """ 
    Importa os registros K301    
    Args
    ----------------------
    efd_id_K300: tuple
        Chave estrangeira do registro K300
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K301': int,
                        'PROD_ID_K300': int,
                        'COD_ITEM': str,
                        'QTD': float
            }

    assert len(efd_id_K300),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K300)}"

    assert isinstance(efd_id_K300, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K300)}"

    # SQL query to fetch K301 data
    query = f""" 
                SELECT
                    PROD_ID_K301,
                    PROD_ID_K300,
                    PROD_CD_ITEM301 AS "COD_ITEM",
                    PROD_QT301 AS "QTD"
                FROM
                    EFD.TB_EFD_PRODUCAOK301
                WHERE
                    PROD_ID_K300 in {efd_id_K300}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K301 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K301


@oracle_manager
def import_efd_K302(efd_id_K300) -> pd.DataFrame:
    """ 
    Importa os registros K302    
    Args
    ----------------------
    efd_id_K300: tuple
        Chave estrangeira do registro K300
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K302': int,
                        'PROD_ID_K300': int,
                        'COD_ITEM': str,
                        'QTD': float
            }

    assert len(efd_id_K300),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_K300)}"

    assert isinstance(efd_id_K300, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_K300)}"

    # SQL query to fetch K302 data
    query = f""" 
                SELECT
                    PROD_ID_K302,
                    PROD_ID_K300,
                    PROD_CD_ITEM302 AS "COD_ITEM",
                    PROD_QT302 AS "QTD"
                FROM
                    EFD.TB_EFD_PRODUCAOK302
                WHERE
                    PROD_ID_K300 in {efd_id_K300}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K302 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K302



@oracle_manager
def import_efd_K990(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros K990    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """

    # Creating SQL query
    columns = {         
                        'PROD_ID_K990': int,
                        'CABE_ID_0000': int,
                        'QTD_LIN_K': int
            }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # SQL query to fetch K990 data
    query = f""" 
                SELECT
                    PROD_ID_K990,
                    CABE_ID_0000,
                    PROD_QT_LIN_K990 AS "QTD_LIN_K"
                FROM
                    EFD.TB_EFD_PRODUCAOK990
                WHERE
                    CABE_ID_0000 in {efd_id_0000}
            """

    # Connecting to the database and fetching data
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        efd_K990 = pd.read_sql_query(query, con=connection, dtype=columns)

    return efd_K990
