# Third-party modules import
import pandas as pd 

from ...EFDLocal.local_database_maker.models import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func



# Create a connection to the local SQLite database
engine = create_engine('sqlite:///efd_data.db')
Session = sessionmaker(bind=engine)
session = Session()

# Import built-in modules
import warnings 


# Disabling all warnings
warnings.filterwarnings('ignore')

def import_efd_E001(efd_id_0000) -> pd.DataFrame:
    """ 
    Imports the E001 records    
    Args
    ----------------------
    efd_id_0000: tuple
        Foreign key of the 0000 record
    """

    # Defining columns based on the local database model
    columns = {         
        'ID_E001': int,
        'ID_0000': int,
        'IND_MOV': str
    }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    # assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # Creating a query for the local database
    query = session.query(
        RegistroE001.ID_E001,
        RegistroE001.ID_0000,
        RegistroE001.IND_MOV
    ).filter(RegistroE001.ID_0000.in_(tuple(map(int, efd_id_0000))))

    results = query.all()
    efd_E001 = pd.DataFrame(results, columns=columns.keys())

    return efd_E001


def import_efd_E100(efd_id_E001) -> pd.DataFrame:
    """ 
    Imports the E100 records    
    Args
    ----------------------
    efd_id_E001: tuple
        Foreign key of the E001 record
    """

    # Defining columns based on the local database model
    columns = {         
        'ID_E100': int,
        'ID_E001': int,
        'DT_INI': 'datetime64[ns]',
        'DT_FIN': 'datetime64[ns]'
    }

    assert len(efd_id_E001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E001)}"

    # assert isinstance(efd_id_E001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E001)}"

    # Creating a query for the local database
    query = session.query(
        RegistroE100.ID_E100,
        RegistroE100.ID_E001,
        RegistroE100.DT_INI,
        RegistroE100.DT_FIN
    ).filter(RegistroE100.ID_E001.in_(tuple(map(int, efd_id_E001))))

    results = query.all()
    efd_E100 = pd.DataFrame(results, columns=columns.keys())

    return efd_E100


def import_efd_E110(efd_id_E100) -> pd.DataFrame:
    """ 
    Imports the E110 records    
    Args
    ----------------------
    efd_id_E100: tuple
        Foreign key of the E100 record
    """

    # Defining columns based on the local database model
    columns = {         
        'ID_E110': int,
        'ID_E100': int,
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

    # assert isinstance(efd_id_E100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroE110.ID_E110,
        RegistroE110.ID_E100,
        RegistroE110.VL_TOT_DEBITOS,
        RegistroE110.VL_AJ_DEBITOS,
        RegistroE110.VL_TOT_AJ_DEBITOS,
        RegistroE110.VL_TOT_CREDITOS,
        RegistroE110.VL_AJ_CREDITOS,
        RegistroE110.VL_TOT_AJ_CREDITOS,
        RegistroE110.VL_ESTORNOS_DEB,
        RegistroE110.VL_ESTORNOS_CRED,
        RegistroE110.VL_SLD_CREDOR_ANT,
        RegistroE110.VL_SLD_APURADO,
        RegistroE110.VL_TOT_DED,
        RegistroE110.VL_ICMS_RECOLHER,
        RegistroE110.VL_SLD_CREDOR_TRANSPORTAR,
        RegistroE110.DEB_ESP
    ).filter(RegistroE110.ID_E100.in_(tuple(map(int, efd_id_E100))))

    results = query.all()
    efd_E110 = pd.DataFrame(results, columns=columns.keys())

    return efd_E110


def import_efd_E111(efd_id_E110) -> pd.DataFrame:
    """ 
    Imports the E111 records    
    Args
    ----------------------
    efd_id_E110: tuple
        Foreign key of the E110 record
    """

    # Defining columns based on the local database model
    columns = {         
        'ID_E111': int,
        'ID_E110': int,
        'COD_AJ_APUR': str,
        'DESCR_COMPL_AJ': str,
        'VL_AJ_APUR': float
    }

    assert len(efd_id_E110),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E110)}"

    # assert isinstance(efd_id_E110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E110)}"

    # Creating a query for the local database
    query = session.query(
        RegistroE111.ID_E111,
        RegistroE111.ID_E110,
        RegistroE111.COD_AJ_APUR,
        RegistroE111.DESCR_COMPL_AJ,
        RegistroE111.VL_AJ_APUR
    ).filter(RegistroE111.ID_E110.in_(tuple(map(int, efd_id_E110))))

    results = query.all()
    efd_E111 = pd.DataFrame(results, columns=columns.keys())

    return efd_E111


def import_efd_E112(efd_id_E111) -> pd.DataFrame:
    """ 
    Imports the E112 records    
    Args
    ----------------------
    efd_id_E111: tuple
        Foreign key of the E111 record
    """

    # Defining columns based on the local database model
    columns = {         
        'ID_E112': int,
        'ID_E111': int,
        'NR_NUM_DA': str,
        'NR_NUM_PROC': str,
        'TP_IND_PROC': str,
        'DS_PROC': str,
        'DS_TXT_COMPL': str
    }

    assert len(efd_id_E111),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E111)}"

    # assert isinstance(efd_id_E111, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E111)}"

    # Creating a query for the local database
    query = session.query(
        RegistroE112.ID_E112,
        RegistroE112.ID_E111,
        RegistroE112.NR_NUM_DA,
        RegistroE112.NR_NUM_PROC,
        RegistroE112.TP_IND_PROC,
        RegistroE112.DS_PROC,
        RegistroE112.DS_TXT_COMPL
    ).filter(RegistroE112.ID_E111.in_(tuple(map(int, efd_id_E111))))

    results = query.all()
    efd_E112 = pd.DataFrame(results, columns=columns.keys())

    return efd_E112


def import_efd_E113(efd_id_E111) -> pd.DataFrame:
    """ 
    Imports the E113 records    
    Args
    ----------------------
    efd_id_E111: tuple
        Foreign key of the E111 record
    """

    # Defining columns based on the local database model
    columns = {         
        'ID_E113': int,
        'ID_E111': int,
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

    assert len(efd_id_E111),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E111)}"

    # assert isinstance(efd_id_E111, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E111)}"

    # Creating a query for the local database
    query = session.query(
        RegistroE113.ID_E113,
        RegistroE113.ID_E111,
        RegistroE113.COD_PART,
        RegistroE113.COD_MOD,
        RegistroE113.SER,
        RegistroE113.SUB,
        RegistroE113.NUM_DOC,
        RegistroE113.DT_DOC,
        RegistroE113.CD_ITEM,
        RegistroE113.VL_AJ_ITEM,
        RegistroE113.CHV_DOCe
    ).filter(RegistroE113.ID_E111.in_(tuple(map(int, efd_id_E111))))

    results = query.all()
    efd_E113 = pd.DataFrame(results, columns=columns.keys())

    return efd_E113


def import_efd_E115(efd_id_E110) -> pd.DataFrame:
    """ 
    Imports the E115 records    
    Args
    ----------------------
    efd_id_E110: tuple
        Foreign key of the E110 record
    """

    # Defining columns based on the local database model
    columns = {         
        'ID_E115': int,
        'ID_E110': int,
        'COD_INF_ADIC': str,
        'VL_INF_ADIC': float,
        'DESCR_COMPL_AJ': str
    }

    assert len(efd_id_E110),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E110)}"

    # assert isinstance(efd_id_E110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E110)}"

    # Creating a query for the local database
    query = session.query(
        RegistroE115.ID_E115,
        RegistroE115.ID_E110,
        RegistroE115.COD_INF_ADIC,
        RegistroE115.VL_INF_ADIC,
        RegistroE115.DESCR_COMPL_AJ
    ).filter(RegistroE115.ID_E110.in_(tuple(map(int, efd_id_E110))))

    results = query.all()
    efd_E115 = pd.DataFrame(results, columns=columns.keys())

    return efd_E115


def import_efd_E116(efd_id_E110) -> pd.DataFrame:
    """ 
    Imports the E116 records    
    Args
    ----------------------
    efd_id_E110: tuple
        Foreign key of the E110 record
    """

    # Defining columns based on the local database model
    columns = {         
        'ID_E116': int,
        'ID_E110': int,
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

    # assert isinstance(efd_id_E110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E110)}"

    # Creating a query for the local database
    query = session.query(
        RegistroE116.ID_E116,
        RegistroE116.ID_E110,
        RegistroE116.COD_OR,
        RegistroE116.VL_OR,
        RegistroE116.DT_VCTO,
        RegistroE116.COD_REC,
        RegistroE116.NUM_PROC,
        RegistroE116.IND_PROC,
        RegistroE116.PROC,
        RegistroE116.TXT_COMPL,
        RegistroE116.MES_REF
    ).filter(RegistroE116.ID_E110.in_(tuple(map(int, efd_id_E110))))

    results = query.all()
    efd_E116 = pd.DataFrame(results, columns=columns.keys())

    return efd_E116


def import_efd_E200(efd_id_E001) -> pd.DataFrame:
    """ 
    Imports the E200 records    
    Args
    ----------------------
    efd_id_E001: tuple
        Foreign key of the E001 record
    """

    # Defining columns based on the local database model
    columns = {         
        'ID_E200': int,
        'ID_E001': int,
        'UF': str,
        'DT_INI': 'datetime64[ns]',
        'DT_FIN': 'datetime64[ns]'
    }

    assert len(efd_id_E001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_E001)}"

    # assert isinstance(efd_id_E001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_E001)}"

    # Creating a query for the local database
    query = session.query(
        RegistroE200.ID_E200,
        RegistroE200.ID_E001,
        RegistroE200.UF,
        RegistroE200.DT_INI,
        RegistroE200.DT_FIN
    ).filter(RegistroE200.ID_E001.in_(tuple(map(int, efd_id_E001))))

    results = query.all()
    efd_E200 = pd.DataFrame(results, columns=columns.keys())

    return efd_E200


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
                    'ID_E210': int,
                    'ID_E200': int,
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

    # assert isinstance(efd_id_200, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_200)}"

    # SQL query to fetch E210 data
    query = session.query(
        RegistroE210.ID_E210,
        RegistroE210.ID_E200,
        RegistroE210.IND_MOV_ST,
        RegistroE210.VL_SLD_CRED_ANT_ST,
        RegistroE210.VL_DEVOL_ST,
        RegistroE210.VL_RESSARC_ST,
        RegistroE210.VL_OUT_CRED_ST,
        RegistroE210.VL_AJ_CREDITOS_ST,
        RegistroE210.VL_RETENCAO_ST,
        RegistroE210.VL_OUT_DEB_ST,
        RegistroE210.VL_AJ_DEBITOS_ST,
        RegistroE210.VL_SLD_DEV_ANT_ST,
        RegistroE210.VL_DEDUCOES_ST,
        RegistroE210.VL_ICMS_RECOL_ST,
        RegistroE210.DEB_ESP_ST,
        RegistroE210.VL_SLD_CRED_ST_TRANSPORTAR
    ).filter(RegistroE210.ID_E200.in_(tuple(map(int, efd_id_200))))

    results = query.all()
    efd_E210 = pd.DataFrame(results, columns=columns.keys())

    return efd_E210


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
                    'ID_E220': int,
                    'ID_E210': int,
                    'COD_AJ_APUR': str,
                    'DESCR_COMPL_AJ': str,
                    'VL_AJ_APUR': float
            }

    assert len(efd_id_210),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_210)}"

    # assert isinstance(efd_id_210, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_210)}"

    # SQL query to fetch E220 data
    query = session.query(
        RegistroE220.ID_E220,
        RegistroE220.ID_E210,
        RegistroE220.COD_AJ_APUR,
        RegistroE220.DESCR_COMPL_AJ,
        RegistroE220.VL_AJ_APUR
    ).filter(RegistroE220.ID_E210.in_(tuple(map(int, efd_id_210))))

    results = query.all()
    efd_E220 = pd.DataFrame(results, columns=columns.keys())

    return efd_E220


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
                    'ID_E230': int,
                    'ID_E220': int,
                    'NUM_DA': str,
                    'NUM_PROC': str,
                    'IND_PROC': str,
                    'PROC': str,
                    'TXT_COMPL': str
            }

    assert len(efd_id_220),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_220)}"

    # assert isinstance(efd_id_220, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_220)}"

    # SQL query to fetch E230 data
    query = session.query(
        RegistroE230.ID_E230,
        RegistroE230.ID_E220,
        RegistroE230.NUM_DA,
        RegistroE230.NUM_PROC,
        RegistroE230.IND_PROC,
        RegistroE230.PROC,
        RegistroE230.TXT_COMPL
    ).filter(RegistroE230.ID_E220.in_(tuple(map(int, efd_id_220))))

    results = query.all()
    efd_E230 = pd.DataFrame(results, columns=columns.keys())

    return efd_E230

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
                    'ID_E240': int,
                    'ID_E220': int,
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

    # assert isinstance(efd_id_220, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_220)}"

    # SQL query to fetch E240 data
    query = session.query(
        RegistroE240.ID_E240,
        RegistroE240.ID_E220,
        RegistroE240.COD_PART,
        RegistroE240.COD_MOD,
        RegistroE240.SER,
        RegistroE240.SUB,
        RegistroE240.NUM_DOC,
        RegistroE240.DT_DOC,
        RegistroE240.CD_ITEM,
        RegistroE240.VL_AJ_ITEM,
        RegistroE240.CHV_DOCe
    ).filter(RegistroE240.ID_E220.in_(tuple(map(int, efd_id_220))))

    results = query.all()
    efd_E240 = pd.DataFrame(results, columns=columns.keys())

    return efd_E240


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
                    'ID_E250': int,
                    'ID_E210': int,
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

    # assert isinstance(efd_id_210, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_210)}"

    # SQL query to fetch E250 data
    query = session.query(
        RegistroE250.ID_E250,
        RegistroE250.ID_E210,
        RegistroE250.COD_OR,
        RegistroE250.VL_OR,
        RegistroE250.DT_VCTO,
        RegistroE250.COD_REC,
        RegistroE250.NUM_PROC,
        RegistroE250.IND_PROC,
        RegistroE250.PROC,
        RegistroE250.TXT_COMPL,
        RegistroE250.MES_REF
    ).filter(RegistroE250.ID_E210.in_(tuple(map(int, efd_id_210))))

    results = query.all()
    efd_E250 = pd.DataFrame(results, columns=columns.keys())

    return efd_E250

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
                    'ID_E300': int,
                    'ID_E001': int,
                    'UF': str,
                    'DT_INI': 'datetime64[ns]',
                    'DT_FIN': 'datetime64[ns]'
            }

    assert len(efd_id_001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_001)}"

    # assert isinstance(efd_id_001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_001)}"

    # SQL query to fetch E300 data
    query = session.query(
        RegistroE300.ID_E300,
        RegistroE300.ID_E001,
        RegistroE300.UF,
        RegistroE300.DT_INI,
        RegistroE300.DT_FIN
    ).filter(RegistroE300.ID_E001.in_(tuple(map(int, efd_id_001))))

    results = query.all()
    efd_E300 = pd.DataFrame(results, columns=columns.keys())

    return efd_E300


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
                    'ID_E310': int,
                    'ID_E300': int,
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

    # assert isinstance(efd_id_300, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_300)}"

    # SQL query to fetch E310 data
    query = session.query(
        RegistroE310.ID_E310,
        RegistroE310.ID_E300,
        RegistroE310.IND_MOV_DIFAL,
        RegistroE310.VL_SLD_CRED_ANT_DIFAL,
        RegistroE310.VL_TOT_DEBITOS_DIFAL,
        RegistroE310.VL_OUT_DEB_DIFAL,
        RegistroE310.VL_TOT_CREDITOS_DIFAL,
        RegistroE310.VL_OUT_CRED_DIFAL,
        RegistroE310.VL_SLD_DEV_ANT_DIFAL,
        RegistroE310.VL_DEDUCOES_DIFAL,
        RegistroE310.VL_RECOL_DIFAL,
        RegistroE310.VL_SLD_CRED_TRANS_DIF,
        RegistroE310.VL_DEB_ESP_DIFAL,
        RegistroE310.VL_SLD_CRED_ANT_FCP,
        RegistroE310.VL_TOT_DEB_FCP,
        RegistroE310.VL_OUT_DEB_FCP,
        RegistroE310.VL_TOT_CRED_FCP,
        RegistroE310.VL_OUT_CRED_FCP,
        RegistroE310.VL_SLD_DEV_ANT_FCP,
        RegistroE310.VL_DEDUCOES_FCP,
        RegistroE310.VL_RECOL_FCP,
        RegistroE310.VL_SLD_CRED_TRANSPORTAR_FCP
    ).filter(RegistroE310.ID_E300.in_(tuple(map(int, efd_id_300))))

    results = query.all()
    efd_E310 = pd.DataFrame(results, columns=columns.keys())

    return efd_E310


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
                    'ID_E311': int,
                    'ID_E310': int,
                    'COD_AJ_APUR': str,
                    'DESCR_COMPL_AJ': str,
                    'VL_AJ_APUR': float
            }

    assert len(efd_id_310),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_310)}"

    # assert isinstance(efd_id_310, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_310)}"

    # SQL query to fetch E311 data
    query = session.query(
        RegistroE311.ID_E311,
        RegistroE311.ID_E310,
        RegistroE311.COD_AJ_APUR,
        RegistroE311.DESCR_COMPL_AJ,
        RegistroE311.VL_AJ_APUR
    ).filter(RegistroE311.ID_E310.in_(tuple(map(int, efd_id_310))))

    results = query.all()
    efd_E311 = pd.DataFrame(results, columns=columns.keys())

    return efd_E311


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
                    'ID_E312': int,
                    'ID_E311': int,
                    'NUM_DA': str,
                    'NUM_PROC': str,
                    'IND_PROC': str,
                    'PROC': str,
                    'TXT_COMPL': str
            }

    assert len(efd_id_311),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_311)}"

    # assert isinstance(efd_id_311, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_311)}"

    # SQL query to fetch E312 data
    query = session.query(
        RegistroE312.ID_E312,
        RegistroE312.ID_E311,
        RegistroE312.NUM_DA,
        RegistroE312.NUM_PROC,
        RegistroE312.IND_PROC,
        RegistroE312.PROC,
        RegistroE312.TXT_COMPL
    ).filter(RegistroE312.ID_E311.in_(tuple(map(int, efd_id_311))))

    results = query.all()
    efd_E312 = pd.DataFrame(results, columns=columns.keys())

    return efd_E312


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
                    'ID_E313': int,
                    'ID_E311': int,
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

    # assert isinstance(efd_id_311, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_311)}"

    # SQL query to fetch E313 data
    query = session.query(
        RegistroE313.ID_E313,
        RegistroE313.ID_E311,
        RegistroE313.COD_PART,
        RegistroE313.COD_MOD,
        RegistroE313.SER,
        RegistroE313.SUB,
        RegistroE313.NUM_DOC,
        RegistroE313.DT_DOC,
        RegistroE313.COD_ITEM,
        RegistroE313.VL_AJ_ITEM,
        RegistroE313.CHV_DOCe
    ).filter(RegistroE313.ID_E311.in_(tuple(map(int, efd_id_311))))

    results = query.all()
    efd_E313 = pd.DataFrame(results, columns=columns.keys())

    return efd_E313


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
                    'ID_E316': int,
                    'ID_E310': int,
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

    # assert isinstance(efd_id_310, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_310)}"

    # SQL query to fetch E316 data
    query = session.query(
        RegistroE316.ID_E316,
        RegistroE316.ID_E310,
        RegistroE316.COD_OR,
        RegistroE316.VL_OR,
        RegistroE316.DT_VCTO,
        RegistroE316.COD_REC,
        RegistroE316.NUM_PROC,
        RegistroE316.IND_PROC,
        RegistroE316.PROC,
        RegistroE316.TXT_COMPL,
        RegistroE316.MES_REF
    ).filter(RegistroE316.ID_E310.in_(tuple(map(int, efd_id_310))))

    results = query.all()
    efd_E316 = pd.DataFrame(results, columns=columns.keys())

    return efd_E316


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
                    'ID_E500': int,
                    'ID_E001': int,
                    'IND_APUR': str,
                    'DT_INI': 'datetime64[ns]',
                    'DT_FIN': 'datetime64[ns]'
            }

    assert len(efd_id_001),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_001)}"

    # assert isinstance(efd_id_001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_001)}"

    # SQL query to fetch E500 data
    query = session.query(
        RegistroE500.ID_E500,
        RegistroE500.ID_E001,
        RegistroE500.IND_APUR,
        RegistroE500.DT_INI,
        RegistroE500.DT_FIN
    ).filter(RegistroE500.ID_E001.in_(tuple(map(int, efd_id_001))))

    results = query.all()
    efd_E500 = pd.DataFrame(results, columns=columns.keys())

    return efd_E500


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
                    'ID_E510': int,
                    'ID_E500': int,
                    'CFOP': str,
                    'CST_IPI': str,
                    'VL_CONT_IPI': float,
                    'VL_BC_IPI': float,
                    'VL_IPI': float
            }

    assert len(efd_id_500),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_500)}"

    # assert isinstance(efd_id_500, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_500)}"

    # SQL query to fetch E510 data
    query = session.query(
        RegistroE510.ID_E510,
        RegistroE510.ID_E500,
        RegistroE510.CFOP,
        RegistroE510.CST_IPI,
        RegistroE510.VL_CONT_IPI,
        RegistroE510.VL_BC_IPI,
        RegistroE510.VL_IPI
    ).filter(RegistroE510.ID_E500.in_(tuple(map(int, efd_id_500))))

    results = query.all()
    efd_E510 = pd.DataFrame(results, columns=columns.keys())

    return efd_E510


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
                    'ID_E520': int,
                    'ID_E500': int,
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

    # assert isinstance(efd_id_500, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_500)}"

    # SQL query to fetch E520 data
    query = session.query(
        RegistroE520.ID_E520,
        RegistroE520.ID_E500,
        RegistroE520.VL_SD_ANT_IPI,
        RegistroE520.VL_DEB_IPI,
        RegistroE520.VL_CRED_IPI,
        RegistroE520.VL_OD_IPI,
        RegistroE520.VL_OC_IPI,
        RegistroE520.VL_SC_IPI,
        RegistroE520.VL_SD_IPI
    ).filter(RegistroE520.ID_E500.in_(tuple(map(int, efd_id_500))))

    results = query.all()
    efd_E520 = pd.DataFrame(results, columns=columns.keys())

    return efd_E520


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
                    'ID_E530': int,
                    'ID_E520': int,
                    'IND_AJ': str,
                    'VL_AJ': float,
                    'COD_AJ': str,
                    'IND_DOC': str,
                    'NUM_DOC': str,
                    'DESCR_AJ': str
            }

    assert len(efd_id_520),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_520)}"

    # assert isinstance(efd_id_520, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_520)}"

    # SQL query to fetch E530 data
    query = session.query(
        RegistroE530.ID_E530,
        RegistroE530.ID_E520,
        RegistroE530.IND_AJ,
        RegistroE530.VL_AJ,
        RegistroE530.COD_AJ,
        RegistroE530.IND_DOC,
        RegistroE530.NUM_DOC,
        RegistroE530.DESCR_AJ
    ).filter(RegistroE530.ID_E520.in_(tuple(map(int, efd_id_520))))

    results = query.all()
    efd_E530 = pd.DataFrame(results, columns=columns.keys())

    return efd_E530


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
                    'ID_E531': int,
                    'ID_E530': int,
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

    # assert isinstance(efd_id_530, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_530)}"

    # SQL query to fetch E531 data
    query = session.query(
        RegistroE531.ID_E531,
        RegistroE531.ID_E530,
        RegistroE531.COD_PART,
        RegistroE531.COD_MOD,
        RegistroE531.SER,
        RegistroE531.SUB,
        RegistroE531.NUM_DOC,
        RegistroE531.DT_DOC,
        RegistroE531.COD_ITEM,
        RegistroE531.VL_AJ_ITEM,
        RegistroE531.CHV_NFE
    ).filter(RegistroE531.ID_E530.in_(tuple(map(int, efd_id_530))))

    results = query.all()
    efd_E531 = pd.DataFrame(results, columns=columns.keys())

    return efd_E531



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
                    'ID_0000': int,
                    'QTD_LIN_E': int
            }

    assert len(efd_id_0000),\
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1.000 units! Actual length = {len(efd_id_0000)}"

    # assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # SQL query to fetch E990 data
    query = session.query(
        RegistroE990.APUR_ID_990,
        RegistroE990.ID_0000,
        RegistroE990.QTD_LIN_E
    ).filter(RegistroE990.ID_0000.in_(tuple(map(int, efd_id_0000))))

    results = query.all()
    efd_E990 = pd.DataFrame(results, columns=columns.keys())

    return efd_E990

