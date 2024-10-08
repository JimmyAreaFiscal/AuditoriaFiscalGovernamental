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

def import_efd_C001(efd_id_0000) -> pd.DataFrame:
    columns = {
        'ID_0000': int,
        'ID_C001': int,
        'IND_MOV': str
    }

    assert len(efd_id_0000) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_0000)}"
    # assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    results = session.query(
        RegistroC001.ID_0000,
        RegistroC001.ID_C001,
        RegistroC001.IND_MOV
    ).filter(RegistroC001.ID_0000.in_(tuple(map(int, efd_id_0000)))).all()

    efd_C001 = pd.DataFrame(results, columns=columns.keys())
    return efd_C001


def import_efd_C100(efd_id_C001, nfe_only=False) -> pd.DataFrame:
    columns = {
        'ID_0000': int,
        'ID_C001': int,
        'ID_C100': int,
        'IND_OPER': str,
        'IND_EMIT': str,
        'COD_PART': str,
        'COD_MOD': str,
        'COD_SIT': str,
        'SER': str,
        'NUM_DOC': str,
        'CHV_NFE': str,
        'DT_DOC': 'datetime64[ns]',
        'DT_E_S': 'datetime64[ns]',
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

    assert len(efd_id_C001) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C001)}"
    # assert isinstance(efd_id_C001, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C001)}"

    query = session.query(
        RegistroC100.ID_0000,
        RegistroC100.ID_C001,
        RegistroC100.ID_C100,
        RegistroC100.IND_OPER,
        RegistroC100.IND_EMIT,
        RegistroC100.COD_PART,
        RegistroC100.COD_MOD,
        RegistroC100.COD_SIT,
        RegistroC100.SER,
        RegistroC100.NUM_DOC,
        RegistroC100.CHV_NFE,
        RegistroC100.DT_DOC,
        RegistroC100.DT_E_S,
        RegistroC100.VL_DOC,
        RegistroC100.IND_PGTO,
        RegistroC100.VL_DESC,
        RegistroC100.VL_ABAT_NT,
        RegistroC100.VL_MERC,
        RegistroC100.IND_FRT,
        RegistroC100.VL_FRT,
        RegistroC100.VL_SEG,
        RegistroC100.VL_OUT_DA,
        RegistroC100.VL_BC_ICMS,
        RegistroC100.VL_ICMS,
        RegistroC100.VL_BC_ICMS_ST,
        RegistroC100.VL_ICMS_ST,
        RegistroC100.VL_IPI,
        RegistroC100.VL_PIS,
        RegistroC100.VL_COFINS,
        RegistroC100.VL_PIS_ST,
        RegistroC100.VL_COFINS_ST
    ).filter(RegistroC100.ID_C001.in_(tuple(map(int, efd_id_C001))))

    if nfe_only:
        query = query.filter(RegistroC100.COD_MOD == '55')

    results = query.all()

    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_C101(efd_id_C100) -> pd.DataFrame:
    columns = {
        'ID_C100': int,
        'ID_C101': int,
        'VL_FCP_UF_DEST': float,
        'VL_ICMS_UF_DEST': float,
        'VL_ICMS_UF_REM': float
    }

    assert len(efd_id_C100) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C100)}"
    # assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    results = session.query(
        RegistroC101.ID_C100,
        RegistroC101.ID_C101,
        RegistroC101.VL_FCP_UF_DEST,
        RegistroC101.VL_ICMS_UF_DEST,
        RegistroC101.VL_ICMS_UF_REM
    ).filter(RegistroC101.ID_C100.in_(tuple(map(int, efd_id_C100)))).all()

    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_C105(efd_id_C100) -> pd.DataFrame:
    """
    Função para importar C105

    Args
    ----------------------
    efd_id_C100: tuple
        Chave estrangeira do registro C100

    """

    # Defining columns based on the local database model
    columns = {
        'ID_C100': int,
        'ID_C105': int,
        'OPER': str,
        'UF': str
    }

    # Check input constraints
    assert len(efd_id_C100) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C100)}"
    # assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC105.ID_C100,
        RegistroC105.ID_C105,
        RegistroC105.OPER,
        RegistroC105.UF
    ).filter(RegistroC105.ID_C100.in_(tuple(map(int, efd_id_C100))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results




def import_efd_C110(efd_id_C100) -> pd.DataFrame:
    """
    Function to import the register C110.

    Args
    ----------------------
    efd_id_C100: tuple
        Foreign key of the C100 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C100': int,
        'ID_C110': int,
        'COD_INF': str,
        'TXT_COMPL': str
    }

    # Check input constraints
    assert len(efd_id_C100) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C100)}"
    # assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC110.ID_C100,
        RegistroC110.ID_C110,
        RegistroC110.COD_INF,
        RegistroC110.TXT_COMPL
    ).filter(RegistroC110.ID_C100.in_(tuple(map(int, efd_id_C100))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_C111(efd_id_C110) -> pd.DataFrame:
    """
    Function to import the register C111.

    Args
    ----------------------
    efd_id_C110: tuple
        Foreign key of the C110 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C110': int,
        'ID_C111': int,
        'NUM_PROC': str,
        'IND_PROC': str
    }

    # Check input constraints
    assert len(efd_id_C110) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C110)}"
    # assert isinstance(efd_id_C110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C110)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC111.ID_C110,
        RegistroC111.ID_C111,
        RegistroC111.NUM_PROC,
        RegistroC111.IND_PROC
    ).filter(RegistroC111.ID_C110.in_(tuple(map(int, efd_id_C110))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results




def import_efd_C112(efd_id_C110) -> pd.DataFrame:
    """
    Function to import the register C112.

    Args
    ----------------------
    efd_id_C110: tuple
        Foreign key of the C110 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C110': int,
        'ID_C112': int,
        'COD_DA': str,
        'UF': str,
        'NUM_DA': str,
        'COD_AUT': str,
        'VL_DA': float
    }

    # Check input constraints
    assert len(efd_id_C110) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C110)}"
    # assert isinstance(efd_id_C110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C110)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC112.ID_C110,
        RegistroC112.ID_C112,
        RegistroC112.COD_DA,
        RegistroC112.UF,
        RegistroC112.NUM_DA,
        RegistroC112.COD_AUT,
        RegistroC112.VL_DA,
        RegistroC112.DT_VCTO,
        RegistroC112.DT_PGTO
    ).filter(RegistroC112.ID_C110.in_(tuple(map(int, efd_id_C110))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C113(efd_id_C110) -> pd.DataFrame:
    """
    Function to import the register C113.

    Args
    ----------------------
    efd_id_C110: tuple
        Foreign key of the C110 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C110': int,
        'ID_C113': int,
        'IND_OPER': str,
        'IND_EMIT': str,
        'COD_PART': str,
        'COD_MOD': str,
        'SER': str,
        'SUB': str,
        'NUM_DOC': str,
        'CHV_DOC': str
    }

    # Check input constraints
    assert len(efd_id_C110) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C110)}"
    # assert isinstance(efd_id_C110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C110)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC113.ID_C110,
        RegistroC113.ID_C113,
        RegistroC113.IND_OPER,
        RegistroC113.IND_EMIT,
        RegistroC113.COD_PART,
        RegistroC113.COD_MOD,
        RegistroC113.SER,
        RegistroC113.SUB,
        RegistroC113.NUM_DOC,
        RegistroC113.DT_DOC,
        RegistroC113.CHV_DOC
    ).filter(RegistroC113.ID_C110.in_(tuple(map(int, efd_id_C110))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C114(efd_id_C110) -> pd.DataFrame:
    """
    Function to import the register C114.

    Args
    ----------------------
    efd_id_C110: tuple
        Foreign key of the C110 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C114': str,
        'ID_C110': str,
        'COD_MOD': str,
        'NR_ECF_FAB': str,
        'NR_ECF_CX': str,
        'NR_NUM_DOC': str
    }

    # Check input constraints
    assert len(efd_id_C110) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C110)}"
    # assert isinstance(efd_id_C110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C110)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC114.ID_C114,
        RegistroC114.ID_C110,
        RegistroC114.COD_MOD,
        RegistroC114.ECF_FAB,
        RegistroC114.ECF_CX,
        RegistroC114.NUM_DOC,
        RegistroC114.DT_DOC
    ).filter(RegistroC114.ID_C110.in_(tuple(map(int, efd_id_C110))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C115(efd_id_C110) -> pd.DataFrame:
    """
    Function to import the register C115.

    Args
    ----------------------
    efd_id_C110: tuple
        Foreign key of the C110 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C115': int,
        'ID_C110': int,
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

    # Check input constraints
    assert len(efd_id_C110) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C110)}"
    # assert isinstance(efd_id_C110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C110)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC115.ID_C115,
        RegistroC115.ID_C110,
        RegistroC115.IND_CARGA,
        RegistroC115.CNPJ_COL,
        RegistroC115.IE_COL,
        RegistroC115.CPF_COL,
        RegistroC115.COD_MUN_COL,
        RegistroC115.CNPJ_ENTG,
        RegistroC115.IE_ENTG,
        RegistroC115.CPF_ENTG,
        RegistroC115.COD_MUN_ENTG
    ).filter(RegistroC115.ID_C110.in_(tuple(map(int, efd_id_C110))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C116(efd_id_C110) -> pd.DataFrame:
    """
    Function to import the register C116.

    Args
    ----------------------
    efd_id_C110: tuple
        Foreign key of the C110 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C116': str,
        'ID_C110': str,
        'COD_MOD': str,
        'NR_SAT': str,
        'NR_CHV_CFE': str,
        'NR_NUM_CFE': str,
        'DT_DOC': str
    }

    # Check input constraints
    assert len(efd_id_C110) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C110)}"
    # assert isinstance(efd_id_C110, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C110)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC116.ID_C116,
        RegistroC116.ID_C110,
        RegistroC116.COD_MOD,
        RegistroC116.NR_SAT,
        RegistroC116.CHV_CFE,
        RegistroC116.NUM_CFE,
        RegistroC116.DT_DOC
    ).filter(RegistroC116.ID_C110.in_(tuple(map(int, efd_id_C110))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C120(efd_id_C100) -> pd.DataFrame:
    """
    Function to import the register C120.

    Args
    ----------------------
    efd_id_C100: tuple
        Foreign key of the C100 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C120': int,
        'ID_C100': int,
        'COD_DOC_IMP': str,
        'NUM_DOC_IMP': str,
        'VL_PIS_IMP': float,
        'VL_COFINS_IMP': float,
        'NUM_ACDRAW': str
    }

    # Check input constraints
    assert len(efd_id_C100) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C100)}"
    # assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC120.ID_C120,
        RegistroC120.ID_C100,
        RegistroC120.COD_DOC_IMP,
        RegistroC120.NUM_DOC_IMP,
        RegistroC120.PIS_IMP,
        RegistroC120.COFINS_IMP,
        RegistroC120.NUM_ACDRAW
    ).filter(RegistroC120.ID_C100.in_(tuple(map(int, efd_id_C100))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C130(efd_id_C100) -> pd.DataFrame:
    """
    Function to import the register C130.

    Args
    ----------------------
    efd_id_C100: tuple
        Foreign key of the C100 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C130': int,
        'ID_C100': int,
        'VL_SERV_NT': float,
        'VL_BC_ISSQN': float,
        'VL_ISSQN': float,
        'VL_BC_IRRF': float,
        'VL_IRRF': float,
        'VL_BC_PREV': float,
        'VL_PREV': float
    }

    # Check input constraints
    assert len(efd_id_C100) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C100)}"
    # assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC130.ID_C130,
        RegistroC130.ID_C100,
        RegistroC130.VL_SERV_NT,
        RegistroC130.VL_BC_ISSQN,
        RegistroC130.VL_ISSQN,
        RegistroC130.VL_BC_IRRF,
        RegistroC130.VL_IRRF,
        RegistroC130.VL_BC_PREV,
        RegistroC130.VL_PREV
    ).filter(RegistroC130.ID_C100.in_(tuple(map(int, efd_id_C100))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C160(efd_id_C100) -> pd.DataFrame:
    """
    Function to import the register C160.

    Args
    ----------------------
    efd_id_C100: tuple
        Foreign key of the C100 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C160': int,
        'ID_C100': int,
        'COD_PART': str,
        'VEIC_ID': str,
        'QTD_VOL': int,
        'PESO_BRT': float,
        'PESO_LIQ': float,
        'UF_ID': str
    }

    # Check input constraints
    assert len(efd_id_C100) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C100)}"
    # assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC160.ID_C160,
        RegistroC160.ID_C100,
        RegistroC160.COD_PART,
        RegistroC160.VEIC_ID,
        RegistroC160.QTD_VOL,
        RegistroC160.PESO_BRT,
        RegistroC160.PESO_LIQ,
        RegistroC160.UF_ID
    ).filter(RegistroC160.ID_C100.in_(tuple(map(int, efd_id_C100))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_C165(efd_id_C100) -> pd.DataFrame:
    """
    Function to import the register C165.

    Args
    ----------------------
    efd_id_C100: tuple
        Foreign key of the C100 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C165': int,
        'ID_C100': int,
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

    # Check input constraints
    assert len(efd_id_C100) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C100)}"
    # assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC165.ID_C165,
        RegistroC165.ID_C100,
        RegistroC165.COD_PART,
        RegistroC165.VEIC_ID,
        RegistroC165.COD_AUT,
        RegistroC165.NR_PASSE,
        RegistroC165.HORA,
        RegistroC165.TEMPER,
        RegistroC165.QTD_VOL,
        RegistroC165.PESO_BRT,
        RegistroC165.PESO_LIQ,
        RegistroC165.CPF,
        RegistroC165.NOM_MOT,
        RegistroC165.UF_ID
    ).filter(RegistroC165.ID_C100.in_(tuple(map(int, efd_id_C100))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C170(efd_id_C100) -> pd.DataFrame:
    """
    Function to import the register C170.

    Args
    ----------------------
    efd_id_C100: tuple
        Foreign key of the C100 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C170': int,
        'ID_C100': int,
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

    # Check input constraints
    assert len(efd_id_C100) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C100)}"
    # assert isinstance(efd_id_C100, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC170.ID_C170,
        RegistroC170.ID_C100,
        RegistroC170.NUM_ITEM,
        RegistroC170.COD_ITEM,
        RegistroC170.DESCR_COMPL,
        RegistroC170.QTD,
        RegistroC170.UNID,
        RegistroC170.VL_ITEM,
        RegistroC170.VL_DESC,
        RegistroC170.IND_MOV,
        RegistroC170.CST_ICMS,
        RegistroC170.CFOP,
        RegistroC170.COD_NAT,
        RegistroC170.VL_BC_ICMS,
        RegistroC170.ALIQ_ICMS,
        RegistroC170.VL_ICMS,
        RegistroC170.VL_BC_ICMS_ST,
        RegistroC170.ALIQ_ST,
        RegistroC170.VL_ICMS_ST,
        RegistroC170.IND_APUR,
        RegistroC170.CST_IPI,
        RegistroC170.COD_ENQ,
        RegistroC170.VL_BC_IPI,
        RegistroC170.ALIQ_IPI,
        RegistroC170.VL_IPI,
        RegistroC170.CST_PIS,
        RegistroC170.VL_BC_PIS,
        RegistroC170.ALIQ_PIS_PERCENT,
        RegistroC170.QUANT_BC_PIS,
        RegistroC170.ALIQ_PIS_REAIS,
        RegistroC170.VL_PIS,
        RegistroC170.CST_COFINS,
        RegistroC170.VL_BC_COFINS,
        RegistroC170.ALIQ_COFINS_PERCENT,
        RegistroC170.QUANT_BC_COFINS,
        RegistroC170.ALIQ_COFINS_REAIS,
        RegistroC170.VL_COFINS,
        RegistroC170.COD_CTA,
        RegistroC170.VL_ABAT_NT
    ).filter(RegistroC170.ID_C100.in_(tuple(map(int, efd_id_C100))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results



def import_efd_C171(efd_id_C170) -> pd.DataFrame:
    """
    Function to import the register C171.

    Args
    ----------------------
    efd_id_C170: tuple
        Foreign key of the C170 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C170': int,
        'ID_C171': int,
        'NUM_TANQUE': str,
        'QTDE': float
    }

    # Check input constraints
    assert len(efd_id_C170) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C170)}"
    # assert isinstance(efd_id_C170, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C170)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC171.ID_C170,
        RegistroC171.ID_C171,
        RegistroC171.NUM_TANQUE,
        RegistroC171.QTDE
    ).filter(RegistroC171.ID_C170.in_(tuple(map(int, efd_id_C170))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C172(efd_id_C170) -> pd.DataFrame:
    """
    Function to import the register C172.

    Args
    ----------------------
    efd_id_C170: tuple
        Foreign key of the C170 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C172': int,
        'ID_C170': int,
        'VL_BC_ISSQN': float,
        'VL_ALIQ_ISSQN': float,
        'VL_ISSQN': float
    }

    # Check input constraints
    assert len(efd_id_C170) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C170)}"
    # assert isinstance(efd_id_C170, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C170)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC172.ID_C172,
        RegistroC172.ID_C170,
        RegistroC172.VL_BC_ISSQN,
        RegistroC172.ALIQ_ISSQN,
        RegistroC172.VL_ISSQN
    ).filter(RegistroC172.ID_C170.in_(tuple(map(int, efd_id_C170))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C173(efd_id_C170) -> pd.DataFrame:
    """
    Function to import the register C173.

    Args
    ----------------------
    efd_id_C170: tuple
        Foreign key of the C170 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C170': int,
        'ID_C173': int,
        'LOTE_MED': str,
        'QTD_ITEM': int,
        'IND_MED': str,
        'TP_PROD': str,
        'VL_TAB_MAX': float
    }

    date_columns = ['DT_FAB', 'DT_VAL']

    # Check input constraints
    assert len(efd_id_C170) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C170)}"
    # assert isinstance(efd_id_C170, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C170)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC173.ID_C170,
        RegistroC173.ID_C173,
        RegistroC173.LOTE_MED,
        RegistroC173.QTD_ITEM,
        RegistroC173.DT_FAB,
        RegistroC173.DT_VAL,
        RegistroC173.IND_MED,
        RegistroC173.TP_PROD,
        RegistroC173.VL_TAB_MAX
    ).filter(RegistroC173.ID_C170.in_(tuple(map(int, efd_id_C170))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results



def import_efd_C174(efd_id_C170) -> pd.DataFrame:
    """
    Function to import the register C174.

    Args
    ----------------------
    efd_id_C170: tuple
        Foreign key of the C170 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C170': int,
        'ID_C174': int,
        'IND_ARM': str,
        'NUM_ARM': str,
        'DESCR_COMPL': object
    }

    # Check input constraints
    assert len(efd_id_C170) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C170)}"
    # assert isinstance(efd_id_C170, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C170)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC174.ID_C170,
        RegistroC174.ID_C174,
        RegistroC174.IND_ARM,
        RegistroC174.NUM_ARM,
        RegistroC174.DESCR_COMPL
    ).filter(RegistroC174.ID_C170.in_(tuple(map(int, efd_id_C170))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C175(efd_id_C170) -> pd.DataFrame:
    """
    Function to import the register C175.

    Args
    ----------------------
    efd_id_C170: tuple
        Foreign key of the C170 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C170': int,
        'ID_C175': int,
        'IND_VEIC_OPER': str,
        'CNPJ': str,
        'UF': str,
        'CHASSI_VEIC': object
    }

    # Check input constraints
    assert len(efd_id_C170) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_C170)}"
    # assert isinstance(efd_id_C170, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_C170)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC175.ID_C170,
        RegistroC175.ID_C175,
        RegistroC175.IND_VEIC_OPER,
        RegistroC175.CNPJ,
        RegistroC175.UF,
        RegistroC175.CHASSI_VEIC
    ).filter(RegistroC175.ID_C170.in_(tuple(map(int, efd_id_C170))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_C176(efd_id_C170) -> pd.DataFrame:
    """
    Function to import the register C176.

    Args
    ----------------------
    efd_id_C170: tuple
        Foreign key of the C170 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C176': int,
        'ID_C170': int,
        'COD_MOD_ULT_E': str,
        'NUM_DOC_ULT_E': str,
        'SER_ULT_E': str,
        'DT_ULT_E': str,
        'COD_PART_ULT_E': str,
        'QUANT_ULT_E': float,
        'VL_UNIT_BC_ST': float,
        'CHAVE_NFE_ULT_E': str,
        'NUM_ITEM_ULT_E': str,
        'VL_UNIT_BC_ICMS_ULT_E': float,
        'VL_UNIT_LIMITE_BC_ICMS_ULT_E': float,
        'VL_UNIT_ICMS_ULT_E': float,
        'ALIQ_ST_ULT_E': float,
        'VL_UNIT_RES': float,
        'COD_RESP_RET': str,
        'COD_MOT_RES': str,
        'CHAVE_NFE_RET': str,
        'COD_PART_NFE_RET': str,
        'NR_SER_RET': str,
        'NUM_NFE_RET': str,
        'ITEM_NFE_RET': str,
        'COD_DA': str,
        'NUM_DA': str,
        'VL_UNIT_RES_FCP_ST': float
    }

    # Check input constraints
    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC176.ID_C176,
        RegistroC176.ID_C170,
        RegistroC176.COD_MOD_ULT_E,
        RegistroC176.NUM_DOC_ULT_E,
        RegistroC176.SER_ULT_E,
        RegistroC176.DT_ULT_E,
        RegistroC176.COD_PART_ULT_E,
        RegistroC176.QUANT_ULT_E,
        RegistroC176.VL_UNIT_BC_ST,
        RegistroC176.CHAVE_NFE_ULT_E,
        RegistroC176.NUM_ITEM_ULT_E,
        RegistroC176.VL_UNIT_BC_ICMS_ULT_E,
        RegistroC176.VL_UNIT_LIMITE_BC_ICMS_ULT_E,
        RegistroC176.VL_UNIT_ICMS_ULT_E,
        RegistroC176.ALIQ_ST_ULT_E,
        RegistroC176.VL_UNIT_RES,
        RegistroC176.COD_RESP_RET,
        RegistroC176.COD_MOT_RES,
        RegistroC176.CHAVE_NFE_RET,
        RegistroC176.COD_PART_NFE_RET,
        RegistroC176.SER_NFE_RET,
        RegistroC176.NUM_NFE_RET,
        RegistroC176.ITEM_NFE_RET,
        RegistroC176.COD_DA,
        RegistroC176.NUM_DA,
        RegistroC176.VL_UNIT_RES_FCP_ST
    ).filter(RegistroC176.ID_C170.in_(tuple(map(int, efd_id_C170))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_C177(efd_id_C170) -> pd.DataFrame:
    """
    Function to import the register C177.

    Args
    ----------------------
    efd_id_C170: tuple
        Foreign key of the C170 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C177': int,
        'ID_C170': int,
        'COD_SELO_IPI': str,
        'QT_SELO_IPI': int,
        'COD_INF_ITEM': str
    }

    # Check input constraints
    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC177.ID_C177,
        RegistroC177.ID_C170,
        RegistroC177.COD_INF_ITEM
    ).filter(RegistroC177.ID_C170.in_(tuple(map(int, efd_id_C170))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_C178(efd_id_C170) -> pd.DataFrame:
    """
    Function to import the register C178.

    Args
    ----------------------
    efd_id_C170: tuple
        Foreign key of the C170 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C178': int,
        'ID_C170': int,
        'CL_ENQ': str,
        'VL_UNID': float,
        'QUANT_PAD': float
    }

    # Check input constraints
    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC178.ID_C178,
        RegistroC178.ID_C170,
        RegistroC178.CL_ENQ,
        RegistroC178.VL_UNID,
        RegistroC178.QUANT_PAD
    ).filter(RegistroC178.ID_C170.in_(tuple(map(int, efd_id_C170))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C179(efd_id_C170) -> pd.DataFrame:
    """
    Function to import the register C179.

    Args
    ----------------------
    efd_id_C170: tuple
        Foreign key of the C170 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C179': int,
        'ID_C170': int,
        'VL_BC_ST_ORIG_DEST': float,
        'VL_ICMS_ST_REP': float,
        'VL_ICMS_ST_COMPL': float,
        'VL_BC_RET': float,
        'VL_ICMS_RET': float
    }

    # Check input constraints
    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC179.ID_C179,
        RegistroC179.ID_C170,
        RegistroC179.BC_ST_ORIG_DEST,
        RegistroC179.ICMS_ST_REP,
        RegistroC179.ICMS_ST_COMPL,
        RegistroC179.BC_RET,
        RegistroC179.ICMS_RET
    ).filter(RegistroC179.ID_C170.in_(tuple(map(int, efd_id_C170))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C180(efd_id_C170) -> pd.DataFrame:
    """
    Function to import the register C180.

    Args
    ----------------------
    efd_id_C170: tuple
        Foreign key of the C170 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C180': int,
        'ID_C170': int,
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

    # Check input constraints
    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC180.ID_C180,
        RegistroC180.ID_C170,
        RegistroC180.COD_RESP_RET,
        RegistroC180.QUANT_CONV,
        RegistroC180.UNID,
        RegistroC180.VL_UNIT_CONV,
        RegistroC180.VL_UNIT_ICMS_OP_CONV,
        RegistroC180.VL_UNIT_BC_ICMS_ST_CONV,
        RegistroC180.VL_UNIT_ICMS_ST_CONV,
        RegistroC180.VL_UNIT_FCP_ST_CONV,
        RegistroC180.COD_DA,
        RegistroC180.NUM_DA
    ).filter(RegistroC180.ID_C170.in_(tuple(map(int, efd_id_C170))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C181(efd_id_C170) -> pd.DataFrame:
    """
    Function to import the register C181.

    Args
    ----------------------
    efd_id_C170: tuple
        Foreign key of the C170 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C181': int,
        'ID_C170': int,
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

    # Check input constraints
    assert len(efd_id_C170) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C170, tuple), f"Input is not a tuple: {type(efd_id_C170)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC181.ID_C181,
        RegistroC181.ID_C170,
        RegistroC181.COD_MOT_REST_COMPL,
        RegistroC181.QUANT_CONV,
        RegistroC181.UNID,
        RegistroC181.COD_MOD_SAIDA,
        RegistroC181.SERIE_SAIDA,
        RegistroC181.ECF_FAB_SAIDA,
        RegistroC181.NUM_DOC_SAIDA,
        RegistroC181.CHV_DFE_SAIDA,
        RegistroC181.DT_DOC_SAIDA,
        RegistroC181.NUM_ITEM_SAIDA,
        RegistroC181.VL_UNIT_CONV_SAIDA,
        RegistroC181.VL_UNIT_ICMS_OP_ESTOQUE_CONV_SAIDA,
        RegistroC181.VL_UNIT_ICMS_ST_ESTOQUE_CONV_SAIDA,
        RegistroC181.VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV_SAIDA,
        RegistroC181.VL_UNIT_ICMS_NA_OPERACAO_CONV_SAIDA,
        RegistroC181.VL_UNIT_ICMS_OP_CONV_SAIDA,
        RegistroC181.VL_UNIT_ICMS_ST_CONV_REST,
        RegistroC181.VL_UNIT_FCP_ST_CONV_REST,
        RegistroC181.VL_UNIT_ICMS_ST_CONV_COMPL,
        RegistroC181.VL_UNIT_FCP_ST_CONV_COMPL
    ).filter(RegistroC181.ID_C170.in_(tuple(map(int, efd_id_C170))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_C185(efd_id_C100) -> pd.DataFrame:
    """
    Function to import the register C185.

    Args
    ----------------------
    efd_id_C100: tuple
        Foreign key of the C100 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C185': int,
        'ID_C100': int,
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

    # Check input constraints
    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC185.ID_C185,
        RegistroC185.ID_C100,
        RegistroC185.NUM_ITEM,
        RegistroC185.COD_ITEM,
        RegistroC185.CST_ICMS,
        RegistroC185.CFOP,
        RegistroC185.COD_MOT_REST_COMPL,
        RegistroC185.QUANT_CONV,
        RegistroC185.UNID,
        RegistroC185.VL_UNIT_CONV,
        RegistroC185.VL_UNIT_ICMS_NA_OPERACAO_CONV,
        RegistroC185.VL_UNIT_ICMS_OP_CONV,
        RegistroC185.VL_UNIT_ICMS_OP_ESTOQUE_CONV,
        RegistroC185.VL_UNIT_ICMS_ST_ESTOQUE_CONV,
        RegistroC185.VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV,
        RegistroC185.VL_UNIT_ICMS_ST_CONV_REST,
        RegistroC185.VL_UNIT_FCP_ST_CONV_REST,
        RegistroC185.VL_UNIT_ICMS_ST_CONV_COMPL,
        RegistroC185.VL_UNIT_FCP_ST_CONV_COMPL
    ).filter(RegistroC185.ID_C100.in_(tuple(map(int, efd_id_C100))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results




def import_efd_C186(efd_id_C100) -> pd.DataFrame:
    """
    Function to import the register C186.

    Args
    ----------------------
    efd_id_C100: tuple
        Foreign key of the C100 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C186': int,
        'ID_C100': int,
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

    # Check input constraints
    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC186.ID_C186,
        RegistroC186.ID_C100,
        RegistroC186.NUM_ITEM,
        RegistroC186.COD_ITEM,
        RegistroC186.CST_ICMS,
        RegistroC186.CFOP,
        RegistroC186.COD_MOT_REST_COMPL,
        RegistroC186.QUANT_CONV,
        RegistroC186.UNID,
        RegistroC186.COD_MOD_ENTRADA,
        RegistroC186.SERIE_ENTRADA,
        RegistroC186.NUM_DOC_ENTRADA,
        RegistroC186.CHV_DFE_ENTRADA,
        RegistroC186.DT_DOC_ENTRADA,
        RegistroC186.NUM_ITEM_ENTRADA,
        RegistroC186.VL_UNIT_CONV_ENTRADA,
        RegistroC186.VL_UNIT_ICMS_OP_CONV_ENTRADA,
        RegistroC186.VL_UNIT_BC_ICMS_ST_CONV_ENTRADA,
        RegistroC186.VL_UNIT_ICMS_ST_CONV_ENTRADA,
        RegistroC186.VL_UNIT_FCP_ST_CONV_ENTRADA
    ).filter(RegistroC186.ID_C100.in_(tuple(map(int, efd_id_C100))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results



def import_efd_C190(efd_id_C100) -> pd.DataFrame:
    """
    Function to import the register C190.

    Args
    ----------------------
    efd_id_C100: tuple
        Foreign key of the C100 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C190': int,
        'ID_C100': int,
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

    # Check input constraints
    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC190.ID_C190,
        RegistroC190.ID_C100,
        RegistroC190.CST_ICMS,
        RegistroC190.CFOP,
        RegistroC190.ALIQ_ICMS,
        RegistroC190.VL_OPR,
        RegistroC190.VL_BC_ICMS,
        RegistroC190.VL_ICMS,
        RegistroC190.VL_BC_ICMS_ST,
        RegistroC190.VL_ICMS_ST,
        RegistroC190.VL_RED_BC,
        RegistroC190.VL_IPI,
        RegistroC190.COD_OBS
    ).filter(RegistroC190.ID_C100.in_(tuple(map(int, efd_id_C100))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C191(efd_id_C190) -> pd.DataFrame:
    """
    Function to import the register C191.

    Args
    ----------------------
    efd_id_C190: tuple
        Foreign key of the C190 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C191': int,
        'ID_C190': int,
        'VL_FCP_OP': float,
        'VL_FCP_ST': float,
        'VL_FCP_RET': float
    }

    # Check input constraints
    assert len(efd_id_C190) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C190, tuple), f"Input is not a tuple: {type(efd_id_C190)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC191.ID_C191,
        RegistroC191.ID_C190,
        RegistroC191.VL_FCP_OP,
        RegistroC191.VL_FCP_ST,
        RegistroC191.VL_FCP_RET
    ).filter(RegistroC191.ID_C190.in_(tuple(map(int, efd_id_C190))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C195(efd_id_C100) -> pd.DataFrame:
    """
    Function to import the register C195, with fiscal observations from invoices declared.

    Args
    ----------------------
    efd_id_C100: tuple
        Foreign key of the C100 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C100': int,
        'ID_C195': int,
        'COD_OBS': str,
        'TXT_COMPL': str
    }

    # Check input constraints
    assert len(efd_id_C100) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C100, tuple), f"Input is not a tuple: {type(efd_id_C100)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC195.ID_C100,
        RegistroC195.ID_C195,
        RegistroC195.COD_OBS,
        RegistroC195.TXT_COMPL
    ).filter(RegistroC195.ID_C100.in_(tuple(map(int, efd_id_C100))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C197(efd_id_C195) -> pd.DataFrame:
    """
    Function to import the register C197, with fiscal adjustments from invoices declared.

    Args
    ----------------------
    efd_id_C195: tuple
        Foreign key of the C195 register.
    """

    # Defining columns based on the local database model
    columns = {
        'ID_C197': int,
        'ID_C195': int,
        'COD_AJ': str,
        'DESCR_COMPL_AJ': str,
        'COD_ITEM': str,
        'VL_BC_ICMS': float,
        'ALIQ_ICMS': float,
        'VL_ICMS': float,
        'VL_OUTROS': float
    }

    # Check input constraints
    assert len(efd_id_C195) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C195, tuple), f"Input is not a tuple: {type(efd_id_C195)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC197.ID_C197,
        RegistroC197.ID_C195,
        RegistroC197.COD_AJ,
        RegistroC197.DESCR_COMPL_AJ,
        RegistroC197.COD_ITEM,
        RegistroC197.VL_BC_ICMS,
        RegistroC197.ALIQ_ICMS,
        RegistroC197.VL_ICMS,
        RegistroC197.VL_OUTROS
    ).filter(RegistroC197.ID_C195.in_(tuple(map(int, efd_id_C195))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C500(efd_id_C001) -> pd.DataFrame:
    """
    Function to import the register C500.

    Args
    ----------------------
    efd_id_C001: tuple
        Foreign key of the C001 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C500': int,
        'ID_C001': int,
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

    # Check input constraints
    assert len(efd_id_C001) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C001, tuple), f"Input is not a tuple: {type(efd_id_C001)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC500.ID_C500,
        RegistroC500.ID_C001,
        RegistroC500.IND_OPER,
        RegistroC500.IND_EMIT,
        RegistroC500.COD_PART,
        RegistroC500.COD_MOD,
        RegistroC500.COD_SIT,
        RegistroC500.SER,
        RegistroC500.SUB,
        RegistroC500.COD_CONS,
        RegistroC500.NUM_DOC,
        RegistroC500.DT_DOC,
        RegistroC500.DT_E_S,
        RegistroC500.VL_DOC,
        RegistroC500.VL_DESC,
        RegistroC500.VL_FORN,
        RegistroC500.VL_SERV_NT,
        RegistroC500.VL_TERC,
        RegistroC500.VL_DA,
        RegistroC500.VL_BC_ICMS,
        RegistroC500.VL_ICMS,
        RegistroC500.VL_BC_ICMS_ST,
        RegistroC500.VL_ICMS_ST,
        RegistroC500.COD_INF,
        RegistroC500.VL_PIS,
        RegistroC500.VL_COFINS,
        RegistroC500.TP_LIGACAO,
        RegistroC500.COD_GRUPO_TENSAO,
        RegistroC500.CHV_DOCe,
        RegistroC500.FIN_DOCe,
        RegistroC500.CHV_DOCe_REF,
        RegistroC500.IND_DEST,
        RegistroC500.COD_MUN_DEST,
        RegistroC500.COD_CTA,
        RegistroC500.COD_MOD_DOC_REF,
        RegistroC500.HASH_DOC_REF,
        RegistroC500.SER_DOC_REF,
        RegistroC500.NUM_DOC_REF,
        RegistroC500.MES_DOC_REF,
        RegistroC500.ENER_INJET,
        RegistroC500.OUTRAS_DED
    ).filter(RegistroC500.ID_C001.in_(tuple(map(int, efd_id_C001))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results



def import_efd_C510(efd_id_C500) -> pd.DataFrame:
    """
    Function to import the register C510.

    Args
    ----------------------
    efd_id_C500: tuple
        Foreign key of the C500 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C510': int,
        'ID_C500': int,
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

    # Check input constraints
    assert len(efd_id_C500) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C500, tuple), f"Input is not a tuple: {type(efd_id_C500)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC510.ID_C510,
        RegistroC510.ID_C500,
        RegistroC510.NUM_ITEM,
        RegistroC510.COD_ITEM,
        RegistroC510.COD_CLASS,
        RegistroC510.QTD,
        RegistroC510.UNID,
        RegistroC510.VL_ITEM,
        RegistroC510.VL_DESC,
        RegistroC510.CST_ICMS,
        RegistroC510.CFOP,
        RegistroC510.VL_BC_ICMS,
        RegistroC510.ALIQ_ICMS,
        RegistroC510.VL_ICMS,
        RegistroC510.VL_BC_ICMS_ST,
        RegistroC510.ALIQ_ST,
        RegistroC510.VL_ICMS_ST,
        RegistroC510.IND_REC,
        RegistroC510.COD_PART,
        RegistroC510.VL_PIS,
        RegistroC510.VL_COFINS,
        RegistroC510.COD_CTA
    ).filter(RegistroC510.ID_C500.in_(tuple(map(int, efd_id_C500))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results




def import_efd_C590(efd_id_C500) -> pd.DataFrame:
    """
    Function to import the register C590.

    Args
    ----------------------
    efd_id_C500: tuple
        Foreign key of the C500 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C590': int,
        'ID_C500': int,
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

    # Check input constraints
    assert len(efd_id_C500) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C500, tuple), f"Input is not a tuple: {type(efd_id_C500)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC590.ID_C590,
        RegistroC590.ID_C500,
        RegistroC590.CST_ICMS,
        RegistroC590.CFOP,
        RegistroC590.ALIQ_ICMS,
        RegistroC590.VL_OPR,
        RegistroC590.VL_BC_ICMS,
        RegistroC590.VL_ICMS,
        RegistroC590.VL_BC_ICMS_ST,
        RegistroC590.VL_ICMS_ST,
        RegistroC590.VL_RED_BC,
        RegistroC590.COD_OBS
    ).filter(RegistroC590.ID_C500.in_(tuple(map(int, efd_id_C500))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C591(efd_id_C590) -> pd.DataFrame:
    """
    Function to import the register C591.

    Args
    ----------------------
    efd_id_C590: tuple
        Foreign key of the C590 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C591': int,
        'ID_C590': int,
        'VL_FCP_OP': float,
        'VL_FCP_ST': float
    }

    # Check input constraints
    assert len(efd_id_C590) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C590, tuple), f"Input is not a tuple: {type(efd_id_C590)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC591.ID_C591,
        RegistroC591.ID_C590,
        RegistroC591.VL_FCP_OP,
        RegistroC591.VL_FCP_ST
    ).filter(RegistroC591.ID_C590.in_(tuple(map(int, efd_id_C590))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C595(efd_id_C500) -> pd.DataFrame:
    """
    Function to import the register C595.

    Args
    ----------------------
    efd_id_C500: tuple
        Foreign key of the C500 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C595': int,
        'ID_C500': int,
        'COD_OBS': str,
        'TXT_COMPL': str
    }

    # Check input constraints
    assert len(efd_id_C500) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C500, tuple), f"Input is not a tuple: {type(efd_id_C500)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC595.ID_C595,
        RegistroC595.ID_C500,
        RegistroC595.COD_OBS,
        RegistroC595.TXT_COMPL
    ).filter(RegistroC595.ID_C500.in_(tuple(map(int, efd_id_C500))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C597(efd_id_C595) -> pd.DataFrame:
    """
    Function to import the register C597.

    Args
    ----------------------
    efd_id_C595: tuple
        Foreign key of the C595 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C597': int,
        'ID_C595': int,
        'COD_AJ': str,
        'DESCR_COMPL_AJ': str,
        'COD_ITEM': str,
        'VL_BC_ICMS': float,
        'ALIQ_ICMS': float,
        'VL_ICMS': float,
        'VL_OUTROS': float
    }

    # Check input constraints
    assert len(efd_id_C595) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C595, tuple), f"Input is not a tuple: {type(efd_id_C595)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC597.ID_C597,
        RegistroC597.ID_C595,
        RegistroC597.COD_AJ,
        RegistroC597.DESCR_COMPL_AJ,
        RegistroC597.COD_ITEM,
        RegistroC597.VL_BC_ICMS,
        RegistroC597.ALIQ_ICMS,
        RegistroC597.VL_ICMS,
        RegistroC597.VL_OUTROS
    ).filter(RegistroC597.ID_C595.in_(tuple(map(int, efd_id_C595))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_C600(efd_id_C001) -> pd.DataFrame:
    """
    Function to import the register C600.

    Args
    ----------------------
    efd_id_C001: tuple
        Foreign key of the C001 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C600': int,
        'ID_C001': int,
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

    # Check input constraints
    assert len(efd_id_C001) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C001, tuple), f"Input is not a tuple: {type(efd_id_C001)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC600.ID_C600,
        RegistroC600.ID_C001,
        RegistroC600.COD_MOD,
        RegistroC600.COD_MUN,
        RegistroC600.SER,
        RegistroC600.SUB,
        RegistroC600.COD_CONS,
        RegistroC600.QTD_CONS,
        RegistroC600.QTD_CANC,
        RegistroC600.DT_DOC,
        RegistroC600.VL_DOC,
        RegistroC600.VL_DESC,
        RegistroC600.CONS,
        RegistroC600.VL_FORN,
        RegistroC600.VL_SERV_NT,
        RegistroC600.VL_TERC,
        RegistroC600.VL_DA,
        RegistroC600.VL_BC_ICMS,
        RegistroC600.VL_ICMS,
        RegistroC600.VL_BC_ICMS_ST,
        RegistroC600.VL_ICMS_ST,
        RegistroC600.VL_PIS,
        RegistroC600.VL_COFINS
    ).filter(RegistroC600.ID_C001.in_(tuple(map(int, efd_id_C001))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C601(efd_id_C600) -> pd.DataFrame:
    """
    Function to import the register C601.

    Args
    ----------------------
    efd_id_C600: tuple
        Foreign key of the C600 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C601': int,
        'ID_C600': int,
        'NUM_DOC_CANC': str
    }

    # Check input constraints
    assert len(efd_id_C600) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C600, tuple), f"Input is not a tuple: {type(efd_id_C600)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC601.ID_C601,
        RegistroC601.ID_C600,
        RegistroC601.NUM_DOC_CANC
    ).filter(RegistroC601.ID_C600.in_(tuple(map(int, efd_id_C600))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C610(efd_id_C600) -> pd.DataFrame:
    """
    Function to import the register C610.

    Args
    ----------------------
    efd_id_C600: tuple
        Foreign key of the C600 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C610': int,
        'ID_C600': int,
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

    # Check input constraints
    assert len(efd_id_C600) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C600, tuple), f"Input is not a tuple: {type(efd_id_C600)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC610.ID_C610,
        RegistroC610.ID_C600,
        RegistroC610.COD_CLASS,
        RegistroC610.COD_ITEM,
        RegistroC610.QTD,
        RegistroC610.UNID,
        RegistroC610.VL_ITEM,
        RegistroC610.VL_DESC,
        RegistroC610.CST_ICMS,
        RegistroC610.CFOP,
        RegistroC610.ALIQ_ICMS,
        RegistroC610.VL_BC_ICMS,
        RegistroC610.VL_ICMS,
        RegistroC610.VL_BC_ICMS_ST,
        RegistroC610.VL_ICMS_ST,
        RegistroC610.VL_PIS,
        RegistroC610.VL_COFINS,
        RegistroC610.COD_CTA
    ).filter(RegistroC610.ID_C600.in_(tuple(map(int, efd_id_C600))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C690(efd_id_C600) -> pd.DataFrame:
    """
    Function to import the register C690.

    Args
    ----------------------
    efd_id_C600: tuple
        Foreign key of the C600 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C690': int,
        'ID_C600': int,
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

    # Check input constraints
    assert len(efd_id_C600) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C600, tuple), f"Input is not a tuple: {type(efd_id_C600)}"

    # Creating a query for the local database
    query = session.query(
        RegistroC690.ID_C690,
        RegistroC690.ID_C600,
        RegistroC690.CST_ICMS,
        RegistroC690.CFOP,
        RegistroC690.ALIQ_ICMS,
        RegistroC690.VL_OPR,
        RegistroC690.VL_BC_ICMS,
        RegistroC690.VL_ICMS,
        RegistroC690.VL_BC_ICMS_ST,
        RegistroC690.VL_ICMS_ST,
        RegistroC690.VL_RED_BC,
        RegistroC690.COD_OBS
    ).filter(RegistroC690.ID_C600.in_(tuple(map(int, efd_id_C600))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_C700(efd_id_C001) -> pd.DataFrame:
    """
    Function to import the register C700.

    Args
    ----------------------
    efd_id_C001: tuple
        Foreign key of the C001 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C700': int,
        'ID_C001': int,
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
    # assert isinstance(efd_id_C001, tuple), f"Input is not a tuple: {type(efd_id_C001)}"
    
    # Creating a query for the local database
    query = session.query(
        RegistroC700.ID_C700,
        RegistroC700.ID_C001,
        RegistroC700.COD_MOD,
        RegistroC700.SER,
        RegistroC700.NRO_ORD_INI,
        RegistroC700.NRO_ORD_FIN,
        RegistroC700.DT_DOC_INI,
        RegistroC700.DT_DOC_FIN,
        RegistroC700.NOM_MEST,
        RegistroC700.CHV_COD_DIG
    ).filter(RegistroC700.ID_C001.in_(tuple(map(int, efd_id_C001))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C790(efd_id_C700) -> pd.DataFrame:
    """
    Function to import the register C790.

    Args
    ----------------------
    efd_id_C700: tuple
        Foreign key of the C700 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C790': int,
        'ID_C700': int,
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
    # assert isinstance(efd_id_C700, tuple), f"Input is not a tuple: {type(efd_id_C700)}"
    
    # Creating a query for the local database
    query = session.query(
        RegistroC790.ID_C790,
        RegistroC790.ID_C700,
        RegistroC790.CST_ICMS,
        RegistroC790.CFOP,
        RegistroC790.ALIQ_ICMS,
        RegistroC790.VL_OPR,
        RegistroC790.VL_BC_ICMS,
        RegistroC790.VL_ICMS,
        RegistroC790.VL_BC_ICMS_ST,
        RegistroC790.VL_ICMS_ST,
        RegistroC790.VL_RED_BC,
        RegistroC790.COD_OBS
    ).filter(RegistroC790.ID_C700.in_(tuple(map(int, efd_id_C700))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results

def import_efd_C791(efd_id_C790) -> pd.DataFrame:
    """
    Function to import the register C791.

    Args
    ----------------------
    efd_id_C790: tuple
        Foreign key of the C790 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C791': int,
        'ID_C790': int,
        'SG_UF': str,
        'VL_BC_ICMS_ST': float,
        'VL_ICMS_ST': float
    }

    assert len(efd_id_C790) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_C790, tuple), f"Input is not a tuple: {type(efd_id_C790)}"
    
    # Creating a query for the local database
    query = session.query(
        RegistroC791.ID_C791,
        RegistroC791.ID_C790,
        RegistroC791.UF,
        RegistroC791.VL_BC_ICMS_ST,
        RegistroC791.VL_ICMS_ST
    ).filter(RegistroC791.ID_C790.in_(tuple(map(int, efd_id_C790))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_C990(efd_id_0000) -> pd.DataFrame:
    """
    Function to import the register C990.

    Args
    ----------------------
    efd_id_0000: tuple
        Foreign key of the 0000 register.
    """
    
    # Defining columns based on the local database model
    columns = {
        'ID_C990': int,
        'ID_0000': int,
        'QTD_LIN_C': int
    }

    assert len(efd_id_0000) <= 1000, "Limit exceeded!"
    # assert isinstance(efd_id_0000, tuple), f"Input is not a tuple: {type(efd_id_0000)}"
    
    # Creating a query for the local database
    query = session.query(
        RegistroC990.ID_C990,
        RegistroC990.ID_0000,
        RegistroC990.QTD_LIN_C
    ).filter(RegistroC990.ID_0000.in_(tuple(map(int, efd_id_0000))))

    results = query.all()
    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results






