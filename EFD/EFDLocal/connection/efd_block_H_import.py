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



def import_efd_H001(efd_id_0000) -> pd.DataFrame:
    """ 
    Importa os registros H001    
    Args
    ----------------------
    efd_id_0000: tuple
        Chave estrangeira do registro 0000
    """
    
    columns = {
        'ID_0000': int,
        'ID_H001': int,
        'IND_MOV': str
    }

    assert len(efd_id_0000) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_0000)}"
    
    # Query to fetch H001 data using SQLAlchemy
    query = session.query(
        RegistroH001.ID_0000,
        RegistroH001.ID_H001,
        RegistroH001.IND_MOV
    ).filter(RegistroH001.ID_0000.in_(tuple(map(int, efd_id_0000))))

    results = query.all()

    # Converting results to pandas DataFrame
    results = pd.DataFrame(results, columns=columns.keys())

    # Ensuring correct data types
    for col, dtype in columns.items():
        results[col] = results[col].astype(dtype, errors='ignore')

    return results


def import_efd_H005(efd_id_001) -> pd.DataFrame:
    columns = {
        'ID_H005': int,
        'ID_H001': int,
        'DT_INV': 'datetime64[ns]',
        'VL_INV': float,
        'MOT_INV': str
    }

    assert len(efd_id_001) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_001)}"

    query = session.query(
        RegistroH005.ID_H005,
        RegistroH005.ID_H001,
        RegistroH005.DT_INV,
        RegistroH005.VL_INV,
        RegistroH005.MOT_INV
    ).filter(RegistroH005.ID_H001.in_(tuple(map(int, efd_id_001))))

    results = query.all()

    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        if dtype == 'datetime64[ns]':
            results[col] = pd.to_datetime(results[col], errors='coerce')
        else:
            results[col] = results[col].astype(dtype, errors='ignore')

    return results



def import_efd_H010(efd_id_005) -> pd.DataFrame:
    columns = {
        'ID_H010': int,
        'ID_H005': int,
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

    assert len(efd_id_005) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_005)}"

    query = session.query(
        RegistroH010.ID_H010,
        RegistroH010.ID_H005,
        RegistroH010.COD_ITEM,
        RegistroH010.UNID,
        RegistroH010.QTD,
        RegistroH010.VL_UNIT,
        RegistroH010.VL_ITEM,
        RegistroH010.IND_PROP,
        RegistroH010.COD_PART,
        RegistroH010.TXT_COMPL,
        RegistroH010.COD_CTA,
        RegistroH010.VL_ITEM_IR
    ).filter(RegistroH010.ID_H005.in_(tuple(map(int, efd_id_005))))

    results = query.all()

    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        results[col] = results[col].astype(dtype, errors='ignore')

    return results



def import_efd_H020(efd_id_010) -> pd.DataFrame:
    columns = {
        'ID_H020': int,
        'ID_H010': int,
        'CST_ICMS': str,
        'BC_ICMS': float,
        'VL_ICMS': float
    }

    assert len(efd_id_010) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_010)}"

    query = session.query(
        RegistroH020.ID_H020,
        RegistroH020.ID_H010,
        RegistroH020.CST_ICMS,
        RegistroH020.BC_ICMS,
        RegistroH020.VL_ICMS
    ).filter(RegistroH020.ID_H010.in_(tuple(map(int, efd_id_010))))

    results = query.all()

    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        results[col] = results[col].astype(dtype, errors='ignore')

    return results



def import_efd_H030(efd_id_010) -> pd.DataFrame:
    columns = {
        'ID_H030': int,
        'ID_H010': int,
        'VL_ICMS_OP': float,
        'VL_BC_ICMS_ST': float,
        'VL_ICMS_ST': float,
        'VL_FCP': float
    }

    assert len(efd_id_010) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_010)}"

    query = session.query(
        RegistroH030.ID_H030,
        RegistroH030.ID_H010,
        RegistroH030.VL_ICMS_OP,
        RegistroH030.VL_BC_ICMS_ST,
        RegistroH030.VL_ICMS_ST,
        RegistroH030.VL_FCP
    ).filter(RegistroH030.ID_H010.in_(tuple(map(int, efd_id_010))))

    results = query.all()

    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        results[col] = results[col].astype(dtype, errors='ignore')

    return results



def import_efd_H990(efd_id_0000) -> pd.DataFrame:
    columns = {
        'ID_H990': int,
        'ID_0000': int,
        'QTD_LIN_H': int
    }

    assert len(efd_id_0000) <= 1000, f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_0000)}"

    query = session.query(
        RegistroH990.ID_H990,
        RegistroH990.ID_0000,
        RegistroH990.QTD_LIN_H
    ).filter(RegistroH990.ID_0000.in_(tuple(map(int, efd_id_0000))))

    results = query.all()

    results = pd.DataFrame(results, columns=columns.keys())

    for col, dtype in columns.items():
        results[col] = results[col].astype(dtype, errors='ignore')

    return results

