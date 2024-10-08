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


def import_efd_title(mes_ref, cnpj) -> pd.DataFrame:
    efd_column = {
        'ID_0000': int,
        'ID_0001': int,
        'ID_C001': int,
        'ID_D001': int,
        'ID_E001': int,
        'ID_G001': int,
        'ID_H001': int,
        'ID_K001': int,
        'ID_9001': int,
        'ID_1001': int,
        'cnpj': str,
        'COD_FIN': str,
        "MES_REF": str
    }

    if isinstance(mes_ref, str):
        # Query the local database using SQLAlchemy
        results = session.query(
            func.coalesce(Registro0000.ID_0000, 0).label('ID_0000'),
            func.coalesce(Registro0001.ID_0001, 0).label('ID_0001'),
            func.coalesce(RegistroC001.ID_C001, 0).label('ID_C001'),
            func.coalesce(RegistroD001.ID_D001, 0).label('ID_D001'),
            func.coalesce(RegistroE001.ID_E001, 0).label('ID_E001'),
            func.coalesce(RegistroG001.ID_G001, 0).label('ID_G001'),
            func.coalesce(RegistroH001.ID_H001, 0).label('ID_H001'),
            func.coalesce(RegistroK001.ID_K001, 0).label('ID_K001'),
            func.coalesce(Registro1001.ID_1001, 0).label('ID_1001'),
            func.coalesce(Registro9001.ID_9001, 0).label('ID_9001'),
            Registro0000.CNPJ.label('cnpj'),
            Registro0000.COD_FIN.label('COD_FIN'),
            func.strftime('%m/%Y', Registro0000.DT_INI).label('MES_REF')
                ).join(Registro0001, Registro0001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroC001, RegistroC001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroD001, RegistroD001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroE001, RegistroE001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroG001, RegistroG001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroH001, RegistroH001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroK001, RegistroK001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(Registro1001, Registro1001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(Registro9001, Registro9001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .filter(func.strftime('%m/%Y', Registro0000.DT_INI) == mes_ref,
                Registro0000.CNPJ == cnpj).all()

    elif isinstance(mes_ref, list) or isinstance(mes_ref, tuple):
        # Query the local database using SQLAlchemy
        results = session.query(
            func.coalesce(Registro0000.ID_0000, 0).label('ID_0000'),
            func.coalesce(Registro0001.ID_0001, 0).label('ID_0001'),
            func.coalesce(RegistroC001.ID_C001, 0).label('ID_C001'),
            func.coalesce(RegistroD001.ID_D001, 0).label('ID_D001'),
            func.coalesce(RegistroE001.ID_E001, 0).label('ID_E001'),
            func.coalesce(RegistroG001.ID_G001, 0).label('ID_G001'),
            func.coalesce(RegistroH001.ID_H001, 0).label('ID_H001'),
            func.coalesce(RegistroK001.ID_K001, 0).label('ID_K001'),
            func.coalesce(Registro1001.ID_1001, 0).label('ID_1001'),
            func.coalesce(Registro9001.ID_9001, 0).label('ID_9001'),
            Registro0000.CNPJ.label('cnpj'),
            Registro0000.COD_FIN.label('COD_FIN'),
            func.strftime('%m/%Y', Registro0000.DT_INI).label('MES_REF')
                ).join(Registro0001, Registro0001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroC001, RegistroC001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroD001, RegistroD001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroE001, RegistroE001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroG001, RegistroG001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroH001, RegistroH001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(RegistroK001, RegistroK001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(Registro1001, Registro1001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .join(Registro9001, Registro9001.ID_0000 == Registro0000.ID_0000, isouter=True) \
                .filter(func.strftime('%m/%Y', Registro0000.DT_INI).in_(mes_ref),
                Registro0000.CNPJ == cnpj).all()
    # Convert query results to a DataFrame
    efd_title = pd.DataFrame(results, columns=efd_column.keys())

    return efd_title


def import_efd_0000(efd_id_0000) -> pd.DataFrame:
    """ 
    Function to import the register 0000, having all enterprise basic information.

    Args
    ----------------------
    efd_id_0000: tuple
        Refers to a list of CABE_ID_0000 from EFD to import.
        It shouldn't be given more than 1,000 IDs to be searched for.
    """
    columns = {
        'ID_0000': int,
        'DT_INI': str,
        'DT_FIN': str,
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

    assert len(efd_id_0000) <= 1000, \
        f"Limit has been exceeded! Check the input! Its length shouldn't be greater than 1,000 units! Actual length = {len(efd_id_0000)}"
    
    # assert isinstance(efd_id_0000, tuple), f"The efd_id input isn't a tuple! Check the input! {type(efd_id_0000)}"

    # Query the local database using SQLAlchemy
    results = session.query(
        Registro0000.ID_0000,
        Registro0000.DT_INI,
        Registro0000.DT_FIN,
        Registro0000.NOME,
        Registro0000.CNPJ,
        Registro0000.CPF,
        Registro0000.UF,
        Registro0000.IE,
        Registro0000.COD_MUN,
        Registro0000.IM,
        Registro0000.SUFRAMA,
        Registro0000.IND_PERFIL,
        Registro0000.IND_ATIV,
        Registro0000.COD_FIN,
        Registro0000.COD_VER
    ).filter(Registro0000.ID_0000.in_(tuple(map(int, efd_id_0000)))).all()

    # Convert query results to a DataFrame
    efd_0000 = pd.DataFrame(results, columns=columns.keys())

    # Assuring that dates are datetime
    efd_0000['DT_INI'] = pd.to_datetime(efd_0000['DT_INI'])
    efd_0000['DT_FIN'] = pd.to_datetime(efd_0000['DT_FIN'])

    return efd_0000


def import_efd_0001(efd_id_0000) -> pd.DataFrame:
    columns = {
        'ID_0000': int,
        'ID_0001': int,
        'IND_MOV': str
    }

    assert len(efd_id_0000) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0000))
    # assert isinstance(efd_id_0000, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0000))

    results = session.query(
        Registro0001.ID_0000,
        Registro0001.ID_0001,
        Registro0001.IND_MOV
    ).filter(Registro0001.ID_0000.in_(tuple(map(int, efd_id_0000)))).all()

    efd_0001 = pd.DataFrame(results, columns=columns.keys())
    return efd_0001

def import_efd_0002(efd_id_0001: tuple) -> pd.DataFrame:
    columns = {
        'ID_0002': int,
        'ID_0001': int,
        'CLAS_ESTAB_IND': str
    }

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0002.ID_0001,
        Registro0002.ID_0002,
        Registro0002.CLAS_ESTAB_IND
    ).filter(Registro0002.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0002 = pd.DataFrame(results, columns=columns.keys())
    return efd_0002

def import_efd_0005(efd_id_0001) -> pd.DataFrame:
    columns = {
        'ID_0001': int,
        'ID_0005': int,
        'FANTASIA': str,
        'CEP': str,
        'END': str,
        'NUM': str,
        'COMPL': str,
        'BAIRRO': str,
        'FONE': str,
        'FAX': str,
        'EMAIL': str
    }

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0005.ID_0001,
        Registro0005.ID_0005,
        Registro0005.FANTASIA,
        Registro0005.CEP,
        Registro0005.END,
        Registro0005.NUM,
        Registro0005.COMPL,
        Registro0005.BAIRRO,
        Registro0005.FONE,
        Registro0005.FAX,
        Registro0005.EMAIL
    ).filter(Registro0005.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0005 = pd.DataFrame(results, columns=columns.keys())
    return efd_0005

def import_efd_0015(efd_id_0001) -> pd.DataFrame:
    columns = {
        'ID_0001': int,
        'ID_0015': int,
        'UF_ST': str,
        'IE_ST': str
    }

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0015.ID_0001,
        Registro0015.ID_0015,
        Registro0015.UF_ST,
        Registro0015.IE_ST
    ).filter(Registro0015.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0015 = pd.DataFrame(results, columns=columns.keys())
    return efd_0015

def import_efd_0100(efd_id_0001) -> pd.DataFrame:
    columns = {
        'ID_0001': int,
        'ID_0100': int,
        'NOME': str,
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

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0100.ID_0001,
        Registro0100.ID_0100,
        Registro0100.NOME,
        Registro0100.CPF,
        Registro0100.CRC,
        Registro0100.CNPJ,
        Registro0100.CEP,
        Registro0100.END,
        Registro0100.NUM,
        Registro0100.COMPL,
        Registro0100.BAIRRO,
        Registro0100.FONE,
        Registro0100.FAX,
        Registro0100.EMAIL,
        Registro0100.COD_MUN
    ).filter(Registro0100.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0100 = pd.DataFrame(results, columns=columns.keys())
    return efd_0100




def import_efd_0150(efd_id_0001) -> pd.DataFrame:
    columns = {
        'ID_0001': int,
        'ID_0150': int,
        'COD_PART': str,
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

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0150.ID_0001,
        Registro0150.ID_0150,
        Registro0150.COD_PART,
        Registro0150.NOME,
        Registro0150.COD_PAIS,
        Registro0150.CNPJ,
        Registro0150.CPF,
        Registro0150.IE,
        Registro0150.COD_MUN,
        Registro0150.SUFRAMA,
        Registro0150.END,
        Registro0150.NUM,
        Registro0150.COMPL,
        Registro0150.BAIRRO
    ).filter(Registro0150.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0150 = pd.DataFrame(results, columns=columns.keys())
    return efd_0150

def import_efd_0175(efd_id_0150) -> pd.DataFrame:
    columns = {
        'ID_0150': int,
        'ID_0175': int,
        'DT_ALT': str,
        'NR_CAMPO': str,
        'CONT_ANT': str
    }

    assert len(efd_id_0150) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0150))
    # assert isinstance(efd_id_0150, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0150))

    results = session.query(
        Registro0175.ID_0150,
        Registro0175.ID_0175,
        Registro0175.DT_ALT,
        Registro0175.NR_CAMPO,
        Registro0175.CONT_ANT
    ).filter(Registro0175.ID_0150.in_(tuple(map(int, efd_id_0150)))).all()

    efd_0175 = pd.DataFrame(results, columns=columns.keys())
    efd_0175['DT_ALT'] = pd.to_datetime(efd_0175['DT_ALT'])
    return efd_0175

def import_efd_0190(efd_id_0001) -> pd.DataFrame:
    columns = {
        'ID_0001': int,
        'ID_0190': int,
        'UNID': str,
        'DESCR': str
    }

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0190.ID_0001,
        Registro0190.ID_0190,
        Registro0190.UNID,
        Registro0190.DESCR
    ).filter(Registro0190.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0190 = pd.DataFrame(results, columns=columns.keys())
    return efd_0190

def import_efd_0200(efd_id_0001) -> pd.DataFrame:
    columns = {
        'ID_0001': int,
        'ID_0200': int,
        'COD_ITEM': str,
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

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0200.ID_0001,
        Registro0200.ID_0200,
        Registro0200.COD_ITEM,
        Registro0200.DESCR_ITEM,
        Registro0200.COD_BARRA,
        Registro0200.COD_ANT_ITEM,
        Registro0200.UNID_INV,
        Registro0200.TIPO_ITEM,
        Registro0200.COD_NCM,
        Registro0200.EX_IPI,
        Registro0200.COD_GEN,
        Registro0200.COD_LST,
        Registro0200.ALIQ_ICMS,
        Registro0200.CEST
    ).filter(Registro0200.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0200 = pd.DataFrame(results, columns=columns.keys())
    return efd_0200

def import_efd_0205(efd_id_0200) -> pd.DataFrame:
    columns = {
        'ID_0200': int,
        'ID_0205': int,
        'DESCR_ANT_ITEM': str,
        'DT_INI': str,
        'DT_FIM': str,
        'COD_ANT_ITEM': str
    }

    assert len(efd_id_0200) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0200))
    # assert isinstance(efd_id_0200, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0200))

    results = session.query(
        Registro0205.ID_0200,
        Registro0205.ID_0205,
        Registro0205.DESCR_ANT_ITEM,
        Registro0205.DT_INI,
        Registro0205.DT_FIM,
        Registro0205.COD_ANT_ITEM
    ).filter(Registro0205.ID_0200.in_(tuple(map(int, efd_id_0200)))).all()

    efd_0205 = pd.DataFrame(results, columns=columns.keys())
    efd_0205['DT_INI'] = pd.to_datetime(efd_0205['DT_INI'])
    efd_0205['DT_FIM'] = pd.to_datetime(efd_0205['DT_FIM'])
    return efd_0205


def import_efd_0206(efd_id_0200) -> pd.DataFrame:
    columns = {
        'ID_0200': int,
        'ID_0206': int,
        'COD_COMB': str
    }

    assert len(efd_id_0200) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0200))
    # assert isinstance(efd_id_0200, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0200))

    results = session.query(
        Registro0206.ID_0200,
        Registro0206.ID_0206,
        Registro0206.COD_COMB
    ).filter(Registro0206.ID_0200.in_(tuple(map(int, efd_id_0200)))).all()

    efd_0206 = pd.DataFrame(results, columns=columns.keys())
    return efd_0206

def import_efd_0210(efd_id_0200) -> pd.DataFrame:
    columns = {
        'ID_0200': int,
        'ID_0210': int,
        'COD_ITEM_COMP': str,
        'QTD_COMP': float,
        'PERDA': float
    }

    assert len(efd_id_0200) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0200))
    # assert isinstance(efd_id_0200, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0200))

    results = session.query(
        Registro0210.ID_0200,
        Registro0210.ID_0210,
        Registro0210.COD_ITEM_COMP,
        Registro0210.QTD_COMP,
        Registro0210.PERDA
    ).filter(Registro0210.ID_0200.in_(tuple(map(int, efd_id_0200)))).all()

    efd_0210 = pd.DataFrame(results, columns=columns.keys())
    return efd_0210

def import_efd_0220(efd_id_0200) -> pd.DataFrame:
    columns = {
        'ID_0200': int,
        'ID_0220': int,
        'UNID_CONV': str,
        'FAT_CONV': float,
        'COD_BARRA': str
    }

    assert len(efd_id_0200) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0200))
    # assert isinstance(efd_id_0200, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0200))

    results = session.query(
        Registro0220.ID_0200,
        Registro0220.ID_0220,
        Registro0220.UNID_CONV,
        Registro0220.FAT_CONV,
        Registro0220.COD_BARRA
    ).filter(Registro0220.ID_0200.in_(tuple(map(int, efd_id_0200)))).all()

    efd_0220 = pd.DataFrame(results, columns=columns.keys())
    return efd_0220

def import_efd_0221(efd_id_0200) -> pd.DataFrame:
    columns = {
        'ID_0200': int,
        'ID_0221': int,
        'COD_ITEM_ATOMICO': str,
        'QTD_CONTIDA': float
    }

    assert len(efd_id_0200) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0200))
    # assert isinstance(efd_id_0200, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0200))

    results = session.query(
        Registro0221.ID_0200,
        Registro0221.ID_0221,
        Registro0221.COD_ITEM_ATOMICO,
        Registro0221.QTD_CONTIDA
    ).filter(Registro0221.ID_0200.in_(tuple(map(int, efd_id_0200)))).all()

    efd_0221 = pd.DataFrame(results, columns=columns.keys())
    return efd_0221

def import_efd_0300(efd_id_0001) -> pd.DataFrame:
    columns = {
        'ID_0001': int,
        'ID_0300': int,
        'COD_IND_BEM': str,
        'IDENT_MERC': str,
        'DESCR_ITEM': str,
        'COD_PRNC': str,
        'COD_CTA': str,
        'NR_PARC': int
    }

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0300.ID_0001,
        Registro0300.ID_0300,
        Registro0300.COD_IND_BEM,
        Registro0300.IDENT_MERC,
        Registro0300.DESCR_ITEM,
        Registro0300.COD_PRNC,
        Registro0300.COD_CTA,
        Registro0300.NR_PARC
    ).filter(Registro0300.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0300 = pd.DataFrame(results, columns=columns.keys())
    return efd_0300

def import_efd_0305(efd_id_0300: tuple) -> pd.DataFrame:
    columns = {
        'ID_0300': int,
        'ID_0305': int,
        'COD_CCUS': str,
        'FUNC': str,
        'VIDA_UTIL': int
    }

    assert len(efd_id_0300) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0300))
    # assert isinstance(efd_id_0300, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0300))

    results = session.query(
        Registro0305.ID_0300,
        Registro0305.ID_0305,
        Registro0305.COD_CCUS,
        Registro0305.FUNC,
        Registro0305.VIDA_UTIL
    ).filter(Registro0305.ID_0300.in_(tuple(map(int, efd_id_0300)))).all()

    efd_0305 = pd.DataFrame(results, columns=columns.keys())
    return efd_0305

def import_efd_0400(efd_id_0001) -> pd.DataFrame:
    columns = {
        'ID_0400': int,
        'ID_0001': int,
        'COD_NAT': object,
        'DESCR_NAT': object
    }

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0400.ID_0001,
        Registro0400.ID_0400,
        Registro0400.COD_NAT,
        Registro0400.DESCR_NAT
    ).filter(Registro0400.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0400 = pd.DataFrame(results, columns=columns.keys())
    return efd_0400

def import_efd_0450(efd_id_0001: tuple) -> pd.DataFrame:
    columns = {
        'ID_0450': int,
        'ID_0001': int,
        'COD_INF': object,
        'TXT': object
    }

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0450.ID_0001,
        Registro0450.ID_0450,
        Registro0450.COD_INF,
        Registro0450.TXT
    ).filter(Registro0450.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0450 = pd.DataFrame(results, columns=columns.keys())
    return efd_0450

def import_efd_0460(efd_id_0001) -> pd.DataFrame:
    columns = {
        'ID_0001': int,
        'ID_0460': int,
        'COD_OBS': str,
        'TXT': str
    }

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0460.ID_0001,
        Registro0460.ID_0460,
        Registro0460.COD_OBS,
        Registro0460.TXT
    ).filter(Registro0460.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0460 = pd.DataFrame(results, columns=columns.keys())
    return efd_0460

def import_efd_0500(efd_id_0001: tuple) -> pd.DataFrame:
    columns = {
        'ID_0001': object,
        'ID_0500': object,
        'COD_NAT_CC': object,
        'IND_CTA': object,
        'NIVEL': int,
        'COD_CTA': object,
        'NOME_CTA': object,
        'DT_ALT': str
    }

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0500.ID_0001,
        Registro0500.ID_0500,
        Registro0500.COD_NAT_CC,
        Registro0500.IND_CTA,
        Registro0500.NIVEL,
        Registro0500.COD_CTA,
        Registro0500.NOME_CTA,
        Registro0500.DT_ALT
    ).filter(Registro0500.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0500 = pd.DataFrame(results, columns=columns.keys())
    efd_0500['DT_ALT'] = pd.to_datetime(efd_0500['DT_ALT'])
    return efd_0500

def import_efd_0600(efd_id_0001: tuple) -> pd.DataFrame:
    columns = {
        'ID_0001': int,
        'ID_0600': int,
        'COD_CCUS': str,
        'CCUS': str,
        'DT_ALT': str
    }

    assert len(efd_id_0001) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0001))
    # assert isinstance(efd_id_0001, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0001))

    results = session.query(
        Registro0600.ID_0001,
        Registro0600.ID_0600,
        Registro0600.COD_CCUS,
        Registro0600.CCUS,
        Registro0600.DT_ALT
    ).filter(Registro0600.ID_0001.in_(tuple(map(int, efd_id_0001)))).all()

    efd_0600 = pd.DataFrame(results, columns=columns.keys())
    efd_0600['DT_ALT'] = pd.to_datetime(efd_0600['DT_ALT'])
    return efd_0600

def import_efd_0990(efd_id_0000: tuple) -> pd.DataFrame:
    columns = {
        'ID_0000': int,
        'ID_0990': int,
        'QTD_LIN_0': int
    }

    assert len(efd_id_0000) <= 1000, "Limit has been exceeded! Actual length = {}".format(len(efd_id_0000))
    # assert isinstance(efd_id_0000, tuple), "The efd_id input isn't a tuple! Got: {}".format(type(efd_id_0000))

    results = session.query(
        Registro0990.ID_0000,
        Registro0990.ID_0990,
        Registro0990.QTD_LIN_0
    ).filter(Registro0990.ID_0000.in_(tuple(map(int, efd_id_0000)))).all()

    efd_0990 = pd.DataFrame(results, columns=columns.keys())
    return efd_0990
