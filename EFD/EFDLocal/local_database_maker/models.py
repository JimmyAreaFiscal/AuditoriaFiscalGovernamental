from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Numeric
from sqlalchemy.orm import declarative_base
import datetime

Base = declarative_base()

########################## BLOCO 0 ##########################


class Registro0000(Base):
    __tablename__ = 'INFO0000'
    ID_0000 = Column(Integer, primary_key=True)
    COD_VER = Column(String)
    COD_FIN = Column(String)
    DT_INI = Column(Date)
    DT_FIN = Column(Date)
    NOME = Column(String)
    CNPJ = Column(String)
    CPF = Column(String)
    UF = Column(String)
    IE = Column(String)
    COD_MUN = Column(String)
    IM = Column(String)
    SUFRAMA = Column(String)
    IND_PERFIL = Column(String)
    IND_ATIV = Column(String)

class Registro0001(Base):
    __tablename__ = 'INFO0001'
    ID_0001 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    IND_MOV = Column(String)

class Registro0002(Base):
    __tablename__ = 'INFO0002'
    ID_0002 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    CLAS_ESTAB_IND = Column(String)

class Registro0005(Base):
    __tablename__ = 'INFO0005'
    ID_0005 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    FANTASIA = Column(String)
    CEP = Column(String)
    END = Column(String)
    NUM = Column(String)
    COMPL = Column(String)
    BAIRRO = Column(String)
    FONE = Column(String)
    FAX = Column(String)
    EMAIL = Column(String)

class Registro0015(Base):
    __tablename__ = 'INFO0015'
    ID_0015 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    UF_ST = Column(String)
    IE_ST = Column(String)

class Registro0100(Base):
    __tablename__ = 'INFO0100'
    ID_0100 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    NOME = Column(String)
    CPF = Column(String)
    CRC = Column(String)
    CNPJ = Column(String)
    CEP = Column(String)
    END = Column(String)
    NUM = Column(String)
    COMPL = Column(String)
    BAIRRO = Column(String)
    FONE = Column(String)
    FAX = Column(String)
    EMAIL = Column(String)
    COD_MUN = Column(String)

class Registro0150(Base):
    __tablename__ = 'INFO0150'
    ID_0150 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    COD_PART = Column(String)
    NOME = Column(String)
    COD_PAIS = Column(String)
    CNPJ = Column(String)
    CPF = Column(String)
    IE = Column(String)
    COD_MUN = Column(String)
    SUFRAMA = Column(String)
    END = Column(String)
    NUM = Column(String)
    COMPL = Column(String)
    BAIRRO = Column(String)

class Registro0175(Base):
    __tablename__ = 'INFO0175'
    ID_0175 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    ID_0150 = Column(Integer)
    DT_ALT = Column(String)
    NR_CAMPO = Column(String)
    CONT_ANT = Column(String)

class Registro0190(Base):
    __tablename__ = 'INFO0190'
    ID_0190 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    UNID = Column(String)
    DESCR = Column(String)

class Registro0200(Base):
    __tablename__ = 'INFO0200'
    ID_0200 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    COD_ITEM = Column(String)
    DESCR_ITEM = Column(String)
    COD_BARRA = Column(String)
    COD_ANT_ITEM = Column(String)
    UNID_INV = Column(String)
    TIPO_ITEM = Column(String)
    COD_NCM = Column(String)
    EX_IPI = Column(String)
    COD_GEN = Column(String)
    COD_LST = Column(String)
    ALIQ_ICMS = Column(String)
    CEST = Column(String)

class Registro0205(Base):
    __tablename__ = 'INFO0205'
    ID_0205 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    ID_0200 = Column(Integer)
    DESCR_ANT_ITEM = Column(String)
    DT_INI = Column(String)
    DT_FIM = Column(String)
    COD_ANT_ITEM = Column(String)

class Registro0206(Base):
    __tablename__ = 'INFO0206'
    ID_0206 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    ID_0200 = Column(Integer)
    COD_COMB = Column(String)

class Registro0210(Base):
    __tablename__ = 'INFO0210'
    ID_0210 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    ID_0200 = Column(Integer)
    COD_ITEM_COMP = Column(String)
    QTD_COMP = Column(String)
    PERDA = Column(String)

class Registro0220(Base):
    __tablename__ = 'INFO0220'
    ID_0220 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    ID_0200 = Column(Integer)
    UNID_CONV = Column(String)
    FAT_CONV = Column(String)
    COD_BARRA = Column(String)

class Registro0221(Base):
    __tablename__ = 'INFO0221'
    ID_0221 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    ID_0200 = Column(Integer)
    COD_ITEM = Column(String)
    COD_ITEM_ATOMICO = Column(String)
    QTD_CONTIDA = Column(String)

class Registro0300(Base):
    __tablename__ = 'INFO0300'
    ID_0300 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    COD_IND_BEM = Column(String)
    IDENT_MERC = Column(String)
    DESCR_ITEM = Column(String)
    COD_PRNC = Column(String)
    COD_CTA = Column(String)
    NR_PARC = Column(String)

class Registro0305(Base):
    __tablename__ = 'INFO0305'
    ID_0305 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    ID_0300 = Column(Integer)
    COD_CCUS = Column(String)
    FUNC = Column(String)
    VIDA_UTIL = Column(String)

class Registro0400(Base):
    __tablename__ = 'INFO0400'
    ID_0400 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    COD_NAT = Column(String)
    DESCR_NAT = Column(String)

class Registro0450(Base):
    __tablename__ = 'INFO0450'
    ID_0450 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    COD_INF = Column(String)
    TXT = Column(String)

class Registro0460(Base):
    __tablename__ = 'INFO0460'
    ID_0460 = Column(Integer, primary_key=True)
    ID_0001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_OBS = Column(String)
    TXT = Column(String)

class Registro0500(Base):
    __tablename__ = 'INFO0500'
    ID_0500 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    DT_ALT = Column(String)
    COD_NAT_CC = Column(String)
    IND_CTA = Column(String)
    NIVEL = Column(String)
    COD_CTA = Column(String)
    NOME_CTA = Column(String)

class Registro0600(Base):
    __tablename__ = 'INFO0600'
    ID_0600 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    DT_ALT = Column(String)
    COD_CCUS = Column(String)
    CCUS = Column(String)

class Registro0990(Base):
    __tablename__ = 'INFO0990'
    ID_0990 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_0001 = Column(Integer)
    QTD_LIN_0 = Column(Integer)



########################## BLOCO B ##########################

class RegistroB001(Base):
    __tablename__ = 'B001'
    ID_B001 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    IND_DAD = Column(String)

class RegistroB020(Base):
    __tablename__ = 'B020'
    ID_B020 = Column(Integer, primary_key=True)
    ID_B001 = Column(Integer)
    IND_OPER = Column(String)
    IND_EMIT = Column(String)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    COD_SIT = Column(String)
    SER = Column(String)
    NUM_DOC = Column(String)
    CHV_NFE = Column(String)
    DT_DOC = Column(String)
    COD_MUN_SERV = Column(String)
    VL_CONT = Column(String)
    VL_MAT_TER = Column(String)
    VL_SUB = Column(String)
    VL_ISNT_ISS = Column(String)
    VL_DED_BC = Column(String)
    VL_BC_ISS = Column(String)
    VL_BC_ISS_RT = Column(String)
    VL_ISS_RT = Column(String)
    VL_ISS = Column(String)
    COD_INF_OBS = Column(String)

class RegistroB025(Base):
    __tablename__ = 'B025'
    ID_B025 = Column(Integer, primary_key=True)
    ID_B020 = Column(Integer)
    ID_B001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_CONT_P = Column(String)
    VL_BC_ISS_P = Column(String)
    ALIQ_ISS = Column(String)
    VL_ISS_P = Column(String)
    VL_ISNT_ISS_P = Column(String)
    COD_SERV = Column(String)

class RegistroB030(Base):
    __tablename__ = 'B030'
    ID_B030 = Column(Integer, primary_key=True)
    ID_B001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    SER = Column(String)
    NUM_DOC_INI = Column(String)
    NUM_DOC_FIN = Column(String)
    DT_DOC = Column(String)
    QTD_CANC = Column(String)
    VL_CONT = Column(String)
    VL_ISNT_ISS = Column(String)
    VL_BC_ISS = Column(String)
    VL_ISS = Column(String)
    COD_INF_OBS = Column(String)

class RegistroB035(Base):
    __tablename__ = 'B035'
    ID_B035 = Column(Integer, primary_key=True)
    ID_B030 = Column(Integer)
    ID_B001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_CONT_P = Column(String)
    VL_BC_ISS_P = Column(String)
    ALIQ_ISS = Column(String)
    VL_ISS_P = Column(String)
    VL_ISNT_ISS_P = Column(String)
    COD_SERV = Column(String)

class RegistroB350(Base):
    __tablename__ = 'B350'
    ID_B350 = Column(Integer, primary_key=True)
    ID_B001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_CTD = Column(String)
    CTA_ISS = Column(String)
    CTA_COSIF = Column(String)
    QTD_OCOR = Column(String)
    COD_SERV = Column(String)
    VL_CONT = Column(String)
    VL_BC_ISS = Column(String)
    ALIQ_ISS = Column(String)
    VL_ISS = Column(String)
    COD_INF_OBS = Column(String)

class RegistroB420(Base):
    __tablename__ = 'B420'
    ID_B420 = Column(Integer, primary_key=True)
    ID_B001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_CONT = Column(String)
    VL_BC_ISS = Column(String)
    ALIQ_ISS = Column(String)
    VL_ISNT_ISS = Column(String)
    VL_ISS = Column(String)
    COD_SERV = Column(String)

class RegistroB440(Base):
    __tablename__ = 'B440'
    ID_B440 = Column(Integer, primary_key=True)
    ID_B001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_OPER = Column(String)
    COD_PART = Column(String)
    VL_CONT_RT = Column(String)
    VL_BC_ISS_RT = Column(String)
    VL_ISS_RT = Column(String)

class RegistroB460(Base):
    __tablename__ = 'B460'
    ID_B460 = Column(Integer, primary_key=True)
    ID_B001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_DED = Column(String)
    VL_DED = Column(String)
    NUM_PROC = Column(String)
    IND_PROC = Column(String)
    PROC = Column(String)
    COD_INF_OBS = Column(String)
    IND_OBR = Column(String)

class RegistroB470(Base):
    __tablename__ = 'B470'
    ID_B470 = Column(Integer, primary_key=True)
    ID_B001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_CONT = Column(String)
    VL_MAT_TERC = Column(String)
    VL_MAT_PROP = Column(String)
    VL_SUB = Column(String)
    VL_ISNT = Column(String)
    VL_DED_BC = Column(String)
    VL_BC_ISS = Column(String)
    VL_BC_ISS_RT = Column(String)
    VL_ISS = Column(String)
    VL_ISS_RT = Column(String)
    VL_DED = Column(String)
    VL_ISS_REC = Column(String)
    VL_ISS_ST = Column(String)
    VL_ISS_REC_UNI = Column(String)

class RegistroB500(Base):
    __tablename__ = 'B500'
    ID_B500 = Column(Integer, primary_key=True)
    ID_B001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_REC = Column(String)
    QTD_PROF = Column(String)
    VL_OR = Column(String)

class RegistroB510(Base):
    __tablename__ = 'B510'
    ID_B510 = Column(Integer, primary_key=True)
    ID_B500 = Column(Integer)
    ID_B001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_PROF = Column(String)
    IND_ESC = Column(String)
    IND_SOC = Column(String)
    CPF = Column(String)
    NOME = Column(String)

class RegistroB990(Base):
    __tablename__ = 'B990'
    ID_B990 = Column(Integer, primary_key=True)
    ID_B001 = Column(Integer)
    ID_0000 = Column(Integer)
    QTD_LIN_B = Column(Integer)


########################## BLOCO C ##########################

class RegistroC001(Base):
    __tablename__ = 'C001'
    ID_C001 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    IND_MOV = Column(String)

class RegistroC100(Base):
    __tablename__ = 'C100'
    ID_C100 = Column(Integer, primary_key=True)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_OPER = Column(String)
    IND_EMIT = Column(String)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    COD_SIT = Column(String)
    SER = Column(String)
    NUM_DOC = Column(String)
    CHV_NFE = Column(String)
    DT_DOC = Column(Date)
    DT_E_S = Column(Date)
    VL_DOC = Column(Numeric(12, 2))
    IND_PGTO = Column(String)
    VL_DESC = Column(Numeric(12, 2))
    VL_ABAT_NT = Column(Numeric(12, 2))
    VL_MERC = Column(Numeric(12, 2))
    IND_FRT = Column(String)
    VL_FRT = Column(Numeric(12, 2))
    VL_SEG = Column(Numeric(12, 2))
    VL_OUT_DA = Column(Numeric(12, 2))
    VL_BC_ICMS = Column(Numeric(12, 2))
    VL_ICMS = Column(Numeric(12, 2))
    VL_BC_ICMS_ST = Column(Numeric(12, 2))
    VL_ICMS_ST = Column(Numeric(12, 2))
    VL_IPI = Column(Numeric(12, 2))
    VL_PIS = Column(Numeric(12, 2))
    VL_COFINS = Column(Numeric(12, 2))
    VL_PIS_ST = Column(Numeric(12, 2))
    VL_COFINS_ST = Column(Numeric(12, 2))


class RegistroC101(Base):
    __tablename__ = 'C101'
    ID_C101 = Column(Integer, primary_key=True)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_FCP_UF_DEST = Column(Numeric(12, 2))
    VL_ICMS_UF_DEST = Column(Numeric(12, 2))
    VL_ICMS_UF_REM = Column(Numeric(12, 2))

class RegistroC105(Base):
    __tablename__ = 'C105'
    ID_C105 = Column(Integer, primary_key=True)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    OPER = Column(String)
    UF = Column(String)

class RegistroC110(Base):
    __tablename__ = 'C110'
    ID_C110 = Column(Integer, primary_key=True)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_INF = Column(String)
    TXT_COMPL = Column(String)

class RegistroC111(Base):
    __tablename__ = 'C111'
    ID_C111 = Column(Integer, primary_key=True)
    ID_C110 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_PROC = Column(String)
    IND_PROC = Column(String)

class RegistroC112(Base):
    __tablename__ = 'C112'
    ID_C112 = Column(Integer, primary_key=True)
    ID_C110 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_DA = Column(String)
    UF = Column(String)
    NUM_DA = Column(String)
    COD_AUT = Column(String)
    VL_DA = Column(String)
    DT_VCTO = Column(String)
    DT_PGTO = Column(String)

class RegistroC113(Base):
    __tablename__ = 'C113'
    ID_C113 = Column(Integer, primary_key=True)
    ID_C110 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_OPER = Column(String)
    IND_EMIT = Column(String)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC = Column(String)
    DT_DOC = Column(String)
    CHV_DOC = Column(String)

class RegistroC114(Base):
    __tablename__ = 'C114'
    ID_C114 = Column(Integer, primary_key=True)
    ID_C110 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    ECF_FAB = Column(String)
    ECF_CX = Column(String)
    NUM_DOC = Column(String)
    DT_DOC = Column(String)

class RegistroC115(Base):
    __tablename__ = 'C115'
    ID_C115 = Column(Integer, primary_key=True)
    ID_C110 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_CARGA = Column(String)
    CNPJ_COL = Column(String)
    IE_COL = Column(String)
    CPF_COL = Column(String)
    COD_MUN_COL = Column(String)
    CNPJ_ENTG = Column(String)
    IE_ENTG = Column(String)
    CPF_ENTG = Column(String)
    COD_MUN_ENTG = Column(String)

class RegistroC116(Base):
    __tablename__ = 'C116'
    ID_C116 = Column(Integer, primary_key=True)
    ID_C110 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    NR_SAT = Column(String)
    CHV_CFE = Column(String)
    NUM_CFE = Column(String)
    DT_DOC = Column(String)

class RegistroC120(Base):
    __tablename__ = 'C120'
    ID_C120 = Column(Integer, primary_key=True)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_DOC_IMP = Column(Integer)
    NUM_DOC_IMP = Column(String)
    PIS_IMP = Column(Float)
    COFINS_IMP = Column(Float)
    NUM_ACDRAW = Column(String)

class RegistroC130(Base):
    __tablename__ = 'C130'
    ID_C130 = Column(Integer, primary_key=True)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_SERV_NT = Column(Float)
    VL_BC_ISSQN = Column(Float)
    VL_ISSQN = Column(Float)
    VL_BC_IRRF = Column(Float)
    VL_IRRF = Column(Float)
    VL_BC_PREV = Column(Float)
    VL_PREV = Column(Float)

class RegistroC140(Base):
    __tablename__ = 'C140'
    ID_C140 = Column(Integer, primary_key=True)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_EMIT = Column(Integer)
    IND_TIT = Column(Integer)
    DESC_TIT = Column(String)
    NUM_TIT = Column(String)
    QTD_PARC = Column(Integer)
    VL_TIT = Column(Float)

class RegistroC141(Base):
    __tablename__ = 'C141'
    ID_C141 = Column(Integer, primary_key=True)
    ID_C140 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_PARC = Column(Integer)
    DT_VCTO = Column(Date)
    VL_PARC = Column(Float)

class RegistroC160(Base):
    __tablename__ = 'C160'
    ID_C160 = Column(Integer, primary_key=True)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART = Column(String)
    VEIC_ID = Column(String)
    QTD_VOL = Column(Integer)
    PESO_BRT = Column(Float)
    PESO_LIQ = Column(Float)
    UF_ID = Column(String)

class RegistroC165(Base):
    __tablename__ = 'C165'
    ID_C165 = Column(Integer, primary_key=True)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART = Column(String)
    VEIC_ID = Column(String)
    COD_AUT = Column(String)
    NR_PASSE = Column(String)
    HORA = Column(String)
    TEMPER = Column(Float)
    QTD_VOL = Column(Integer)
    PESO_BRT = Column(Float)
    PESO_LIQ = Column(Float)
    NOM_MOT = Column(String)
    CPF = Column(String)
    UF_ID = Column(String)

class RegistroC170(Base):
    __tablename__ = 'C170'
    ID_C170 = Column(Integer, primary_key=True)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_ITEM = Column(Integer)
    COD_ITEM = Column(String)
    DESCR_COMPL = Column(String)
    QTD = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    VL_DESC = Column(Float)
    IND_MOV = Column(String)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    COD_NAT = Column(String)
    VL_BC_ICMS = Column(Float)
    ALIQ_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    ALIQ_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    IND_APUR = Column(String)
    CST_IPI = Column(String)
    COD_ENQ = Column(String)
    VL_BC_IPI = Column(Float)
    ALIQ_IPI = Column(Float)
    VL_IPI = Column(Float)
    CST_PIS = Column(String)
    VL_BC_PIS = Column(Float)
    ALIQ_PIS_PERCENT = Column(Float)
    QUANT_BC_PIS = Column(Float)
    ALIQ_PIS_REAIS = Column(Float)
    VL_PIS = Column(Float)
    CST_COFINS = Column(String)
    VL_BC_COFINS = Column(Float)
    ALIQ_COFINS_PERCENT = Column(Float)
    QUANT_BC_COFINS = Column(Float)
    ALIQ_COFINS_REAIS = Column(Float)
    VL_COFINS = Column(Float)
    COD_CTA = Column(String)
    VL_ABAT_NT = Column(Float)

class RegistroC171(Base):
    __tablename__ = 'C171'
    ID_C171 = Column(Integer, primary_key=True)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_TANQUE = Column(String)
    QTDE = Column(Float)

class RegistroC172(Base):
    __tablename__ = 'C172'
    ID_C172 = Column(Integer, primary_key=True)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_BC_ISSQN = Column(Float)
    ALIQ_ISSQN = Column(Float)
    VL_ISSQN = Column(Float)

class RegistroC173(Base):
    __tablename__ = 'C173'
    ID_C173 = Column(Integer, primary_key=True)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    LOTE_MED = Column(String)
    QTD_ITEM = Column(Float)
    DT_FAB = Column(Date)
    DT_VAL = Column(Date)
    IND_MED = Column(String)
    TP_PROD = Column(String)
    VL_TAB_MAX = Column(Float)

class RegistroC174(Base):
    __tablename__ = 'C174'
    ID_C174 = Column(Integer, primary_key=True)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_ARM = Column(String)
    NUM_ARM = Column(String)
    DESCR_COMPL = Column(String)

class RegistroC175(Base):
    __tablename__ = 'C175'
    ID_C175 = Column(Integer, primary_key=True)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_VEIC_OPER = Column(String)
    CNPJ = Column(String)
    UF = Column(String)
    CHASSI_VEIC = Column(String)

class RegistroC176(Base):
    __tablename__ = 'C176'
    ID_C176 = Column(Integer, primary_key=True)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD_ULT_E = Column(String)
    NUM_DOC_ULT_E = Column(String)
    SER_ULT_E = Column(String)
    DT_ULT_E = Column(Date)
    COD_PART_ULT_E = Column(String)
    QUANT_ULT_E = Column(Float)
    VL_UNIT_ULT_E = Column(Float)
    VL_UNIT_BC_ST = Column(Float)
    CHAVE_NFE_ULT_E = Column(String)
    NUM_ITEM_ULT_E = Column(Integer)
    VL_UNIT_BC_ICMS_ULT_E = Column(Float)
    ALIQ_ICMS_ULT_E = Column(Float)
    VL_UNIT_LIMITE_BC_ICMS_ULT_E = Column(Float)
    VL_UNIT_ICMS_ULT_E = Column(Float)
    ALIQ_ST_ULT_E = Column(Float)
    VL_UNIT_RES = Column(Float)
    COD_RESP_RET = Column(String)
    COD_MOT_RES = Column(String)
    CHAVE_NFE_RET = Column(String)
    COD_PART_NFE_RET = Column(String)
    SER_NFE_RET = Column(String)
    NUM_NFE_RET = Column(String)
    ITEM_NFE_RET = Column(Integer)
    COD_DA = Column(String)
    NUM_DA = Column(String)
    VL_UNIT_RES_FCP_ST = Column(Float)

class RegistroC177(Base):
    __tablename__ = 'C177'
    ID_C177 = Column(Integer, primary_key=True)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_INF_ITEM = Column(String)

class RegistroC178(Base):
    __tablename__ = 'C178'
    ID_C178 = Column(Integer, primary_key=True)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    CL_ENQ = Column(String)
    VL_UNID = Column(Float)
    QUANT_PAD = Column(Float)

class RegistroC179(Base):
    __tablename__ = 'C179'
    ID_C179 = Column(Integer, primary_key=True)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    BC_ST_ORIG_DEST = Column(Float)
    ICMS_ST_REP = Column(Float)
    ICMS_ST_COMPL = Column(Float)
    BC_RET = Column(Float)
    ICMS_RET = Column(Float)

class RegistroC180(Base):
    __tablename__ = 'C180'
    ID_C180 = Column(Integer, primary_key=True)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_RESP_RET = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    VL_UNIT_CONV = Column(Float)
    VL_UNIT_ICMS_OP_CONV = Column(Float)
    VL_UNIT_BC_ICMS_ST_CONV = Column(Float)
    VL_UNIT_ICMS_ST_CONV = Column(Float)
    VL_UNIT_FCP_ST_CONV = Column(Float)
    COD_DA = Column(String)
    NUM_DA = Column(String)

class RegistroC181(Base):
    __tablename__ = 'C181'
    ID_C181 = Column(Integer, primary_key=True)
    ID_C180 = Column(Integer)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOT_REST_COMPL = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    COD_MOD_SAIDA = Column(String)
    SERIE_SAIDA = Column(String)
    ECF_FAB_SAIDA = Column(String)
    NUM_DOC_SAIDA = Column(String)
    CHV_DFE_SAIDA = Column(String)
    DT_DOC_SAIDA = Column(Date)
    NUM_ITEM_SAIDA = Column(Integer)
    VL_UNIT_CONV_SAIDA = Column(Float)
    VL_UNIT_ICMS_OP_ESTOQUE_CONV_SAIDA = Column(Float)
    VL_UNIT_ICMS_ST_ESTOQUE_CONV_SAIDA = Column(Float)
    VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV_SAIDA = Column(Float)
    VL_UNIT_ICMS_NA_OPERACAO_CONV_SAIDA = Column(Float)
    VL_UNIT_ICMS_OP_CONV_SAIDA = Column(Float)
    VL_UNIT_ICMS_ST_CONV_REST = Column(Float)
    VL_UNIT_FCP_ST_CONV_REST = Column(Float)
    VL_UNIT_ICMS_ST_CONV_COMPL = Column(Float)
    VL_UNIT_FCP_ST_CONV_COMPL = Column(Float)

class RegistroC185(Base):
    __tablename__ = 'C185'
    ID_C185 = Column(Integer, primary_key=True)
    ID_C180 = Column(Integer)
    ID_C170 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_ITEM = Column(Integer)
    COD_ITEM = Column(String)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    COD_MOT_REST_COMPL = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    VL_UNIT_CONV = Column(String)
    VL_UNIT_ICMS_NA_OPERACAO_CONV = Column(Float)
    VL_UNIT_ICMS_OP_CONV = Column(Float)
    VL_UNIT_ICMS_OP_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_CONV_REST = Column(Float)
    VL_UNIT_FCP_ST_CONV_REST = Column(Float)
    VL_UNIT_ICMS_ST_CONV_COMPL = Column(Float)
    VL_UNIT_FCP_ST_CONV_COMPL = Column(Float)

class RegistroC186(Base):
    __tablename__ = 'C186'
    ID_C186 = Column(Integer, primary_key=True)
    ID_C185 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_ITEM = Column(Integer)
    COD_ITEM = Column(String)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    COD_MOT_REST_COMPL = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    COD_MOD_ENTRADA = Column(String)
    SERIE_ENTRADA = Column(String)
    ECF_FAB_ENTRADA = Column(String)
    NUM_DOC_ENTRADA = Column(String)
    CHV_DFE_ENTRADA = Column(String)
    DT_DOC_ENTRADA = Column(Date)
    NUM_ITEM_ENTRADA = Column(Integer)
    VL_UNIT_CONV_ENTRADA = Column(Float)
    VL_UNIT_ICMS_OP_CONV_ENTRADA = Column(Float)
    VL_UNIT_BC_ICMS_ST_CONV_ENTRADA = Column(Float)
    VL_UNIT_ICMS_ST_CONV_ENTRADA = Column(Float)
    VL_UNIT_FCP_ST_CONV_ENTRADA = Column(Float)


class RegistroC190(Base):
    __tablename__ = 'C190'
    ID_C190 = Column(Integer, primary_key=True)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    VL_RED_BC = Column(Float)
    VL_IPI = Column(Float)
    COD_OBS = Column(String)

class RegistroC191(Base):
    __tablename__ = 'C191'
    ID_C191 = Column(Integer, primary_key=True)
    ID_C190 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_FCP_OP = Column(Float)
    VL_FCP_ST = Column(Float)
    VL_FCP_RET = Column(Float)

class RegistroC195(Base):
    __tablename__ = 'C195'
    ID_C195 = Column(Integer, primary_key=True)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_OBS = Column(String)
    TXT_COMPL = Column(String)

class RegistroC197(Base):
    __tablename__ = 'C197'
    ID_C197 = Column(Integer, primary_key=True)
    ID_C195 = Column(Integer)
    ID_C100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_AJ = Column(String)
    DESCR_COMPL_AJ = Column(String)
    COD_ITEM = Column(String)
    VL_BC_ICMS = Column(Float)
    ALIQ_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_OUTROS = Column(Float)

class RegistroC300(Base):
    __tablename__ = 'C300'
    ID_C300 = Column(Integer, primary_key=True)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC_INI = Column(Integer)
    NUM_DOC_FIN = Column(Integer)
    DT_DOC = Column(Date)
    VL_DOC = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    COD_CTA = Column(String)

class RegistroC310(Base):
    __tablename__ = 'C310'
    ID_C310 = Column(Integer, primary_key=True)
    ID_C300 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_DOC_CANC = Column(String)

class RegistroC320(Base):
    __tablename__ = 'C320'
    ID_C320 = Column(Integer, primary_key=True)
    ID_C300 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_RED_BC = Column(Float)
    COD_OBS = Column(String)

class RegistroC321(Base):
    __tablename__ = 'C321'
    ID_C321 = Column(Integer, primary_key=True)
    ID_C320 = Column(Integer)
    ID_C300 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    VL_DESC = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)

class RegistroC330(Base):
    __tablename__ = 'C330'
    ID_C330 = Column(Integer, primary_key=True)
    ID_C300 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOT_REST_COMPL = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    VL_UNIT_CONV = Column(Float)
    VL_UNIT_ICMS_NA_OPERACAO_CONV = Column(Float)
    VL_UNIT_ICMS_OP_CONV = Column(Float)
    VL_UNIT_ICMS_OP_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_CONV_REST = Column(Float)
    VL_UNIT_FCP_ICMS_ST_CONV_REST = Column(Float)
    VL_UNIT_ICMS_ST_CONV_COMPL = Column(Float)
    VL_UNIT_FCP_ST_CONV_COMPL = Column(Float)

class RegistroC350(Base):
    __tablename__ = 'C350'
    ID_C350 = Column(Integer, primary_key=True)
    ID_C300 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC = Column(Integer)
    DT_DOC = Column(Date)
    CNPJ_CPF = Column(String)
    VL_MERC = Column(Float)
    VL_DOC = Column(Float)
    VL_DESC = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    COD_CTA = Column(String)

class RegistroC370(Base):
    __tablename__ = 'C370'
    ID_C370 = Column(Integer, primary_key=True)
    ID_C350 = Column(Integer)
    ID_C300 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_ITEM = Column(Integer)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    VL_DESC = Column(Float)

class RegistroC380(Base):
    __tablename__ = 'C380'
    ID_C380 = Column(Integer, primary_key=True)
    ID_C300 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOT_REST_COMPL = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    VL_UNIT_CONV = Column(Float)
    VL_UNIT_ICMS_NA_OPERACAO_CONV = Column(Float)
    VL_UNIT_ICMS_OP_CONV = Column(Float)
    VL_UNIT_ICMS_OP_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_CONV_REST = Column(Float)
    VL_UNIT_FCP_ST_CONV_REST = Column(Float)
    VL_UNIT_ICMS_ST_CONV_COMPL = Column(Float)
    VL_UNIT_FCP_ST_CONV_COMPL = Column(Float)
    CST_ICMS = Column(String)
    CFOP = Column(String)

class RegistroC390(Base):
    __tablename__ = 'C390'
    ID_C390 = Column(Integer, primary_key=True)
    ID_C300 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_RED_BC = Column(Float)
    COD_OBS = Column(String)

class RegistroC400(Base):
    __tablename__ = 'C400'
    ID_C400 = Column(Integer, primary_key=True)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    ECF_MOD = Column(String)
    ECF_FAB = Column(String)
    ECF_CX = Column(Integer)

class RegistroC405(Base):
    __tablename__ = 'C405'
    ID_C405 = Column(Integer, primary_key=True)
    ID_C400 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_DOC = Column(Date)
    CRO = Column(Integer)
    CRZ = Column(Integer)
    NUM_COO_FIN = Column(Integer)
    GT_FIN = Column(Float)
    VL_BRT = Column(Float)

class RegistroC410(Base):
    __tablename__ = 'C410'
    ID_C410 = Column(Integer, primary_key=True)
    ID_C405 = Column(Integer)
    ID_C400 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)

class RegistroC420(Base):
    __tablename__ = 'C420'
    ID_C420 = Column(Integer, primary_key=True)
    ID_C400 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_TOT_PAR = Column(String)
    VL_ACUM_TOT = Column(Float)
    NR_TOT = Column(Integer)
    DESCR_NR_TOT = Column(String)

class RegistroC425(Base):
    __tablename__ = 'C425'
    ID_C425 = Column(Integer, primary_key=True)
    ID_C420 = Column(Integer)
    ID_C405 = Column(Integer)
    ID_C400 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)

class RegistroC430(Base):
    __tablename__ = 'C430'
    ID_C430 = Column(Integer, primary_key=True)
    ID_C425 = Column(Integer)
    ID_C420 = Column(Integer)
    ID_C405 = Column(Integer)
    ID_C400 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOT_REST_COMPL = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    VL_UNIT_CONV = Column(Float)
    VL_UNIT_ICMS_NA_OPERACAO_CONV = Column(Float)
    VL_UNIT_ICMS_OP_CONV = Column(Float)
    VL_UNIT_ICMS_OP_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_CONV_REST = Column(Float)
    VL_UNIT_FCP_ST_CONV_REST = Column(Float)
    VL_UNIT_ICMS_ST_CONV_COMPL = Column(Float)
    VL_UNIT_FCP_ST_CONV_COMPL = Column(Float)
    CST_ICMS = Column(String)
    CFOP = Column(String)

class RegistroC460(Base):
    __tablename__ = 'C460'
    ID_C460 = Column(Integer, primary_key=True)
    ID_C405 = Column(Integer)
    ID_C400 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    COD_SIT = Column(String)
    NUM_DOC = Column(String)
    DT_DOC = Column(Date)
    VL_DOC = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    CPF_CNPJ = Column(String)
    NOM_ADQ = Column(String)

class RegistroC465(Base):
    __tablename__ = 'C465'
    ID_C465 = Column(Integer, primary_key=True)
    ID_C460 = Column(Integer)
    ID_C405 = Column(Integer)
    ID_C400 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    CHV_CFE = Column(String)
    NUM_CCF = Column(Integer)

class RegistroC470(Base):
    __tablename__ = 'C470'
    ID_C470 = Column(Integer, primary_key=True)
    ID_C460 = Column(Integer)
    ID_C405 = Column(Integer)
    ID_C400 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    QTD_CANC = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)

class RegistroC480(Base):
    __tablename__ = 'C480'
    ID_C480 = Column(Integer, primary_key=True)
    ID_C470 = Column(Integer)
    ID_C460 = Column(Integer)
    ID_C405 = Column(Integer)
    ID_C400 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOT_REST_COMPL = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    VL_UNIT_CONV = Column(Float)
    VL_UNIT_ICMS_NA_OPERACAO_CONV = Column(Float)
    VL_UNIT_ICMS_OP_CONV = Column(Float)
    VL_UNIT_ICMS_OP_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_CONV_REST = Column(Float)
    VL_UNIT_FCP_ST_CONV_REST = Column(Float)
    VL_UNIT_ICMS_ST_CONV_COMPL = Column(Float)
    VL_UNIT_FCP_ST_CONV_COMPL = Column(Float)
    CST_ICMS = Column(String)
    CFOP = Column(String)

class RegistroC490(Base):
    __tablename__ = 'C490'
    ID_C490 = Column(Integer, primary_key=True)
    ID_C405 = Column(Integer)
    ID_C400 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    COD_OBS = Column(String)

class RegistroC495(Base):
    __tablename__ = 'C495'
    ID_C495 = Column(Integer, primary_key=True)
    ID_C405 = Column(Integer)
    ID_C400 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    ALIQ_ICMS = Column(Float)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    QTD_CANC = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    VL_DESC = Column(Float)
    VL_CANC = Column(Float)
    VL_ACMO = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_ISNT = Column(Float)
    VL_NT = Column(Float)
    VL_ICMS_ST = Column(Float)

class RegistroC500(Base):
    __tablename__ = 'C500'
    ID_C500 = Column(Integer, primary_key=True)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_OPER = Column(String)
    IND_EMIT = Column(String)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    COD_SIT = Column(String)
    SER = Column(String)
    SUB = Column(String)
    COD_CONS = Column(String)
    NUM_DOC = Column(String)
    DT_DOC = Column(Date)
    DT_E_S = Column(Date)
    VL_DOC = Column(Float)
    VL_DESC = Column(Float)
    VL_FORN = Column(Float)
    VL_SERV_NT = Column(Float)
    VL_TERC = Column(Float)
    VL_DA = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    COD_INF = Column(String)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    TP_LIGACAO = Column(String)
    COD_GRUPO_TENSAO = Column(String)
    CHV_DOCe = Column(String)
    FIN_DOCe = Column(String)
    CHV_DOCe_REF = Column(String)
    IND_DEST = Column(String)
    COD_MUN_DEST = Column(String)
    COD_CTA = Column(String)
    COD_MOD_DOC_REF = Column(String)
    HASH_DOC_REF = Column(String)
    SER_DOC_REF = Column(String)
    NUM_DOC_REF = Column(String)
    MES_DOC_REF = Column(String)
    ENER_INJET = Column(Float)
    OUTRAS_DED = Column(Float)


class RegistroC510(Base):
    __tablename__ = 'C510'
    ID_C510 = Column(Integer, primary_key=True)
    ID_C500 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_ITEM = Column(Integer)
    COD_ITEM = Column(String)
    COD_CLASS = Column(String)
    QTD = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    VL_DESC = Column(Float)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    VL_BC_ICMS = Column(Float)
    ALIQ_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    ALIQ_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    IND_REC = Column(String)
    COD_PART = Column(String)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    COD_CTA = Column(String)

class RegistroC590(Base):
    __tablename__ = 'C590'
    ID_C590 = Column(Integer, primary_key=True)
    ID_C500 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    VL_RED_BC = Column(Float)
    COD_OBS = Column(String)

class RegistroC591(Base):
    __tablename__ = 'C591'
    ID_C591 = Column(Integer, primary_key=True)
    ID_C590 = Column(Integer)
    ID_C500 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_FCP_OP = Column(Float)
    VL_FCP_ST = Column(Float)

class RegistroC595(Base):
    __tablename__ = 'C595'
    ID_C595 = Column(Integer, primary_key=True)
    ID_C500 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_OBS = Column(String)
    TXT_COMPL = Column(String)

class RegistroC597(Base):
    __tablename__ = 'C597'
    ID_C597 = Column(Integer, primary_key=True)
    ID_C595 = Column(Integer)
    ID_C500 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_AJ = Column(String)
    DESCR_COMPL_AJ = Column(String)
    COD_ITEM = Column(String)
    VL_BC_ICMS = Column(Float)
    ALIQ_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_OUTROS = Column(Float)

class RegistroC600(Base):
    __tablename__ = 'C600'
    ID_C600 = Column(Integer, primary_key=True)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    COD_MUN = Column(String)
    SER = Column(String)
    SUB = Column(String)
    COD_CONS = Column(String)
    QTD_CONS = Column(Float)
    QTD_CANC = Column(Float)
    DT_DOC = Column(Date)
    VL_DOC = Column(Float)
    VL_DESC = Column(Float)
    CONS = Column(Float)
    VL_FORN = Column(Float)
    VL_SERV_NT = Column(Float)
    VL_TERC = Column(Float)
    VL_DA = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)

class RegistroC601(Base):
    __tablename__ = 'C601'
    ID_C601 = Column(Integer, primary_key=True)
    ID_C600 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_DOC_CANC = Column(String)

class RegistroC610(Base):
    __tablename__ = 'C610'
    ID_C610 = Column(Integer, primary_key=True)
    ID_C600 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_CLASS = Column(String)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    VL_DESC = Column(Float)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    COD_CTA = Column(String)

class RegistroC690(Base):
    __tablename__ = 'C690'
    ID_C690 = Column(Integer, primary_key=True)
    ID_C600 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_RED_BC = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    COD_OBS = Column(String)

class RegistroC700(Base):
    __tablename__ = 'C700'
    ID_C700 = Column(Integer, primary_key=True)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    SER = Column(String)
    NRO_ORD_INI = Column(String)
    NRO_ORD_FIN = Column(String)
    DT_DOC_INI = Column(Date)
    DT_DOC_FIN = Column(Date)
    NOM_MEST = Column(String)
    CHV_COD_DIG = Column(String)

class RegistroC790(Base):
    __tablename__ = 'C790'
    ID_C790 = Column(Integer, primary_key=True)
    ID_C700 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    VL_RED_BC = Column(Float)
    COD_OBS = Column(String)

class RegistroC791(Base):
    __tablename__ = 'C791'
    ID_C791 = Column(Integer, primary_key=True)
    ID_C790 = Column(Integer)
    ID_C700 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    UF = Column(String)
    VL_BC_ICMS_ST = Column(Float)
    VL_ICMS_ST = Column(Float)

class RegistroC800(Base):
    __tablename__ = 'C800'
    ID_C800 = Column(Integer, primary_key=True)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    COD_SIT = Column(String)
    NUM_CFE = Column(String)
    DT_DOC = Column(Date)
    VL_CFE = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    CNPJ_CPF = Column(String)
    NR_SAT = Column(String)
    CHV_CFE = Column(String)
    VL_DESC = Column(Float)
    VL_MERC = Column(Float)
    VL_OUT_DA = Column(Float)
    VL_ICMS = Column(Float)
    VL_PIS_ST = Column(Float)
    VL_COFINS_ST = Column(Float)

class RegistroC810(Base):
    __tablename__ = 'C810'
    ID_C810 = Column(Integer, primary_key=True)
    ID_C800 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_ITEM = Column(String)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    CST_ICMS = Column(String)
    CFOP = Column(String)

class RegistroC815(Base):
    __tablename__ = 'C815'
    ID_C815 = Column(Integer, primary_key=True)
    ID_C810 = Column(Integer)
    ID_C800 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOT_REST_COMPL = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    VL_UNIT_CONV = Column(Float)
    VL_UNIT_ICMS_NA_OPERACAO_CONV = Column(Float)
    VL_UNIT_ICMS_OP_CONV = Column(Float)
    VL_UNIT_ICMS_OP_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_CONV_REST = Column(Float)
    VL_UNIT_FCP_ST_CONV_REST = Column(Float)
    VL_UNIT_ICMS_ST_CONV_COMPL = Column(Float)
    VL_UNIT_FCP_ST_CONV_COMPL = Column(Float)

class RegistroC850(Base):
    __tablename__ = 'C850'
    ID_C850 = Column(Integer, primary_key=True)
    ID_C800 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    COD_OBS = Column(String)

class RegistroC855(Base):
    __tablename__ = 'C855'
    ID_C855 = Column(Integer, primary_key=True)
    ID_C850 = Column(Integer)
    ID_C800 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_OBS = Column(String)
    TXT_COMPL = Column(String)

class RegistroC857(Base):
    __tablename__ = 'C857'
    ID_C857 = Column(Integer, primary_key=True)
    ID_C855 = Column(Integer)
    ID_C850 = Column(Integer)
    ID_C800 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_AJ = Column(String)
    DESCR_COMPL_AJ = Column(String)
    COD_ITEM = Column(String)
    VL_BC_ICMS = Column(Float)
    ALIQ_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_OUTROS = Column(Float)

class RegistroC860(Base):
    __tablename__ = 'C860'
    ID_C860 = Column(Integer, primary_key=True)
    ID_C800 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    NR_SAT = Column(String)
    DT_DOC = Column(Date)
    DOC_INI = Column(String)
    DOC_FIM = Column(String)

class RegistroC870(Base):
    __tablename__ = 'C870'
    ID_C870 = Column(Integer, primary_key=True)
    ID_C860 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    UNID = Column(String)
    CST_ICMS = Column(String)
    CFOP = Column(String)

class RegistroC880(Base):
    __tablename__ = 'C880'
    ID_C880 = Column(Integer, primary_key=True)
    ID_C870 = Column(Integer)
    ID_C860 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOT_REST_COMPL = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    VL_UNIT_CONV = Column(Float)
    VL_UNIT_ICMS_NA_OPERACAO_CONV = Column(Float)
    VL_UNIT_ICMS_OP_CONV = Column(Float)
    VL_UNIT_ICMS_OP_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_CONV_REST = Column(Float)
    VL_UNIT_FCP_ST_CONV_REST = Column(Float)
    VL_UNIT_ICMS_ST_CONV_COMPL = Column(Float)
    VL_UNIT_FCP_ST_CONV_COMPL = Column(Float)

class RegistroC890(Base):
    __tablename__ = 'C890'
    ID_C890 = Column(Integer, primary_key=True)
    ID_C860 = Column(Integer)
    ID_C800 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOT_REST_COMPL = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    VL_UNIT_CONV = Column(Float)
    VL_UNIT_ICMS_NA_OPERACAO_CONV = Column(Float)
    VL_UNIT_ICMS_OP_CONV = Column(Float)
    VL_UNIT_ICMS_OP_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_CONV_REST = Column(Float)
    VL_UNIT_FCP_ST_CONV_REST = Column(Float)
    VL_UNIT_ICMS_ST_CONV_COMPL = Column(Float)
    VL_UNIT_FCP_ST_CONV_COMPL = Column(Float)

class RegistroC895(Base):
    __tablename__ = 'C895'
    ID_C895 = Column(Integer, primary_key=True)
    ID_C890 = Column(Integer)
    ID_C860 = Column(Integer)
    ID_C800 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOT_REST_COMPL = Column(String)
    QUANT_CONV = Column(Float)
    UNID = Column(String)
    VL_UNIT_CONV = Column(Float)
    VL_UNIT_ICMS_NA_OPERACAO_CONV = Column(Float)
    VL_UNIT_ICMS_OP_CONV = Column(Float)
    VL_UNIT_ICMS_OP_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV = Column(Float)
    VL_UNIT_ICMS_ST_CONV_REST = Column(Float)
    VL_UNIT_FCP_ST_CONV_REST = Column(Float)
    VL_UNIT_ICMS_ST_CONV_COMPL = Column(Float)
    VL_UNIT_FCP_ST_CONV_COMPL = Column(Float)

class RegistroC897(Base):
    __tablename__ = 'C897'
    ID_C897 = Column(Integer, primary_key=True)
    ID_C895 = Column(Integer)
    ID_C890 = Column(Integer)
    ID_C860 = Column(Integer)
    ID_C800 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_AJ = Column(String)
    DESCR_COMPL_AJ = Column(String)
    COD_ITEM = Column(String)
    VL_BC_ICMS = Column(Float)
    ALIQ_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_OUTROS = Column(Float)

class RegistroC990(Base):
    __tablename__ = 'C990'
    ID_C990 = Column(Integer, primary_key=True)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    QTD_LIN_C = Column(Integer)


########################## BLOCO D ##########################

class RegistroD001(Base):
    __tablename__ = 'D001'
    ID_D001 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    IND_MOV = Column(String)


class RegistroD100(Base):
    __tablename__ = 'D100'
    ID_D100 = Column(Integer, primary_key=True)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_OPER = Column(String)
    IND_EMIT = Column(String)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    COD_SIT = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC = Column(String)
    CHV_CTE = Column(String)
    DT_DOC = Column(Date)
    DT_A_P = Column(Date)
    TP_CT_e = Column(String)
    CHV_CTE_REF = Column(String)
    VL_DOC = Column(Float)
    VL_DESC = Column(Float)
    IND_FRT = Column(String)
    VL_SERV = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_NT = Column(Float)
    COD_INF = Column(String)
    COD_CTA = Column(String)
    COD_MUN_ORIG = Column(String)
    COD_MUN_DEST = Column(String)

class RegistroD101(Base):
    __tablename__ = 'D101'
    ID_D101 = Column(Integer, primary_key=True)
    ID_D100 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_FCP_UF_DEST = Column(Float)
    VL_ICMS_UF_DEST = Column(Float)
    VL_ICMS_UF_REM = Column(Float)

class RegistroD110(Base):
    __tablename__ = 'D110'
    ID_D110 = Column(Integer, primary_key=True)
    ID_D100 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_ITEM = Column(String)
    COD_ITEM = Column(String)
    VL_SERV = Column(Float)
    VL_OUT = Column(Float)

class RegistroD120(Base):
    __tablename__ = 'D120'
    ID_D120 = Column(Integer, primary_key=True)
    ID_D100 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MUN_ORIG = Column(String)
    COD_MUN_DEST = Column(String)
    VEIC_ID = Column(String)
    UF_ID = Column(String)

class RegistroD130(Base):
    __tablename__ = 'D130'
    ID_D130 = Column(Integer, primary_key=True)
    ID_D100 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART_CONSG = Column(String)
    COD_PART_RED = Column(String)
    IND_FRT_RED = Column(String)
    COD_MUN_ORIG = Column(String)
    COD_MUN_DEST = Column(String)
    VEIC_ID = Column(String)
    VL_LIQ_FRT = Column(Float)
    VL_SEC_CAT = Column(Float)
    VL_DESP = Column(Float)
    VL_TAR = Column(Float)
    VL_ADI = Column(Float)
    VL_PEDG = Column(Float)
    VL_OUT = Column(Float)
    UF_ID = Column(String)

class RegistroD140(Base):
    __tablename__ = 'D140'
    ID_D140 = Column(Integer, primary_key=True)
    ID_D100 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART_CONSG = Column(String)
    COD_MUN_ORIG = Column(String)
    COD_MUN_DEST = Column(String)
    IND_VEIC = Column(String)
    VEIC_ID = Column(String)
    IND_NAV = Column(String)
    VIAGEM = Column(String)
    VL_LIQ_FRT = Column(Float)
    VL_DESP_PORT = Column(Float)
    VL_DESP_CAR_DESC = Column(Float)
    VL_OUT = Column(Float)
    VL_FRT_BRT = Column(Float)
    VL_FRT_MM = Column(Float)

class RegistroD150(Base):
    __tablename__ = 'D150'
    ID_D150 = Column(Integer, primary_key=True)
    ID_D100 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MUN_ORIG = Column(String)
    COD_MUN_DEST = Column(String)
    VEIC_ID = Column(String)
    VIAGEM = Column(String)
    IND_TFA = Column(String)
    VL_PESO_TX = Column(Float)
    VL_TX_TERR = Column(Float)
    VL_TX_RED = Column(Float)
    VL_OUT = Column(Float)
    VL_TX_ADV = Column(Float)

class RegistroD160(Base):
    __tablename__ = 'D160'
    ID_D160 = Column(Integer, primary_key=True)
    ID_D100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    
    DESPACHO = Column(String)
    CNPJ_CPF_REM = Column(String)
    IE_REM = Column(String)
    COD_MUN_ORI = Column(String)
    CNPJ_CPF_DEST = Column(String)
    IE_DEST = Column(String)
    COD_MUN_DEST = Column(String)

class RegistroD161(Base):
    __tablename__ = 'D161'
    ID_D161 = Column(Integer, primary_key=True)
    ID_D160 = Column(Integer)
    ID_D100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    
    IND_CARGA = Column(String)
    CNPJ_CPF_COL = Column(String)
    IE_COL = Column(String)
    COD_MUN_COL = Column(String)
    CNPJ_CPF_ENTG = Column(String)
    IE_ENTG = Column(String)
    COD_MUN_ENTG = Column(String)

class RegistroD162(Base):
    __tablename__ = 'D162'
    ID_D162 = Column(Integer, primary_key=True)
    ID_D160 = Column(Integer)
    ID_D100 = Column(Integer)
    ID_C001 = Column(Integer)
    ID_0000 = Column(Integer)
    
    COD_MOD = Column(String)
    SER = Column(String)
    NUM_DOC = Column(String)
    DT_DOC = Column(Date)
    VL_DOC = Column(Float)
    VL_MERC = Column(Float)
    QTD_VOL = Column(Float)
    PESO_BRT = Column(Float)
    PESO_LIQ = Column(Float)

class RegistroD170(Base):
    __tablename__ = 'D170'
    ID_D170 = Column(Integer, primary_key=True)
    ID_D100 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART_CONSG = Column(String)
    COD_PART_RED = Column(String)
    COD_MUN_ORIG = Column(String)
    COD_MUN_DEST = Column(String)
    OTM = Column(String)
    IND_NAT_FRT = Column(String)
    VL_LIQ_FRT = Column(Float)
    VL_GRIS = Column(Float)
    VL_PDG = Column(Float)
    VL_OUT = Column(Float)
    VL_FRT = Column(Float)
    VEIC_ID = Column(String)
    UF_ID = Column(String)

class RegistroD180(Base):
    __tablename__ = 'D180'
    ID_D180 = Column(Integer, primary_key=True)
    ID_D100 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_SEQ = Column(Integer)
    IND_EMIT = Column(String)
    CNPJ_CPF_EMIT = Column(String)
    UF_EMIT = Column(String)
    IE_EMIT = Column(String)
    COD_MUN_ORIG = Column(String)
    CNPJ_CPF_TOM = Column(String)
    UF_TOM = Column(String)
    IE_TOM = Column(String)
    COD_MUN_DEST = Column(String)
    COD_MOD = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC = Column(String)
    DT_DOC = Column(Date)
    VL_DOC = Column(Float)

class RegistroD190(Base):
    __tablename__ = 'D190'
    ID_D190 = Column(Integer, primary_key=True)
    ID_D100 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_RED_BC = Column(Float)
    COD_OBS = Column(String)

class RegistroD195(Base):
    __tablename__ = 'D195'
    ID_D195 = Column(Integer, primary_key=True)
    ID_D100 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_OBS = Column(String)
    TXT_COMPL = Column(String)

class RegistroD197(Base):
    __tablename__ = 'D197'
    ID_D197 = Column(Integer, primary_key=True)
    ID_D195 = Column(Integer)
    ID_D100 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_AJ = Column(String)
    DESCR_COMPL_AJ = Column(String)
    COD_ITEM = Column(String)
    VL_BC_ICMS = Column(Float)
    ALIQ_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_OUTROS = Column(Float)

class RegistroD300(Base):
    __tablename__ = 'D300'
    ID_D300 = Column(Integer, primary_key=True)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC_INI = Column(String)
    NUM_DOC_FIN = Column(String)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    DT_DOC = Column(Date)
    VL_OPR = Column(Float)
    VL_DESC = Column(Float)
    VL_SERV = Column(Float)
    VL_SEG = Column(Float)
    VL_OUT_DESP = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_RED_BC = Column(Float)
    COD_OBS = Column(String)
    COD_CTA = Column(String)

class RegistroD301(Base):
    __tablename__ = 'D301'
    ID_D301 = Column(Integer, primary_key=True)
    ID_D300 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_DOC_CANC = Column(String)

class RegistroD310(Base):
    __tablename__ = 'D310'
    ID_D310 = Column(Integer, primary_key=True)
    ID_D300 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MUN_ORIG = Column(String)
    VL_SERV = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)

class RegistroD350(Base):
    __tablename__ = 'D350'
    ID_D350 = Column(Integer, primary_key=True)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    ECF_MOD = Column(String)
    ECF_FAB = Column(String)
    ECF_CX = Column(String)

class RegistroD355(Base):
    __tablename__ = 'D355'
    ID_D355 = Column(Integer, primary_key=True)
    ID_D350 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_DOC = Column(Date)
    CRO = Column(String)
    CRZ = Column(String)
    NUM_COO_FIN = Column(String)
    GT_FIN = Column(Float)
    VL_BRT = Column(Float)

class RegistroD360(Base):
    __tablename__ = 'D360'
    ID_D360 = Column(Integer, primary_key=True)
    ID_D355 = Column(Integer)
    ID_D350 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)

class RegistroD365(Base):
    __tablename__ = 'D365'
    ID_D365 = Column(Integer, primary_key=True)
    ID_D355 = Column(Integer)
    ID_D350 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_TOT_PAR = Column(String)
    VLR_ACUM_TOT = Column(Float)
    NR_TOT = Column(String)
    DESCR_NR_TOT = Column(String)

class RegistroD370(Base):
    __tablename__ = 'D370'
    ID_D370 = Column(Integer, primary_key=True)
    ID_D365 = Column(Integer)
    ID_D355 = Column(Integer)
    ID_D350 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MUN_ORIG = Column(String)
    VL_SERV = Column(Float)
    QTD_BILH = Column(Integer)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)

class RegistroD390(Base):
    __tablename__ = 'D390'
    ID_D390 = Column(Integer, primary_key=True)
    ID_D355 = Column(Integer)
    ID_D350 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    ALIQ_ISSQN = Column(Float)
    VL_ISSQN = Column(Float)
    VL_BC_ISSQN = Column(Float)
    VL_ICMS = Column(Float)
    COD_OBS = Column(String)

class RegistroD400(Base):
    __tablename__ = 'D400'
    ID_D400 = Column(Integer, primary_key=True)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    COD_SIT = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC = Column(String)
    DT_DOC = Column(Date)
    VL_DESC = Column(Float)
    VL_SERV = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    COD_CTA = Column(String)

class RegistroD410(Base):
    __tablename__ = 'D410'
    ID_D410 = Column(Integer, primary_key=True)
    ID_D400 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC_INI = Column(String)
    NUM_DOC_FIN = Column(String)
    DT_DOC = Column(Date)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_DESC = Column(Float)
    VL_SERV = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)

class RegistroD411(Base):
    __tablename__ = 'D411'
    ID_D411 = Column(Integer, primary_key=True)
    ID_D410 = Column(Integer)
    ID_D400 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_DOC_CANC = Column(String)

class RegistroD420(Base):
    __tablename__ = 'D420'
    ID_D420 = Column(Integer, primary_key=True)
    ID_D400 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MUN_ORIG = Column(String)
    VL_SERV = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)

class RegistroD500(Base):
    __tablename__ = 'D500'
    ID_D500 = Column(Integer, primary_key=True)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer) 
    IND_OPER = Column(String)
    IND_EMIT = Column(String)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    COD_SIT = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC = Column(Integer)
    DT_DOC = Column(Date)
    DT_A_P = Column(Date)
    VL_DOC = Column(Float)
    VL_DESC = Column(Float)
    VL_SERV = Column(Float)
    VL_SERV_NT = Column(Float)
    VL_TERC = Column(Float)
    VL_DA = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    COD_INF = Column(String)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    COD_CTA = Column(String)
    TP_ASSINANTE = Column(String)

class RegistroD510(Base):
    __tablename__ = 'D510'
    ID_D510 = Column(Integer, primary_key=True)
    ID_D500 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_ITEM = Column(Integer)
    COD_ITEM = Column(String)
    COD_CLASS = Column(String)
    QTD = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    VL_DESC = Column(Float)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    VL_BC_ICMS = Column(Float)
    ALIQ_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_UF = Column(Float)
    VL_ICMS_UF = Column(Float)
    IND_REC = Column(String)
    COD_PART = Column(String)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    COD_CTA = Column(String)

class RegistroD530(Base):
    __tablename__ = 'D530'
    ID_D530 = Column(Integer, primary_key=True)
    ID_D500 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_SERV = Column(String)
    DT_INI_SERV = Column(String)
    DT_FIN_SERV = Column(String)
    PER_FISCAL = Column(String)
    COD_AREA = Column(String)
    TERMINAL = Column(String)

class RegistroD590(Base):
    __tablename__ = 'D590'
    ID_D590 = Column(Integer, primary_key=True)
    ID_D500 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_UF = Column(Float)
    VL_ICMS_UF = Column(Float)
    VL_RED_BC = Column(Float)
    COD_OBS = Column(String)

class RegistroD600(Base):
    __tablename__ = 'D600'
    ID_D600 = Column(Integer, primary_key=True)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    COD_MUN = Column(String)
    SER = Column(String)
    SUB = Column(String)
    COD_CONS = Column(String)
    QTD_CONS = Column(Integer)
    DT_DOC = Column(Date)
    VL_DOC = Column(Float)
    VL_DESC = Column(Float)
    VL_SERV = Column(Float)
    VL_SERV_NT = Column(Float)
    VL_TERC = Column(Float)
    VL_DA = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)

class RegistroD610(Base):
    __tablename__ = 'D610'
    ID_D610 = Column(Integer, primary_key=True)
    ID_D600 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_CLASS = Column(String)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    VL_DESC = Column(Float)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_UF = Column(Float)
    VL_ICMS_UF = Column(Float)
    VL_RED_BC = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    COD_CTA = Column(String)

class RegistroD690(Base):
    __tablename__ = 'D690'
    ID_D690 = Column(Integer, primary_key=True)
    ID_D600 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_UF = Column(Float)
    VL_ICMS_UF = Column(Float)
    VL_RED_BC = Column(Float)
    COD_OBS = Column(String)

class RegistroD695(Base):
    __tablename__ = 'D695'
    ID_D695 = Column(Integer, primary_key=True)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    SER = Column(String)
    NRO_ORD_INI = Column(Integer)
    NRO_ORD_FIN = Column(Integer)
    DT_DOC_INI = Column(Date)
    DT_DOC_FIN = Column(Date)
    NOM_MEST = Column(String)
    CHV_COD_DIG = Column(String)

class RegistroD696(Base):
    __tablename__ = 'D696'
    ID_D696 = Column(Integer, primary_key=True)
    ID_D695 = Column(Integer)
    ID_D600 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_UF = Column(Float)
    VL_ICMS_UF = Column(Float)
    VL_RED_BC = Column(Float)
    COD_OBS = Column(String)

class RegistroD697(Base):
    __tablename__ = 'D697'
    ID_D697 = Column(Integer, primary_key=True)
    ID_D696 = Column(Integer)
    ID_D695 = Column(Integer)
    ID_D600 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    UF = Column(String)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)

class RegistroD700(Base):
    __tablename__ = 'D700'
    ID_D700 = Column(Integer, primary_key=True)
    ID_D600 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_OPER = Column(String)
    IND_EMIT = Column(String)
    COD_PART = Column(Integer)
    COD_MOD = Column(String)
    COD_SIT = Column(String)
    SER = Column(String)
    NUM_DOC = Column(Integer)
    DT_DOC = Column(Date)
    DT_E_S = Column(Date)
    VL_DOC = Column(Float)
    VL_DESC = Column(Float)
    VL_SERV = Column(Float)
    VL_SERV_NT = Column(Float)
    VL_TERC = Column(Float)
    VL_DA = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    COD_INF = Column(String)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    CHV_DOCE = Column(String)
    FIN_DOCE = Column(String)
    TIP_FAT = Column(String)
    COD_MOD_DOC_REF = Column(String)
    CHV_DOCE_REF = Column(String)
    HASH_DOC_REF = Column(String)
    SER_DOC_REF = Column(String)
    NUM_DOC_REF = Column(Integer)
    MES_DOC_REF = Column(String)
    COD_MUN_DEST = Column(String)

class RegistroD730(Base):
    __tablename__ = 'D730'
    ID_D730 = Column(Integer, primary_key=True)
    ID_D700 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_RED_BC = Column(Float)
    COD_OBS = Column(String)

class RegistroD731(Base):
    __tablename__ = 'D731'
    ID_D731 = Column(Integer, primary_key=True)
    ID_D700 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_FCP_OP = Column(Float)

class RegistroD735(Base):
    __tablename__ = 'D735'
    ID_D735 = Column(Integer, primary_key=True)
    ID_D700 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_OBS = Column(String)
    TXT_COMPL = Column(String)

class RegistroD737(Base):
    __tablename__ = 'D737'
    ID_D737 = Column(Integer, primary_key=True)
    ID_D735 = Column(Integer)
    ID_D700 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_AJ = Column(String)
    DESCR_COMPL_AJ = Column(String)
    COD_ITEM = Column(String)
    VL_BC_ICMS = Column(Float)
    ALIQ_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_OUTROS = Column(Float)

class RegistroD750(Base):
    __tablename__ = 'D750'
    ID_D750 = Column(Integer, primary_key=True)
    ID_D700 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    SER = Column(String)
    DT_DOC = Column(Date)
    QTD_CONS = Column(Integer)
    IND_PREPAGO = Column(Integer)
    VL_DOC = Column(Float)
    VL_SERV = Column(Float)
    VL_SERV_NT = Column(Float)
    VL_TERC = Column(Float)
    VL_DESC = Column(Float)
    VL_DA = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)

class RegistroD760(Base):
    __tablename__ = 'D760'
    ID_D760 = Column(Integer, primary_key=True)
    ID_D750 = Column(Integer)
    ID_D700 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    ALIQ_ICMS = Column(Float)
    VL_OPR = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_RED_BC = Column(Float)
    COD_OBS = Column(String)

class RegistroD761(Base):
    __tablename__ = 'D761'
    ID_D761 = Column(Integer, primary_key=True)
    ID_D760 = Column(Integer)
    ID_D750 = Column(Integer)
    ID_D700 = Column(Integer)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_FCP_OP = Column(Float)

class RegistroD990(Base):
    __tablename__ = 'D990'
    ID_D990 = Column(Integer, primary_key=True)
    ID_D001 = Column(Integer)
    ID_0000 = Column(Integer)
    QTD_LIN_D = Column(Integer)


########################## BLOCO E ##########################

class RegistroE001(Base):
    __tablename__ = 'E001'
    ID_E001 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    IND_MOV = Column(String)

class RegistroE100(Base):
    __tablename__ = 'E100'
    ID_E100 = Column(Integer, primary_key=True)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_INI = Column(String)
    DT_FIN = Column(String)

class RegistroE110(Base):
    __tablename__ = 'E110'
    ID_E110 = Column(Integer, primary_key=True)
    ID_E100 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_TOT_DEBITOS = Column(Float)
    VL_AJ_DEBITOS = Column(Float)
    VL_TOT_AJ_DEBITOS = Column(Float)
    VL_ESTORNOS_CRED = Column(Float)
    VL_TOT_CREDITOS = Column(Float)
    VL_AJ_CREDITOS = Column(Float)
    VL_TOT_AJ_CREDITOS = Column(Float)
    VL_ESTORNOS_DEB = Column(Float)
    VL_SLD_CREDOR_ANT = Column(Float)
    VL_SLD_APURADO = Column(Float)
    VL_TOT_DED = Column(Float)
    VL_ICMS_RECOLHER = Column(Float)
    VL_SLD_CREDOR_TRANSPORTAR = Column(Float)
    DEB_ESP = Column(Float)

class RegistroE111(Base):
    __tablename__ = 'E111'
    ID_E111 = Column(Integer, primary_key=True)
    ID_E110 = Column(Integer)
    ID_E100 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_AJ_APUR = Column(String)
    DESCR_COMPL_AJ = Column(String)
    VL_AJ_APUR = Column(Float)

class RegistroE112(Base):
    __tablename__ = 'E112'
    ID_E112 = Column(Integer, primary_key=True)
    ID_E111 = Column(Integer)
    ID_E110 = Column(Integer)
    ID_E100 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_DA = Column(String)
    NUM_PROC = Column(String)
    IND_PROC = Column(String)
    PROC = Column(String)
    TXT_COMPL = Column(String)

class RegistroE113(Base):
    __tablename__ = 'E113'
    ID_E113 = Column(Integer, primary_key=True)
    ID_E111 = Column(Integer)
    ID_E110 = Column(Integer)
    ID_E100 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC = Column(String)
    DT_DOC = Column(String)
    COD_ITEM = Column(String)
    VL_AJ_ITEM = Column(Float)
    CHV_DOCe = Column(String)

class RegistroE115(Base):
    __tablename__ = 'E115'
    ID_E115 = Column(Integer, primary_key=True)
    ID_E110 = Column(Integer)
    ID_E100 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_INF_ADIC = Column(String)
    VL_INF_ADIC = Column(Float)
    DESCR_COMPL_AJ = Column(String)

class RegistroE116(Base):
    __tablename__ = 'E116'
    ID_E116 = Column(Integer, primary_key=True)
    ID_E110 = Column(Integer)
    ID_E100 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_OR = Column(String)
    VL_OR = Column(Float)
    DT_VCTO = Column(Date)
    COD_REC = Column(String)
    NUM_PROC = Column(String)
    IND_PROC = Column(String)
    PROC = Column(String)
    TXT_COMPL = Column(String)
    MES_REF = Column(String)

class RegistroE200(Base):
    __tablename__ = 'E200'
    ID_E200 = Column(Integer, primary_key=True)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    UF = Column(String)
    DT_INI = Column(Date)
    DT_FIN = Column(Date)

class RegistroE210(Base):
    __tablename__ = 'E210'
    ID_E210 = Column(Integer, primary_key=True)
    ID_E200 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_MOV_ST = Column(String)
    VL_SLD_CRED_ANT_ST = Column(Float)
    VL_DEVOL_ST = Column(Float)
    VL_RESSARC_ST = Column(Float)
    VL_OUT_CRED_ST = Column(Float)
    VL_OUT_DEB_ST = Column(Float)
    VL_SLD_CRED_ST_TRANSPORTAR = Column(Float)
    VL_RETENCAO_ST = Column(Float)
    VL_ICMS_RECOL_ST = Column(Float)
    VL_SLD_DEB_ANT_ST = Column(Float)
    VL_DEDUCOES_ST = Column(Float)
    VL_SLD_CREDOR_TRANS_ST = Column(Float)
    VL_ICMS_RECOLHER_ST = Column(Float)
    VL_DEDUCOES_ST = Column(Float)
    DEB_ESP_ST = Column(Float)

class RegistroE220(Base):
    __tablename__ = 'E220'
    ID_E220 = Column(Integer, primary_key=True)
    ID_E210 = Column(Integer)
    ID_E100 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_AJ_APUR = Column(String)
    DESCR_COMPL_AJ = Column(String)
    VL_AJ_APUR = Column(Float)

class RegistroE230(Base):
    __tablename__ = 'E230'
    ID_E230 = Column(Integer, primary_key=True)
    ID_E220 = Column(Integer)
    ID_E210 = Column(Integer)
    ID_E100 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_DA = Column(String)
    NUM_PROC = Column(String)
    IND_PROC = Column(String)
    PROC = Column(String)
    TXT_COMPL = Column(String)

class RegistroE240(Base):
    __tablename__ = 'E240'
    ID_E240 = Column(Integer, primary_key=True)
    ID_E220 = Column(Integer)
    ID_E210 = Column(Integer)
    ID_E100 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC = Column(String)
    DT_DOC = Column(Date)
    COD_ITEM = Column(String)
    VL_AJ_ITEM = Column(Float)
    CHV_DOCE = Column(String)


class RegistroE250(Base):
    __tablename__ = 'E250'
    ID_E250 = Column(Integer, primary_key=True)
    ID_E210 = Column(Integer)
    ID_E200 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_OR = Column(String)
    VL_OR = Column(Float)
    DT_VCTO = Column(Date)
    COD_REC = Column(String)
    NUM_PROC = Column(String)
    IND_PROC = Column(String)
    PROC = Column(String)
    TXT_COMPL = Column(String)
    MES_REF = Column(String)

class RegistroE300(Base):
    __tablename__ = 'E300'
    ID_E300 = Column(Integer, primary_key=True)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    UF = Column(String)
    DT_INI = Column(Date)
    DT_FIN = Column(Date)

class RegistroE310(Base):
    __tablename__ = 'E310'
    ID_E310 = Column(Integer, primary_key=True)
    ID_E300 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_MOV_FCP_DIFAL = Column(String)
    VL_SLD_CRED_ANT_DIFAL = Column(Float)
    VL_TOT_DEBITOS_DIFAL = Column(Float)
    VL_OUT_DEB_DIFAL = Column(Float)
    VL_TOT_CREDITOS_DIFAL = Column(Float)
    VL_OUT_CRED_DIFAL = Column(Float)
    VL_SLD_DEV_ANT_DIFAL = Column(Float)
    VL_DEDUCOES_DIFAL = Column(Float)
    VL_RECOL_DIFAL = Column(Float)
    VL_SLD_CRED_TRANSPORTAR_DIFAL = Column(Float)
    DEB_ESP_DIFAL = Column(Float)
    VL_SLD_CRED_ANT_FCP = Column(Float)
    VL_TOT_DEB_FCP = Column(Float)
    VL_OUT_DEB_FCP = Column(Float)
    VL_TOT_CRED_FCP = Column(Float)
    VL_OUT_CRED_FCP = Column(Float)
    VL_SLD_DEV_ANT_FCP = Column(Float)
    VL_DEDUCOES_FCP = Column(Float)
    VL_RECOL_FCP = Column(Float)
    VL_SLD_CRED_TRANSPORTAR_FCP = Column(Float)
    DEB_ESP_FCP = Column(Float)

class RegistroE311(Base):
    __tablename__ = 'E311'
    ID_E311 = Column(Integer, primary_key=True)
    ID_E310 = Column(Integer)
    ID_E300 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_AJ_APUR = Column(String)
    DESCR_COMPL_AJ = Column(String)
    VL_AJ_APUR = Column(Float)

class RegistroE312(Base):
    __tablename__ = 'E312'
    ID_E312 = Column(Integer, primary_key=True)
    ID_E311 = Column(Integer)
    ID_E310 = Column(Integer)
    ID_E300 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_DA = Column(String)
    NUM_PROC = Column(String)
    IND_PROC = Column(String)
    PROC = Column(String)
    TXT_COMPL = Column(String)

class RegistroE313(Base):
    __tablename__ = 'E313'
    ID_E313 = Column(Integer, primary_key=True)
    ID_E311 = Column(Integer)
    ID_E310 = Column(Integer)
    ID_E300 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC = Column(String)
    CHV_DOCE = Column(String)
    DT_DOC = Column(String)
    COD_ITEM = Column(String)
    VL_AJ_ITEM = Column(Float)

class RegistroE316(Base):
    __tablename__ = 'E316'
    ID_E316 = Column(Integer, primary_key=True)
    ID_E310 = Column(Integer)
    ID_E300 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_OR = Column(String)
    VL_OR = Column(Float)
    DT_VCTO = Column(String)
    COD_REC = Column(String)
    NUM_PROC = Column(String)
    IND_PROC = Column(String)
    PROC = Column(String)
    TXT_COMPL = Column(String)
    MES_REF = Column(String)

class RegistroE500(Base):
    __tablename__ = 'E500'
    ID_E500 = Column(Integer, primary_key=True)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_APUR = Column(String)
    DT_INI = Column(String)
    DT_FIN = Column(String)

class RegistroE510(Base):
    __tablename__ = 'E510'
    ID_E510 = Column(Integer, primary_key=True)
    ID_E500 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    CFOP = Column(String)
    CST_IPI = Column(String)
    VL_CONT_IPI = Column(Float)
    VL_BC_IPI = Column(Float)
    VL_IPI = Column(Float)

class RegistroE520(Base):
    __tablename__ = 'E520'
    ID_E520 = Column(Integer, primary_key=True)
    ID_E500 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_SD_ANT_IPI = Column(Float)
    VL_DEB_IPI = Column(Float)
    VL_CRED_IPI = Column(Float)
    VL_OD_IPI = Column(Float)
    VL_OC_IPI = Column(Float)
    VL_SC_IPI = Column(Float)
    VL_SD_IPI = Column(Float)

class RegistroE530(Base):
    __tablename__ = 'E530'
    ID_E530 = Column(Integer, primary_key=True)
    ID_E520 = Column(Integer)
    ID_E500 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_AJ = Column(Integer)
    VL_AJ = Column(Float)
    COD_AJ = Column(String)
    IND_DOC = Column(Integer)
    NUM_DOC = Column(String)
    DESCR_AJ = Column(String)

class RegistroE531(Base):
    __tablename__ = 'E531'
    ID_E531 = Column(Integer, primary_key=True)
    ID_E530 = Column(Integer)
    ID_E520 = Column(Integer)
    ID_E500 = Column(Integer)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC = Column(String)
    DT_DOC = Column(String)
    COD_ITEM = Column(String)
    VL_AJ_ITEM = Column(Float)
    CHV_NFE = Column(String)

class RegistroE990(Base):
    __tablename__ = 'E990'
    ID_E990 = Column(Integer, primary_key=True)
    ID_E001 = Column(Integer)
    ID_0000 = Column(Integer)
    QTD_LIN_E = Column(Integer)


########################## BLOCO G ##########################


class RegistroG001(Base):
    __tablename__ = 'G001'
    ID_G001 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    IND_MOV = Column(Integer)

class RegistroG110(Base):
    __tablename__ = 'G110'
    ID_G110 = Column(Integer, primary_key=True)
    ID_G001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_INI = Column(String)
    DT_FIN = Column(String)
    SALDO_IN_ICMS = Column(Float)
    SOM_PARC = Column(Float)
    VL_TRIB_EXP = Column(Float)
    VL_TOTAL = Column(Float)
    IND_PER_SAI = Column(Float)
    ICMS_APROP = Column(Float)
    SOM_ICMS_OC = Column(Float)

class RegistroG125(Base):
    __tablename__ = 'G125'
    ID_G125 = Column(Integer, primary_key=True)
    ID_G110 = Column(Integer)
    ID_G001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_IND_BEM = Column(String)
    DT_MOV = Column(String)
    TIPO_MOV = Column(String)
    VL_IMOB_ICMS_OP = Column(Float)
    VL_IMOB_ICMS_ST = Column(Float)
    VL_IMOB_ICMS_FRT = Column(Float)
    VL_IMOB_ICMS_DIF = Column(Float)
    NUM_PARC = Column(Integer)
    VL_PARC_PASS = Column(Float)

class RegistroG126(Base):
    __tablename__ = 'G126'
    ID_G126 = Column(Integer, primary_key=True)
    ID_G125 = Column(Integer)
    ID_G110 = Column(Integer)
    ID_G001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_INI = Column(String)
    DT_FIM = Column(String)
    NUM_PARC = Column(Integer)
    VL_PARC_PASS = Column(Float)
    VL_TRIB_OC = Column(Float)
    VL_TOTAL = Column(Float)
    IND_PER_SAI = Column(Float)
    VL_PARC_APROP = Column(Float)

class RegistroG130(Base):
    __tablename__ = 'G130'
    ID_G130 = Column(Integer, primary_key=True)
    ID_G125 = Column(Integer)
    ID_G110 = Column(Integer)
    ID_G001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_EMIT = Column(Integer)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    SER = Column(String)
    NUM_DOC = Column(String)
    CHV_NFE_CTE = Column(String)
    DT_DOC = Column(Date)
    NUM_DA = Column(String)

class RegistroG140(Base):
    __tablename__ = 'G140'
    ID_G140 = Column(Integer, primary_key=True)
    ID_G130 = Column(Integer)
    ID_G125 = Column(Integer)
    ID_G110 = Column(Integer)
    ID_G001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_ITEM = Column(Integer)
    COD_ITEM = Column(String)
    QTDE = Column(Float)
    UNID = Column(String)
    VL_ICMS_OP_APLICADO = Column(Float)
    VL_ICMS_ST_APLICADO = Column(Float)
    VL_ICMS_FRT_APLICADO = Column(Float)
    VL_ICMS_DIF_APLICADO = Column(Float)

class RegistroG990(Base):
    __tablename__ = 'G990'
    ID_G990 = Column(Integer, primary_key=True)
    ID_G001 = Column(Integer)
    ID_0000 = Column(Integer)
    QTD_LIN_G = Column(Integer)


########################## BLOCO H ##########################

class RegistroH001(Base):
    __tablename__ = 'H001'
    ID_H001 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    
    IND_MOV = Column(String)

class RegistroH005(Base):
    __tablename__ = 'H005'
    ID_H005 = Column(Integer, primary_key=True)
    ID_H001 = Column(Integer)
    ID_0000 = Column(Integer)
    
    DT_INV = Column(Date)
    VL_INV = Column(Float)
    MOT_INV = Column(String)

class RegistroH010(Base):
    __tablename__ = 'H010'
    ID_H010 = Column(Integer, primary_key=True)
    ID_H005 = Column(Integer)
    ID_0000 = Column(Integer)
    
    COD_ITEM = Column(String)
    UNID = Column(String)
    QTD = Column(Float)
    VL_UNIT = Column(Float)
    VL_ITEM = Column(Float)
    IND_PROP = Column(String)
    COD_PART = Column(String)
    TXT_COMPL = Column(String)
    COD_CTA = Column(String)
    VL_ITEM_IR = Column(Float)

class RegistroH020(Base):
    __tablename__ = 'H020'
    ID_H020 = Column(Integer, primary_key=True)
    ID_H010 = Column(Integer)
    ID_H005 = Column(Integer)
    ID_H001 = Column(Integer)
    ID_0000 = Column(Integer)
    CST_ICMS = Column(String)
    BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)

class RegistroH030(Base):
    __tablename__ = 'H030'
    ID_H030 = Column(Integer, primary_key=True)
    ID_H010 = Column(Integer)
    ID_H005 = Column(Integer)
    ID_H001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_ICMS_OP = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    VL_FCP = Column(Float)

class RegistroH990(Base):
    __tablename__ = 'H990'
    ID_H990 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    QTD_LIN_H = Column(Integer)


########################## BLOCO K ##########################


class RegistroK001(Base):
    __tablename__ = 'K001'
    ID_K001 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    IND_MOV = Column(String)

class RegistroK010(Base):
    __tablename__ = 'K010'
    ID_K010 = Column(Integer, primary_key=True)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_TP_LEIAUTE = Column(String)

class RegistroK100(Base):
    __tablename__ = 'K100'
    ID_K100 = Column(Integer, primary_key=True)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_INI = Column(String)
    DT_FIN = Column(String)

class RegistroK200(Base):
    __tablename__ = 'K200'
    ID_K200 = Column(Integer, primary_key=True)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_EST = Column(String)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    IND_EST = Column(Integer)
    COD_PART = Column(String)

class RegistroK210(Base):
    __tablename__ = 'K210'
    ID_K210 = Column(Integer, primary_key=True)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_INI_OS = Column(String)
    DT_FIN_OS = Column(String)
    COD_DOC_OS = Column(String)
    COD_ITEM_ORI = Column(String)
    QTD_ORI = Column(Float)

class RegistroK215(Base):
    __tablename__ = 'K215'
    ID_K215 = Column(Integer, primary_key=True)
    ID_K210 = Column(Integer)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM_DES = Column(String)
    QTD_DES = Column(Float)

class RegistroK220(Base):
    __tablename__ = 'K220'
    ID_K220 = Column(Integer, primary_key=True)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_MOV = Column(Date)
    COD_ITEM_ORI = Column(String)
    COD_ITEM_DEST = Column(String)
    QTD_ORI = Column(Float)
    QTD_DEST = Column(Float)

class RegistroK230(Base):
    __tablename__ = 'K230'
    ID_K230 = Column(Integer, primary_key=True)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_INI_OP = Column(Date)
    DT_FIN_OP = Column(Date)
    COD_DOC_OP = Column(String)
    COD_ITEM = Column(String)
    QTD_ENC = Column(Float)

class RegistroK235(Base):
    __tablename__ = 'K235'
    ID_K235 = Column(Integer, primary_key=True)
    ID_K230 = Column(Integer)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_SAIDA = Column(Date)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    COD_INS_SUBST = Column(String)

class RegistroK250(Base):
    __tablename__ = 'K250'
    ID_K250 = Column(Integer, primary_key=True)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_PROD = Column(Date)
    COD_ITEM = Column(String)
    QTD = Column(Float)

class RegistroK255(Base):
    __tablename__ = 'K255'
    ID_K255 = Column(Integer, primary_key=True)
    ID_K250 = Column(Integer)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_CONS = Column(Date)
    COD_ITEM = Column(String)
    QTD = Column(Float)
    COD_INS_SUBST = Column(String)

class RegistroK260(Base):
    __tablename__ = 'K260'
    ID_K260 = Column(Integer, primary_key=True)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_OPS = Column(String)
    COD_ITEM = Column(String)
    QTD_RET = Column(Float)
    DT_RET = Column(Date)

class RegistroK265(Base):
    __tablename__ = 'K265'
    ID_K265 = Column(Integer, primary_key=True)
    ID_K260 = Column(Integer)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    QTD_CONS = Column(Float)
    QTD_RET = Column(Float)

class RegistroK270(Base):
    __tablename__ = 'K270'
    ID_K270 = Column(Integer, primary_key=True)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_INI_AP = Column(Date)
    DT_FIN_AP = Column(Date)
    COD_OP_OS = Column(String)
    COD_ITEM = Column(String)
    QTD_COR_POS = Column(Float)
    QTD_COR_NEG = Column(Float)
    ORIGEM = Column(Integer)

class RegistroK275(Base):
    __tablename__ = 'K275'
    ID_K275 = Column(Integer, primary_key=True)
    ID_K270 = Column(Integer)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    QTD_COR_POS = Column(Float)
    QTD_COR_NEG = Column(Float)
    COD_INS_SUBST = Column(String)

class RegistroK280(Base):
    __tablename__ = 'K280'
    ID_K280 = Column(Integer, primary_key=True)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_EST = Column(Date)
    COD_ITEM = Column(String)
    QTD_COR_POS = Column(Float)
    QTD_COR_NEG = Column(Float)
    IND_EST = Column(Integer)
    COD_PART = Column(String)

class RegistroK290(Base):
    __tablename__ = 'K290'
    ID_K290 = Column(Integer, primary_key=True)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_DOC_OP = Column(String)
    QTD = Column(Float)

class RegistroK291(Base):
    __tablename__ = 'K291'
    ID_K291 = Column(Integer, primary_key=True)
    ID_K290 = Column(Integer)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    QTD = Column(Float)

class RegistroK292(Base):
    __tablename__ = 'K292'
    ID_K292 = Column(Integer, primary_key=True)
    ID_K290 = Column(Integer)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    QTD = Column(Float)

class RegistroK300(Base):
    __tablename__ = 'K300'
    ID_K300 = Column(Integer, primary_key=True)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_PROD = Column(Date)

class RegistroK301(Base):
    __tablename__ = 'K301'
    ID_K301 = Column(Integer, primary_key=True)
    ID_K300 = Column(Integer)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    QTD = Column(Float)

class RegistroK302(Base):
    __tablename__ = 'K302'
    ID_K302 = Column(Integer, primary_key=True)
    ID_K300 = Column(Integer)
    ID_K100 = Column(Integer)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    QTD = Column(Float)

class RegistroK990(Base):
    __tablename__ = 'K990'
    ID_K990 = Column(Integer, primary_key=True)
    ID_K001 = Column(Integer)
    ID_0000 = Column(Integer)
    QTD_LIN_K = Column(Integer)


########################## BLOCO 1 ##########################

class Registro1001(Base):
    __tablename__ = 'INFO1001'
    ID_1001 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    IND_MOV = Column(Integer)


class Registro1010(Base):
    __tablename__ = 'INFO1010'
    ID_1010 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_EXP = Column(String)
    IND_CCRF = Column(String)
    IND_COMB = Column(String)
    IND_USINA = Column(String)
    IND_VA = Column(String)
    IND_EE = Column(String)
    IND_CART = Column(String)
    IND_FORM = Column(String)
    IND_AER = Column(String)
    IND_GIAF1 = Column(String)
    IND_GIAF3 = Column(String)
    IND_GIAF4 = Column(String)
    IND_REST_RESSARC_COMPL_ICMS = Column(String)

class Registro1100(Base):
    __tablename__ = 'INFO1100'
    ID_1100 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_DOC = Column(Integer)
    NRO_DE = Column(String)
    DT_DE = Column(String)
    NAT_EXP = Column(Integer)
    NRO_RE = Column(Integer)
    DT_RE = Column(String)
    CHC_EMB = Column(String)
    DT_CHC = Column(String)
    DT_AVB = Column(String)
    TP_CHC = Column(Integer)
    PAIS = Column(String)

class Registro1105(Base):
    __tablename__ = 'INFO1105'
    ID_1105 = Column(Integer, primary_key=True)
    ID_1100 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOD = Column(String)
    SERIE = Column(String)
    NUM_DOC = Column(String)
    CHV_NFE = Column(String)
    DT_DOC = Column(String)
    COD_ITEM = Column(String)

class Registro1110(Base):
    __tablename__ = 'INFO1110'
    ID_1110 = Column(Integer, primary_key=True)
    ID_1105 = Column(Integer)
    ID_1100 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    SER = Column(String)
    NUM_DOC = Column(Integer)
    DT_DOC = Column(String)
    CHV_NFE = Column(String)
    NR_MEMO = Column(String)
    QTD = Column(Float)
    UNID = Column(String)

class Registro1200(Base):
    __tablename__ = 'INFO1200'
    ID_1200 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_AJ_APUR = Column(String)
    SLD_CRED = Column(Float)
    CRED_APR = Column(Float)
    CRED_RECEB = Column(Float)
    CRED_UTIL = Column(Float)
    SLD_CRED_FIM = Column(Float)

class Registro1210(Base):
    __tablename__ = 'INFO1210'
    ID_1210 = Column(Integer, primary_key=True)
    ID_1200 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    TIPO_UTIL = Column(String)
    NR_DOC = Column(String)
    VL_CRED_UTIL = Column(Float)
    CHV_DOCe = Column(String)

class Registro1250(Base):
    __tablename__ = 'INFO1250'
    ID_1250 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_CREDITO_ICMS_OP = Column(Float)
    VL_ICMS_ST_REST = Column(Float)
    VL_FCP_ST_REST = Column(Float)
    VL_ICMS_ST_COMPL = Column(Float)
    VL_FCP_ST_COMPL = Column(Float)

class Registro1255(Base):
    __tablename__ = 'INFO1255'
    ID_1255 = Column(Integer, primary_key=True)
    ID_1250 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_MOT_REST_COMPL = Column(String)
    VL_CREDITO_ICMS_OP_MOT = Column(Float)
    VL_ICMS_ST_REST_MOT = Column(Float)
    VL_FCP_ST_REST_MOT = Column(Float)
    VL_ICMS_ST_COMPL_MOT = Column(Float)
    VL_FCP_ST_COMPL_MOT = Column(Float)

class Registro1300(Base):
    __tablename__ = 'INFO1300'
    ID_1300 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    DT_FECH = Column(Date)
    ESTQ_ABERT = Column(Float)
    VOL_ENTR = Column(Float)
    VOL_DISP = Column(Float)
    VOL_SAIDAS = Column(Float)
    ESTQ_ESCR = Column(Float)
    VAL_AJ_PERDA = Column(Float)
    VAL_AJ_GANHO = Column(Float)
    FECH_FISICO = Column(Float)

class Registro1310(Base):
    __tablename__ = 'INFO1310'
    ID_1310 = Column(Integer, primary_key=True)
    ID_1300 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_TANQUE = Column(String)
    ESTQ_ABERT = Column(Float)
    VOL_ENTR = Column(Float)
    VOL_DISP = Column(Float)
    VOL_SAIDAS = Column(Float)
    ESTQ_ESCR = Column(Float)
    VAL_AJ_PERDA = Column(Float)
    VAL_AJ_GANHO = Column(Float)
    FECH_FISICO = Column(Float)

class Registro1320(Base):
    __tablename__ = 'INFO1320'
    ID_1320 = Column(Integer, primary_key=True)
    ID_1310 = Column(Integer)
    ID_1300 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_BICO = Column(Integer)
    NUM_INTERV = Column(Integer)
    MOT_INTERV = Column(String)
    NOM_INTERV = Column(String)
    CNPJ_INTERV = Column(String)
    CPF_INTERV = Column(String)
    VAL_FECHA = Column(Float)
    VAL_ABERT = Column(Float)
    VOL_AFERI = Column(Float)
    VOL_VENDAS = Column(Float)

class Registro1350(Base):
    __tablename__ = 'INFO1350'
    ID_1350 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    SERIE = Column(String)
    FABRICANTE = Column(String)
    MODELO = Column(String)
    TIPO_MEDICAO = Column(Integer)

class Registro1360(Base):
    __tablename__ = 'INFO1360'
    ID_1360 = Column(Integer, primary_key=True)
    ID_1350 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_LACRE = Column(String)
    DT_APLICACAO = Column(String)

class Registro1370(Base):
    __tablename__ = 'INFO1370'
    ID_1370 = Column(Integer, primary_key=True)
    ID_1350 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_BICO = Column(Integer)
    COD_ITEM = Column(String)
    NUM_TANQUE = Column(Integer)

class Registro1390(Base):
    __tablename__ = 'INFO1390'
    ID_1390 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PROD = Column(String)

class Registro1391(Base):
    __tablename__ = 'INFO1391'
    ID_1391 = Column(Integer, primary_key=True)
    ID_1390 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_REGISTRO = Column(String)
    QTD_MOD = Column(Float)
    ESTQ_INI = Column(Float)
    QTD_PRODUZ = Column(Float)
    ENT_ANID_HID = Column(Float)
    OUTR_ENTR = Column(Float)
    PERDA = Column(Float)
    CONS = Column(Float)
    SAI_ANI_HID = Column(Float)
    SAIDAS = Column(Float)
    ESTQ_FIN = Column(Float)
    ESTQ_INI_MEL = Column(Float)
    PROD_DIA_MEL = Column(Float)
    UTIL_MEL = Column(Float)
    PROD_ALC_MEL = Column(Float)
    OBS = Column(String)
    COD_ITEM = Column(String)
    TP_RESIDUO = Column(Integer)
    QTD_RESIDUO = Column(Float)
    QTD_RESIDUO_DDG = Column(Float)
    QTD_RESIDUO_WDG = Column(Float)
    QTD_RESIDUO_CANA = Column(Float)

class Registro1400(Base):
    __tablename__ = 'INFO1400'
    ID_1400 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_1001 = Column(Integer)
    COD_ITEM_IPM = Column(String)
    MUN = Column(String)
    VALOR = Column(Float)

class Registro1500(Base):
    __tablename__ = 'INFO1500'
    ID_1500 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    ID_1001 = Column(Integer)
    IND_OPER = Column(Integer)
    IND_EMIT = Column(Integer)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    COD_SIT = Column(String)
    SER = Column(String)
    SUB = Column(String)
    COD_CONS = Column(Integer)
    NUM_DOC = Column(Integer)
    DT_DOC = Column(Date)
    DT_E_S = Column(Date)
    VL_DOC = Column(Float)
    VL_DESC = Column(Float)
    VL_FORN = Column(Float)
    VL_SERV_NT = Column(Float)
    VL_TERC = Column(Float)
    VL_DA = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    COD_INF = Column(String)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    TP_LIGACAO = Column(Integer)
    COD_GRUPO_TENSAO = Column(Integer)

class Registro1510(Base):
    __tablename__ = 'INFO1510'
    ID_1510 = Column(Integer, primary_key=True)
    ID_1500 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_ITEM = Column(String)
    COD_CLASS = Column(String)
    QTD = Column(Float)
    UNID = Column(String)
    VL_ITEM = Column(Float)
    VL_DESC = Column(Float)
    CST_ICMS = Column(String)
    CFOP = Column(String)
    VL_BC_ICMS = Column(Float)
    ALIQ_ICMS = Column(Float)
    VL_ICMS = Column(Float)
    VL_BC_ICMS_ST = Column(Float)
    ALIQ_ST = Column(Float)
    VL_ICMS_ST = Column(Float)
    IND_REC = Column(Integer)
    COD_PART = Column(String)
    VL_PIS = Column(Float)
    VL_COFINS = Column(Float)
    COD_CTA = Column(String)

class Registro1600(Base):
    __tablename__ = 'INFO1600'
    ID_1600 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART = Column(String)
    TOT_CREDITO = Column(Float)
    
class Registro1601(Base):
    __tablename__ = 'INFO1601'
    ID_1601 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART_IP = Column(String)
    COD_PART_IT = Column(String)
    TOT_VS = Column(Float)
    TOT_ISS = Column(Float)
    TOT_OUTROS = Column(Float)

class Registro1700(Base):
    __tablename__ = 'INFO1700'
    ID_1700 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_DISP = Column(String)
    COD_MOD = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC_INI = Column(Integer)
    NUM_DOC_FIN = Column(Integer)
    NUM_AUT = Column(Integer)


class Registro1710(Base):
    __tablename__ = 'INFO1710'
    ID_1710 = Column(Integer, primary_key=True)
    ID_1700 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_DOC_INI = Column(Integer)
    NUM_DOC_FIN = Column(Integer)


class Registro1800(Base):
    __tablename__ = 'INFO1800'
    ID_1800 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_CARGA = Column(Float)
    VL_PASS = Column(Float)
    VL_FAT = Column(Float)
    IND_RAT = Column(Float)
    VL_ICMS_ANT = Column(Float)
    VL_BC_ICMS = Column(Float)
    VL_ICMS_APUR = Column(Float)
    VL_BC_ICMS_APUR = Column(Float)
    VL_DIF = Column(Float)

class Registro1900(Base):
    __tablename__ = 'INFO1900'
    ID_1900 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_APUR_ICMS = Column(String)
    DESCR_COMPL_OUT_APUR = Column(String)

class Registro1910(Base):
    __tablename__ = 'INFO1910'
    ID_1910 = Column(Integer, primary_key=True)
    ID_1900 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    DT_INI = Column(Date)
    DT_FIN = Column(Date)

class Registro1920(Base):
    __tablename__ = 'INFO1920'
    ID_1920 = Column(Integer, primary_key=True)
    ID_1910 = Column(Integer)
    ID_1900 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    VL_TOT_TRANSF_DEBITOS_OA = Column(Float)
    VL_TOT_AJ_DEBITOS_OA = Column(Float)
    VL_ESTORNOS_CRED_OA = Column(Float)
    VL_TOT_TRANSF_CREDITOS_OA = Column(Float)
    VL_TOT_AJ_CREDITOS_OA = Column(Float)
    VL_ESTORNOS_DEB_OA = Column(Float)
    VL_SLD_CREDOR_ANT_OA = Column(Float)
    VL_SLD_APURADO_OA = Column(Float)
    VL_TOT_DED_OA = Column(Float)
    VL_ICMS_RECOLHER_OA = Column(Float)
    VL_SLD_CREDOR_TRANSP_OA = Column(Float)
    DEB_ESP_OA = Column(Float)

class Registro1921(Base):
    __tablename__ = 'INFO1921'
    ID_1921 = Column(Integer, primary_key=True)
    ID_1920 = Column(Integer)
    ID_1910 = Column(Integer)
    ID_1900 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_AJ_APUR = Column(String)
    DESCR_COMPL_AJ = Column(String)
    VL_AJ_APUR = Column(Float)

class Registro1922(Base):
    __tablename__ = 'INFO1922'
    ID_1922 = Column(Integer, primary_key=True)
    ID_1921 = Column(Integer)
    ID_1920 = Column(Integer)
    ID_1910 = Column(Integer)
    ID_1900 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    NUM_DA = Column(String)
    NUM_PROC = Column(String)
    IND_PROC = Column(String)
    PROC = Column(String)
    TXT_COMPL = Column(String)

class Registro1923(Base):
    __tablename__ = 'INFO1923'
    ID_1923 = Column(Integer, primary_key=True)
    ID_1921 = Column(Integer)
    ID_1920 = Column(Integer)
    ID_1910 = Column(Integer)
    ID_1900 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_PART = Column(String)
    COD_MOD = Column(String)
    SER = Column(String)
    SUB = Column(String)
    NUM_DOC = Column(Integer)
    DT_DOC = Column(Date)
    COD_ITEM = Column(String)
    VL_AJ_ITEM = Column(Float)
    CHV_DOCe = Column(String)

class Registro1925(Base):
    __tablename__ = 'INFO1925'
    ID_1925 = Column(Integer, primary_key=True)
    ID_1920 = Column(Integer)
    ID_1910 = Column(Integer)
    ID_1900 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_INF_ADIC = Column(String)
    VL_INF_ADIC = Column(Float)
    DESCR_COMPL_AJ = Column(String)

class Registro1926(Base):
    __tablename__ = 'INFO1926'
    ID_1926 = Column(Integer, primary_key=True)
    ID_1920 = Column(Integer)
    ID_1910 = Column(Integer)
    ID_1900 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    COD_OR = Column(String)
    VL_OR = Column(Float)
    DT_VCTO = Column(String)
    COD_REC = Column(String)
    NUM_PROC = Column(String)
    IND_PROC = Column(Integer)
    PROC = Column(String)
    TXT_COMPL = Column(String)
    MES_REF = Column(String)

class Registro1960(Base):
    __tablename__ = 'INFO1960'
    ID_1960 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_AP = Column(String)
    G1_01 = Column(Float)
    G1_02 = Column(Float)
    G1_03 = Column(Float)
    G1_04 = Column(Float)
    G1_05 = Column(Float)
    G1_06 = Column(Float)
    G1_07 = Column(Float)
    G1_08 = Column(Float)
    G1_09 = Column(Float)
    G1_10 = Column(Float)
    G1_11 = Column(Float)

class Registro1970(Base):
    __tablename__ = 'INFO1970'
    ID_1970 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_AP = Column(Integer)
    G3_01 = Column(Float)
    G3_02 = Column(Float)
    G3_03 = Column(Float)
    G3_04 = Column(Float)
    G3_05 = Column(Float)
    G3_06 = Column(Float)
    G3_07 = Column(Float)
    G3_08 = Column(Float)
    G3_09 = Column(Float)
    G3_10 = Column(Float)
    G3_11 = Column(Float)
    G3_12 = Column(Float)


class Registro1975(Base):
    __tablename__ = 'INFO1975'
    ID_1975 = Column(Integer, primary_key=True)
    ID_1970 = Column(Integer)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    ALIQ_IMP_BASE = Column(Float)


class Registro1980(Base):
    __tablename__ = 'INFO1980'
    ID_1980 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    IND_AP = Column(Integer)
    G4_01 = Column(Float)
    G4_02 = Column(Float)
    G4_03 = Column(Float)
    G4_04 = Column(Float)
    G4_05 = Column(Float)
    G4_06 = Column(Float)
    G4_07 = Column(Float)
    G4_08 = Column(Float)
    G4_09 = Column(Float)
    G4_10 = Column(Float)
    G4_11 = Column(Float)
    G4_12 = Column(Float)
    G4_13 = Column(Float)
    G4_14 = Column(Float)

class Registro1990(Base):
    __tablename__ = 'INFO1990'
    ID_1990 = Column(Integer, primary_key=True)
    ID_1001 = Column(Integer)
    ID_0000 = Column(Integer)
    QTD_LIN_1 = Column(Integer)

########################## BLOCO 9 ##########################

class Registro9001(Base):
    __tablename__ = 'INFO9001'
    ID_9001 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    IND_MOV = Column(Integer)

class Registro9900(Base):
    __tablename__ = 'INFO9900'
    ID_9900 = Column(Integer, primary_key=True)
    ID_9001 = Column(Integer)
    ID_0000 = Column(Integer)
    REG_BLC = Column(String)
    QTD_REG_BLC = Column(Integer)

class Registro9990(Base):
    __tablename__ = 'INFO9990'
    ID_9990 = Column(Integer, primary_key=True)
    ID_9001 = Column(Integer)
    ID_0000 = Column(Integer)
    QTD_LIN_9 = Column(Integer)

class Registro9999(Base):
    __tablename__ = 'INFO9999'
    ID_9999 = Column(Integer, primary_key=True)
    ID_0000 = Column(Integer)
    QTD_LIN = Column(Integer)

