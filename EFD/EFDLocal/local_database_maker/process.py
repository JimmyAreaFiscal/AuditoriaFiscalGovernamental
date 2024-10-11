from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from EFD.EFDLocal.local_database_maker.models import *
import datetime


def replace_comma_with_dot(value):
    if not value:
        return None
    return float(value.replace(',', '.'))

def convert_to_datetime(value):
    try:
        return datetime.datetime.strptime(value, '%d%m%Y').date()
    except ValueError:
        return None

id_counters = {
    "0000": 1, "0001": 1, "0002": 1, "0005": 1, "0015": 1,
    "0100": 1, "0150": 1, "0175": 1, "0190": 1, "0200": 1,
    "0205": 1, "0206": 1, "0210": 1, "0220": 1, "0221": 1,
    "0300": 1, "0305": 1, "0400": 1, "0450": 1, "0460": 1,
    "0500": 1, "0600": 1, "0990": 1, "B001": 1, "B020": 1,
    "B025": 1, "B030": 1, "B035": 1, "B350": 1, "B420": 1,
    "B440": 1, "B460": 1, "B470": 1, "B500": 1, "B510": 1,
    "B990": 1, "C001": 1, "C100": 1, "C101": 1, "C105": 1,
    "C110": 1, "C111": 1, "C112": 1, "C113": 1, "C114": 1,
    "C115": 1, "C116": 1, "C120": 1, "C130": 1, "C140": 1,
    "C141": 1, "C160": 1, "C165": 1, "C170": 1, "C171": 1,
    "C172": 1, "C173": 1, "C174": 1, "C175": 1, "C176": 1,
    "C177": 1, "C178": 1, "C179": 1, "C180": 1, "C181": 1,
    "C185": 1, "C186": 1, "C190": 1, "C191": 1, "C195": 1,
    "C197": 1, "C300": 1, "C310": 1, "C320": 1, "C321": 1,
    "C330": 1, "C350": 1, "C370": 1, "C380": 1, "C390": 1,
    "C400": 1, "C405": 1, "C410": 1, "C420": 1, "C425": 1,
    "C430": 1, "C460": 1, "C465": 1, "C470": 1, "C490": 1,
    "C495": 1, "C500": 1, "C510": 1, "C590": 1, "C591": 1,
    "C595": 1, "C597": 1, "C600": 1, "C601": 1, "C610": 1,
    "C690": 1, "C700": 1, "C790": 1, "C791": 1, "C800": 1,
    "C810": 1, "C815": 1, "C850": 1, "C855": 1, "C857": 1,
    "C860": 1, "C870": 1, "C880": 1, "C890": 1, "C895": 1,
    "C897": 1, "C990": 1, "D001": 1, "D100": 1, "D101": 1,
    "D110": 1, "D120": 1, "D130": 1, "D140": 1, "D150": 1,
    "D160": 1, "D161": 1, "D170": 1, "D180": 1, "D190": 1,
    "D195": 1, "D197": 1, "D300": 1, "D301": 1, "D310": 1,
    "D350": 1, "D355": 1, "D360": 1, "D365": 1, "D370": 1,
    "D390": 1, "D400": 1, "D410": 1, "D411": 1, "D420": 1,
    "D500": 1, "D510": 1, "D530": 1, "D590": 1, "D600": 1,
    "D610": 1, "D690": 1, "D695": 1, "D696": 1, "D697": 1,
    "D700": 1, "D730": 1, "D731": 1, "D735": 1, "D737": 1,
    "D750": 1, "D760": 1, "D761": 1, "D990": 1, "E001": 1,
    "E100": 1, "E110": 1, "E111": 1, "E112": 1, "E113": 1,
    "E115": 1, "E116": 1, "E200": 1, "E210": 1, "E220": 1,
    "E230": 1, "E240": 1, "E250": 1, "E300": 1, "E310": 1,
    "E311": 1, "E312": 1, "E313": 1, "E316": 1, "E500": 1,
    "E510": 1, "E520": 1, "E530": 1, "E531": 1, "E990": 1,
    "G001": 1, "G110": 1, "G125": 1, "G126": 1, "G130": 1,
    "G140": 1, "G990": 1, "H001": 1, "H005": 1, "H010": 1,
    "H020": 1, "H030": 1, "H990": 1, "K001": 1, "K010": 1,
    "K100": 1, "K200": 1, "K210": 1, "K215": 1, "K220": 1,
    "K230": 1, "K235": 1, "K250": 1, "K255": 1, "K260": 1,
    "K265": 1, "K270": 1, "K275": 1, "K280": 1, "K290": 1,
    "K291": 1, "K292": 1, "K300": 1, "K301": 1, "K302": 1,
    "K990": 1, "1001": 1, "1010": 1, "1100": 1, "1105": 1,
    "1110": 1, "1200": 1, "1210": 1, "1250": 1, "1255": 1,
    "1300": 1, "1310": 1, "1320": 1, "1350": 1, "1360": 1,
    "1370": 1, "1390": 1, "1391": 1, "1400": 1, "1500": 1,
    "1510": 1, "1600": 1, "1601": 1, "1700": 1, "1710": 1,
    "1800": 1, "1900": 1, "1910": 1, "1920": 1, "1921": 1,
    "1922": 1, "1923": 1, "1925": 1, "1926": 1, "1960": 1,
    "1970": 1, "1975": 1, "1980": 1, "1990": 1, "9001": 1,
    "9900": 1, "9990": 1, "9999": 1
}

cod_ver = None

def process_efd_file(file_path):
    engine = create_engine('sqlite:///efd_data.db')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    with open(file_path, 'r', encoding='latin-1') as file:
        lines = file.readlines()

    for line in lines:
        campo = line.strip().split('|')[2:]

############################## BLOCO 0 ##############################

        if line.startswith('|0000|'):
            registro = Registro0000(
                ID_0000=id_counters['0000'],
                COD_VER=campo[0],
                COD_FIN=campo[1],
                DT_INI=convert_to_datetime(campo[2]),
                DT_FIN=convert_to_datetime(campo[3]),
                NOME=campo[4],
                CNPJ=campo[5],
                CPF=campo[6],
                UF=campo[7],
                IE=campo[8],
                COD_MUN=campo[9],
                IM=campo[10],
                SUFRAMA=campo[11],
                IND_PERFIL=campo[12],
                IND_ATIV=campo[13]
            )
            session.add(registro)
            id_counters['0000'] += 1
            cod_ver = campo[0]
        
        if line.startswith('|0001|'):
            registro = Registro0001(
                ID_0001=id_counters['0001'],
                ID_0000=id_counters['0000'] - 1,
                IND_MOV=campo[0]
            )
            session.add(registro)
            id_counters['0001'] += 1


        if line.startswith('|0002|'):
            registro = Registro0002(
                ID_0002=id_counters['0002'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                CLAS_ESTAB_IND=campo[0]
            )
            session.add(registro)
            id_counters['0002'] += 1

        
        if line.startswith('|0005|'):
            registro = Registro0005(
                ID_0005=id_counters['0005'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                FANTASIA=campo[0],
                CEP=campo[1],
                END=campo[2],
                NUM=campo[3],
                COMPL=campo[4],
                BAIRRO=campo[5],
                FONE=campo[6],
                FAX=campo[7],
                EMAIL=campo[8]
            )
            session.add(registro)
            id_counters['0005'] += 1


        if line.startswith('|0015|'):
            registro = Registro0015(
                ID_0015=id_counters['0015'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                UF_ST=campo[0],
                IE_ST=campo[1]
            )
            session.add(registro)
            id_counters['0015'] += 1


        if line.startswith('|0100|'):
            registro = Registro0100(
                ID_0100=id_counters['0100'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                NOME=campo[0],
                CPF=campo[1],
                CRC=campo[2],
                CNPJ=campo[3],
                CEP=campo[4],
                END=campo[5],
                NUM=campo[6],
                COMPL=campo[7],
                BAIRRO=campo[8],
                FONE=campo[9],
                FAX=campo[10],
                EMAIL=campo[11],
                COD_MUN=campo[12]
            )
            session.add(registro)
            id_counters['0100'] += 1


        if line.startswith('|0150|'):
            registro = Registro0150(
                ID_0150=id_counters['0150'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                COD_PART=campo[0],
                NOME=campo[1],
                COD_PAIS=campo[2],
                CNPJ=campo[3],
                CPF=campo[4],
                IE=campo[5],
                COD_MUN=campo[6],
                SUFRAMA=campo[7],
                END=campo[8],
                NUM=campo[9],
                COMPL=campo[10],
                BAIRRO=campo[11]
            )
            session.add(registro)
            id_counters['0150'] += 1


        if line.startswith('|0175|'):
            registro = Registro0175(
                ID_0175=id_counters['0175'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                ID_0150=id_counters['0150'] - 1,
                DT_ALT=convert_to_datetime(campo[0]),
                NR_CAMPO=campo[1],
                CONT_ANT=campo[2]
            )
            session.add(registro)
            id_counters['0175'] += 1


        if line.startswith('|0190|'):
            registro = Registro0190(
                ID_0190=id_counters['0190'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                UNID=campo[0],
                DESCR=campo[1]
            )
            session.add(registro)
            id_counters['0190'] += 1


        if line.startswith('|0200|'):
            registro = Registro0200(
                ID_0200=id_counters['0200'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                COD_ITEM=campo[0],
                DESCR_ITEM=campo[1],
                COD_BARRA=campo[2],
                COD_ANT_ITEM=campo[3],
                UNID_INV=campo[4],
                TIPO_ITEM=campo[5],
                COD_NCM=campo[6],
                EX_IPI=campo[7],
                COD_GEN=campo[8],
                COD_LST=campo[9],
                ALIQ_ICMS=campo[10],
                CEST=campo[11]
            )
            session.add(registro)
            id_counters['0200'] += 1


        if line.startswith('|0205|'):
            registro = Registro0205(
                ID_0205=id_counters['0205'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                ID_0200=id_counters['0200'] - 1,
                DESCR_ANT_ITEM=campo[0],
                DT_INI=convert_to_datetime(campo[1]),
                DT_FIM=convert_to_datetime(campo[2]),
                COD_ANT_ITEM=campo[3]
            )
            session.add(registro)
            id_counters['0205'] += 1


        if line.startswith('|0206|'):
            registro = Registro0206(
                ID_0206=id_counters['0206'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                ID_0200=id_counters['0200'] - 1,
                COD_COMB=campo[0]
            )
            session.add(registro)
            id_counters['0206'] += 1


        if line.startswith('|0210|'):
            registro = Registro0210(
                ID_0210=id_counters['0210'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                ID_0200=id_counters['0200'] - 1,
                COD_ITEM_COMP=campo[0],
                QTD_COMP=replace_comma_with_dot(campo[1]),
                PERDA=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['0210'] += 1


        if line.startswith('|0220|'):
            registro = Registro0220(
                ID_0220=id_counters['0220'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                ID_0200=id_counters['0200'] - 1,
                UNID_CONV=campo[0],
                FAT_CONV=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['0220'] += 1


        if line.startswith('|0221|'):
            registro = Registro0221(
                ID_0221=id_counters['0221'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                ID_0200=id_counters['0200'] - 1,
                COD_ITEM=campo[0],
                COD_ITEM_ATOMICO=campo[1],
                QTD_CONTIDA=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['0221'] += 1


        if line.startswith('|0300|'):
            registro = Registro0300(
                ID_0300=id_counters['0300'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                COD_IND_BEM=campo[0],
                IDENT_MERC=campo[1],
                DESCR_ITEM=campo[2],
                COD_PRNC=campo[3],
                COD_CTA=campo[4],
                NR_PARC=campo[5]
            )
            session.add(registro)
            id_counters['0300'] += 1


        if line.startswith('|0305|'):
            registro = Registro0305(
                ID_0305=id_counters['0305'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                ID_0300=id_counters['0300'] - 1,
                COD_CCUS=campo[0],
                FUNC=campo[1],
                VIDA_UTIL=campo[2]
            )
            session.add(registro)
            id_counters['0305'] += 1


        if line.startswith('|0400|'):
            registro = Registro0400(
                ID_0400=id_counters['0400'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                COD_NAT=campo[0],
                DESCR_NAT=campo[1]
            )
            session.add(registro)
            id_counters['0400'] += 1


        if line.startswith('|0450|'):
            registro = Registro0450(
                ID_0450=id_counters['0450'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                COD_INF=campo[0],
                TXT=campo[1]
            )
            session.add(registro)
            id_counters['0450'] += 1


        if line.startswith('|0460|'):
            registro = Registro0460(
                ID_0460=id_counters['0460'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                COD_OBS=campo[0],
                TXT=campo[1]
            )
            session.add(registro)
            id_counters['0460'] += 1


        if line.startswith('|0500|'):
            registro = Registro0500(
                ID_0500=id_counters['0500'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                DT_ALT=convert_to_datetime(campo[0]),
                COD_NAT_CC=campo[1],
                IND_CTA=campo[2],
                NIVEL=campo[3],
                COD_CTA=campo[4],
                NOME_CTA=campo[5]
            )
            session.add(registro)
            id_counters['0500'] += 1


        if line.startswith('|0600|'):
            registro = Registro0600(
                ID_0600=id_counters['0600'],
                ID_0000=id_counters['0000'] - 1,
                ID_0001=id_counters['0001'] - 1,
                DT_ALT=convert_to_datetime(campo[0]),
                COD_CCUS=campo[1],
                CCUS=campo[2]
            )
            session.add(registro)
            id_counters['0600'] += 1


        if line.startswith('|0990|'):
            registro = Registro0990(
                ID_0990=id_counters['0990'],
                ID_0001=id_counters['0001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                QTD_LIN_0=campo[0]
            )
            session.add(registro)
            id_counters['0990'] += 1


############################## BLOCO B ##############################

        if line.startswith('|B001|'):
            registro = RegistroB001(
                ID_B001=id_counters['B001'],
                ID_0000=id_counters['0000'] - 1,
                IND_DAD=campo[0]
            )
            session.add(registro)
            id_counters['B001'] += 1


        if line.startswith('|B020|'):
            registro = RegistroB020(
                ID_B020=id_counters['B020'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                IND_OPER=campo[0],
                IND_EMIT=campo[1],
                COD_PART=campo[2],
                COD_MOD=campo[3],
                COD_SIT=campo[4],
                SER=campo[5],
                NUM_DOC=campo[6],
                CHV_NFE=campo[7],
                DT_DOC=convert_to_datetime(campo[8]),
                COD_MUN_SERV=campo[9],
                VL_CONT=replace_comma_with_dot(campo[10]),
                VL_MAT_TER=replace_comma_with_dot(campo[11]),
                VL_SUB=replace_comma_with_dot(campo[12]),
                VL_ISNT_ISS=replace_comma_with_dot(campo[13]),
                VL_DED_BC=replace_comma_with_dot(campo[14]),
                VL_BC_ISS=replace_comma_with_dot(campo[15]),
                VL_BC_ISS_RT=replace_comma_with_dot(campo[16]),
                VL_ISS_RT=replace_comma_with_dot(campo[17]),
                VL_ISS=replace_comma_with_dot(campo[18]),
                COD_INF_OBS=campo[19]
            )
            session.add(registro)
            id_counters['B020'] += 1

        if line.startswith('|B025|'):
            registro = RegistroB025(
                ID_B025=id_counters['B025'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                ID_B020=id_counters['B020'] - 1,
                VL_CONT_P=replace_comma_with_dot(campo[0]),
                VL_BC_ISS_P=replace_comma_with_dot(campo[1]),
                ALIQ_ISS=replace_comma_with_dot(campo[2]),
                VL_ISS_P=replace_comma_with_dot(campo[3]),
                VL_ISNT_ISS_P=replace_comma_with_dot(campo[4]),
                COD_SERV=campo[5]
            )
            session.add(registro)
            id_counters['B025'] += 1

        if line.startswith('|B030|'):
            registro = RegistroB030(
                ID_B030=id_counters['B030'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                COD_MOD=campo[0],
                SER=campo[1],
                NUM_DOC_INI=campo[2],
                NUM_DOC_FIN=campo[3],
                DT_DOC=convert_to_datetime(campo[4]),
                QTD_CANC=replace_comma_with_dot(campo[5]),
                VL_CONT=replace_comma_with_dot(campo[6]),
                VL_ISNT_ISS=replace_comma_with_dot(campo[7]),
                VL_BC_ISS=replace_comma_with_dot(campo[8]),
                VL_ISS=replace_comma_with_dot(campo[9]),
                COD_INF_OBS=campo[10]
            )
            session.add(registro)
            id_counters['B030'] += 1

        if line.startswith('|B035|'):
            registro = RegistroB035(
                ID_B035=id_counters['B035'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                ID_B030=id_counters['B030'] - 1,
                VL_CONT_P=replace_comma_with_dot(campo[0]),
                VL_BC_ISS_P=replace_comma_with_dot(campo[1]),
                ALIQ_ISS=replace_comma_with_dot(campo[2]),
                VL_ISS_P=replace_comma_with_dot(campo[3]),
                VL_ISNT_ISS_P=replace_comma_with_dot(campo[4]),
                COD_SERV=campo[5]
            )
            session.add(registro)
            id_counters['B035'] += 1

        if line.startswith('|B350|'):
            registro = RegistroB350(
                ID_B350=id_counters['B350'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                COD_CTD=campo[0],
                CTA_ISS=campo[1],
                CTA_COSIF=campo[2],
                QTD_OCOR=campo[3],
                COD_SERV=campo[4],
                VL_CONT=replace_comma_with_dot(campo[5]),
                VL_BC_ISS=replace_comma_with_dot(campo[6]),
                ALIQ_ISS=replace_comma_with_dot(campo[7]),
                VL_ISS=replace_comma_with_dot(campo[8]),
                COD_INF_OBS=campo[9]
            )
            session.add(registro)
            id_counters['B350'] += 1

        if line.startswith('|B420|'):
            registro = RegistroB420(
                ID_B420=id_counters['B420'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                VL_CONT=replace_comma_with_dot(campo[0]),
                VL_BC_ISS=replace_comma_with_dot(campo[1]),
                ALIQ_ISS=replace_comma_with_dot(campo[2]),
                VL_ISNT_ISS=replace_comma_with_dot(campo[3]),
                VL_ISS=replace_comma_with_dot(campo[4]),
                COD_SERV=campo[5]
            )
            session.add(registro)
            id_counters['B420'] += 1

        if line.startswith('|B440|'):
            registro = RegistroB440(
                ID_B440=id_counters['B440'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                IND_OPER=campo[0],
                COD_PART=campo[1],
                VL_CONT_RT=replace_comma_with_dot(campo[2]),
                VL_BC_ISS_RT=replace_comma_with_dot(campo[3]),
                VL_ISS_RT=replace_comma_with_dot(campo[4])
            )
            session.add(registro)
            id_counters['B440'] += 1

        if line.startswith('|B460|'):
            registro = RegistroB460(
                ID_B460=id_counters['B460'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                IND_DED=campo[0],
                VL_DED=replace_comma_with_dot(campo[1]),
                NUM_PROC=campo[2],
                IND_PROC=campo[3],
                PROC=campo[4],
                COD_INF_OBS=campo[5],
                IND_OBR=campo[6]
            )
            session.add(registro)
            id_counters['B460'] += 1

        if line.startswith('|B470|'):
            registro = RegistroB470(
                ID_B470=id_counters['B470'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                VL_CONT=replace_comma_with_dot(campo[0]),
                VL_MAT_TERC=replace_comma_with_dot(campo[1]),
                VL_MAT_PROP=replace_comma_with_dot(campo[2]),
                VL_SUB=replace_comma_with_dot(campo[3]),
                VL_ISNT=replace_comma_with_dot(campo[4]),
                VL_DED_BC=replace_comma_with_dot(campo[5]),
                VL_BC_ISS=replace_comma_with_dot(campo[6]),
                VL_BC_ISS_RT=replace_comma_with_dot(campo[7]),
                VL_ISS=replace_comma_with_dot(campo[8]),
                VL_ISS_RT=replace_comma_with_dot(campo[9]),
                VL_DED=replace_comma_with_dot(campo[10]),
                VL_ISS_REC=replace_comma_with_dot(campo[11]),
                VL_ISS_ST=replace_comma_with_dot(campo[12]),
                VL_ISS_REC_UNI=replace_comma_with_dot(campo[13])
            )
            session.add(registro)
            id_counters['B470'] += 1

        if line.startswith('|B500|'):
            registro = RegistroB500(
                ID_B500=id_counters['B500'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                VL_REC=replace_comma_with_dot(campo[0]),
                QTD_PROF=replace_comma_with_dot(campo[1]),
                VL_OR=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['B500'] += 1

        if line.startswith('|B510|'):
            registro = RegistroB510(
                ID_B510=id_counters['B510'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                ID_B500=id_counters['B500'] - 1,
                IND_PROF=campo[0],
                IND_ESC=campo[1],
                IND_SOC=campo[2],
                CPF=campo[3],
                NOME=campo[4]
            )
            session.add(registro)
            id_counters['B510'] += 1

        if line.startswith('|B990|'):
            registro = RegistroB990(
                ID_B990=id_counters['B990'],
                ID_0000=id_counters['0000'] - 1,
                ID_B001=id_counters['B001'] - 1,
                QTD_LIN_B=campo[0]
            )
            session.add(registro)
            id_counters['B990'] += 1


        ############################## BLOCO C ##############################

        if line.startswith('|C001|'):
            registro = RegistroC001(
                ID_C001=id_counters['C001'],
                ID_0000=id_counters['0000'] - 1,
                IND_MOV=campo[0]
            )
            session.add(registro)
            id_counters['C001'] += 1

        if line.startswith('|C100|'):
            registro = RegistroC100(
                ID_C100=id_counters['C100'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                IND_OPER=campo[0],
                IND_EMIT=campo[1],
                COD_PART=campo[2],
                COD_MOD=campo[3],
                COD_SIT=campo[4],
                SER=campo[5],
                NUM_DOC=campo[6],
                CHV_NFE=campo[7],
                DT_DOC=convert_to_datetime(campo[8]),
                DT_E_S=convert_to_datetime(campo[9]),
                VL_DOC=replace_comma_with_dot(campo[10]),
                IND_PGTO=campo[11],
                VL_DESC=replace_comma_with_dot(campo[12]),
                VL_ABAT_NT=replace_comma_with_dot(campo[13]),
                VL_MERC=replace_comma_with_dot(campo[14]),
                IND_FRT=campo[15],
                VL_FRT=replace_comma_with_dot(campo[16]),
                VL_SEG=replace_comma_with_dot(campo[17]),
                VL_OUT_DA=replace_comma_with_dot(campo[18]),
                VL_BC_ICMS=replace_comma_with_dot(campo[19]),
                VL_ICMS=replace_comma_with_dot(campo[20]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[21]),
                VL_ICMS_ST=replace_comma_with_dot(campo[22]),
                VL_IPI=replace_comma_with_dot(campo[23]),
                VL_PIS=replace_comma_with_dot(campo[24]),
                VL_COFINS=replace_comma_with_dot(campo[25]),
                VL_PIS_ST=replace_comma_with_dot(campo[26]),
                VL_COFINS_ST=replace_comma_with_dot(campo[27])
            )
            session.add(registro)
            id_counters['C100'] += 1

        if line.startswith('|C101|'):
            registro = RegistroC101(
                ID_C101=id_counters['C101'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                VL_FCP_UF_DEST=replace_comma_with_dot(campo[0]),
                VL_ICMS_UF_DEST=replace_comma_with_dot(campo[1]),
                VL_ICMS_UF_REM=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['C101'] += 1

        if line.startswith('|C105|'):
            registro = RegistroC105(
                ID_C105=id_counters['C105'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                OPER=campo[0],
                UF=campo[1]
            )
            session.add(registro)
            id_counters['C105'] += 1

        if line.startswith('|C110|'):
            registro = RegistroC110(
                ID_C110=id_counters['C110'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                COD_INF=campo[0],
                TXT_COMPL=campo[1]
            )
            session.add(registro)
            id_counters['C110'] += 1

        if line.startswith('|C111|'):
            registro = RegistroC111(
                ID_C111=id_counters['C111'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C110=id_counters['C110'] - 1,
                NUM_PROC=campo[0],
                IND_PROC=campo[1]
            )
            session.add(registro)
            id_counters['C111'] += 1

        if line.startswith('|C112|'):
            registro = RegistroC112(
                ID_C112=id_counters['C112'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C110=id_counters['C110'] - 1,
                COD_DA=campo[0],
                UF=campo[1],
                NUM_DA=campo[2],
                COD_AUT=campo[3],
                VL_DA=replace_comma_with_dot(campo[4]),
                DT_VCTO=convert_to_datetime(campo[5]),
                DT_PGTO=convert_to_datetime(campo[6])
            )
            session.add(registro)
            id_counters['C112'] += 1

        if line.startswith('|C113|'):
            registro = RegistroC113(
                ID_C113=id_counters['C113'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C110=id_counters['C110'] - 1,
                IND_OPER=campo[0],
                IND_EMIT=campo[1],
                COD_PART=campo[2],
                COD_MOD=campo[3],
                SER=campo[4],
                SUB=campo[5],
                NUM_DOC=campo[6],
                DT_DOC=convert_to_datetime(campo[7]),
                CHV_DOC=campo[8]
            )

        if line.startswith('|C114|'):
            registro = RegistroC114(
                ID_C114=id_counters['C114'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C110=id_counters['C110'] - 1,
                COD_MOD=campo[0],
                ECF_FAB=campo[1],
                ECF_CX=campo[2],
                NUM_DOC=campo[3],
                DT_DOC=convert_to_datetime(campo[4])
            )
            session.add(registro)
            id_counters['C114'] += 1

        if line.startswith('|C115|'):
            registro = RegistroC115(
                ID_C115=id_counters['C115'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                IND_CARGA=campo[0],
                CNPJ_COL=campo[1],
                IE_COL=campo[2],
                CPF_COL=campo[3],
                COD_MUN_COL=campo[4],
                CNPJ_ENTG=campo[5],
                IE_ENTG=campo[6],
                CPF_ENTG=campo[7],
                COD_MUN_ENTG=campo[8]
            )
            session.add(registro)
            id_counters['C115'] += 1

        if line.startswith('|C116|'):
            registro = RegistroC116(
                ID_C116=id_counters['C116'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                COD_MOD=campo[0],
                NR_SAT=campo[1],
                CHV_CFE=campo[2],
                NUM_CFE=campo[3],
                DT_DOC=convert_to_datetime(campo[4])
            )
            session.add(registro)
            id_counters['C116'] += 1

        if line.startswith('|C120|'):
            registro = RegistroC120(
                ID_C120=id_counters['C120'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                COD_DOC_IMP=campo[0],
                NUM_DOC_IMP=campo[1],
                PIS_IMP=replace_comma_with_dot(campo[2]),
                COFINS_IMP=replace_comma_with_dot(campo[3]),
                NUM_ACDRAW=campo[4]
            )
            session.add(registro)
            id_counters['C120'] += 1

        if line.startswith('|C130|'):
            registro = RegistroC130(
                ID_C130=id_counters['C130'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                VL_SERV_NT=replace_comma_with_dot(campo[0]),
                VL_BC_ISSQN=replace_comma_with_dot(campo[1]),
                VL_ISSQN=replace_comma_with_dot(campo[2]),
                VL_BC_IRRF=replace_comma_with_dot(campo[3]),
                VL_IRRF=replace_comma_with_dot(campo[4]),
                VL_BC_PREV=replace_comma_with_dot(campo[5]),
                VL_PREV=replace_comma_with_dot(campo[6])
            )
            session.add(registro)
            id_counters['C130'] += 1

        if line.startswith('|C140|'):
            registro = RegistroC140(
                ID_C140=id_counters['C140'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                IND_EMIT=campo[0],
                IND_TIT=campo[1],
                DESC_TIT=campo[2],
                NUM_TIT=campo[3],
                QTD_PARC=campo[4],
                VL_TIT=replace_comma_with_dot(campo[5])
            )
            session.add(registro)
            id_counters['C140'] += 1

        if line.startswith('|C141|'):
            registro = RegistroC141(
                ID_C141=id_counters['C141'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C140=id_counters['C140'] - 1,
                NUM_PARC=campo[0],
                DT_VCTO=convert_to_datetime(campo[1]),
                VL_PARC=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['C141'] += 1

        if line.startswith('|C160|'):
            registro = RegistroC160(
                ID_C160=id_counters['C160'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                COD_PART=campo[0],
                VEIC_ID=campo[1],
                QTD_VOL=campo[2],
                PESO_BRT=replace_comma_with_dot(campo[3]),
                PESO_LIQ=replace_comma_with_dot(campo[4]),
                UF_ID=campo[5]
            )
            session.add(registro)
            id_counters['C160'] += 1

        if line.startswith('|C165|'):
            registro = RegistroC165(
                ID_C165=id_counters['C165'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                COD_PART=campo[0],
                VEIC_ID=campo[1],
                COD_AUT=campo[2],
                NR_PASSE=campo[3],
                HORA=campo[4],
                TEMPER=replace_comma_with_dot(campo[5]),
                QTD_VOL=campo[6],
                PESO_BRT=replace_comma_with_dot(campo[7]),
                PESO_LIQ=replace_comma_with_dot(campo[8]),
                NOM_MOT=campo[9],
                CPF=campo[10],
                UF_ID=campo[11]
            )
            session.add(registro)
            id_counters['C165'] += 1

        if line.startswith('|C170|'):
            registro = RegistroC170(
                ID_C170=id_counters['C170'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                NUM_ITEM=campo[0],
                COD_ITEM=campo[1],
                DESCR_COMPL=campo[2],
                QTD=replace_comma_with_dot(campo[3]),
                UNID=campo[4],
                VL_ITEM=replace_comma_with_dot(campo[5]),
                VL_DESC=replace_comma_with_dot(campo[6]),
                IND_MOV=campo[7],
                CST_ICMS=campo[8],
                CFOP=campo[9],
                COD_NAT=campo[10],
                VL_BC_ICMS=replace_comma_with_dot(campo[11]),
                ALIQ_ICMS=replace_comma_with_dot(campo[12]),
                VL_ICMS=replace_comma_with_dot(campo[13]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[14]),
                ALIQ_ST=replace_comma_with_dot(campo[15]),
                VL_ICMS_ST=replace_comma_with_dot(campo[16]),
                IND_APUR=campo[17],
                CST_IPI=campo[18],
                COD_ENQ=campo[19],
                VL_BC_IPI=replace_comma_with_dot(campo[20]),
                ALIQ_IPI=replace_comma_with_dot(campo[21]),
                VL_IPI=replace_comma_with_dot(campo[22]),
                CST_PIS=campo[23],
                VL_BC_PIS=replace_comma_with_dot(campo[24]),
                ALIQ_PIS_PERCENT=replace_comma_with_dot(campo[25]),
                QUANT_BC_PIS=replace_comma_with_dot(campo[26]),
                ALIQ_PIS_REAIS=replace_comma_with_dot(campo[27]),
                VL_PIS=replace_comma_with_dot(campo[28]),
                CST_COFINS=campo[29],
                VL_BC_COFINS=replace_comma_with_dot(campo[30]),
                ALIQ_COFINS_PERCENT=replace_comma_with_dot(campo[31]),
                QUANT_BC_COFINS=replace_comma_with_dot(campo[32]),
                ALIQ_COFINS_REAIS=replace_comma_with_dot(campo[33]),
                VL_COFINS=replace_comma_with_dot(campo[34]),
                COD_CTA=campo[35],
                VL_ABAT_NT=replace_comma_with_dot(campo[36])
            )
            session.add(registro)
            id_counters['C170'] += 1

        if line.startswith('|C171|'):
            registro = RegistroC171(
                ID_C171=id_counters['C171'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C170=id_counters['C170'] - 1,
                NUM_TANQUE=campo[0],
                QTDE=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['C171'] += 1

        if line.startswith('|C172|'):
            registro = RegistroC172(
                ID_C172=id_counters['C172'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C170=id_counters['C170'] - 1,
                VL_BC_ISSQN=replace_comma_with_dot(campo[0]),
                ALIQ_ISSQN=replace_comma_with_dot(campo[1]),
                VL_ISSQN=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['C172'] += 1

        if line.startswith('|C173|'):
            registro = RegistroC173(
                ID_C173=id_counters['C173'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C170=id_counters['C170'] - 1,
                LOTE_MED=campo[0],
                QTD_ITEM=replace_comma_with_dot(campo[1]),
                DT_FAB=convert_to_datetime(campo[2]),
                DT_VAL=convert_to_datetime(campo[3]),
                IND_MED=campo[4],
                TP_PROD=campo[5],
                VL_TAB_MAX=replace_comma_with_dot(campo[6])
            )
            session.add(registro)
            id_counters['C173'] += 1

        if line.startswith('|C174|'):
            registro = RegistroC174(
                ID_C174=id_counters['C174'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C170=id_counters['C170'] - 1,
                IND_ARM=campo[0],
                NUM_ARM=campo[1],
                DESCR_COMPL=campo[2]
            )
            session.add(registro)
            id_counters['C174'] += 1

        if line.startswith('|C175|'):
            registro = RegistroC175(
                ID_C175=id_counters['C175'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C170=id_counters['C170'] - 1,
                IND_VEIC_OPER=campo[0],
                CNPJ=campo[1],
                UF=campo[2],
                CHASSI_VEIC=campo[3]
            )
            session.add(registro)
            id_counters['C175'] += 1

        if line.startswith('|C176|'):
            registro = RegistroC176(
                ID_C176=id_counters['C176'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C170=id_counters['C170'] - 1,
                COD_MOD_ULT_E=campo[0],
                NUM_DOC_ULT_E=campo[1],
                SER_ULT_E=campo[2],
                DT_ULT_E=convert_to_datetime(campo[3]),
                COD_PART_ULT_E=campo[4],
                QUANT_ULT_E=replace_comma_with_dot(campo[5]),
                VL_UNIT_ULT_E=replace_comma_with_dot(campo[6]),
                VL_UNIT_BC_ST=replace_comma_with_dot(campo[7]),
                CHAVE_NFE_ULT_E=campo[8],
                NUM_ITEM_ULT_E=campo[9],
                VL_UNIT_BC_ICMS_ULT_E=replace_comma_with_dot(campo[10]),
                ALIQ_ICMS_ULT_E=replace_comma_with_dot(campo[11]),
                VL_UNIT_LIMITE_BC_ICMS_ULT_E=replace_comma_with_dot(campo[12]),
                VL_UNIT_ICMS_ULT_E=replace_comma_with_dot(campo[13]),
                ALIQ_ST_ULT_E=replace_comma_with_dot(campo[14]),
                VL_UNIT_RES=replace_comma_with_dot(campo[15]),
                COD_RESP_RET=campo[16],
                COD_MOT_RES=campo[17],
                CHAVE_NFE_RET=campo[18],
                COD_PART_NFE_RET=campo[19],
                SER_NFE_RET=campo[20],
                NUM_NFE_RET=campo[21],
                ITEM_NFE_RET=campo[22],
                COD_DA=campo[23],
                NUM_DA=campo[24],
                VL_UNIT_RES_FCP_ST=replace_comma_with_dot(campo[25])
            )
            session.add(registro)
            id_counters['C176'] += 1

        if line.startswith('|C177|'):
            registro = RegistroC177(
                ID_C177=id_counters['C177'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C170=id_counters['C170'] - 1,
                COD_INF_ITEM=campo[0]
            )
            session.add(registro)
            id_counters['C177'] += 1

        if line.startswith('|C178|'):
            registro = RegistroC178(
                ID_C178=id_counters['C178'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C170=id_counters['C170'] - 1,
                CL_ENQ=campo[0],
                VL_UNID=replace_comma_with_dot(campo[1]),
                QUANT_PAD=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['C178'] += 1

        if line.startswith('|C179|'):
            registro = RegistroC179(
                ID_C179=id_counters['C179'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                BC_ST_ORIG_DEST=replace_comma_with_dot(campo[0]),
                ICMS_ST_REP=replace_comma_with_dot(campo[1]),
                ICMS_ST_COMPL=replace_comma_with_dot(campo[2]),
                BC_RET=replace_comma_with_dot(campo[3]),
                ICMS_RET=replace_comma_with_dot(campo[4])
            )
            session.add(registro)
            id_counters['C179'] += 1

        if line.startswith('|C180|'):
            registro = RegistroC180(
                ID_C180=id_counters['C180'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C170=id_counters['C170'] - 1,
                COD_RESP_RET=campo[0],
                QUANT_CONV=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                VL_UNIT_CONV=replace_comma_with_dot(campo[3]),
                VL_UNIT_ICMS_OP_CONV=replace_comma_with_dot(campo[4]),
                VL_UNIT_BC_ICMS_ST_CONV=replace_comma_with_dot(campo[5]),
                VL_UNIT_ICMS_ST_CONV=replace_comma_with_dot(campo[6]),
                VL_UNIT_FCP_ST_CONV=replace_comma_with_dot(campo[7]),
                COD_DA=campo[8],
                NUM_DA=campo[9]
            )
            session.add(registro)
            id_counters['C180'] += 1

        if line.startswith('|C181|'):
            registro = RegistroC181(
                ID_C181=id_counters['C181'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C170=id_counters['C170'] - 1,
                ID_C180=id_counters['C180'] - 1,
                COD_MOT_REST_COMPL=campo[0],
                QUANT_CONV=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                COD_MOD_SAIDA=campo[3],
                SERIE_SAIDA=campo[4],
                ECF_FAB_SAIDA=campo[5],
                NUM_DOC_SAIDA=campo[6],
                CHV_DFE_SAIDA=campo[7],
                DT_DOC_SAIDA=convert_to_datetime(campo[8]),
                NUM_ITEM_SAIDA=campo[9],
                VL_UNIT_CONV_SAIDA=replace_comma_with_dot(campo[10]),
                VL_UNIT_ICMS_OP_ESTOQUE_CONV_SAIDA=replace_comma_with_dot(campo[11]),
                VL_UNIT_ICMS_ST_ESTOQUE_CONV_SAIDA=replace_comma_with_dot(campo[12]),
                VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV_SAIDA=replace_comma_with_dot(campo[13]),
                VL_UNIT_ICMS_NA_OPERACAO_CONV_SAIDA=replace_comma_with_dot(campo[14]),
                VL_UNIT_ICMS_OP_CONV_SAIDA=replace_comma_with_dot(campo[15]),
                VL_UNIT_ICMS_ST_CONV_REST=replace_comma_with_dot(campo[16]),
                VL_UNIT_FCP_ST_CONV_REST=replace_comma_with_dot(campo[17]),
                VL_UNIT_ICMS_ST_CONV_COMPL=replace_comma_with_dot(campo[18]),
                VL_UNIT_FCP_ST_CONV_COMPL=replace_comma_with_dot(campo[19])
            )
            session.add(registro)
            id_counters['C181'] += 1

        if line.startswith('|C185|'):
            registro = RegistroC185(
                ID_C185=id_counters['C185'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C170=id_counters['C170'] - 1,
                ID_C180=id_counters['C180'] - 1,
                NUM_ITEM=campo[0],
                COD_ITEM=campo[1],
                QUANT_CONV=replace_comma_with_dot(campo[2]),
                UNID=campo[3],
                VL_UNIT_ICMS_NA_OPERACAO_CONV=replace_comma_with_dot(campo[4]),
                VL_UNIT_ICMS_OP_ESTOQUE_CONV=replace_comma_with_dot(campo[5]),
                VL_UNIT_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[6]),
                VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[7]),
                VL_UNIT_ICMS_ST_CONV_REST=replace_comma_with_dot(campo[8]),
                VL_UNIT_FCP_ST_CONV_REST=replace_comma_with_dot(campo[9]),
                VL_UNIT_ICMS_ST_CONV_COMPL=replace_comma_with_dot(campo[10]),
                VL_UNIT_FCP_ST_CONV_COMPL=replace_comma_with_dot(campo[11])
            )
            session.add(registro)
            id_counters['C185'] += 1

        if line.startswith('|C186|'):
            registro = RegistroC186(
                ID_C186=id_counters['C186'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C185=id_counters['C185'] - 1,
                NUM_ITEM=campo[0],
                COD_ITEM=campo[1],
                CST_ICMS=campo[2],
                CFOP=campo[3],
                COD_MOT_REST_COMPL=campo[4],
                QUANT_CONV=replace_comma_with_dot(campo[5]),
                UNID=campo[6],
                COD_MOD_ENTRADA=campo[7],
                SERIE_ENTRADA=campo[8],
                ECF_FAB_ENTRADA=campo[9],
                NUM_DOC_ENTRADA=campo[10],
                CHV_DFE_ENTRADA=campo[11],
                DT_DOC_ENTRADA=convert_to_datetime(campo[12]),
                NUM_ITEM_ENTRADA=campo[13],
                VL_UNIT_CONV_ENTRADA=replace_comma_with_dot(campo[14]),
                VL_UNIT_ICMS_OP_CONV_ENTRADA=replace_comma_with_dot(campo[15]),
                VL_UNIT_BC_ICMS_ST_CONV_ENTRADA=replace_comma_with_dot(campo[16]),
                VL_UNIT_ICMS_ST_CONV_ENTRADA=replace_comma_with_dot(campo[17]),
                VL_UNIT_FCP_ICMS_ST_CONV_ENTRADA=replace_comma_with_dot(campo[18])
            )
            session.add(registro)
            id_counters['C186'] += 1

        if line.startswith('|C190|'):
            registro = RegistroC190(
                ID_C190=id_counters['C190'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[6]),
                VL_ICMS_ST=replace_comma_with_dot(campo[7]),
                VL_RED_BC=replace_comma_with_dot(campo[8]),
                VL_IPI=replace_comma_with_dot(campo[9]),
                COD_OBS=campo[10]
            )
            session.add(registro)
            id_counters['C190'] += 1

        if line.startswith('|C191|'):
            registro = RegistroC191(
                ID_C191=id_counters['C191'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                VL_FCP_OP=replace_comma_with_dot(campo[0]),
                VL_FCP_ST=replace_comma_with_dot(campo[1]),
                VL_FCP_RET=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['C191'] += 1

        if line.startswith('|C195|'):
            registro = RegistroC195(
                ID_C195=id_counters['C195'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                COD_OBS=campo[0],
                TXT_COMPL=campo[1]
            )
            session.add(registro)
            id_counters['C195'] += 1

        if line.startswith('|C197|'):
            registro = RegistroC197(
                ID_C197=id_counters['C197'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C100=id_counters['C100'] - 1,
                ID_C190=id_counters['C190'] - 1,
                COD_AJ=campo[0],
                DESCR_COMPL_AJ=campo[1],
                COD_ITEM=campo[2],
                VL_BC_ICMS=replace_comma_with_dot(campo[3]),
                ALIQ_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_OUTROS=replace_comma_with_dot(campo[6])
            )
            session.add(registro)
            id_counters['C197'] += 1



############################## BLOCO D ##############################
############################## BLOCO E ##############################
############################## BLOCO G ##############################
############################## BLOCO H ##############################
############################## BLOCO K ##############################
############################## BLOCO 1 ##############################
############################## BLOCO 9 ##############################

        if line.startswith('|9001|'):
            registro = Registro9001(
                ID_9001=id_counters['9001'],
                ID_0000=id_counters['0000'] - 1,
                IND_MOV=campo[0]
            )
            session.add(registro)
            id_counters['9001'] += 1

        if line.startswith('|9900|'):
            registro = Registro9900(
                ID_9900=id_counters['9900'],
                ID_9001=id_counters['9001'] - 1, 
                ID_0000=id_counters['0000'] - 1,
                REG_BLC=campo[0],
                QTD_REG_BLC=campo[1]
            )
            session.add(registro)
            id_counters['9900'] += 1

        if line.startswith('|9990|'):
            registro = Registro9990(
                ID_9990=id_counters['9990'],
                ID_9001=id_counters['9001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                QTD_LIN_9=campo[0]
            )
            session.add(registro)
            id_counters['9990'] += 1

        if line.startswith('|9999|'):
            registro = Registro9999(
                ID_9999=id_counters['9999'],
                ID_0000=id_counters['0000'] - 1,
                QTD_LIN=campo[0]
            )
            session.add(registro)
            id_counters['9999'] += 1

        if line.startswith('|C300|'):
            registro = RegistroC300(
                ID_C300=id_counters['C300'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                COD_MOD=campo[0],
                SER=campo[1],
                SUB=campo[2],
                NUM_DOC_INI=campo[3],
                NUM_DOC_FIN=campo[4],
                DT_DOC=convert_to_datetime(campo[5]),
                VL_DOC=replace_comma_with_dot(campo[6]),
                VL_PIS=replace_comma_with_dot(campo[7]),
                VL_COFINS=replace_comma_with_dot(campo[8]),
                COD_CTA=campo[9]
            )
            session.add(registro)
            id_counters['C300'] += 1

        if line.startswith('|C310|'):
            registro = RegistroC310(
                ID_C310=id_counters['C310'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C300=id_counters['C300'] - 1,
                NUM_DOC_CANC=campo[0]
            )
            session.add(registro)
            id_counters['C310'] += 1

        if line.startswith('|C320|'):
            registro = RegistroC320(
                ID_C320=id_counters['C320'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C300=id_counters['C300'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_RED_BC=replace_comma_with_dot(campo[6]),
                COD_OBS=campo[7]
            )
            session.add(registro)
            id_counters['C320'] += 1

        if line.startswith('|C321|'):
            registro = RegistroC321(
                ID_C321=id_counters['C321'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C300=id_counters['C300'] - 1,
                ID_C320=id_counters['C320'] - 1,
                COD_ITEM=campo[0],
                QTD=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                VL_ITEM=replace_comma_with_dot(campo[3]),
                VL_DESC=replace_comma_with_dot(campo[4]),
                VL_BC_ICMS=replace_comma_with_dot(campo[5]),
                VL_ICMS=replace_comma_with_dot(campo[6]),
                VL_PIS=replace_comma_with_dot(campo[7]),
                VL_COFINS=replace_comma_with_dot(campo[8])
            )
            session.add(registro)
            id_counters['C321'] += 1

        if line.startswith('|C330|'):
            registro = RegistroC330(
                ID_C330=id_counters['C330'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C300=id_counters['C300'] - 1,
                COD_MOT_REST_COMPL=campo[0],
                QUANT_CONV=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                VL_UNIT_CONV=replace_comma_with_dot(campo[3]),
                VL_UNIT_ICMS_NA_OPERACAO_CONV=replace_comma_with_dot(campo[4]),
                VL_UNIT_ICMS_OP_CONV=replace_comma_with_dot(campo[5]),
                VL_UNIT_ICMS_OP_ESTOQUE_CONV=replace_comma_with_dot(campo[6]),
                VL_UNIT_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[7]),
                VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[8]),
                VL_UNIT_ICMS_ST_CONV_REST=replace_comma_with_dot(campo[9]),
                VL_UNIT_FCP_ICMS_ST_CONV_REST=replace_comma_with_dot(campo[10]),
                VL_UNIT_ICMS_ST_CONV_COMPL=replace_comma_with_dot(campo[11]),
                VL_UNIT_FCP_ST_CONV_COMPL=replace_comma_with_dot(campo[12])
            )
            session.add(registro)
            id_counters['C330'] += 1

        if line.startswith('|C350|'):
            registro = RegistroC350(
                ID_C350=id_counters['C350'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C300=id_counters['C300'] - 1,
                SER=campo[0],
                SUB=campo[1],
                NUM_DOC=campo[2],
                DT_DOC=convert_to_datetime(campo[3]),
                CNPJ_CPF=campo[4],
                VL_MERC=replace_comma_with_dot(campo[5]),
                VL_DOC=replace_comma_with_dot(campo[6]),
                VL_DESC=replace_comma_with_dot(campo[7]),
                VL_PIS=replace_comma_with_dot(campo[8]),
                VL_COFINS=replace_comma_with_dot(campo[9]),
                COD_CTA=campo[10]
            )
            session.add(registro)
            id_counters['C350'] += 1

        if line.startswith('|C370|'):
            registro = RegistroC370(
                ID_C370=id_counters['C370'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C300=id_counters['C300'] - 1,
                ID_C350=id_counters['C350'] - 1,
                NUM_ITEM=campo[0],
                COD_ITEM=campo[1],
                QTD=replace_comma_with_dot(campo[2]),
                UNID=campo[3],
                VL_ITEM=replace_comma_with_dot(campo[4]),
                VL_DESC=replace_comma_with_dot(campo[5])
            )
            session.add(registro)
            id_counters['C370'] += 1

        if line.startswith('|C380|'):
            registro = RegistroC380(
                ID_C380=id_counters['C380'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C300=id_counters['C300'] - 1,
                COD_MOT_REST_COMPL=campo[0],
                QUANT_CONV=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                VL_UNIT_CONV=replace_comma_with_dot(campo[3]),
                VL_UNIT_ICMS_NA_OPERACAO_CONV=replace_comma_with_dot(campo[4]),
                VL_UNIT_ICMS_OP_CONV=replace_comma_with_dot(campo[5]),
                VL_UNIT_ICMS_OP_ESTOQUE_CONV=replace_comma_with_dot(campo[6]),
                VL_UNIT_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[7]),
                VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[8]),
                VL_UNIT_ICMS_ST_CONV_REST=replace_comma_with_dot(campo[9]),
                VL_UNIT_FCP_ST_CONV_REST=replace_comma_with_dot(campo[10]),
                VL_UNIT_ICMS_ST_CONV_COMPL=replace_comma_with_dot(campo[11]),
                VL_UNIT_FCP_ST_CONV_COMPL=replace_comma_with_dot(campo[12]),
                CST_ICMS=campo[13],
                CFOP=campo[14]
            )
            session.add(registro)
            id_counters['C380'] += 1

        if line.startswith('|C390|'):
            registro = RegistroC390(
                ID_C390=id_counters['C390'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C300=id_counters['C300'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_RED_BC=replace_comma_with_dot(campo[6]),
                COD_OBS=campo[7]
            )
            session.add(registro)
            id_counters['C390'] += 1

        if line.startswith('|C400|'):
            registro = RegistroC400(
                ID_C400=id_counters['C400'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                COD_MOD=campo[0],
                ECF_MOD=campo[1],
                ECF_FAB=campo[2],
                ECF_CX=campo[3]
            )
            session.add(registro)
            id_counters['C400'] += 1

        if line.startswith('|C405|'):
            registro = RegistroC405(
                ID_C405=id_counters['C405'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C400=id_counters['C400'] - 1,
                DT_DOC=convert_to_datetime(campo[0]),
                CRO=campo[1],
                CRZ=campo[2],
                NUM_COO_FIN=campo[3],
                GT_FIN=replace_comma_with_dot(campo[4]),
                VL_BRT=replace_comma_with_dot(campo[5])
            )
            session.add(registro)
            id_counters['C405'] += 1

        if line.startswith('|C410|'):
            registro = RegistroC410(
                ID_C410=id_counters['C410'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C400=id_counters['C400'] - 1,
                ID_C405=id_counters['C405'] - 1,
                VL_PIS=replace_comma_with_dot(campo[0]),
                VL_COFINS=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['C410'] += 1

        if line.startswith('|C420|'):
            registro = RegistroC420(
                ID_C420=id_counters['C420'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C400=id_counters['C400'] - 1,
                COD_TOT_PAR=campo[0],
                VL_ACUM_TOT=replace_comma_with_dot(campo[1]),
                NR_TOT=campo[2],
                DESCR_NR_TOT=campo[3]
            )
            session.add(registro)
            id_counters['C420'] += 1

        if line.startswith('|C425|'):
            registro = RegistroC425(
                ID_C425=id_counters['C425'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C400=id_counters['C400'] - 1,
                ID_C405=id_counters['C405'] - 1,
                ID_C420=id_counters['C420'] - 1,
                COD_ITEM=campo[0],
                QTD=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                VL_ITEM=replace_comma_with_dot(campo[3]),
                VL_PIS=replace_comma_with_dot(campo[4]),
                VL_COFINS=replace_comma_with_dot(campo[5])
            )
            session.add(registro)
            id_counters['C425'] += 1

        if line.startswith('|C430|'):
            registro = RegistroC430(
                ID_C430=id_counters['C430'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C400=id_counters['C400'] - 1,
                ID_C405=id_counters['C405'] - 1,
                ID_C420=id_counters['C420'] - 1,
                ID_C425=id_counters['C425'] - 1,
                COD_MOT_REST_COMPL=campo[0],
                QUANT_CONV=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                VL_UNIT_CONV=replace_comma_with_dot(campo[3]),
                VL_UNIT_ICMS_NA_OPERACAO_CONV=replace_comma_with_dot(campo[4]),
                VL_UNIT_ICMS_OP_CONV=replace_comma_with_dot(campo[5]),
                VL_UNIT_ICMS_OP_ESTOQUE_CONV=replace_comma_with_dot(campo[6]),
                VL_UNIT_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[7]),
                VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[8]),
                VL_UNIT_ICMS_ST_CONV_REST=replace_comma_with_dot(campo[9]),
                VL_UNIT_FCP_ST_CONV_REST=replace_comma_with_dot(campo[10]),
                VL_UNIT_ICMS_ST_CONV_COMPL=replace_comma_with_dot(campo[11]),
                VL_UNIT_FCP_ST_CONV_COMPL=replace_comma_with_dot(campo[12]),
                CST_ICMS=campo[13],
                CFOP=campo[14]
            )
            session.add(registro)
            id_counters['C430'] += 1

        if line.startswith('|C460|'):
            registro = RegistroC460(
                ID_C460=id_counters['C460'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C400=id_counters['C400'] - 1,
                ID_C405=id_counters['C405'] - 1,
                COD_MOD=campo[0],
                COD_SIT=campo[1],
                NUM_DOC=campo[2],
                DT_DOC=convert_to_datetime(campo[3]),
                VL_DOC=replace_comma_with_dot(campo[4]),
                VL_PIS=replace_comma_with_dot(campo[5]),
                VL_COFINS=replace_comma_with_dot(campo[6]),
                CPF_CNPJ=campo[7],
                NOM_ADQ=campo[8]
            )
            session.add(registro)
            id_counters['C460'] += 1

        if line.startswith('|C465|'):
            registro = RegistroC465(
                ID_C465=id_counters['C465'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C400=id_counters['C400'] - 1,
                ID_C405=id_counters['C405'] - 1,
                ID_C460=id_counters['C460'] - 1,
                CHV_CFE=campo[0],
                NUM_CCF=campo[1]
            )
            session.add(registro)
            id_counters['C465'] += 1

        if line.startswith('|C470|'):
            registro = RegistroC470(
                ID_C470=id_counters['C470'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C400=id_counters['C400'] - 1,
                ID_C405=id_counters['C405'] - 1,
                ID_C460=id_counters['C460'] - 1,
                COD_ITEM=campo[0],
                QTD=replace_comma_with_dot(campo[1]),
                QTD_CANC=replace_comma_with_dot(campo[2]),
                UNID=campo[3],
                VL_ITEM=replace_comma_with_dot(campo[4]),
                CST_ICMS=campo[5],
                CFOP=campo[6],
                ALIQ_ICMS=replace_comma_with_dot(campo[7]),
                VL_PIS=replace_comma_with_dot(campo[8]),
                VL_COFINS=replace_comma_with_dot(campo[9])
            )
            session.add(registro)
            id_counters['C470'] += 1

        if line.startswith('|C480|'):
            registro = RegistroC480(
                ID_C480=id_counters['C480'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C400=id_counters['C400'] - 1,
                ID_C405=id_counters['C405'] - 1,
                ID_C460=id_counters['C460'] - 1,
                ID_C470=id_counters['C470'] - 1,
                COD_MOT_REST_COMPL=campo[0],
                QUANT_CONV=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                VL_UNIT_CONV=replace_comma_with_dot(campo[3]),
                VL_UNIT_ICMS_NA_OPERACAO_CONV=replace_comma_with_dot(campo[4]),
                VL_UNIT_ICMS_OP_CONV=replace_comma_with_dot(campo[5]),
                VL_UNIT_ICMS_OP_ESTOQUE_CONV=replace_comma_with_dot(campo[6]),
                VL_UNIT_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[7]),
                VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[8]),
                VL_UNIT_ICMS_ST_CONV_REST=replace_comma_with_dot(campo[9]),
                VL_UNIT_FCP_ST_CONV_REST=replace_comma_with_dot(campo[10]),
                VL_UNIT_ICMS_ST_CONV_COMPL=replace_comma_with_dot(campo[11]),
                VL_UNIT_FCP_ST_CONV_COMPL=replace_comma_with_dot(campo[12]),
                CST_ICMS=campo[13],
                CFOP=campo[14]
            )
            session.add(registro)
            id_counters['C480'] += 1

        if line.startswith('|C490|'):
            registro = RegistroC490(
                ID_C490=id_counters['C490'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C400=id_counters['C400'] - 1,
                ID_C405=id_counters['C405'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                COD_OBS=campo[6]
            )
            session.add(registro)
            id_counters['C490'] += 1

        if line.startswith('|C495|'):
            registro = RegistroC495(
                ID_C495=id_counters['C495'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C400=id_counters['C400'] - 1,
                ID_C405=id_counters['C405'] - 1,
                ALIQ_ICMS=replace_comma_with_dot(campo[0]),
                COD_ITEM=campo[1],
                QTD=replace_comma_with_dot(campo[2]),
                QTD_CANC=replace_comma_with_dot(campo[3]),
                UNID=campo[4],
                VL_ITEM=replace_comma_with_dot(campo[5]),
                VL_DESC=replace_comma_with_dot(campo[6]),
                VL_CANC=replace_comma_with_dot(campo[7]),
                VL_ACMO=replace_comma_with_dot(campo[8]),
                VL_BC_ICMS=replace_comma_with_dot(campo[9]),
                VL_ICMS=replace_comma_with_dot(campo[10]),
                VL_ISNT=replace_comma_with_dot(campo[11]),
                VL_NT=replace_comma_with_dot(campo[12]),
                VL_ICMS_ST=replace_comma_with_dot(campo[13])
            )
            session.add(registro)
            id_counters['C495'] += 1

        if line.startswith('|C500|'):
            if int(cod_ver) >= 16:
                registro = RegistroC500(
                    ID_C500=id_counters['C500'],
                    ID_0000=id_counters['0000'] - 1,
                    ID_C001=id_counters['C001'] - 1,
                    IND_OPER=campo[0],
                    IND_EMIT=campo[1],
                    COD_PART=campo[2],
                    COD_MOD=campo[3],
                    COD_SIT=campo[4],
                    SER=campo[5],
                    SUB=campo[6],
                    COD_CONS=campo[7],
                    NUM_DOC=campo[8],
                    DT_DOC=convert_to_datetime(campo[9]),
                    DT_E_S=convert_to_datetime(campo[10]),
                    VL_DOC=replace_comma_with_dot(campo[11]),
                    VL_DESC=replace_comma_with_dot(campo[12]),
                    VL_FORN=replace_comma_with_dot(campo[13]),
                    VL_SERV_NT=replace_comma_with_dot(campo[14]),
                    VL_TERC=replace_comma_with_dot(campo[15]),
                    VL_DA=replace_comma_with_dot(campo[16]),
                    VL_BC_ICMS=replace_comma_with_dot(campo[17]),
                    VL_ICMS=replace_comma_with_dot(campo[18]),
                    VL_BC_ICMS_ST=replace_comma_with_dot(campo[19]),
                    VL_ICMS_ST=replace_comma_with_dot(campo[20]),
                    COD_INF=campo[21],
                    VL_PIS=replace_comma_with_dot(campo[22]),
                    VL_COFINS=replace_comma_with_dot(campo[23]),
                    TP_LIGACAO=campo[24],
                    COD_GRUPO_TENSAO=campo[25],
                    CHV_DOCe=campo[26],
                    FIN_DOCe=campo[27],
                    CHV_DOCe_REF=campo[28],
                    IND_DEST=campo[29],
                    COD_MUN_DEST=campo[30],
                    COD_CTA=campo[31],
                    COD_MOD_DOC_REF=campo[32],
                    HASH_DOC_REF=campo[33],
                    SER_DOC_REF=campo[34],
                    NUM_DOC_REF=campo[35],
                    MES_DOC_REF=campo[36],
                    ENER_INJET=replace_comma_with_dot(campo[37]),
                    OUTRAS_DED=replace_comma_with_dot(campo[38])
                )
            else:
                registro = RegistroC500(
                    ID_C500=id_counters['C500'],
                    ID_0000=id_counters['0000'] - 1,
                    ID_C001=id_counters['C001'] - 1,
                    IND_OPER=campo[0],
                    IND_EMIT=campo[1],
                    COD_PART=campo[2],
                    COD_MOD=campo[3],
                    COD_SIT=campo[4],
                    SER=campo[5],
                    SUB=campo[6],
                    COD_CONS=campo[7],
                    NUM_DOC=campo[8],
                    DT_DOC=convert_to_datetime(campo[9]),
                    DT_E_S=convert_to_datetime(campo[10]),
                    VL_DOC=replace_comma_with_dot(campo[11]),
                    VL_DESC=replace_comma_with_dot(campo[12]),
                    VL_FORN=replace_comma_with_dot(campo[13]),
                    VL_SERV_NT=replace_comma_with_dot(campo[14]),
                    VL_TERC=replace_comma_with_dot(campo[15]),
                    VL_DA=replace_comma_with_dot(campo[16]),
                    VL_BC_ICMS=replace_comma_with_dot(campo[17]),
                    VL_ICMS=replace_comma_with_dot(campo[18]),
                    VL_BC_ICMS_ST=replace_comma_with_dot(campo[19]),
                    VL_ICMS_ST=replace_comma_with_dot(campo[20]),
                    COD_INF=campo[21],
                    VL_PIS=replace_comma_with_dot(campo[22]),
                    VL_COFINS=replace_comma_with_dot(campo[23]),
                    TP_LIGACAO=campo[24],
                    COD_GRUPO_TENSAO=campo[25])
            session.add(registro)
            id_counters['C500'] += 1

        if line.startswith('|C510|'):
            registro = RegistroC510(
                ID_C510=id_counters['C510'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C500=id_counters['C500'] - 1,
                NUM_ITEM=campo[0],
                COD_ITEM=campo[1],
                COD_CLASS=campo[2],
                QTD=replace_comma_with_dot(campo[3]),
                UNID=campo[4],
                VL_ITEM=replace_comma_with_dot(campo[5]),
                VL_DESC=replace_comma_with_dot(campo[6]),
                CST_ICMS=campo[7],
                CFOP=campo[8],
                VL_BC_ICMS=replace_comma_with_dot(campo[9]),
                ALIQ_ICMS=replace_comma_with_dot(campo[10]),
                VL_ICMS=replace_comma_with_dot(campo[11]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[12]),
                ALIQ_ST=replace_comma_with_dot(campo[13]),
                VL_ICMS_ST=replace_comma_with_dot(campo[14]),
                IND_REC=campo[15],
                COD_PART=campo[16],
                VL_PIS=replace_comma_with_dot(campo[17]),
                VL_COFINS=replace_comma_with_dot(campo[18]),
                COD_CTA=campo[19]
            )
            session.add(registro)
            id_counters['C510'] += 1

        if line.startswith('|C590|'):
            registro = RegistroC590(
                ID_C590=id_counters['C590'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C500=id_counters['C500'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[6]),
                VL_ICMS_ST=replace_comma_with_dot(campo[7]),
                VL_RED_BC=replace_comma_with_dot(campo[8]),
                COD_OBS=campo[9]
            )
            session.add(registro)
            id_counters['C590'] += 1

        if line.startswith('|C591|'):
            registro = RegistroC591(
                ID_C591=id_counters['C591'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C500=id_counters['C500'] - 1,
                ID_C590=id_counters['C590'] - 1,
                VL_FCP_OP=replace_comma_with_dot(campo[0]),
                VL_FCP_ST=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['C591'] += 1

        if line.startswith('|C595|'):
            registro = RegistroC595(
                ID_C595=id_counters['C595'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C500=id_counters['C500'] - 1,
                COD_OBS=campo[0],
                TXT_COMPL=campo[1]
            )
            session.add(registro)
            id_counters['C595'] += 1

        if line.startswith('|C597|'):
            registro = RegistroC597(
                ID_C597=id_counters['C597'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C500=id_counters['C500'] - 1,
                ID_C595=id_counters['C595'] - 1,
                COD_AJ=campo[0],
                DESCR_COMPL_AJ=campo[1],
                COD_ITEM=campo[2],
                VL_BC_ICMS=replace_comma_with_dot(campo[3]),
                ALIQ_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_OUTROS=replace_comma_with_dot(campo[6])
            )
            session.add(registro)
            id_counters['C597'] += 1

        if line.startswith('|C600|'):
            registro = RegistroC600(
                ID_C600=id_counters['C600'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                COD_MOD=campo[0],
                COD_MUN=campo[1],
                SER=campo[2],
                SUB=campo[3],
                COD_CONS=campo[4],
                QTD_CONS=replace_comma_with_dot(campo[5]),
                QTD_CANC=replace_comma_with_dot(campo[6]),
                DT_DOC=convert_to_datetime(campo[7]),
                VL_DOC=replace_comma_with_dot(campo[8]),
                VL_DESC=replace_comma_with_dot(campo[9]),
                CONS=replace_comma_with_dot(campo[10]),
                VL_FORN=replace_comma_with_dot(campo[11]),
                VL_SERV_NT=replace_comma_with_dot(campo[12]),
                VL_TERC=replace_comma_with_dot(campo[13]),
                VL_DA=replace_comma_with_dot(campo[14]),
                VL_BC_ICMS=replace_comma_with_dot(campo[15]),
                VL_ICMS=replace_comma_with_dot(campo[16]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[17]),
                VL_ICMS_ST=replace_comma_with_dot(campo[18]),
                VL_PIS=replace_comma_with_dot(campo[19]),
                VL_COFINS=replace_comma_with_dot(campo[20])
            )
            session.add(registro)
            id_counters['C600'] += 1

        if line.startswith('|C601|'):
            registro = RegistroC601(
                ID_C601=id_counters['C601'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C600=id_counters['C600'] - 1,
                NUM_DOC_CANC=campo[0]
            )
            session.add(registro)
            id_counters['C601'] += 1

        if line.startswith('|C610|'):
            registro = RegistroC610(
                ID_C610=id_counters['C610'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C600=id_counters['C600'] - 1,
                COD_CLASS=campo[0],
                COD_ITEM=campo[1],
                QTD=replace_comma_with_dot(campo[2]),
                UNID=campo[3],
                VL_ITEM=replace_comma_with_dot(campo[4]),
                VL_DESC=replace_comma_with_dot(campo[5]),
                CST_ICMS=campo[6],
                CFOP=campo[7],
                ALIQ_ICMS=replace_comma_with_dot(campo[8]),
                VL_BC_ICMS=replace_comma_with_dot(campo[9]),
                VL_ICMS=replace_comma_with_dot(campo[10]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[11]),
                VL_ICMS_ST=replace_comma_with_dot(campo[12]),
                VL_PIS=replace_comma_with_dot(campo[13]),
                VL_COFINS=replace_comma_with_dot(campo[14]),
                COD_CTA=campo[15]
            )
            session.add(registro)
            id_counters['C610'] += 1

        if line.startswith('|C690|'):
            registro = RegistroC690(
                ID_C690=id_counters['C690'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C600=id_counters['C600'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_RED_BC=replace_comma_with_dot(campo[6]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[7]),
                VL_ICMS_ST=replace_comma_with_dot(campo[8]),
                COD_OBS=campo[9]
            )
            session.add(registro)
            id_counters['C690'] += 1


        if line.startswith('|C700|'):
            registro = RegistroC700(
                ID_C700=id_counters['C700'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                COD_MOD=campo[0],
                SER=campo[1],
                NRO_ORD_INI=campo[2],
                NRO_ORD_FIN=campo[3],
                DT_DOC_INI=convert_to_datetime(campo[4]),
                DT_DOC_FIN=convert_to_datetime(campo[5]),
                NOM_MEST=campo[6],
                CHV_COD_DIG=campo[7]
            )
            session.add(registro)
            id_counters['C700'] += 1

        if line.startswith('|C790|'):
            registro = RegistroC790(
                ID_C790=id_counters['C790'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C700=id_counters['C700'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[6]),
                VL_ICMS_ST=replace_comma_with_dot(campo[7]),
                VL_RED_BC=replace_comma_with_dot(campo[8]),
                COD_OBS=campo[9]
            )
            session.add(registro)
            id_counters['C790'] += 1

        if line.startswith('|C791|'):
            registro = RegistroC791(
                ID_C791=id_counters['C791'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C700=id_counters['C700'] - 1,
                ID_C790=id_counters['C790'] - 1,
                UF=campo[0],
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[1]),
                VL_ICMS_ST=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['C791'] += 1

        if line.startswith('|C800|'):
            registro = RegistroC800(
                ID_C800=id_counters['C800'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                COD_MOD=campo[0],
                COD_SIT=campo[1],
                NUM_CFE=campo[2],
                DT_DOC=convert_to_datetime(campo[3]),
                VL_CFE=replace_comma_with_dot(campo[4]),
                VL_PIS=replace_comma_with_dot(campo[5]),
                VL_COFINS=replace_comma_with_dot(campo[6]),
                CNPJ_CPF=campo[7],
                NR_SAT=campo[8],
                CHV_CFE=campo[9],
                VL_DESC=replace_comma_with_dot(campo[10]),
                VL_MERC=replace_comma_with_dot(campo[11]),
                VL_OUT_DA=replace_comma_with_dot(campo[12]),
                VL_ICMS=replace_comma_with_dot(campo[13]),
                VL_PIS_ST=replace_comma_with_dot(campo[14]),
                VL_COFINS_ST=replace_comma_with_dot(campo[15])
            )
            session.add(registro)
            id_counters['C800'] += 1

        if line.startswith('|C810|'):
            registro = RegistroC810(
                ID_C810=id_counters['C810'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C800=id_counters['C800'] - 1,
                NUM_ITEM=campo[0],
                COD_ITEM=campo[1],
                QTD=replace_comma_with_dot(campo[2]),
                UNID=campo[3],
                VL_ITEM=replace_comma_with_dot(campo[4]),
                CST_ICMS=campo[5],
                CFOP=campo[6]
            )
            session.add(registro)
            id_counters['C810'] += 1

        if line.startswith('|C815|'):
            registro = RegistroC815(
                ID_C815=id_counters['C815'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C800=id_counters['C800'] - 1,
                ID_C810=id_counters['C810'] - 1,
                COD_MOT_REST_COMPL=campo[0],
                QUANT_CONV=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                VL_UNIT_CONV=replace_comma_with_dot(campo[3]),
                VL_UNIT_ICMS_NA_OPERACAO_CONV=replace_comma_with_dot(campo[4]),
                VL_UNIT_ICMS_OP_CONV=replace_comma_with_dot(campo[5]),
                VL_UNIT_ICMS_OP_ESTOQUE_CONV=replace_comma_with_dot(campo[6]),
                VL_UNIT_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[7]),
                VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[8]),
                VL_UNIT_ICMS_ST_CONV_REST=replace_comma_with_dot(campo[9]),
                VL_UNIT_FCP_ST_CONV_REST=replace_comma_with_dot(campo[10]),
                VL_UNIT_ICMS_ST_CONV_COMPL=replace_comma_with_dot(campo[11]),
                VL_UNIT_FCP_ST_CONV_COMPL=replace_comma_with_dot(campo[12])
            )
            session.add(registro)
            id_counters['C815'] += 1

        if line.startswith('|C850|'):
            registro = RegistroC850(
                ID_C850=id_counters['C850'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C800=id_counters['C800'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                COD_OBS=campo[6]
            )
            session.add(registro)
            id_counters['C850'] += 1

        if line.startswith('|C855|'):
            registro = RegistroC855(
                ID_C855=id_counters['C855'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C800=id_counters['C800'] - 1,
                ID_C850=id_counters['C850'] - 1,
                COD_OBS=campo[0],
                TXT_COMPL=campo[1]
            )
            session.add(registro)
            id_counters['C855'] += 1

        if line.startswith('|C857|'):
            registro = RegistroC857(
                ID_C857=id_counters['C857'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C800=id_counters['C800'] - 1,
                ID_C850=id_counters['C850'] - 1,
                ID_C855=id_counters['C855'] - 1,
                COD_AJ=campo[0],
                DESCR_COMPL_AJ=campo[1],
                COD_ITEM=campo[2],
                VL_BC_ICMS=replace_comma_with_dot(campo[3]),
                ALIQ_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_OUTROS=replace_comma_with_dot(campo[6])
            )
            session.add(registro)
            id_counters['C857'] += 1

        if line.startswith('|C860|'):
            registro = RegistroC860(
                ID_C860=id_counters['C860'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C800=id_counters['C800'] - 1,
                COD_MOD=campo[0],
                NR_SAT=campo[1],
                DT_DOC=convert_to_datetime(campo[2]),
                DOC_INI=campo[3],
                DOC_FIM=campo[4]
            )
            session.add(registro)
            id_counters['C860'] += 1

        if line.startswith('|C870|'):
            registro = RegistroC870(
                ID_C870=id_counters['C870'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C860=id_counters['C860'] - 1,
                COD_ITEM=campo[0],
                QTD=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                CST_ICMS=campo[3],
                CFOP=campo[4]
            )
            session.add(registro)
            id_counters['C870'] += 1

        if line.startswith('|C880|'):
            registro = RegistroC880(
                ID_C880=id_counters['C880'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C860=id_counters['C860'] - 1,
                ID_C870=id_counters['C870'] - 1,
                COD_MOT_REST_COMPL=campo[0],
                QUANT_CONV=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                VL_UNIT_CONV=replace_comma_with_dot(campo[3]),
                VL_UNIT_ICMS_NA_OPERACAO_CONV=replace_comma_with_dot(campo[4]),
                VL_UNIT_ICMS_OP_CONV=replace_comma_with_dot(campo[5]),
                VL_UNIT_ICMS_OP_ESTOQUE_CONV=replace_comma_with_dot(campo[6]),
                VL_UNIT_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[7]),
                VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[8]),
                VL_UNIT_ICMS_ST_CONV_REST=replace_comma_with_dot(campo[9]),
                VL_UNIT_FCP_ST_CONV_REST=replace_comma_with_dot(campo[10]),
                VL_UNIT_ICMS_ST_CONV_COMPL=replace_comma_with_dot(campo[11]),
                VL_UNIT_FCP_ST_CONV_COMPL=replace_comma_with_dot(campo[12])
            )
            session.add(registro)
            id_counters['C880'] += 1

        if line.startswith('|C890|'):
            registro = RegistroC890(
                ID_C890=id_counters['C890'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C800=id_counters['C800'] - 1,
                ID_C860=id_counters['C860'] - 1,
                COD_MOT_REST_COMPL=campo[0],
                QUANT_CONV=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                VL_UNIT_CONV=replace_comma_with_dot(campo[3]),
                VL_UNIT_ICMS_NA_OPERACAO_CONV=replace_comma_with_dot(campo[4]),
                VL_UNIT_ICMS_OP_CONV=replace_comma_with_dot(campo[5]),
                VL_UNIT_ICMS_OP_ESTOQUE_CONV=replace_comma_with_dot(campo[6]),
                VL_UNIT_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[7]),
                VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[8]),
                VL_UNIT_ICMS_ST_CONV_REST=replace_comma_with_dot(campo[9]),
                VL_UNIT_FCP_ST_CONV_REST=replace_comma_with_dot(campo[10]),
                VL_UNIT_ICMS_ST_CONV_COMPL=replace_comma_with_dot(campo[11]),
                VL_UNIT_FCP_ST_CONV_COMPL=replace_comma_with_dot(campo[12])
            )
            session.add(registro)
            id_counters['C890'] += 1

        if line.startswith('|C895|'):
            registro = RegistroC895(
                ID_C895=id_counters['C895'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C800=id_counters['C800'] - 1,
                ID_C860=id_counters['C860'] - 1,
                ID_C890=id_counters['C890'] - 1,
                COD_MOT_REST_COMPL=campo[0],
                QUANT_CONV=replace_comma_with_dot(campo[1]),
                UNID=campo[2],
                VL_UNIT_CONV=replace_comma_with_dot(campo[3]),
                VL_UNIT_ICMS_NA_OPERACAO_CONV=replace_comma_with_dot(campo[4]),
                VL_UNIT_ICMS_OP_CONV=replace_comma_with_dot(campo[5]),
                VL_UNIT_ICMS_OP_ESTOQUE_CONV=replace_comma_with_dot(campo[6]),
                VL_UNIT_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[7]),
                VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV=replace_comma_with_dot(campo[8]),
                VL_UNIT_ICMS_ST_CONV_REST=replace_comma_with_dot(campo[9]),
                VL_UNIT_FCP_ST_CONV_REST=replace_comma_with_dot(campo[10]),
                VL_UNIT_ICMS_ST_CONV_COMPL=replace_comma_with_dot(campo[11]),
                VL_UNIT_FCP_ST_CONV_COMPL=replace_comma_with_dot(campo[12])
            )
            session.add(registro)
            id_counters['C895'] += 1

        if line.startswith('|C897|'):
            registro = RegistroC897(
                ID_C897=id_counters['C897'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_C800=id_counters['C800'] - 1,
                ID_C860=id_counters['C860'] - 1,
                ID_C890=id_counters['C890'] - 1,
                ID_C895=id_counters['C895'] - 1,
                COD_AJ=campo[0],
                DESCR_COMPL_AJ=campo[1],
                COD_ITEM=campo[2],
                VL_BC_ICMS=replace_comma_with_dot(campo[3]),
                ALIQ_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_OUTROS=replace_comma_with_dot(campo[6])
            )
            session.add(registro)
            id_counters['C897'] += 1

        if line.startswith('|C990|'):
            registro = RegistroC990(
                ID_C990=id_counters['C990'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                QTD_LIN_C=campo[0]
            )
            session.add(registro)
            id_counters['C990'] += 1

        if line.startswith('|D001|'):
            registro = RegistroD001(
                ID_D001=id_counters['D001'],
                ID_0000=id_counters['0000'] - 1,
                IND_MOV=campo[0]
            )
            session.add(registro)
            id_counters['D001'] += 1

        if line.startswith('|D100|'):
            registro = RegistroD100(
                ID_D100=id_counters['D100'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                IND_OPER=campo[0],
                IND_EMIT=campo[1],
                COD_PART=campo[2],
                COD_MOD=campo[3],
                COD_SIT=campo[4],
                SER=campo[5],
                SUB=campo[6],
                NUM_DOC=campo[7],
                CHV_CTE=campo[8],
                DT_DOC=convert_to_datetime(campo[9]),
                DT_A_P=convert_to_datetime(campo[10]),
                TP_CT_e=campo[11],
                CHV_CTE_REF=campo[12],
                VL_DOC=replace_comma_with_dot(campo[13]),
                VL_DESC=replace_comma_with_dot(campo[14]),
                IND_FRT=campo[15],
                VL_SERV=replace_comma_with_dot(campo[16]),
                VL_BC_ICMS=replace_comma_with_dot(campo[17]),
                VL_ICMS=replace_comma_with_dot(campo[18]),
                VL_NT=replace_comma_with_dot(campo[19]),
                COD_INF=campo[20],
                COD_CTA=campo[21],
                COD_MUN_ORIG=campo[22],
                COD_MUN_DEST=campo[23]
            )
            session.add(registro)
            id_counters['D100'] += 1

        if line.startswith('|D101|'):
            registro = RegistroD101(
                ID_D101=id_counters['D101'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                VL_FCP_UF_DEST=replace_comma_with_dot(campo[0]),
                VL_ICMS_UF_DEST=replace_comma_with_dot(campo[1]),
                VL_ICMS_UF_REM=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['D101'] += 1

        if line.startswith('|D110|'):
            registro = RegistroD110(
                ID_D110=id_counters['D110'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                NUM_ITEM=campo[0],
                COD_ITEM=campo[1],
                VL_SERV=replace_comma_with_dot(campo[2]),
                VL_OUT=replace_comma_with_dot(campo[3])
            )
            session.add(registro)
            id_counters['D110'] += 1

        if line.startswith('|D120|'):
            registro = RegistroD120(
                ID_D120=id_counters['D120'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                COD_MUN_ORIG=campo[0],
                COD_MUN_DEST=campo[1],
                VEIC_ID=campo[2],
                UF_ID=campo[3]
            )
            session.add(registro)
            id_counters['D120'] += 1

        if line.startswith('|D130|'):
            registro = RegistroD130(
                ID_D130=id_counters['D130'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                COD_PART_CONSG=campo[0],
                COD_PART_RED=campo[1],
                IND_FRT_RED=campo[2],
                COD_MUN_ORIG=campo[3],
                COD_MUN_DEST=campo[4],
                VEIC_ID=campo[5],
                VL_LIQ_FRT=replace_comma_with_dot(campo[6]),
                VL_SEC_CAT=replace_comma_with_dot(campo[7]),
                VL_DESP=replace_comma_with_dot(campo[8]),
                VL_TAR=replace_comma_with_dot(campo[9]),
                VL_ADI=replace_comma_with_dot(campo[10]),
                VL_PEDG=replace_comma_with_dot(campo[11]),
                VL_OUT=replace_comma_with_dot(campo[12]),
                UF_ID=campo[13]
            )
            session.add(registro)
            id_counters['D130'] += 1

        if line.startswith('|D140|'):
            registro = RegistroD140(
                ID_D140=id_counters['D140'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                COD_PART_CONSG=campo[0],
                COD_MUN_ORIG=campo[1],
                COD_MUN_DEST=campo[2],
                IND_VEIC=campo[3],
                VEIC_ID=campo[4],
                IND_NAV=campo[5],
                VIAGEM=campo[6],
                VL_LIQ_FRT=replace_comma_with_dot(campo[7]),
                VL_DESP_PORT=replace_comma_with_dot(campo[8]),
                VL_DESP_CAR_DESC=replace_comma_with_dot(campo[9]),
                VL_OUT=replace_comma_with_dot(campo[10]),
                VL_FRT_BRT=replace_comma_with_dot(campo[11]),
                VL_FRT_MM=replace_comma_with_dot(campo[12])
            )
            session.add(registro)
            id_counters['D140'] += 1

        if line.startswith('|D150|'):
            registro = RegistroD150(
                ID_D150=id_counters['D150'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                COD_MUN_ORIG=campo[0],
                COD_MUN_DEST=campo[1],
                VEIC_ID=campo[2],
                VIAGEM=campo[3],
                IND_TFA=campo[4],
                VL_PESO_TX=replace_comma_with_dot(campo[5]),
                VL_TX_TERR=replace_comma_with_dot(campo[6]),
                VL_TX_RED=replace_comma_with_dot(campo[7]),
                VL_OUT=replace_comma_with_dot(campo[8]),
                VL_TX_ADV=replace_comma_with_dot(campo[9])
            )
            session.add(registro)
            id_counters['D150'] += 1

        if line.startswith('|D160|'):
            registro = RegistroD160(
                ID_D160=id_counters['D160'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                
                DESPACHO=campo[1],
                CNPJ_CPF_REM=campo[2],
                IE_REM=campo[3],
                COD_MUN_ORI=campo[4],
                CNPJ_CPF_DEST=campo[5],
                IE_DEST=campo[6],
                COD_MUN_DEST=campo[7]
            )
            session.add(registro)
            id_counters['D160'] += 1

        if line.startswith('|D161|'):
            registro = RegistroD161(
                ID_D161=id_counters['D161'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                ID_D160=id_counters['D160'] - 1,
                
                IND_CARGA=campo[1],
                CNPJ_CPF_COL=campo[2],
                IE_COL=campo[3],
                COD_MUN_COL=campo[4],
                CNPJ_CPF_ENTG=campo[5],
                IE_ENTG=campo[6],
                COD_MUN_ENTG=campo[7]
            )
            session.add(registro)
            id_counters['D161'] += 1

        if line.startswith('|D162|'):
            registro = RegistroD162(
                ID_D162=id_counters['D162'],
                ID_0000=id_counters['0000'] - 1,
                ID_C001=id_counters['C001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                ID_D160=id_counters['D160'] - 1,
                
                COD_MOD=campo[1],
                SER=campo[2],
                NUM_DOC=campo[3],
                DT_DOC=convert_to_datetime(campo[4]),
                VL_DOC=replace_comma_with_dot(campo[5]),
                VL_MERC=replace_comma_with_dot(campo[6]),
                QTD_VOL=replace_comma_with_dot(campo[7]),
                PESO_BRT=replace_comma_with_dot(campo[8]),
                PESO_LIQ=replace_comma_with_dot(campo[9])
            )
            session.add(registro)
            id_counters['D162'] += 1

        if line.startswith('|D170|'):
            registro = RegistroD170(
                ID_D170=id_counters['D170'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                COD_PART_CONSG=campo[0],
                COD_PART_RED=campo[1],
                COD_MUN_ORIG=campo[2],
                COD_MUN_DEST=campo[3],
                OTM=campo[4],
                IND_NAT_FRT=campo[5],
                VL_LIQ_FRT=replace_comma_with_dot(campo[6]),
                VL_GRIS=replace_comma_with_dot(campo[7]),
                VL_PDG=replace_comma_with_dot(campo[8]),
                VL_OUT=replace_comma_with_dot(campo[9]),
                VL_FRT=replace_comma_with_dot(campo[10]),
                VEIC_ID=campo[11],
                UF_ID=campo[12]
            )
            session.add(registro)
            id_counters['D170'] += 1

        if line.startswith('|D180|'):
            registro = RegistroD180(
                ID_D180=id_counters['D180'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                NUM_SEQ=campo[0],
                IND_EMIT=campo[1],
                CNPJ_CPF_EMIT=campo[2],
                UF_EMIT=campo[3],
                IE_EMIT=campo[4],
                COD_MUN_ORIG=campo[5],
                CNPJ_CPF_TOM=campo[6],
                UF_TOM=campo[7],
                IE_TOM=campo[8],
                COD_MUN_DEST=campo[9],
                COD_MOD=campo[10],
                SER=campo[11],
                SUB=campo[12],
                NUM_DOC=campo[13],
                DT_DOC=convert_to_datetime(campo[14]),
                VL_DOC=replace_comma_with_dot(campo[15])
            )
            session.add(registro)
            id_counters['D180'] += 1

        if line.startswith('|D190|'):
            registro = RegistroD190(
                ID_D190=id_counters['D190'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_RED_BC=replace_comma_with_dot(campo[6]),
                COD_OBS=campo[7]
            )
            session.add(registro)
            id_counters['D190'] += 1

        if line.startswith('|D195|'):
            registro = RegistroD195(
                ID_D195=id_counters['D195'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                COD_OBS=campo[0],
                TXT_COMPL=campo[1]
            )
            session.add(registro)
            id_counters['D195'] += 1

        if line.startswith('|D197|'):
            registro = RegistroD197(
                ID_D197=id_counters['D197'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D100=id_counters['D100'] - 1,
                ID_D195=id_counters['D195'] - 1,
                COD_AJ=campo[0],
                DESCR_COMPL_AJ=campo[1],
                COD_ITEM=campo[2],
                VL_BC_ICMS=replace_comma_with_dot(campo[3]),
                ALIQ_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_OUTROS=replace_comma_with_dot(campo[6])
            )
            session.add(registro)
            id_counters['D197'] += 1

        if line.startswith('|D300|'):
            registro = RegistroD300(
                ID_D300=id_counters['D300'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                COD_MOD=campo[0],
                SER=campo[1],
                SUB=campo[2],
                NUM_DOC_INI=campo[3],
                NUM_DOC_FIN=campo[4],
                CST_ICMS=campo[5],
                CFOP=campo[6],
                ALIQ_ICMS=replace_comma_with_dot(campo[7]),
                DT_DOC=convert_to_datetime(campo[8]),
                VL_OPR=replace_comma_with_dot(campo[9]),
                VL_DESC=replace_comma_with_dot(campo[10]),
                VL_SERV=replace_comma_with_dot(campo[11]),
                VL_SEG=replace_comma_with_dot(campo[12]),
                VL_OUT_DESP=replace_comma_with_dot(campo[13]),
                VL_BC_ICMS=replace_comma_with_dot(campo[14]),
                VL_ICMS=replace_comma_with_dot(campo[15]),
                VL_RED_BC=replace_comma_with_dot(campo[16]),
                COD_OBS=campo[17],
                COD_CTA=campo[18]
            )
            session.add(registro)
            id_counters['D300'] += 1

        if line.startswith('|D301|'):
            registro = RegistroD301(
                ID_D301=id_counters['D301'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D300=id_counters['D300'] - 1,
                NUM_DOC_CANC=campo[0]
            )
            session.add(registro)
            id_counters['D301'] += 1

        if line.startswith('|D310|'):
            registro = RegistroD310(
                ID_D310=id_counters['D310'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D300=id_counters['D300'] - 1,
                COD_MUN_ORIG=campo[0],
                VL_SERV=replace_comma_with_dot(campo[1]),
                VL_BC_ICMS=replace_comma_with_dot(campo[2]),
                VL_ICMS=replace_comma_with_dot(campo[3])
            )
            session.add(registro)
            id_counters['D310'] += 1

        if line.startswith('|D350|'):
            registro = RegistroD350(
                ID_D350=id_counters['D350'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                COD_MOD=campo[0],
                ECF_MOD=campo[1],
                ECF_FAB=campo[2],
                ECF_CX=campo[3]
            )
            session.add(registro)
            id_counters['D350'] += 1

        if line.startswith('|D355|'):
            registro = RegistroD355(
                ID_D355=id_counters['D355'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D350=id_counters['D350'] - 1,
                DT_DOC=convert_to_datetime(campo[0]),
                CRO=campo[1],
                CRZ=campo[2],
                NUM_COO_FIN=campo[3],
                GT_FIN=replace_comma_with_dot(campo[4]),
                VL_BRT=replace_comma_with_dot(campo[5])
            )
            session.add(registro)
            id_counters['D355'] += 1

        if line.startswith('|D360|'):
            registro = RegistroD360(
                ID_D360=id_counters['D360'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D350=id_counters['D350'] - 1,
                ID_D355=id_counters['D355'] - 1,
                VL_PIS=replace_comma_with_dot(campo[0]),
                VL_COFINS=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['D360'] += 1

        if line.startswith('|D365|'):
            registro = RegistroD365(
                ID_D365=id_counters['D365'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D350=id_counters['D350'] - 1,
                ID_D355=id_counters['D355'] - 1,
                COD_TOT_PAR=campo[0],
                VLR_ACUM_TOT=replace_comma_with_dot(campo[1]),
                NR_TOT=campo[2],
                DESCR_NR_TOT=campo[3]
            )
            session.add(registro)
            id_counters['D365'] += 1

        if line.startswith('|D370|'):
            registro = RegistroD370(
                ID_D370=id_counters['D370'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D350=id_counters['D350'] - 1,
                ID_D355=id_counters['D355'] - 1,
                ID_D365=id_counters['D365'] - 1,
                COD_MUN_ORIG=campo[0],
                VL_SERV=replace_comma_with_dot(campo[1]),
                QTD_BILH=campo[2],
                VL_BC_ICMS=replace_comma_with_dot(campo[3]),
                VL_ICMS=replace_comma_with_dot(campo[4])
            )
            session.add(registro)
            id_counters['D370'] += 1

        if line.startswith('|D390|'):
            registro = RegistroD390(
                ID_D390=id_counters['D390'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D350=id_counters['D350'] - 1,
                ID_D355=id_counters['D355'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                ALIQ_ISSQN=replace_comma_with_dot(campo[5]),
                VL_ISSQN=replace_comma_with_dot(campo[6]),
                VL_BC_ISSQN=replace_comma_with_dot(campo[7]),
                VL_ICMS=replace_comma_with_dot(campo[8]),
                COD_OBS=campo[9]
            )
            session.add(registro)
            id_counters['D390'] += 1

        if line.startswith('|D400|'):
            registro = RegistroD400(
                ID_D400=id_counters['D400'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                COD_PART=campo[0],
                COD_MOD=campo[1],
                COD_SIT=campo[2],
                SER=campo[3],
                SUB=campo[4],
                NUM_DOC=campo[5],
                DT_DOC=convert_to_datetime(campo[6]),
                VL_DESC=replace_comma_with_dot(campo[7]),
                VL_SERV=replace_comma_with_dot(campo[8]),
                VL_BC_ICMS=replace_comma_with_dot(campo[9]),
                VL_ICMS=replace_comma_with_dot(campo[10]),
                VL_PIS=replace_comma_with_dot(campo[11]),
                VL_COFINS=replace_comma_with_dot(campo[12]),
                COD_CTA=campo[13]
            )
            session.add(registro)
            id_counters['D400'] += 1

        if line.startswith('|D410|'):
            registro = RegistroD410(
                ID_D410=id_counters['D410'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D400=id_counters['D400'] - 1,
                COD_MOD=campo[0],
                SER=campo[1],
                SUB=campo[2],
                NUM_DOC_INI=campo[3],
                NUM_DOC_FIN=campo[4],
                DT_DOC=convert_to_datetime(campo[5]),
                CST_ICMS=campo[6],
                CFOP=campo[7],
                ALIQ_ICMS=replace_comma_with_dot(campo[8]),
                VL_OPR=replace_comma_with_dot(campo[9]),
                VL_DESC=replace_comma_with_dot(campo[10]),
                VL_SERV=replace_comma_with_dot(campo[11]),
                VL_BC_ICMS=replace_comma_with_dot(campo[12]),
                VL_ICMS=replace_comma_with_dot(campo[13])
            )
            session.add(registro)
            id_counters['D410'] += 1

        if line.startswith('|D411|'):
            registro = RegistroD411(
                ID_D411=id_counters['D411'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D400=id_counters['D400'] - 1,
                ID_D410=id_counters['D410'] - 1,
                NUM_DOC_CANC=campo[0]
            )
            session.add(registro)
            id_counters['D411'] += 1

        if line.startswith('|D420|'):
            registro = RegistroD420(
                ID_D420=id_counters['D420'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D400=id_counters['D400'] - 1,
                COD_MUN_ORIG=campo[0],
                VL_SERV=replace_comma_with_dot(campo[1]),
                VL_BC_ICMS=replace_comma_with_dot(campo[2]),
                VL_ICMS=replace_comma_with_dot(campo[3])
            )
            session.add(registro)
            id_counters['D420'] += 1

        if line.startswith('|D500|'):
            registro = RegistroD500(
                ID_D500=id_counters['D500'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                IND_OPER=campo[0],
                IND_EMIT=campo[1],
                COD_PART=campo[2],
                COD_MOD=campo[3],
                COD_SIT=campo[4],
                SER=campo[5],
                SUB=campo[6],
                NUM_DOC=campo[7],
                DT_DOC=convert_to_datetime(campo[8]),
                DT_A_P=convert_to_datetime(campo[9]),
                VL_DOC=replace_comma_with_dot(campo[10]),
                VL_DESC=replace_comma_with_dot(campo[11]),
                VL_SERV=replace_comma_with_dot(campo[12]),
                VL_SERV_NT=replace_comma_with_dot(campo[13]),
                VL_TERC=replace_comma_with_dot(campo[14]),
                VL_DA=replace_comma_with_dot(campo[15]),
                VL_BC_ICMS=replace_comma_with_dot(campo[16]),
                VL_ICMS=replace_comma_with_dot(campo[17]),
                COD_INF=campo[18],
                VL_PIS=replace_comma_with_dot(campo[19]),
                VL_COFINS=replace_comma_with_dot(campo[20]),
                COD_CTA=campo[21],
                TP_ASSINANTE=campo[22]
            )
            session.add(registro)
            id_counters['D500'] += 1

        if line.startswith('|D510|'):
            registro = RegistroD510(
                ID_D510=id_counters['D510'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D500=id_counters['D500'] - 1,
                NUM_ITEM=campo[0],
                COD_ITEM=campo[1],
                COD_CLASS=campo[2],
                QTD=replace_comma_with_dot(campo[3]),
                UNID=campo[4],
                VL_ITEM=replace_comma_with_dot(campo[5]),
                VL_DESC=replace_comma_with_dot(campo[6]),
                CST_ICMS=campo[7],
                CFOP=campo[8],
                VL_BC_ICMS=replace_comma_with_dot(campo[9]),
                ALIQ_ICMS=replace_comma_with_dot(campo[10]),
                VL_ICMS=replace_comma_with_dot(campo[11]),
                VL_BC_ICMS_UF=replace_comma_with_dot(campo[12]),
                VL_ICMS_UF=replace_comma_with_dot(campo[13]),
                IND_REC=campo[14],
                COD_PART=campo[15],
                VL_PIS=replace_comma_with_dot(campo[16]),
                VL_COFINS=replace_comma_with_dot(campo[17]),
                COD_CTA=campo[18]
            )
            session.add(registro)
            id_counters['D510'] += 1


        if line.startswith('|D530|'):
            registro = RegistroD530(
                ID_D530=id_counters['D530'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D500=id_counters['D500'] - 1,
                IND_SERV=campo[0],
                DT_INI_SERV=convert_to_datetime(campo[1]),
                DT_FIN_SERV=convert_to_datetime(campo[2]),
                PER_FISCAL=campo[3],
                COD_AREA=campo[4],
                TERMINAL=campo[5]
            )
            session.add(registro)
            id_counters['D530'] += 1


        if line.startswith('|D590|'):
            registro = RegistroD590(
                ID_D590=id_counters['D590'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D500=id_counters['D500'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_BC_ICMS_UF=replace_comma_with_dot(campo[6]),
                VL_ICMS_UF=replace_comma_with_dot(campo[7]),
                VL_RED_BC=replace_comma_with_dot(campo[8]),
                COD_OBS=campo[9]
            )
            session.add(registro)
            id_counters['D590'] += 1

        if line.startswith('|D600|'):
            registro = RegistroD600(
                ID_D600=id_counters['D600'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                COD_MOD=campo[0],
                COD_MUN=campo[1],
                SER=campo[2],
                SUB=campo[3],
                COD_CONS=campo[4],
                QTD_CONS=campo[5],
                DT_DOC=convert_to_datetime(campo[6]),
                VL_DOC=replace_comma_with_dot(campo[7]),
                VL_DESC=replace_comma_with_dot(campo[8]),
                VL_SERV=replace_comma_with_dot(campo[9]),
                VL_SERV_NT=replace_comma_with_dot(campo[10]),
                VL_TERC=replace_comma_with_dot(campo[11]),
                VL_DA=replace_comma_with_dot(campo[12]),
                VL_BC_ICMS=replace_comma_with_dot(campo[13]),
                VL_ICMS=replace_comma_with_dot(campo[14]),
                VL_PIS=replace_comma_with_dot(campo[15]),
                VL_COFINS=replace_comma_with_dot(campo[16])
            )
            session.add(registro)
            id_counters['D600'] += 1

        if line.startswith('|D610|'):
            registro = RegistroD610(
                ID_D610=id_counters['D610'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D600=id_counters['D600'] - 1,
                COD_CLASS=campo[0],
                COD_ITEM=campo[1],
                QTD=replace_comma_with_dot(campo[2]),
                UNID=campo[3],
                VL_ITEM=replace_comma_with_dot(campo[4]),
                VL_DESC=replace_comma_with_dot(campo[5]),
                CST_ICMS=campo[6],
                CFOP=campo[7],
                ALIQ_ICMS=replace_comma_with_dot(campo[8]),
                VL_BC_ICMS=replace_comma_with_dot(campo[9]),
                VL_ICMS=replace_comma_with_dot(campo[10]),
                VL_BC_ICMS_UF=replace_comma_with_dot(campo[11]),
                VL_ICMS_UF=replace_comma_with_dot(campo[12]),
                VL_RED_BC=replace_comma_with_dot(campo[13]),
                VL_PIS=replace_comma_with_dot(campo[14]),
                VL_COFINS=replace_comma_with_dot(campo[15]),
                COD_CTA=campo[16]
            )
            session.add(registro)
            id_counters['D610'] += 1

        if line.startswith('|D690|'):
            registro = RegistroD690(
                ID_D690=id_counters['D690'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D600=id_counters['D600'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_BC_ICMS_UF=replace_comma_with_dot(campo[6]),
                VL_ICMS_UF=replace_comma_with_dot(campo[7]),
                VL_RED_BC=replace_comma_with_dot(campo[8]),
                COD_OBS=campo[9]
            )
            session.add(registro)
            id_counters['D690'] += 1

        if line.startswith('|D695|'):
            registro = RegistroD695(
                ID_D695=id_counters['D695'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                COD_MOD=campo[0],
                SER=campo[1],
                NRO_ORD_INI=campo[2],
                NRO_ORD_FIN=campo[3],
                DT_DOC_INI=convert_to_datetime(campo[4]),
                DT_DOC_FIN=convert_to_datetime(campo[5]),
                NOM_MEST=campo[6],
                CHV_COD_DIG=campo[7]
            )
            session.add(registro)
            id_counters['D695'] += 1

        if line.startswith('|D696|'):
            registro = RegistroD696(
                ID_D696=id_counters['D696'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D600=id_counters['D600'] - 1,
                ID_D695=id_counters['D695'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_BC_ICMS_UF=replace_comma_with_dot(campo[6]),
                VL_ICMS_UF=replace_comma_with_dot(campo[7]),
                VL_RED_BC=replace_comma_with_dot(campo[8]),
                COD_OBS=campo[9]
            )
            session.add(registro)
            id_counters['D696'] += 1

        if line.startswith('|D697|'):
            registro = RegistroD697(
                ID_D697=id_counters['D697'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D600=id_counters['D600'] - 1,
                ID_D695=id_counters['D695'] - 1,
                ID_D696=id_counters['D696'] - 1,
                UF=campo[0],
                VL_BC_ICMS=replace_comma_with_dot(campo[1]),
                VL_ICMS=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['D697'] += 1

        if line.startswith('|D700|'):
            registro = RegistroD700(
                ID_D700=id_counters['D700'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D600=id_counters['D600'] - 1,
                IND_OPER=campo[0],
                IND_EMIT=campo[1],
                COD_PART=campo[2],
                COD_MOD=campo[3],
                COD_SIT=campo[4],
                SER=campo[5],
                NUM_DOC=campo[6],
                DT_DOC=convert_to_datetime(campo[7]),
                DT_E_S=convert_to_datetime(campo[8]),
                VL_DOC=replace_comma_with_dot(campo[9]),
                VL_DESC=replace_comma_with_dot(campo[10]),
                VL_SERV=replace_comma_with_dot(campo[11]),
                VL_SERV_NT=replace_comma_with_dot(campo[12]),
                VL_TERC=replace_comma_with_dot(campo[13]),
                VL_DA=replace_comma_with_dot(campo[14]),
                VL_BC_ICMS=replace_comma_with_dot(campo[15]),
                VL_ICMS=replace_comma_with_dot(campo[16]),
                COD_INF=campo[17],
                VL_PIS=replace_comma_with_dot(campo[18]),
                VL_COFINS=replace_comma_with_dot(campo[19]),
                CHV_DOCE=campo[20],
                FIN_DOCE=campo[21],
                TIP_FAT=campo[22],
                COD_MOD_DOC_REF=campo[23],
                CHV_DOCE_REF=campo[24],
                HASH_DOC_REF=campo[25],
                SER_DOC_REF=campo[26],
                NUM_DOC_REF=campo[27],
                MES_DOC_REF=campo[28],
                COD_MUN_DEST=campo[29]
            )
            session.add(registro)
            id_counters['D700'] += 1

        if line.startswith('|D730|'):
            registro = RegistroD730(
                ID_D730=id_counters['D730'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D700=id_counters['D700'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_RED_BC=replace_comma_with_dot(campo[6]),
                COD_OBS=campo[7]
            )
            session.add(registro)
            id_counters['D730'] += 1

        if line.startswith('|D731|'):
            registro = RegistroD731(
                ID_D731=id_counters['D731'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D700=id_counters['D700'] - 1,
                VL_FCP_OP=replace_comma_with_dot(campo[0])
            )
            session.add(registro)
            id_counters['D731'] += 1

        if line.startswith('|D735|'):
            registro = RegistroD735(
                ID_D735=id_counters['D735'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D700=id_counters['D700'] - 1,
                COD_OBS=campo[0],
                TXT_COMPL=campo[1]
            )
            session.add(registro)
            id_counters['D735'] += 1

        if line.startswith('|D737|'):
            registro = RegistroD737(
                ID_D737=id_counters['D737'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D700=id_counters['D700'] - 1,
                ID_D735=id_counters['D735'] - 1,
                COD_AJ=campo[0],
                DESCR_COMPL_AJ=campo[1],
                COD_ITEM=campo[2],
                VL_BC_ICMS=replace_comma_with_dot(campo[3]),
                ALIQ_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_OUTROS=replace_comma_with_dot(campo[6])
            )
            session.add(registro)
            id_counters['D737'] += 1

        if line.startswith('|D750|'):
            registro = RegistroD750(
                ID_D750=id_counters['D750'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D700=id_counters['D700'] - 1,
                COD_MOD=campo[0],
                SER=campo[1],
                DT_DOC=convert_to_datetime(campo[2]),
                QTD_CONS=campo[3],
                IND_PREPAGO=campo[4],
                VL_DOC=replace_comma_with_dot(campo[5]),
                VL_SERV=replace_comma_with_dot(campo[6]),
                VL_SERV_NT=replace_comma_with_dot(campo[7]),
                VL_TERC=replace_comma_with_dot(campo[8]),
                VL_DESC=replace_comma_with_dot(campo[9]),
                VL_DA=replace_comma_with_dot(campo[10]),
                VL_BC_ICMS=replace_comma_with_dot(campo[11]),
                VL_ICMS=replace_comma_with_dot(campo[12]),
                VL_PIS=replace_comma_with_dot(campo[13]),
                VL_COFINS=replace_comma_with_dot(campo[14])
            )
            session.add(registro)
            id_counters['D750'] += 1

        if line.startswith('|D760|'):
            registro = RegistroD760(
                ID_D760=id_counters['D760'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D700=id_counters['D700'] - 1,
                ID_D750=id_counters['D750'] - 1,
                CST_ICMS=campo[0],
                CFOP=campo[1],
                ALIQ_ICMS=replace_comma_with_dot(campo[2]),
                VL_OPR=replace_comma_with_dot(campo[3]),
                VL_BC_ICMS=replace_comma_with_dot(campo[4]),
                VL_ICMS=replace_comma_with_dot(campo[5]),
                VL_RED_BC=replace_comma_with_dot(campo[6]),
                COD_OBS=campo[7]
            )
            session.add(registro)
            id_counters['D760'] += 1

        if line.startswith('|D761|'):
            registro = RegistroD761(
                ID_D761=id_counters['D761'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                ID_D700=id_counters['D700'] - 1,
                ID_D750=id_counters['D750'] - 1,
                ID_D760=id_counters['D760'] - 1,
                VL_FCP_OP=replace_comma_with_dot(campo[0])
            )
            session.add(registro)
            id_counters['D761'] += 1

        if line.startswith('|D990|'):
            registro = RegistroD990(
                ID_D990=id_counters['D990'],
                ID_0000=id_counters['0000'] - 1,
                ID_D001=id_counters['D001'] - 1,
                QTD_LIN_D=campo[0]
            )
            session.add(registro)
            id_counters['D990'] += 1

        if line.startswith('|E001|'):
            registro = RegistroE001(
                ID_E001=id_counters['E001'],
                ID_0000=id_counters['0000'] - 1,
                IND_MOV=campo[0]
            )
            session.add(registro)
            id_counters['E001'] += 1

        if line.startswith('|E100|'):
            registro = RegistroE100(
                ID_E100=id_counters['E100'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                DT_INI=convert_to_datetime(campo[0]),
                DT_FIN=convert_to_datetime(campo[1])
            )
            session.add(registro)
            id_counters['E100'] += 1

        if line.startswith('|E110|'):
            registro = RegistroE110(
                ID_E110=id_counters['E110'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E100=id_counters['E100'] - 1,
                VL_TOT_DEBITOS=replace_comma_with_dot(campo[0]),
                VL_AJ_DEBITOS=replace_comma_with_dot(campo[1]),
                VL_TOT_AJ_DEBITOS=replace_comma_with_dot(campo[2]),
                VL_ESTORNOS_CRED=replace_comma_with_dot(campo[3]),
                VL_TOT_CREDITOS=replace_comma_with_dot(campo[4]),
                VL_AJ_CREDITOS=replace_comma_with_dot(campo[5]),
                VL_TOT_AJ_CREDITOS=replace_comma_with_dot(campo[6]),
                VL_ESTORNOS_DEB=replace_comma_with_dot(campo[7]),
                VL_SLD_CREDOR_ANT=replace_comma_with_dot(campo[8]),
                VL_SLD_APURADO=replace_comma_with_dot(campo[9]),
                VL_TOT_DED=replace_comma_with_dot(campo[10]),
                VL_ICMS_RECOLHER=replace_comma_with_dot(campo[11]),
                VL_SLD_CREDOR_TRANSPORTAR=replace_comma_with_dot(campo[12]),
                DEB_ESP=replace_comma_with_dot(campo[13])
            )
            session.add(registro)
            id_counters['E110'] += 1

        if line.startswith('|E111|'):
            registro = RegistroE111(
                ID_E111=id_counters['E111'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E100=id_counters['E100'] - 1,
                ID_E110=id_counters['E110'] - 1,
                COD_AJ_APUR=campo[0],
                DESCR_COMPL_AJ=campo[1],
                VL_AJ_APUR=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['E111'] += 1

        if line.startswith('|E112|'):
            registro = RegistroE112(
                ID_E112=id_counters['E112'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E100=id_counters['E100'] - 1,
                ID_E110=id_counters['E110'] - 1,
                ID_E111=id_counters['E111'] - 1,
                NUM_DA=campo[0],
                NUM_PROC=campo[1],
                IND_PROC=campo[2],
                PROC=campo[3],
                TXT_COMPL=campo[4]
            )
            session.add(registro)
            id_counters['E112'] += 1

        if line.startswith('|E113|'):
            registro = RegistroE113(
                ID_E113=id_counters['E113'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E100=id_counters['E100'] - 1,
                ID_E110=id_counters['E110'] - 1,
                ID_E111=id_counters['E111'] - 1,
                COD_PART=campo[0],
                COD_MOD=campo[1],
                SER=campo[2],
                SUB=campo[3],
                NUM_DOC=campo[4],
                DT_DOC=convert_to_datetime(campo[5]),
                COD_ITEM=campo[6],
                VL_AJ_ITEM=replace_comma_with_dot(campo[7]),
                CHV_DOCe=campo[8]
            )
            session.add(registro)
            id_counters['E113'] += 1

        if line.startswith('|E115|'):
            registro = RegistroE115(
                ID_E115=id_counters['E115'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E100=id_counters['E100'] - 1,
                ID_E110=id_counters['E110'] - 1,
                COD_INF_ADIC=campo[0],
                VL_INF_ADIC=replace_comma_with_dot(campo[1]),
                DESCR_COMPL_AJ=campo[2]
            )
            session.add(registro)
            id_counters['E115'] += 1

        if line.startswith('|E116|'):
            registro = RegistroE116(
                ID_E116=id_counters['E116'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E100=id_counters['E100'] - 1,
                ID_E110=id_counters['E110'] - 1,
                COD_OR=campo[0],
                VL_OR=replace_comma_with_dot(campo[1]),
                DT_VCTO=convert_to_datetime(campo[2]),
                COD_REC=campo[3],
                NUM_PROC=campo[4],
                IND_PROC=campo[5],
                PROC=campo[6],
                TXT_COMPL=campo[7],
                MES_REF=campo[8]
            )
            session.add(registro)
            id_counters['E116'] += 1

        if line.startswith('|E200|'):
            registro = RegistroE200(
                ID_E200=id_counters['E200'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                UF=campo[0],
                DT_INI=convert_to_datetime(campo[1]),
                DT_FIN=convert_to_datetime(campo[2])
            )
            session.add(registro)
            id_counters['E200'] += 1

        if line.startswith('|E210|'):
            registro = RegistroE210(
                ID_E210=id_counters['E210'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E200=id_counters['E200'] - 1,
                IND_MOV_ST=campo[0],
                VL_SLD_CRED_ANT_ST=replace_comma_with_dot(campo[1]),
                VL_DEVOL_ST=replace_comma_with_dot(campo[2]),
                VL_RESSARC_ST=replace_comma_with_dot(campo[3]),
                VL_OUT_CRED_ST=replace_comma_with_dot(campo[4]),
                VL_OUT_DEB_ST=replace_comma_with_dot(campo[5]),
                VL_SLD_CRED_ST_TRANSPORTAR=replace_comma_with_dot(campo[6]),
                VL_RETENCAO_ST=replace_comma_with_dot(campo[7]),
                VL_ICMS_RECOL_ST=replace_comma_with_dot(campo[8]),
                VL_SLD_DEB_ANT_ST=replace_comma_with_dot(campo[9]),
                VL_DEDUCOES_ST=replace_comma_with_dot(campo[10]),
                VL_SLD_CREDOR_TRANS_ST=replace_comma_with_dot(campo[11]),
                VL_ICMS_RECOLHER_ST=replace_comma_with_dot(campo[12]),
                DEB_ESP_ST=replace_comma_with_dot(campo[13])
            )
            session.add(registro)
            id_counters['E210'] += 1

        if line.startswith('|E220|'):
            registro = RegistroE220(
                ID_E220=id_counters['E220'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E100=id_counters['E100'] - 1,
                ID_E210=id_counters['E210'] - 1,
                COD_AJ_APUR=campo[0],
                DESCR_COMPL_AJ=campo[1],
                VL_AJ_APUR=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['E220'] += 1

        if line.startswith('|E230|'):
            registro = RegistroE230(
                ID_E230=id_counters['E230'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E100=id_counters['E100'] - 1,
                ID_E210=id_counters['E210'] - 1,
                ID_E220=id_counters['E220'] - 1,
                NUM_DA=campo[0],
                NUM_PROC=campo[1],
                IND_PROC=campo[2],
                PROC=campo[3],
                TXT_COMPL=campo[4]
            )
            session.add(registro)
            id_counters['E230'] += 1

        if line.startswith('|E240|'):
            registro = RegistroE240(
                ID_E240=id_counters['E240'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E100=id_counters['E100'] - 1,
                ID_E210=id_counters['E210'] - 1,
                ID_E220=id_counters['E220'] - 1,
                COD_PART=campo[0],
                COD_MOD=campo[1],
                SER=campo[2],
                SUB=campo[3],
                NUM_DOC=campo[4],
                DT_DOC=convert_to_datetime(campo[5]),
                COD_ITEM=campo[6],
                VL_AJ_ITEM=replace_comma_with_dot(campo[7]),
                CHV_DOCE=campo[8]
            )
            session.add(registro)
            id_counters['E240'] += 1

        if line.startswith('|E250|'):
            registro = RegistroE250(
                ID_E250=id_counters['E250'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E200=id_counters['E200'] - 1,
                ID_E210=id_counters['E210'] - 1,
                COD_OR=campo[0],
                VL_OR=replace_comma_with_dot(campo[1]),
                DT_VCTO=convert_to_datetime(campo[2]),
                COD_REC=campo[3],
                NUM_PROC=campo[4],
                IND_PROC=campo[5],
                PROC=campo[6],
                TXT_COMPL=campo[7],
                MES_REF=campo[8]
            )
            session.add(registro)
            id_counters['E250'] += 1

        if line.startswith('|E300|'):
            registro = RegistroE300(
                ID_E300=id_counters['E300'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                UF=campo[0],
                DT_INI=convert_to_datetime(campo[1]),
                DT_FIN=convert_to_datetime(campo[2])
            )
            session.add(registro)
            id_counters['E300'] += 1

        if line.startswith('|E310|'):
            registro = RegistroE310(
                ID_E310=id_counters['E310'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E300=id_counters['E300'] - 1,
                IND_MOV_FCP_DIFAL=campo[0],
                VL_SLD_CRED_ANT_DIFAL=replace_comma_with_dot(campo[1]),
                VL_TOT_DEBITOS_DIFAL=replace_comma_with_dot(campo[2]),
                VL_OUT_DEB_DIFAL=replace_comma_with_dot(campo[3]),
                VL_TOT_CREDITOS_DIFAL=replace_comma_with_dot(campo[4]),
                VL_OUT_CRED_DIFAL=replace_comma_with_dot(campo[5]),
                VL_SLD_DEV_ANT_DIFAL=replace_comma_with_dot(campo[6]),
                VL_DEDUCOES_DIFAL=replace_comma_with_dot(campo[7]),
                VL_RECOL_DIFAL=replace_comma_with_dot(campo[8]),
                VL_SLD_CRED_TRANSPORTAR_DIFAL=replace_comma_with_dot(campo[9]),
                DEB_ESP_DIFAL=replace_comma_with_dot(campo[10]),
                VL_SLD_CRED_ANT_FCP=replace_comma_with_dot(campo[11]),
                VL_TOT_DEB_FCP=replace_comma_with_dot(campo[12]),
                VL_OUT_DEB_FCP=replace_comma_with_dot(campo[13]),
                VL_TOT_CRED_FCP=replace_comma_with_dot(campo[14]),
                VL_OUT_CRED_FCP=replace_comma_with_dot(campo[15]),
                VL_SLD_DEV_ANT_FCP=replace_comma_with_dot(campo[16]),
                VL_DEDUCOES_FCP=replace_comma_with_dot(campo[17]),
                VL_RECOL_FCP=replace_comma_with_dot(campo[18]),
                VL_SLD_CRED_TRANSPORTAR_FCP=replace_comma_with_dot(campo[19]),
                DEB_ESP_FCP=replace_comma_with_dot(campo[20])
            )
            session.add(registro)
            id_counters['E310'] += 1

        if line.startswith('|E311|'):
            registro = RegistroE311(
                ID_E311=id_counters['E311'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E300=id_counters['E300'] - 1,
                ID_E310=id_counters['E310'] - 1,
                COD_AJ_APUR=campo[0],
                DESCR_COMPL_AJ=campo[1],
                VL_AJ_APUR=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['E311'] += 1

        if line.startswith('|E312|'):
            registro = RegistroE312(
                ID_E312=id_counters['E312'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E300=id_counters['E300'] - 1,
                ID_E310=id_counters['E310'] - 1,
                ID_E311=id_counters['E311'] - 1,
                NUM_DA=campo[0],
                NUM_PROC=campo[1],
                IND_PROC=campo[2],
                PROC=campo[3],
                TXT_COMPL=campo[4]
            )
            session.add(registro)
            id_counters['E312'] += 1

        if line.startswith('|E313|'):
            registro = RegistroE313(
                ID_E313=id_counters['E313'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E300=id_counters['E300'] - 1,
                ID_E310=id_counters['E310'] - 1,
                ID_E311=id_counters['E311'] - 1,
                COD_PART=campo[0],
                COD_MOD=campo[1],
                SER=campo[2],
                SUB=campo[3],
                NUM_DOC=campo[4],
                CHV_DOCE=campo[5],
                DT_DOC=convert_to_datetime(campo[6]),
                COD_ITEM=campo[7],
                VL_AJ_ITEM=replace_comma_with_dot(campo[8])
            )
            session.add(registro)
            id_counters['E313'] += 1

        if line.startswith('|E316|'):
            registro = RegistroE316(
                ID_E316=id_counters['E316'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E300=id_counters['E300'] - 1,
                ID_E310=id_counters['E310'] - 1,
                COD_OR=campo[0],
                VL_OR=replace_comma_with_dot(campo[1]),
                DT_VCTO=convert_to_datetime(campo[2]),
                COD_REC=campo[3],
                NUM_PROC=campo[4],
                IND_PROC=campo[5],
                PROC=campo[6],
                TXT_COMPL=campo[7],
                MES_REF=campo[8]
            )
            session.add(registro)
            id_counters['E316'] += 1

        if line.startswith('|E500|'):
            registro = RegistroE500(
                ID_E500=id_counters['E500'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                IND_APUR=campo[0],
                DT_INI=convert_to_datetime(campo[1]),
                DT_FIN=convert_to_datetime(campo[2])
            )
            session.add(registro)
            id_counters['E500'] += 1

        if line.startswith('|E510|'):
            registro = RegistroE510(
                ID_E510=id_counters['E510'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E500=id_counters['E500'] - 1,
                CFOP=campo[0],
                CST_IPI=campo[1],
                VL_CONT_IPI=replace_comma_with_dot(campo[2]),
                VL_BC_IPI=replace_comma_with_dot(campo[3]),
                VL_IPI=replace_comma_with_dot(campo[4])
            )
            session.add(registro)
            id_counters['E510'] += 1

        if line.startswith('|E520|'):
            registro = RegistroE520(
                ID_E520=id_counters['E520'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E500=id_counters['E500'] - 1,
                VL_SD_ANT_IPI=replace_comma_with_dot(campo[0]),
                VL_DEB_IPI=replace_comma_with_dot(campo[1]),
                VL_CRED_IPI=replace_comma_with_dot(campo[2]),
                VL_OD_IPI=replace_comma_with_dot(campo[3]),
                VL_OC_IPI=replace_comma_with_dot(campo[4]),
                VL_SC_IPI=replace_comma_with_dot(campo[5]),
                VL_SD_IPI=replace_comma_with_dot(campo[6])
            )
            session.add(registro)
            id_counters['E520'] += 1

        if line.startswith('|E530|'):
            registro = RegistroE530(
                ID_E530=id_counters['E530'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E500=id_counters['E500'] - 1,
                ID_E520=id_counters['E520'] - 1,
                IND_AJ=campo[0],
                VL_AJ=replace_comma_with_dot(campo[1]),
                COD_AJ=campo[2],
                IND_DOC=campo[3],
                NUM_DOC=campo[4],
                DESCR_AJ=campo[5]
            )
            session.add(registro)
            id_counters['E530'] += 1

        if line.startswith('|E531|'):
            registro = RegistroE531(
                ID_E531=id_counters['E531'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                ID_E500=id_counters['E500'] - 1,
                ID_E520=id_counters['E520'] - 1,
                ID_E530=id_counters['E530'] - 1,
                COD_PART=campo[0],
                COD_MOD=campo[1],
                SER=campo[2],
                SUB=campo[3],
                NUM_DOC=campo[4],
                DT_DOC=convert_to_datetime(campo[5]),
                COD_ITEM=campo[6],
                VL_AJ_ITEM=replace_comma_with_dot(campo[7]),
                CHV_NFE=campo[8]
            )
            session.add(registro)
            id_counters['E531'] += 1

        if line.startswith('|E990|'):
            registro = RegistroE990(
                ID_E990=id_counters['E990'],
                ID_0000=id_counters['0000'] - 1,
                ID_E001=id_counters['E001'] - 1,
                QTD_LIN_E=campo[0]
            )
            session.add(registro)
            id_counters['E990'] += 1

        if line.startswith('|G001|'):
            registro = RegistroG001(
                ID_G001=id_counters['G001'],
                ID_0000=id_counters['0000'] - 1,
                IND_MOV=campo[0]
            )
            session.add(registro)
            id_counters['G001'] += 1

        if line.startswith('|G110|'):
            registro = RegistroG110(
                ID_G110=id_counters['G110'],
                ID_0000=id_counters['0000'] - 1,
                ID_G001=id_counters['G001'] - 1,
                DT_INI=convert_to_datetime(campo[0]),
                DT_FIN=convert_to_datetime(campo[1]),
                SALDO_IN_ICMS=replace_comma_with_dot(campo[2]),
                SOM_PARC=replace_comma_with_dot(campo[3]),
                VL_TRIB_EXP=replace_comma_with_dot(campo[4]),
                VL_TOTAL=replace_comma_with_dot(campo[5]),
                IND_PER_SAI=replace_comma_with_dot(campo[6]),
                ICMS_APROP=replace_comma_with_dot(campo[7]),
                SOM_ICMS_OC=replace_comma_with_dot(campo[8])
            )
            session.add(registro)
            id_counters['G110'] += 1

        if line.startswith('|G125|'):
            registro = RegistroG125(
                ID_G125=id_counters['G125'],
                ID_0000=id_counters['0000'] - 1,
                ID_G001=id_counters['G001'] - 1,
                ID_G110=id_counters['G110'] - 1,
                COD_IND_BEM=campo[0],
                DT_MOV=convert_to_datetime(campo[1]),
                TIPO_MOV=campo[2],
                VL_IMOB_ICMS_OP=replace_comma_with_dot(campo[3]),
                VL_IMOB_ICMS_ST=replace_comma_with_dot(campo[4]),
                VL_IMOB_ICMS_FRT=replace_comma_with_dot(campo[5]),
                VL_IMOB_ICMS_DIF=replace_comma_with_dot(campo[6]),
                NUM_PARC=campo[7],
                VL_PARC_PASS=replace_comma_with_dot(campo[8])
            )
            session.add(registro)
            id_counters['G125'] += 1

        if line.startswith('|G126|'):
            registro = RegistroG126(
                ID_G126=id_counters['G126'],
                ID_0000=id_counters['0000'] - 1,
                ID_G001=id_counters['G001'] - 1,
                ID_G110=id_counters['G110'] - 1,
                ID_G125=id_counters['G125'] - 1,
                DT_INI=convert_to_datetime(campo[0]),
                DT_FIM=convert_to_datetime(campo[1]),
                NUM_PARC=campo[2],
                VL_PARC_PASS=replace_comma_with_dot(campo[3]),
                VL_TRIB_OC=replace_comma_with_dot(campo[4]),
                VL_TOTAL=replace_comma_with_dot(campo[5]),
                IND_PER_SAI=replace_comma_with_dot(campo[6]),
                VL_PARC_APROP=replace_comma_with_dot(campo[7])
            )
            session.add(registro)
            id_counters['G126'] += 1

        if line.startswith('|G130|'):
            registro = RegistroG130(
                ID_G130=id_counters['G130'],
                ID_0000=id_counters['0000'] - 1,
                ID_G001=id_counters['G001'] - 1,
                ID_G110=id_counters['G110'] - 1,
                ID_G125=id_counters['G125'] - 1,
                IND_EMIT=campo[0],
                COD_PART=campo[1],
                COD_MOD=campo[2],
                SER=campo[3],
                NUM_DOC=campo[4],
                CHV_NFE_CTE=campo[5],
                DT_DOC=convert_to_datetime(campo[6]),
                NUM_DA=campo[7]
            )
            session.add(registro)
            id_counters['G130'] += 1

        if line.startswith('|G140|'):
            if int(cod_ver) <= 13:
                registro = RegistroG140(
                    ID_G140=id_counters['G140'],
                    ID_0000=id_counters['0000'] - 1,
                    ID_G001=id_counters['G001'] - 1,
                    ID_G110=id_counters['G110'] - 1,
                    ID_G125=id_counters['G125'] - 1,
                    ID_G130=id_counters['G130'] - 1,
                    NUM_ITEM=campo[0],
                    COD_ITEM=campo[1],
                    QTDE=replace_comma_with_dot(campo[2]),
)
            elif int(cod_ver) >= 14:
                   registro = RegistroG140(
                    ID_G140=id_counters['G140'],
                    ID_0000=id_counters['0000'] - 1,
                    ID_G001=id_counters['G001'] - 1,
                    ID_G110=id_counters['G110'] - 1,
                    ID_G125=id_counters['G125'] - 1,
                    ID_G130=id_counters['G130'] - 1,
                    NUM_ITEM=campo[0],
                    COD_ITEM=campo[1],
                    QTDE=replace_comma_with_dot(campo[2]),
                    UNID=campo[3],
                    VL_ICMS_OP_APLICADO=replace_comma_with_dot(campo[4]),
                    VL_ICMS_ST_APLICADO=replace_comma_with_dot(campo[5]),
                    VL_ICMS_FRT_APLICADO=replace_comma_with_dot(campo[6]),
                    VL_ICMS_DIF_APLICADO=replace_comma_with_dot(campo[7]) ) 
            
            session.add(registro)
            id_counters['G140'] += 1

        if line.startswith('|G990|'):
            registro = RegistroG990(
                ID_G990=id_counters['G990'],
                ID_0000=id_counters['0000'] - 1,
                ID_G001=id_counters['G001'] - 1,
                QTD_LIN_G=campo[0]
            )
            session.add(registro)
            id_counters['G990'] += 1


        if line.startswith('|H001|'):
            registro = RegistroH001(
                ID_H001=id_counters['H001'],
                ID_0000=id_counters['0000'] - 1,
                IND_MOV=campo[0]
            )
            session.add(registro)
            id_counters['H001'] += 1

        if line.startswith('|H005|'):
            registro = RegistroH005(
                ID_H005=id_counters['H005'],
                ID_0000=id_counters['0000'] - 1,
                ID_H001=id_counters['H001'] - 1,
                
                DT_INV=convert_to_datetime(campo[0]),
                VL_INV=replace_comma_with_dot(campo[1]),
                MOT_INV=campo[2]
            )
            session.add(registro)
            id_counters['H005'] += 1

        if line.startswith('|H010|'):
            registro = RegistroH010(
                ID_H010=id_counters['H010'],
                ID_0000=id_counters['0000'] - 1,
                ID_H005=id_counters['H005'] - 1,
                
                COD_ITEM=campo[0],
                UNID=campo[1],
                QTD=replace_comma_with_dot(campo[2]),
                VL_UNIT=replace_comma_with_dot(campo[3]),
                VL_ITEM=replace_comma_with_dot(campo[4]),
                IND_PROP=campo[5],
                COD_PART=campo[6],
                TXT_COMPL=campo[7],
                COD_CTA=campo[8],
                VL_ITEM_IR=replace_comma_with_dot(campo[9])
            )
            session.add(registro)
            id_counters['H010'] += 1

        if line.startswith('|H020|'):
            registro = RegistroH020(
                ID_H020=id_counters['H020'],
                ID_0000=id_counters['0000'] - 1,
                ID_H001=id_counters['H001'] - 1,
                ID_H005=id_counters['H005'] - 1,
                ID_H010=id_counters['H010'] - 1,
                CST_ICMS=campo[0],
                BC_ICMS=replace_comma_with_dot(campo[1]),
                VL_ICMS=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['H020'] += 1

        if line.startswith('|H030|'):
            registro = RegistroH030(
                ID_H030=id_counters['H030'],
                ID_0000=id_counters['0000'] - 1,
                ID_H001=id_counters['H001'] - 1,
                ID_H005=id_counters['H005'] - 1,
                ID_H010=id_counters['H010'] - 1,
                VL_ICMS_OP=replace_comma_with_dot(campo[0]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[1]),
                VL_ICMS_ST=replace_comma_with_dot(campo[2]),
                VL_FCP=replace_comma_with_dot(campo[3])
            )
            session.add(registro)
            id_counters['H030'] += 1

        if line.startswith('|H990|'):
            registro = RegistroH990(
                ID_H990=id_counters['H990'],
                ID_0000=id_counters['0000'] - 1,
                QTD_LIN_H=campo[0]
            )
            session.add(registro)
            id_counters['H990'] += 1

        if line.startswith('|K001|'):
            registro = RegistroK001(
                ID_K001=id_counters['K001'],
                ID_0000=id_counters['0000'] - 1,
                IND_MOV=campo[0]
            )
            session.add(registro)
            id_counters['K001'] += 1

        if line.startswith('|K010|'):
            registro = RegistroK010(
                ID_K010=id_counters['K010'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                IND_TP_LEIAUTE=campo[0]
            )
            session.add(registro)
            id_counters['K010'] += 1

        if line.startswith('|K100|'):
            registro = RegistroK100(
                ID_K100=id_counters['K100'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                DT_INI=convert_to_datetime(campo[0]),
                DT_FIN=convert_to_datetime(campo[1])
            )
            session.add(registro)
            id_counters['K100'] += 1

        if line.startswith('|K200|'):
            registro = RegistroK200(
                ID_K200=id_counters['K200'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                DT_EST=convert_to_datetime(campo[0]),
                COD_ITEM=campo[1],
                QTD=replace_comma_with_dot(campo[2]),
                IND_EST=campo[3],
                COD_PART=campo[4]
            )
            session.add(registro)
            id_counters['K200'] += 1

        if line.startswith('|K210|'):
            registro = RegistroK210(
                ID_K210=id_counters['K210'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                DT_INI_OS=convert_to_datetime(campo[0]),
                DT_FIN_OS=convert_to_datetime(campo[1]),
                COD_DOC_OS=campo[2],
                COD_ITEM_ORI=campo[3],
                QTD_ORI=replace_comma_with_dot(campo[4])
            )
            session.add(registro)
            id_counters['K210'] += 1

        if line.startswith('|K215|'):
            registro = RegistroK215(
                ID_K215=id_counters['K215'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                ID_K210=id_counters['K210'] - 1,
                COD_ITEM_DES=campo[0],
                QTD_DES=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['K215'] += 1

        if line.startswith('|K220|'):
            registro = RegistroK220(
                ID_K220=id_counters['K220'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                DT_MOV=convert_to_datetime(campo[0]),
                COD_ITEM_ORI=campo[1],
                COD_ITEM_DEST=campo[2],
                QTD_ORI=replace_comma_with_dot(campo[3]),
                QTD_DEST=replace_comma_with_dot(campo[4])
            )
            session.add(registro)
            id_counters['K220'] += 1

        if line.startswith('|K230|'):
            registro = RegistroK230(
                ID_K230=id_counters['K230'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                DT_INI_OP=convert_to_datetime(campo[0]),
                DT_FIN_OP=convert_to_datetime(campo[1]),
                COD_DOC_OP=campo[2],
                COD_ITEM=campo[3],
                QTD_ENC=replace_comma_with_dot(campo[4])
            )
            session.add(registro)
            id_counters['K230'] += 1

        if line.startswith('|K235|'):
            registro = RegistroK235(
                ID_K235=id_counters['K235'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                ID_K230=id_counters['K230'] - 1,
                DT_SAIDA=convert_to_datetime(campo[0]),
                COD_ITEM=campo[1],
                QTD=replace_comma_with_dot(campo[2]),
                COD_INS_SUBST=campo[3]
            )
            session.add(registro)
            id_counters['K235'] += 1

        if line.startswith('|K250|'):
            registro = RegistroK250(
                ID_K250=id_counters['K250'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                DT_PROD=convert_to_datetime(campo[0]),
                COD_ITEM=campo[1],
                QTD=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['K250'] += 1

        if line.startswith('|K255|'):
            registro = RegistroK255(
                ID_K255=id_counters['K255'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                ID_K250=id_counters['K250'] - 1,
                DT_CONS=convert_to_datetime(campo[0]),
                COD_ITEM=campo[1],
                QTD=replace_comma_with_dot(campo[2]),
                COD_INS_SUBST=campo[3]
            )
            session.add(registro)
            id_counters['K255'] += 1

        if line.startswith('|K260|'):
            registro = RegistroK260(
                ID_K260=id_counters['K260'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                COD_OPS=campo[0],
                COD_ITEM=campo[1],
                QTD_RET=replace_comma_with_dot(campo[2]),
                DT_RET=convert_to_datetime(campo[3])
            )
            session.add(registro)
            id_counters['K260'] += 1

        if line.startswith('|K265|'):
            registro = RegistroK265(
                ID_K265=id_counters['K265'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                ID_K260=id_counters['K260'] - 1,
                COD_ITEM=campo[0],
                QTD_CONS=replace_comma_with_dot(campo[1]),
                QTD_RET=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['K265'] += 1

        if line.startswith('|K270|'):
            registro = RegistroK270(
                ID_K270=id_counters['K270'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                DT_INI_AP=convert_to_datetime(campo[0]),
                DT_FIN_AP=convert_to_datetime(campo[1]),
                COD_OP_OS=campo[2],
                COD_ITEM=campo[3],
                QTD_COR_POS=replace_comma_with_dot(campo[4]),
                QTD_COR_NEG=replace_comma_with_dot(campo[5]),
                ORIGEM=campo[6]
            )
            session.add(registro)
            id_counters['K270'] += 1

        if line.startswith('|K275|'):
            registro = RegistroK275(
                ID_K275=id_counters['K275'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                ID_K270=id_counters['K270'] - 1,
                COD_ITEM=campo[0],
                QTD_COR_POS=replace_comma_with_dot(campo[1]),
                QTD_COR_NEG=replace_comma_with_dot(campo[2]),
                COD_INS_SUBST=campo[3]
            )
            session.add(registro)
            id_counters['K275'] += 1

        if line.startswith('|K280|'):
            registro = RegistroK280(
                ID_K280=id_counters['K280'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                DT_EST=convert_to_datetime(campo[0]),
                COD_ITEM=campo[1],
                QTD_COR_POS=replace_comma_with_dot(campo[2]),
                QTD_COR_NEG=replace_comma_with_dot(campo[3]),
                IND_EST=campo[4],
                COD_PART=campo[5]
            )
            session.add(registro)
            id_counters['K280'] += 1

        if line.startswith('|K290|'):
            registro = RegistroK290(
                ID_K290=id_counters['K290'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                COD_DOC_OP=campo[0],
                QTD=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['K290'] += 1

        if line.startswith('|K291|'):
            registro = RegistroK291(
                ID_K291=id_counters['K291'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                ID_K290=id_counters['K290'] - 1,
                COD_ITEM=campo[0],
                QTD=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['K291'] += 1

        if line.startswith('|K292|'):
            registro = RegistroK292(
                ID_K292=id_counters['K292'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                ID_K290=id_counters['K290'] - 1,
                COD_ITEM=campo[0],
                QTD=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['K292'] += 1

        if line.startswith('|K300|'):
            registro = RegistroK300(
                ID_K300=id_counters['K300'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                DT_PROD=convert_to_datetime(campo[0])
            )
            session.add(registro)
            id_counters['K300'] += 1

        if line.startswith('|K301|'):
            registro = RegistroK301(
                ID_K301=id_counters['K301'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                ID_K300=id_counters['K300'] - 1,
                COD_ITEM=campo[0],
                QTD=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['K301'] += 1

        if line.startswith('|K302|'):
            registro = RegistroK302(
                ID_K302=id_counters['K302'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                ID_K100=id_counters['K100'] - 1,
                ID_K300=id_counters['K300'] - 1,
                COD_ITEM=campo[0],
                QTD=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['K302'] += 1

        if line.startswith('|K990|'):
            registro = RegistroK990(
                ID_K990=id_counters['K990'],
                ID_0000=id_counters['0000'] - 1,
                ID_K001=id_counters['K001'] - 1,
                QTD_LIN_K=campo[0]
            )
            session.add(registro)
            id_counters['K990'] += 1

        if line.startswith('|1001|'):
            registro = Registro1001(
                ID_1001=id_counters['1001'],
                ID_0000=id_counters['0000'] - 1,
                IND_MOV=campo[0]
            )
            session.add(registro)
            id_counters['1001'] += 1

        if line.startswith('|1010|'):
            registro = Registro1010(
                ID_1010=id_counters['1010'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                IND_EXP=campo[0],
                IND_CCRF=campo[1],
                IND_COMB=campo[2],
                IND_USINA=campo[3],
                IND_VA=campo[4],
                IND_EE=campo[5],
                IND_CART=campo[6],
                IND_FORM=campo[7],
                IND_AER=campo[8],
                IND_GIAF1=campo[9],
                IND_GIAF3=campo[10],
                IND_GIAF4=campo[11],
                IND_REST_RESSARC_COMPL_ICMS=campo[12]
            )
            session.add(registro)
            id_counters['1010'] += 1

        if line.startswith('|1100|'):
            registro = Registro1100(
                ID_1100=id_counters['1100'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                IND_DOC=campo[0],
                NRO_DE=campo[1],
                DT_DE=convert_to_datetime(campo[2]),
                NAT_EXP=campo[3],
                NRO_RE=campo[4],
                DT_RE=convert_to_datetime(campo[5]),
                CHC_EMB=campo[6],
                DT_CHC=convert_to_datetime(campo[7]),
                DT_AVB=convert_to_datetime(campo[8]),
                TP_CHC=campo[9],
                PAIS=campo[10]
            )
            session.add(registro)
            id_counters['1100'] += 1

        if line.startswith('|1105|'):
            registro = Registro1105(
                ID_1105=id_counters['1105'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_1100=id_counters['1100'] - 1,
                COD_MOD=campo[0],
                SERIE=campo[1],
                NUM_DOC=campo[2],
                CHV_NFE=campo[3],
                DT_DOC=convert_to_datetime(campo[4]),
                COD_ITEM=campo[5]
            )
            session.add(registro)
            id_counters['1105'] += 1

        if line.startswith('|1110|'):
            registro = Registro1110(
                ID_1110=id_counters['1110'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_1100=id_counters['1100'] - 1,
                ID_1105=id_counters['1105'] - 1,
                COD_PART=campo[0],
                COD_MOD=campo[1],
                SER=campo[2],
                NUM_DOC=campo[3],
                DT_DOC=convert_to_datetime(campo[4]),
                CHV_NFE=campo[5],
                NR_MEMO=campo[6],
                QTD=replace_comma_with_dot(campo[7]),
                UNID=campo[8]
            )
            session.add(registro)
            id_counters['1110'] += 1

        if line.startswith('|1200|'):
            registro = Registro1200(
                ID_1200=id_counters['1200'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                COD_AJ_APUR=campo[0],
                SLD_CRED=replace_comma_with_dot(campo[1]),
                CRED_APR=replace_comma_with_dot(campo[2]),
                CRED_RECEB=replace_comma_with_dot(campo[3]),
                CRED_UTIL=replace_comma_with_dot(campo[4]),
                SLD_CRED_FIM=replace_comma_with_dot(campo[5])
            )
            session.add(registro)
            id_counters['1200'] += 1

        if line.startswith('|1210|'):
            registro = Registro1210(
                ID_1210=id_counters['1210'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_1200=id_counters['1200'] - 1,
                TIPO_UTIL=campo[0],
                NR_DOC=campo[1],
                VL_CRED_UTIL=replace_comma_with_dot(campo[2]),
                CHV_DOCe=campo[3]
            )
            session.add(registro)
            id_counters['1210'] += 1

        if line.startswith('|1250|'):
            registro = Registro1250(
                ID_1250=id_counters['1250'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                VL_CREDITO_ICMS_OP=replace_comma_with_dot(campo[0]),
                VL_ICMS_ST_REST=replace_comma_with_dot(campo[1]),
                VL_FCP_ST_REST=replace_comma_with_dot(campo[2]),
                VL_ICMS_ST_COMPL=replace_comma_with_dot(campo[3]),
                VL_FCP_ST_COMPL=replace_comma_with_dot(campo[4])
            )
            session.add(registro)
            id_counters['1250'] += 1

        if line.startswith('|1255|'):
            registro = Registro1255(
                ID_1255=id_counters['1255'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_1250=id_counters['1250'] - 1,
                COD_MOT_REST_COMPL=campo[0],
                VL_CREDITO_ICMS_OP_MOT=replace_comma_with_dot(campo[1]),
                VL_ICMS_ST_REST_MOT=replace_comma_with_dot(campo[2]),
                VL_FCP_ST_REST_MOT=replace_comma_with_dot(campo[3]),
                VL_ICMS_ST_COMPL_MOT=replace_comma_with_dot(campo[4]),
                VL_FCP_ST_COMPL_MOT=replace_comma_with_dot(campo[5])
            )
            session.add(registro)
            id_counters['1255'] += 1


        if line.startswith('|1300|'):
            registro = Registro1300(
                ID_1300=id_counters['1300'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                COD_ITEM=campo[0],
                DT_FECH=convert_to_datetime(campo[1]),
                ESTQ_ABERT=replace_comma_with_dot(campo[2]),
                VOL_ENTR=replace_comma_with_dot(campo[3]),
                VOL_DISP=replace_comma_with_dot(campo[4]),
                VOL_SAIDAS=replace_comma_with_dot(campo[5]),
                ESTQ_ESCR=replace_comma_with_dot(campo[6]),
                VAL_AJ_PERDA=replace_comma_with_dot(campo[7]),
                VAL_AJ_GANHO=replace_comma_with_dot(campo[8]),
                FECH_FISICO=replace_comma_with_dot(campo[9])
            )
            session.add(registro)
            id_counters['1300'] += 1

        if line.startswith('|1310|'):
            registro = Registro1310(
                ID_1310=id_counters['1310'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_1300=id_counters['1300'] - 1,
                NUM_TANQUE=campo[0],
                ESTQ_ABERT=replace_comma_with_dot(campo[1]),
                VOL_ENTR=replace_comma_with_dot(campo[2]),
                VOL_DISP=replace_comma_with_dot(campo[3]),
                VOL_SAIDAS=replace_comma_with_dot(campo[4]),
                ESTQ_ESCR=replace_comma_with_dot(campo[5]),
                VAL_AJ_PERDA=replace_comma_with_dot(campo[6]),
                VAL_AJ_GANHO=replace_comma_with_dot(campo[7]),
                FECH_FISICO=replace_comma_with_dot(campo[8])
            )
            session.add(registro)
            id_counters['1310'] += 1

        if line.startswith('|1320|'):
            registro = Registro1320(
                ID_1320=id_counters['1320'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_1300=id_counters['1300'] - 1,
                ID_1310=id_counters['1310'] - 1,
                NUM_BICO=campo[0],
                NUM_INTERV=campo[1],
                MOT_INTERV=campo[2],
                NOM_INTERV=campo[3],
                CNPJ_INTERV=campo[4],
                CPF_INTERV=campo[5],
                VAL_FECHA=replace_comma_with_dot(campo[6]),
                VAL_ABERT=replace_comma_with_dot(campo[7]),
                VOL_AFERI=replace_comma_with_dot(campo[8]),
                VOL_VENDAS=replace_comma_with_dot(campo[9])
            )
            session.add(registro)
            id_counters['1320'] += 1

        if line.startswith('|1350|'):
            registro = Registro1350(
                ID_1350=id_counters['1350'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                SERIE=campo[0],
                FABRICANTE=campo[1],
                MODELO=campo[2],
                TIPO_MEDICAO=campo[3]
            )
            session.add(registro)
            id_counters['1350'] += 1

        if line.startswith('|1360|'):
            registro = Registro1360(
                ID_1360=id_counters['1360'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_1350=id_counters['1350'] - 1,
                NUM_LACRE=campo[0],
                DT_APLICACAO=convert_to_datetime(campo[1])
            )
            session.add(registro)
            id_counters['1360'] += 1

        if line.startswith('|1370|'):
            registro = Registro1370(
                ID_1370=id_counters['1370'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_1350=id_counters['1350'] - 1,
                NUM_BICO=campo[0],
                COD_ITEM=campo[1],
                NUM_TANQUE=campo[2]
            )
            session.add(registro)
            id_counters['1370'] += 1

        if line.startswith('|1390|'):
            registro = Registro1390(
                ID_1390=id_counters['1390'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                COD_PROD=campo[0]
            )
            session.add(registro)
            id_counters['1390'] += 1

        if line.startswith('|1391|'):
            registro = Registro1391(
                ID_1391=id_counters['1391'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_1390=id_counters['1390'] - 1,
                DT_REGISTRO=convert_to_datetime(campo[0]),
                QTD_MOD=replace_comma_with_dot(campo[1]),
                ESTQ_INI=replace_comma_with_dot(campo[2]),
                QTD_PRODUZ=replace_comma_with_dot(campo[3]),
                ENT_ANID_HID=replace_comma_with_dot(campo[4]),
                OUTR_ENTR=replace_comma_with_dot(campo[5]),
                PERDA=replace_comma_with_dot(campo[6]),
                CONS=replace_comma_with_dot(campo[7]),
                SAI_ANI_HID=replace_comma_with_dot(campo[8]),
                SAIDAS=replace_comma_with_dot(campo[9]),
                ESTQ_FIN=replace_comma_with_dot(campo[10]),
                ESTQ_INI_MEL=replace_comma_with_dot(campo[11]),
                PROD_DIA_MEL=replace_comma_with_dot(campo[12]),
                UTIL_MEL=replace_comma_with_dot(campo[13]),
                PROD_ALC_MEL=replace_comma_with_dot(campo[14]),
                OBS=campo[15],
                COD_ITEM=campo[16],
                TP_RESIDUO=campo[17],
                QTD_RESIDUO=replace_comma_with_dot(campo[18]),
                QTD_RESIDUO_DDG=replace_comma_with_dot(campo[19]),
                QTD_RESIDUO_WDG=replace_comma_with_dot(campo[20]),
                QTD_RESIDUO_CANA=replace_comma_with_dot(campo[21])
            )
            session.add(registro)
            id_counters['1391'] += 1

        if line.startswith('|1400|'):
            registro = Registro1400(
                ID_1400=id_counters['1400'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                COD_ITEM_IPM=campo[0],
                MUN=campo[1],
                VALOR=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['1400'] += 1

        if line.startswith('|1500|'):
            registro = Registro1500(
                ID_1500=id_counters['1500'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                IND_OPER=campo[0],
                IND_EMIT=campo[1],
                COD_PART=campo[2],
                COD_MOD=campo[3],
                COD_SIT=campo[4],
                SER=campo[5],
                SUB=campo[6],
                COD_CONS=campo[7],
                NUM_DOC=campo[8],
                DT_DOC=convert_to_datetime(campo[9]),
                DT_E_S=convert_to_datetime(campo[10]),
                VL_DOC=replace_comma_with_dot(campo[11]),
                VL_DESC=replace_comma_with_dot(campo[12]),
                VL_FORN=replace_comma_with_dot(campo[13]),
                VL_SERV_NT=replace_comma_with_dot(campo[14]),
                VL_TERC=replace_comma_with_dot(campo[15]),
                VL_DA=replace_comma_with_dot(campo[16]),
                VL_BC_ICMS=replace_comma_with_dot(campo[17]),
                VL_ICMS=replace_comma_with_dot(campo[18]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[19]),
                VL_ICMS_ST=replace_comma_with_dot(campo[20]),
                COD_INF=campo[21],
                VL_PIS=replace_comma_with_dot(campo[22]),
                VL_COFINS=replace_comma_with_dot(campo[23]),
                TP_LIGACAO=campo[24],
                COD_GRUPO_TENSAO=campo[25]
            )
            session.add(registro)
            id_counters['1500'] += 1

        if line.startswith('|1510|'):
            registro = Registro1510(
                ID_1510=id_counters['1510'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_1500=id_counters['1500'] - 1,
                COD_ITEM=campo[0],
                COD_CLASS=campo[1],
                QTD=replace_comma_with_dot(campo[2]),
                UNID=campo[3],
                VL_ITEM=replace_comma_with_dot(campo[4]),
                VL_DESC=replace_comma_with_dot(campo[5]),
                CST_ICMS=campo[6],
                CFOP=campo[7],
                VL_BC_ICMS=replace_comma_with_dot(campo[8]),
                ALIQ_ICMS=replace_comma_with_dot(campo[9]),
                VL_ICMS=replace_comma_with_dot(campo[10]),
                VL_BC_ICMS_ST=replace_comma_with_dot(campo[11]),
                ALIQ_ST=replace_comma_with_dot(campo[12]),
                VL_ICMS_ST=replace_comma_with_dot(campo[13]),
                IND_REC=campo[14],
                COD_PART=campo[15],
                VL_PIS=replace_comma_with_dot(campo[16]),
                VL_COFINS=replace_comma_with_dot(campo[17]),
                COD_CTA=campo[18]
            )
            session.add(registro)
            id_counters['1510'] += 1

        if line.startswith('|1600|'):
            registro = Registro1600(
                ID_1600=id_counters['1600'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                COD_PART=campo[0],
                TOT_CREDITO=replace_comma_with_dot(campo[1])
            )
            session.add(registro)
            id_counters['1600'] += 1

        if line.startswith('|1601|'):
            registro = Registro1601(
                ID_1601=id_counters['1601'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                COD_PART_IP=campo[0],
                COD_PART_IT=campo[1],
                TOT_VS=replace_comma_with_dot(campo[2]),
                TOT_ISS=replace_comma_with_dot(campo[3]),
                TOT_OUTROS=replace_comma_with_dot(campo[4])
            )
            session.add(registro)
            id_counters['1601'] += 1

        if line.startswith('|1700|'):
            registro = Registro1700(
                ID_1700=id_counters['1700'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                COD_DISP=campo[0],
                COD_MOD=campo[1],
                SER=campo[2],
                SUB=campo[3],
                NUM_DOC_INI=campo[4],
                NUM_DOC_FIN=campo[5],
                NUM_AUT=campo[6]
            )
            session.add(registro)
            id_counters['1700'] += 1

        if line.startswith('|1710|'):
            registro = Registro1710(
                ID_1710=id_counters['1710'],
                ID_0000=id_counters['0000'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_1700=id_counters['1700'] - 1,
                NUM_DOC_INI=campo[0],
                NUM_DOC_FIN=campo[1]
            )
            session.add(registro)
            id_counters['1710'] += 1

        if line.startswith('|1800|'):
            registro = Registro1800(
                ID_1800=id_counters['1800'],
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                VL_CARGA=replace_comma_with_dot(campo[0]),
                VL_PASS=replace_comma_with_dot(campo[1]),
                VL_FAT=replace_comma_with_dot(campo[2]),
                IND_RAT=replace_comma_with_dot(campo[3]),
                VL_ICMS_ANT=replace_comma_with_dot(campo[4]),
                VL_BC_ICMS=replace_comma_with_dot(campo[5]),
                VL_ICMS_APUR=replace_comma_with_dot(campo[6]),
                VL_BC_ICMS_APUR=replace_comma_with_dot(campo[7]),
                VL_DIF=replace_comma_with_dot(campo[8])
            )
            session.add(registro)
            id_counters['1800'] += 1

        if line.startswith('|1900|'):
            registro = Registro1900(
                ID_1900=id_counters['1900'],
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                IND_APUR_ICMS=campo[0],
                DESCR_COMPL_OUT_APUR=campo[1]
            )
            session.add(registro)
            id_counters['1900'] += 1

        if line.startswith('|1910|'):
            registro = Registro1910(
                ID_1910=id_counters['1910'],
                ID_1900=id_counters['1900'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                DT_INI=convert_to_datetime(campo[0]),
                DT_FIN=convert_to_datetime(campo[1])
            )
            session.add(registro)
            id_counters['1910'] += 1

        if line.startswith('|1920|'):
            registro = Registro1920(
                ID_1920=id_counters['1920'],
                ID_1910=id_counters['1910'] - 1,
                ID_1900=id_counters['1900'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                VL_TOT_TRANSF_DEBITOS_OA=replace_comma_with_dot(campo[0]),
                VL_TOT_AJ_DEBITOS_OA=replace_comma_with_dot(campo[1]),
                VL_ESTORNOS_CRED_OA=replace_comma_with_dot(campo[2]),
                VL_TOT_TRANSF_CREDITOS_OA=replace_comma_with_dot(campo[3]),
                VL_TOT_AJ_CREDITOS_OA=replace_comma_with_dot(campo[4]),
                VL_ESTORNOS_DEB_OA=replace_comma_with_dot(campo[5]),
                VL_SLD_CREDOR_ANT_OA=replace_comma_with_dot(campo[6]),
                VL_SLD_APURADO_OA=replace_comma_with_dot(campo[7]),
                VL_TOT_DED_OA=replace_comma_with_dot(campo[8]),
                VL_ICMS_RECOLHER_OA=replace_comma_with_dot(campo[9]),
                VL_SLD_CREDOR_TRANSP_OA=replace_comma_with_dot(campo[10]),
                DEB_ESP_OA=replace_comma_with_dot(campo[11])
            )
            session.add(registro)
            id_counters['1920'] += 1

        if line.startswith('|1921|'):
            registro = Registro1921(
                ID_1921=id_counters['1921'],
                ID_1920=id_counters['1920'] - 1,
                ID_1910=id_counters['1910'] - 1,
                ID_1900=id_counters['1900'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                COD_AJ_APUR=campo[0],
                DESCR_COMPL_AJ=campo[1],
                VL_AJ_APUR=replace_comma_with_dot(campo[2])
            )
            session.add(registro)
            id_counters['1921'] += 1

        if line.startswith('|1922|'):
            registro = Registro1922(
                ID_1922=id_counters['1922'],
                ID_1921=id_counters['1921'] - 1,
                ID_1920=id_counters['1920'] - 1,
                ID_1910=id_counters['1910'] - 1,
                ID_1900=id_counters['1900'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                NUM_DA=campo[0],
                NUM_PROC=campo[1],
                IND_PROC=campo[2],
                PROC=campo[3],
                TXT_COMPL=campo[4]
            )
            session.add(registro)
            id_counters['1922'] += 1

        if line.startswith('|1923|'):
            registro = Registro1923(
                ID_1923=id_counters['1923'],
                ID_1921=id_counters['1921'] - 1,
                ID_1920=id_counters['1920'] - 1,
                ID_1910=id_counters['1910'] - 1,
                ID_1900=id_counters['1900'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                COD_PART=campo[0],
                COD_MOD=campo[1],
                SER=campo[2],
                SUB=campo[3],
                NUM_DOC=campo[4],
                DT_DOC=convert_to_datetime(campo[5]),
                COD_ITEM=campo[6],
                VL_AJ_ITEM=replace_comma_with_dot(campo[7]),
                CHV_DOCe=campo[8]
            )
            session.add(registro)
            id_counters['1923'] += 1

        if line.startswith('|1925|'):
            registro = Registro1925(
                ID_1925=id_counters['1925'],
                ID_1920=id_counters['1920'] - 1,
                ID_1910=id_counters['1910'] - 1,
                ID_1900=id_counters['1900'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                COD_INF_ADIC=campo[0],
                VL_INF_ADIC=replace_comma_with_dot(campo[1]),
                DESCR_COMPL_AJ=campo[2]
            )
            session.add(registro)
            id_counters['1925'] += 1

        if line.startswith('|1926|'):
            registro = Registro1926(
                ID_1926=id_counters['1926'],
                ID_1920=id_counters['1920'] - 1,
                ID_1910=id_counters['1910'] - 1,
                ID_1900=id_counters['1900'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                COD_OR=campo[0],
                VL_OR=replace_comma_with_dot(campo[1]),
                DT_VCTO=convert_to_datetime(campo[2]),
                COD_REC=campo[3],
                NUM_PROC=campo[4],
                IND_PROC=campo[5],
                PROC=campo[6],
                TXT_COMPL=campo[7],
                MES_REF=campo[8]
            )
            session.add(registro)
            id_counters['1926'] += 1

        if line.startswith('|1960|'):
            registro = Registro1960(
                ID_1960=id_counters['1960'],
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                IND_AP=campo[0],
                G1_01=replace_comma_with_dot(campo[1]),
                G1_02=replace_comma_with_dot(campo[2]),
                G1_03=replace_comma_with_dot(campo[3]),
                G1_04=replace_comma_with_dot(campo[4]),
                G1_05=replace_comma_with_dot(campo[5]),
                G1_06=replace_comma_with_dot(campo[6]),
                G1_07=replace_comma_with_dot(campo[7]),
                G1_08=replace_comma_with_dot(campo[8]),
                G1_09=replace_comma_with_dot(campo[9]),
                G1_10=replace_comma_with_dot(campo[10]),
                G1_11=replace_comma_with_dot(campo[11])
            )
            session.add(registro)
            id_counters['1960'] += 1

        if line.startswith('|1970|'):
            registro = Registro1970(
                ID_1970=id_counters['1970'],
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                IND_AP=campo[0],
                G3_01=replace_comma_with_dot(campo[1]),
                G3_02=replace_comma_with_dot(campo[2]),
                G3_03=replace_comma_with_dot(campo[3]),
                G3_04=replace_comma_with_dot(campo[4]),
                G3_05=replace_comma_with_dot(campo[5]),
                G3_06=replace_comma_with_dot(campo[6]),
                G3_07=replace_comma_with_dot(campo[7]),
                G3_08=replace_comma_with_dot(campo[8]),
                G3_09=replace_comma_with_dot(campo[9]),
                G3_10=replace_comma_with_dot(campo[10]),
                G3_11=replace_comma_with_dot(campo[11]),
                G3_12=replace_comma_with_dot(campo[12])
            )
            session.add(registro)
            id_counters['1970'] += 1

        if line.startswith('|1975|'):
            registro = Registro1975(
                ID_1975=id_counters['1975'],
                ID_1970=id_counters['1970'] - 1,
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                ALIQ_IMP_BASE=replace_comma_with_dot(campo[0])
            )
            session.add(registro)
            id_counters['1975'] += 1

        if line.startswith('|1980|'):
            registro = Registro1980(
                ID_1980=id_counters['1980'],
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                IND_AP=campo[0],
                G4_01=replace_comma_with_dot(campo[1]),
                G4_02=replace_comma_with_dot(campo[2]),
                G4_03=replace_comma_with_dot(campo[3]),
                G4_04=replace_comma_with_dot(campo[4]),
                G4_05=replace_comma_with_dot(campo[5]),
                G4_06=replace_comma_with_dot(campo[6]),
                G4_07=replace_comma_with_dot(campo[7]),
                G4_08=replace_comma_with_dot(campo[8]),
                G4_09=replace_comma_with_dot(campo[9]),
                G4_10=replace_comma_with_dot(campo[10]),
                G4_11=replace_comma_with_dot(campo[11]),
                G4_12=replace_comma_with_dot(campo[12]),
                G4_13=replace_comma_with_dot(campo[13]),
                G4_14=replace_comma_with_dot(campo[14])
            )
            session.add(registro)
            id_counters['1980'] += 1

        if line.startswith('|1990|'):
            registro = Registro1990(
                ID_1990=id_counters['1990'],
                ID_1001=id_counters['1001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                QTD_LIN_1=campo[0]
            )
            session.add(registro)
            id_counters['1990'] += 1

        if line.startswith('|9001|'):
            registro = Registro9001(
                ID_9001=id_counters['9001'],
                ID_0000=id_counters['0000'] - 1,
                IND_MOV=campo[0]
            )
            session.add(registro)
            id_counters['9001'] += 1

        if line.startswith('|9900|'):
            registro = Registro9900(
                ID_9900=id_counters['9900'],
                ID_9001=id_counters['9001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                REG_BLC=campo[0],
                QTD_REG_BLC=campo[1]
            )
            session.add(registro)
            id_counters['9900'] += 1

        if line.startswith('|9990|'):
            registro = Registro9990(
                ID_9990=id_counters['9990'],
                ID_9001=id_counters['9001'] - 1,
                ID_0000=id_counters['0000'] - 1,
                QTD_LIN_9=campo[0]
            )
            session.add(registro)
            id_counters['9990'] += 1

        if line.startswith('|9999|'):
            registro = Registro9999(
                ID_9999=id_counters['9999'],
                ID_0000=id_counters['0000'] - 1,
                QTD_LIN=campo[0]
            )
            session.add(registro)
            id_counters['9999'] += 1


    session.commit()
    session.close()
