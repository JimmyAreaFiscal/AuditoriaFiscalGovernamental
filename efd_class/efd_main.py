import pandas as pd 
from datetime import datetime as dt 
from datetime import time 


class EFD:

    """ Função para quebra de EFD """

    def __init__(self, txt):
        
        df = open(txt, 'r', encoding = 'Latin-1')

        self.efd = dict()
        self._processar_EFD(df)

    def RetornarCampo(self, registro, campo = 'Todos', filtro = {}):
        resultado_registro = self._iterar_sobre_efd(self.efd, registro)

        if campo != 'Todos':
            return resultado_registro[campo]
        else:
            return resultado_registro

    @property
    def INFO0200(self):
        return self.RetornarCampo('0200')

    

    @property
    def ProdutosEntrada(self):
        C170_mod = pd.DataFrame()
        for n in self.efd['0000']['C001']['C100'].keys():
            C100 = self.efd['0000']['C001']['C100'][n]
            chave_acesso = C100['CHV_NFE'] 
            dt_entrada = C100['DT_E_S']

            C170_temp = pd.DataFrame() 
            
            for n170 in C100['C170'].keys():
                temp = {k:v for k,v in C100['C170'][n170].items() if not isinstance(v, dict)}
                temp_df = pd.DataFrame(temp, index=[n170])
                C170_temp = pd.concat([C170_temp, temp_df])

                C170_temp['CHV_NFE'] = chave_acesso
                C170_temp['DT_E_S'] = dt_entrada 
            C170_mod = pd.concat([C170_mod, C170_temp])
        return C170_mod 
    
    @property
    def RegistroAnaliticoEntrada(self):
        C190_mod = pd.DataFrame()
        for n in self.efd['0000']['C001']['C100'].keys():
            C100 = self.efd['0000']['C001']['C100'][n]
            chave_acesso = C100['CHV_NFE'] 
            dt_entrada = C100['DT_E_S']
            ind_oper = C100['IND_OPER']
            C190_temp = pd.DataFrame() 
            
            # Filtrando por C100 de entrada 
            if ind_oper=='0':
                for n190 in C100['C190'].keys():
                    temp = {k:v for k,v in C100['C190'][n190].items() if not isinstance(v, dict)}
                    temp_df = pd.DataFrame(temp, index=[n190])
                    C190_temp = pd.concat([C190_temp, temp_df])

                    C190_temp['CHV_NFE'] = chave_acesso
                    C190_temp['DT_E_S'] = dt_entrada 
                C190_mod = pd.concat([C190_mod, C190_temp])
        return C190_temp 
    
    

    def _iterar_sobre_efd(self, dicionario, registro):
        lista_respostas = []
        if isinstance(dicionario, dict):
            if registro in dicionario.keys():
                if 0 in dicionario[registro].keys():
                    resultado = []
                    for subdict in dicionario[registro].values():
                        subresultado = {k:v for k,v in subdict.items() if not isinstance(v, dict)}
                        resultado.append(subresultado)
                    return resultado
                else:
                    return {k:v for k,v in dicionario[registro].items() if not isinstance(v, dict) or isinstance(k, int)}
            else:
                for valor in dicionario.values():
                    
                    retorno = self._iterar_sobre_efd(valor, registro)
                    if retorno:     # Caso encontre algo
                        if len(retorno) >=1:
                            lista_respostas.append(retorno)
                
                resposta_tratada = []
                for resposta in lista_respostas:
                    if isinstance(resposta, list):
                        for mini_resposta in resposta:
                            resposta_tratada.append(mini_resposta)
                    elif isinstance(resposta, dict):
                        resposta_tratada.append(resposta)
                return resposta_tratada
        return None 

    def _processar_EFD(self, df):
        # Gera-se o loop para processar a EFD em forma de árvore, com cada registro filho estando no "value" do dicionário com "key" igual ao registro pai
        for line in df:

            if line[:2] == '|0': # BLOCO 0 

                if line[:6] == '|0000|': # Nível 0
                    arq = line.split('|')
                    self.efd['0000'] = dict()
                    self.efd['0000']['COD_VER'] = self._formatar_str(arq[2])
                    self.efd['0000']['COD_FIN'] = self._formatar_str(arq[3])
                    self.efd['0000']['DT_INI'] = self._formatar_data(arq[4])
                    self.efd['0000']['DT_FIN'] = self._formatar_data(arq[5])
                    self.efd['0000']['NOME'] = self._formatar_str(arq[6])
                    self.efd['0000']['CNPJ'] = self._formatar_str(arq[7])
                    self.efd['0000']['CPF'] = self._formatar_str(arq[8])
                    self.efd['0000']['UF'] = self._formatar_str(arq[9])
                    self.efd['0000']['IE'] = self._formatar_str(arq[10])
                    self.efd['0000']['COD_MUN'] = self._formatar_str(arq[11])
                    self.efd['0000']['IM'] = self._formatar_str(arq[12])
                    self.efd['0000']['SUFRAMA'] = self._formatar_str(arq[13])
                    self.efd['0000']['IND_PERFIL'] = self._formatar_str(arq[14])
                    self.efd['0000']['IND_ATIV'] = self._formatar_str(arq[15])
                
                elif line[:6] == '|0001|': # Nível 1

                    n_0015 = 0 # Variável de controle para registro nível 2 que possua várias ocorrências
                    n_0150 = 0
                    n_0190 = 0
                    n_0200 = 0
                    n_0300 = 0
                    n_0400 = 0
                    n_0450 = 0
                    n_0460 = 0
                    n_0500 = 0
                    n_0600 = 0
                    arq = line.split('|')

                    self.efd['0000']['0001'] = dict()

                    self.efd['0000']['0001']['IND_MOV'] = self._formatar_str(arq[2])
                    self.efd['0000']['0001']['0002'] = dict()
                    self.efd['0000']['0001']['0005'] = dict()
                    self.efd['0000']['0001']['0015'] = dict()
                    self.efd['0000']['0001']['0100'] = dict()
                    self.efd['0000']['0001']['0150'] = dict()
                    self.efd['0000']['0001']['0190'] = dict()
                    self.efd['0000']['0001']['0200'] = dict()
                    self.efd['0000']['0001']['0300'] = dict()
                    self.efd['0000']['0001']['0400'] = dict()
                    self.efd['0000']['0001']['0450'] = dict()
                    self.efd['0000']['0001']['0460'] = dict()
                    self.efd['0000']['0001']['0500'] = dict()
                    self.efd['0000']['0001']['0600'] = dict()

                elif line[:6] == '|0002|': # Nível 2
                    arq = line.split('|')
                    self.efd['0000']['0001']['0002']['CLAS_ESTAB_IND'] = self._formatar_str(arq[2])

                elif line[:6] == '|0005|': # Nível 2
                    arq = line.split('|')
                    self.efd['0000']['0001']['0005']['FANTASIA'] = self._formatar_str(arq[2])
                    self.efd['0000']['0001']['0005']['CEP'] = self._formatar_str(arq[3])
                    self.efd['0000']['0001']['0005']['END'] = self._formatar_str(arq[4])
                    self.efd['0000']['0001']['0005']['NUM'] = self._formatar_str(arq[5])
                    self.efd['0000']['0001']['0005']['COMPL'] = self._formatar_str(arq[6])
                    self.efd['0000']['0001']['0005']['BAIRRO'] = self._formatar_str(arq[7])
                    self.efd['0000']['0001']['0005']['FONE'] = self._formatar_str(arq[8])
                    self.efd['0000']['0001']['0005']['FAX'] = self._formatar_str(arq[9])
                    self.efd['0000']['0001']['0005']['EMAIL'] = self._formatar_str(arq[10])

                elif line[:6] == '|0015|': # Nível 2, mas com Ocorrência V (vários por arquivo)

                    arq = line.split('|')
                    
                    
                    self.efd['0000']['0001']['0015'][n_0015] = dict()
                    self.efd['0000']['0001']['0015'][n_0015]['UF_ST'] = self._formatar_str(arq[2])
                    self.efd['0000']['0001']['0015'][n_0015]['IE_ST'] = self._formatar_str(arq[3])
                    n_0015 += 1

                elif line[:6] == '|0100|': # Nível 2

                    arq = line.split('|')
                    
                    self.efd['0000']['0001']['0100']['NOME'] = self._formatar_str(arq[2])
                    self.efd['0000']['0001']['0100']['CPF'] = self._formatar_str(arq[3])
                    self.efd['0000']['0001']['0100']['CRC'] = self._formatar_str(arq[4])
                    self.efd['0000']['0001']['0100']['CNPJ'] = self._formatar_str(arq[5])
                    self.efd['0000']['0001']['0100']['CEP'] = self._formatar_str(arq[6])
                    self.efd['0000']['0001']['0100']['END'] = self._formatar_str(arq[7])
                    self.efd['0000']['0001']['0100']['NUM'] = self._formatar_str(arq[8])
                    self.efd['0000']['0001']['0100']['COMPL'] = self._formatar_str(arq[9])
                    self.efd['0000']['0001']['0100']['BAIRRO'] = self._formatar_str(arq[10])
                    self.efd['0000']['0001']['0100']['FONE'] = self._formatar_str(arq[11])
                    self.efd['0000']['0001']['0100']['FAX'] = self._formatar_str(arq[12])
                    self.efd['0000']['0001']['0100']['EMAIL'] = self._formatar_str(arq[13])
                    self.efd['0000']['0001']['0100']['COD_MUN'] = self._formatar_str(arq[14])

                elif line[:6] == '|0150|': # Nível 2, mas com Ocorrência V (vários por arquivo)
                    

                    n_0175 = 0 # Variável de controle em virtude de ter ocorrência 1:N
                    arq = line.split('|')
                    self.efd['0000']['0001']['0150'][n_0150] = dict()
                    self.efd['0000']['0001']['0150'][n_0150]['COD_PART'] = self._formatar_str(arq[2], inteiro=False)
                    self.efd['0000']['0001']['0150'][n_0150]['NOME'] = self._formatar_str(arq[3])
                    self.efd['0000']['0001']['0150'][n_0150]['COD_PAIS'] = self._formatar_str(arq[4])
                    self.efd['0000']['0001']['0150'][n_0150]['CNPJ'] = self._formatar_str(arq[5])
                    self.efd['0000']['0001']['0150'][n_0150]['CPF'] = self._formatar_str(arq[6])
                    self.efd['0000']['0001']['0150'][n_0150]['IE'] = self._formatar_str(arq[7])
                    self.efd['0000']['0001']['0150'][n_0150]['COD_MUN'] = self._formatar_str(arq[8])
                    self.efd['0000']['0001']['0150'][n_0150]['SUFRAMA'] = self._formatar_str(arq[9])
                    self.efd['0000']['0001']['0150'][n_0150]['END'] = self._formatar_str(arq[10])
                    self.efd['0000']['0001']['0150'][n_0150]['NUM'] = self._formatar_str(arq[11])
                    self.efd['0000']['0001']['0150'][n_0150]['COMPL'] = self._formatar_str(arq[12])
                    self.efd['0000']['0001']['0150'][n_0150]['BAIRRO'] = self._formatar_str(arq[13])
                    self.efd['0000']['0001']['0150'][n_0150]['0175'] = dict()
                    n_0150 += 1 

                elif line[:6] == '|0175|': # Nível 3, mas com Ocorrência 1:N (vários por arquivo)
                    
                    arq = line.split('|')
                    self.efd['0000']['0001']['0150'][n_0150-1]['0175'][n_0175] = dict()
                    self.efd['0000']['0001']['0150'][n_0150-1]['0175'][n_0175]['DT_ALT'] = self._formatar_data(arq[2])
                    self.efd['0000']['0001']['0150'][n_0150-1]['0175'][n_0175]['NR_CAMPO'] = self._formatar_str(arq[3])
                    self.efd['0000']['0001']['0150'][n_0150-1]['0175'][n_0175]['CONT_ANT'] = self._formatar_str(arq[4])
                    n_0175 += 1 

                elif line[:6] == '|0190|': # Nível 2, mas com Ocorrência V (vários por arquivo)
                    arq = line.split('|')
                    self.efd['0000']['0001']['0190'][n_0190] = {
                        'UNID': self._formatar_str(arq[2]),
                        'DESCR': self._formatar_str(arq[3])
                    }
                    
                    n_0190 += 1 
                
                elif line[:6] == '|0200|': # Nível 2, mas com Ocorrência V (vários por arquivo)

                    n_0205 = 0
                    n_0210 = 0
                    n_0220 = 0
                    n_0221 = 0
                    arq = line.split('|')
                    self.efd['0000']['0001']['0200'][n_0200] = {
                        'COD_ITEM': self._formatar_str(arq[2]),
                        'DESCR_ITEM': self._formatar_str(arq[3]),
                        'COD_BARRA': self._formatar_str(arq[4]),
                        'COD_ANT_ITEM': self._formatar_str(arq[5]),
                        'UNID_INV': self._formatar_str(arq[6]),
                        'TIPO_ITEM': self._formatar_str(arq[7]),
                        'COD_NCM': self._formatar_str(arq[8]),
                        'EX_IPI': self._formatar_str(arq[9]),
                        'COD_GEN': self._formatar_str(arq[10]),
                        'COD_LST': self._formatar_str(arq[11]),
                        'ALIQ_ICMS': self._formatar_valor(arq[12]),
                        'CEST': self._formatar_str(arq[13]),
                        '0205': dict(),
                        '0206': dict(),
                        '0210': dict(),
                        '0220': dict(),
                        '0221': dict(),
                    }
                    
                    n_0200 += 1

                elif line[:6] == '|0205|': # Nível 3, mas com Ocorrência V (vários por arquivo)
                    arq = line.split('|')
                    self.efd['0000']['0001']['0200'][n_0200-1]['0205'][n_0205] = {
                        'DESCR_ANT_ITEM': self._formatar_str(arq[2]),
                        'DT_INI': self._formatar_data(arq[3]),
                        'DT_FIM': self._formatar_data(arq[4]),
                        'COD_ANT_ITEM': self._formatar_str(arq[5])
                    }
                    
                    n_0205 += 1 

                elif line[:6] == '|0206|': # Nível 3, mas com Ocorrência 1:1
                    arq = line.split('|')
                    self.efd['0000']['0001']['0200'][n_0200-1]['0206'] = {
                        'COD_COMB': self._formatar_str(arq[2])
                    }
                    
                elif line[:6] == '|0210|': # Nível 3, mas com Ocorrência V (vários por arquivo)
                    arq = line.split('|')
                    self.efd['0000']['0001']['0200'][n_0200-1]['0210'][n_0210] = {
                        'COD_ITEM_COMP': self._formatar_str(arq[2]),
                        'QTD_COMP': self._formatar_valor(arq[3]),
                        'PERDA': self._formatar_valor(arq[4])
                    }
                    
                    n_0210 += 1 

                elif line[:6] == '|0220|': # Nível 3, mas com Ocorrência V (vários por arquivo)
                    arq = line.split('|')
                    self.efd['0000']['0001']['0200'][n_0200-1]['0220'][n_0220] = {
                        'UNID_CONV': self._formatar_str(arq[2]),
                        'FAT_CONV': self._formatar_valor(arq[3]),
                        'COD_BARRA': self._formatar_str(arq[4])
                    }
                    
                    n_0220 += 1 

                elif line[:6] == '|0221|': # Nível 3, mas com Ocorrência V (vários por arquivo)
                    arq = line.split('|')
                    self.efd['0000']['0001']['0200'][n_0200-1]['0221'][n_0221] = {
                        'COD_ITEM_ATOMICO': self._formatar_str(arq[2]),
                        'QTD_CONTIDA': self._formatar_valor(arq[3])
                    }
                    n_0221 += 1 

                elif line[:6] == '|0300|': # Nível 2, mas com Ocorrência V (vários por arquivo)

                    
                    arq = line.split('|')
                    self.efd['0000']['0001']['0300'][n_0300] = {
                        'COD_IND_BEM': self._formatar_str(arq[2]),
                        'IDENT_MERC': self._formatar_str(arq[3]),
                        'DESCR_ITEM': self._formatar_str(arq[4]),
                        'COD_PRNC': self._formatar_str(arq[5]),
                        'COD_CTA': self._formatar_str(arq[6]),
                        'NR_PARC': self._formatar_valor(arq[7])
                    }
                    n_0300 += 1

                elif line[:6] == '|0305|': # Nível 3, mas com Ocorrência 1:1 (vários por arquivo)

                    
                    arq = line.split('|')
                    self.efd['0000']['0001']['0300'][n_0300-1]['0305'] = {
                        'COD_CCUS': self._formatar_str(arq[2]),
                        'FUNC': self._formatar_str(arq[3]),
                        'VIDA_UTIL': self._formatar_valor(arq[4])
                    }               

                elif line[:6] == '|0400|': # Nível 2, mas com Ocorrência V (vários por arquivo)

                    
                    arq = line.split('|')
                    self.efd['0000']['0001']['0400'][n_0400] = {
                        'COD_NAT': self._formatar_str(arq[2]),
                        'DESCR_NAT': self._formatar_str(arq[3])
                    }
                    n_0400 += 1

                elif line[:6] == '|0450|': # Nível 2, mas com Ocorrência V (vários por arquivo)


                    
                    arq = line.split('|')
                    self.efd['0000']['0001']['0450'][n_0450] = {
                        'COD_INF': self._formatar_str(arq[2]),
                        'TXT': self._formatar_str(arq[3])
                    }
                    n_0450 += 1

                elif line[:6] == '|0460|': # Nível 2, mas com Ocorrência V (vários por arquivo)


                    arq = line.split('|')
                    self.efd['0000']['0001']['0460'][n_0460] = {
                        'COD_OBS': self._formatar_str(arq[2]),
                        'TXT': self._formatar_str(arq[3])
                    }
                    n_0460 += 1
            
                elif line[:6] == '|0500|': # Nível 2, mas com Ocorrência V (vários por arquivo)
                    

                    arq = line.split('|')
                    self.efd['0000']['0001']['0500'][n_0500] = {
                        'DT_ALT': self._formatar_data(arq[2]),
                        'NAT_CC': self._formatar_str(arq[3]),
                        'IND_CTA': self._formatar_str(arq[4]),
                        'NÍVEL': self._formatar_str(arq[5]),
                        'COD_CTA': self._formatar_str(arq[6]),
                        'NOME_CTA': self._formatar_str(arq[7]),
                    }
                    n_0500 += 1

                elif line[:6] == '|0600|': # Nível 2, mas com Ocorrência V (vários por arquivo)

                    
                    arq = line.split('|')
                    self.efd['0000']['0001']['0600'][n_0600] = {
                        'DT_ALT': self._formatar_data(arq[2]),
                        'COD_CCUS': self._formatar_str(arq[3]),
                        'CCUS': self._formatar_str(arq[4])
                    }
                    n_0600 += 1

                elif line[:6] == '|0990|': # Nível 2, mas com Ocorrência V (vários por arquivo)

                    
                    arq = line.split('|')
                    self.efd['0000']['0990'] = {
                        'QTD_LIN_0': self._formatar_valor(arq[2])
                    }

                else:
                    raise ValueError(f"Registro em bloco {line[1]} inconsistente: {line[:6]}")

            elif line[:2] == '|C': # BLOCO C
                if line[:6] == '|C001|': # Nível 1
                    n_c100 = 0
                    n_c300 = 0
                    n_c350 = 0
                    n_c400 = 0
                    n_c495 = 0
                    n_c500 = 0
                    n_c600 = 0
                    n_c700 = 0
                    n_c800 = 0
                    n_c860 = 0

                    arq = line.split('|')
                    self.efd['0000']['C001'] = {
                        'IND_MOV': self._formatar_str(arq[2]),
                        'C100': {},
                        'C300': {},
                        'C350': {},
                        'C400': {},
                        'C495': {},
                        'C500': {},
                        'C600': {},
                        'C700': {},
                        'C800': {},
                        'C860': {}
                    }
                    
                elif line[:6] == '|C100|': # Nível 2

                    n_c110 = 0
                    n_c120 = 0
                    n_c165 = 0
                    n_c170 = 0
                    n_c190 = 0
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C100'][n_c100] = {
                        'IND_OPER': self._formatar_str(arq[2]),
                        'IND_EMIT': self._formatar_str(arq[3]),
                        'COD_PART': self._formatar_str(arq[4], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[5]),
                        'COD_SIT': self._formatar_str(arq[6]),
                        'SER': self._formatar_str(arq[7]),
                        'NUM_DOC': self._formatar_str(arq[8]),
                        'CHV_NFE': self._formatar_str(arq[9]),
                        'DT_DOC': self._formatar_data(arq[10]),
                        'DT_E_S': self._formatar_data(arq[11]),
                        'VL_DOC': self._formatar_valor(arq[12]),
                        'IND_PGTO': self._formatar_str(arq[13]),
                        'VL_DESC': self._formatar_valor(arq[14]),
                        'VL_ABAT_NT': self._formatar_valor(arq[15]),
                        'VL_MERC': self._formatar_valor(arq[16]),
                        'IND_FRT': self._formatar_str(arq[17]),
                        'VL_FRT': self._formatar_valor(arq[18]),
                        'VL_SEG': self._formatar_valor(arq[19]),
                        'VL_OUT_DA': self._formatar_valor(arq[20]),
                        'VL_BC_ICMS': self._formatar_valor(arq[21]),
                        'VL_ICMS': self._formatar_valor(arq[22]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[23]),
                        'VL_ICMS_ST': self._formatar_valor(arq[24]),
                        'VL_IPI': self._formatar_valor(arq[25]),
                        'VL_PIS': self._formatar_valor(arq[26]),
                        'VL_COFINS': self._formatar_valor(arq[27]),
                        'VL_PIS_ST': self._formatar_valor(arq[28]),
                        'VL_COFINS_ST': self._formatar_valor(arq[29]),
                        'C101': {},
                        'C105': {},
                        'C110': {},
                        'C120': {},
                        'C130': {},
                        'C140': {},
                        'C160': {}, 
                        'C165': {}, 
                        'C170': {},
                        'C185': {},
                        'C186': {},
                        'C190': {},
                        'C195': {}
                    }

                    n_c100 += 1
                                
                elif line[:6] == '|C101|': # Nível 3
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C100'][n_c100-1]['C101'] = {
                        'VL_FCP_UF_DEST': self._formatar_valor(arq[2]),
                        'VL_ICMS_UF_DEST': self._formatar_valor(arq[3]),
                        'VL_ICMS_UF_REM': self._formatar_valor(arq[4])
                    }
                    
                elif line[:6] == '|C105|': # Nível 3
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C100'][n_c100-1]['C105'] = {
                        'OPER': self._formatar_str(arq[2]),
                        'UF': self._formatar_str(arq[3])
                    }
                    
                elif line[:6] == '|C110|': # Nível 3

                    n_c111 = 0
                    n_c112 = 0
                    n_c113 = 0
                    n_c114 = 0
                    n_c115 = 0
                    n_c116 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C110'][n_c110] = {
                        'COD_INF': self._formatar_str(arq[2]),
                        'TXT_COMPL': self._formatar_str(arq[3]),
                        'C111': {},
                        'C112': {},
                        'C113': {},
                        'C114': {},
                        'C115': {},
                        'C116': {},
                    }
                    n_c110 += 1
                            
                elif line[:6] == '|C111|': # Nível 4

                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C110'][n_c110-1]['C111'][n_c111] = {
                        'NUM_PROC': self._formatar_str(arq[2]),
                        'NUM_PROC': self._formatar_str(arq[3]),
                    }
                    n_c111 += 1
                    
                elif line[:6] == '|C112|': # Nível 4

                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C110'][n_c110-1]['C112'][n_c112] = {
                        'COD_DA': self._formatar_str(arq[2]),
                        'UF': self._formatar_str(arq[3]),
                        'NUM_DA': self._formatar_str(arq[4]),
                        'COD_AUT': self._formatar_str(arq[5]),
                        'VL_DA': self._formatar_valor(arq[6]),
                        'DT_VCTO': self._formatar_data(arq[7]),
                        'DT_PGTO': self._formatar_data(arq[8])
                    }
                    n_c112 += 1
                    
                elif line[:6] == '|C113|': # Nível 4

                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C110'][n_c110-1]['C113'][n_c113] = {
                        'IND_OPER': self._formatar_str(arq[2]),
                        'IND_EMIT': self._formatar_str(arq[3]),
                        'COD_PART': self._formatar_str(arq[4], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[5]),
                        'SER': self._formatar_str(arq[6]),
                        'SUB': self._formatar_str(arq[7]),
                        'NUM_DOC': self._formatar_str(arq[8]),
                        'DT_DOC': self._formatar_data(arq[9]),
                        'CHV_DFE': self._formatar_str(arq[10])
                    }
                    n_c113 += 1
                    
                elif line[:6] == '|C114|': # Nível 4

                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C110'][n_c110-1]['C114'][n_c114] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'ECF_FAB': self._formatar_str(arq[3]),
                        'ECF_CX': self._formatar_str(arq[4]),
                        'NUM_DOC': self._formatar_str(arq[5]),
                        'DT_DOC': self._formatar_data(arq[6])
                    }
                    n_c114 += 1

                elif line[:6] == '|C115|': # Nível 4

                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C110'][n_c110-1]['C115'][n_c115] = {
                        'IND_CARGA': self._formatar_str(arq[2]),
                        'CNPJ_COL': self._formatar_str(arq[3]),
                        'IE_COL': self._formatar_str(arq[4]),
                        'CPF_COL': self._formatar_str(arq[5]),
                        'COD_MUN_COL': self._formatar_str(arq[6]),
                        'CNPJ_ENTG': self._formatar_str(arq[7]),
                        'IE_ENTG': self._formatar_str(arq[8]),
                        'CPF_ENTG': self._formatar_str(arq[9]),
                        'COD_MUN_ENTG': self._formatar_str(arq[10])
                    }
                    n_c115 += 1

                elif line[:6] == '|C116|': # Nível 4

                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C110'][n_c110-1]['C116'][n_c116] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'NR_SAT': self._formatar_str(arq[3]),
                        'CHV_CFE': self._formatar_str(arq[4]),
                        'NUM_CFE': self._formatar_str(arq[5]),
                        'DT_DOC': self._formatar_data(arq[6])
                    }
                    n_c116 += 1
            
                elif line[:6] == '|C120|': # Nível 3

                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C120'][n_c120] = {
                        'COD_DOC_IMP': self._formatar_str(arq[2]),
                        'NUM_DOC_IMP': self._formatar_str(arq[3]),
                        'PIS_IMP': self._formatar_valor(arq[4]),
                        'COFINS_IMP': self._formatar_valor(arq[5]),
                        'NUM_ACDRAW': self._formatar_str(arq[6])
                    }
                    n_c120 += 1

                elif line[:6] == '|C130|': # Nível 3

                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C130'] = {
                        'VL_SERV_NT': self._formatar_valor(arq[2]),
                        'VL_BC_ISSQN': self._formatar_valor(arq[3]),
                        'VL_ISSQN': self._formatar_valor(arq[4]),
                        'VL_BC_IRRF': self._formatar_valor(arq[5]),
                        'VL_IRRF': self._formatar_valor(arq[6]),
                        'VL_BC_PREV': self._formatar_valor(arq[7]),
                        'VL_PREV': self._formatar_valor(arq[8])
                    }

                elif line[:6] == '|C140|': # Nível 3
                    
                    n_c141 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C140'] = {
                        'IND_EMIT': self._formatar_str(arq[2]),
                        'IND_TIT': self._formatar_str(arq[3]),
                        'DESC_TIT': self._formatar_str(arq[4]),
                        'NUM_TIT': self._formatar_str(arq[5]),
                        'QTD_PARC': self._formatar_valor(arq[6]),
                        'VL_TIT': self._formatar_valor(arq[7]),
                        'C141': {}
                    }

                elif line[:6] == '|C141|': # Nível 4

                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C140'][n_c141] = {
                        'NUM_PARC': self._formatar_str(arq[2]),
                        'DT_VCTO': self._formatar_data(arq[3]),
                        'VL_PARC': self._formatar_valor(arq[4])
                    }
                    n_c141 += 1 

                elif line[:6] == '|C160|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C160'] = {
                        'COD_PART': self._formatar_str(arq[2], inteiro=False),
                        'VEIC_ID': self._formatar_str(arq[3]),
                        'QTD_VOL': self._formatar_valor(arq[4]),
                        'PESO_BRT': self._formatar_valor(arq[5]),
                        'PESO_LIQ': self._formatar_valor(arq[6]),
                        'UF_ID': self._formatar_str(arq[7])
                    }

                elif line[:6] == '|C165|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C165'][n_c165] = {
                        'COD_PART': self._formatar_str(arq[2], inteiro=False),
                        'VEIC_ID': self._formatar_str(arq[3]),
                        'COD_AUT': self._formatar_str(arq[4]),
                        'NR_PARC': self._formatar_str(arq[5]),
                        'HORA': self._formatar_hora(arq[6]),
                        'TEMPER': self._formatar_str(arq[7]),

                        'QTD_VOL': self._formatar_str(arq[8]),
                        'PESO_BRT': self._formatar_str(arq[9]),
                        'PESO_LIQ': self._formatar_str(arq[10]),
                        'NOM_MOT': self._formatar_str(arq[11]),
                        'CPF': self._formatar_str(arq[12]),
                        'UF_ID': self._formatar_str(arq[13])
                    }
                    n_c165 += 1

                elif line[:6] == '|C170|': # Nível 3
                    n_c171 = 0
                    n_c173 = 0
                    n_c174 = 0
                    n_c175 = 0
                    n_c176 = 0
                    n_c181 = 0

                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170] = {
                        'NUM_ITEM': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'DESCR_COMPL': self._formatar_str(arq[4]),
                        'QTD': self._formatar_valor(arq[5]),
                        'UNID': self._formatar_str(arq[6]),
                        'VL_ITEM': self._formatar_valor(arq[7]),
                        'VL_DESC': self._formatar_valor(arq[8]),
                        'IND_MOV': self._formatar_str(arq[9]),
                        'CST_ICMS': self._formatar_str(arq[10]),
                        'CFOP': self._formatar_str(arq[11]),
                        'COD_NAT': self._formatar_str(arq[12]),
                        'VL_BC_ICMS': self._formatar_valor(arq[13]),
                        'ALIQ_ICMS': self._formatar_valor(arq[14]),
                        'VL_ICMS': self._formatar_valor(arq[15]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[16]),
                        'ALIQ_ST': self._formatar_valor(arq[17]),
                        'VL_ICMS_ST': self._formatar_valor(arq[18]),
                        'IND_APUR': self._formatar_str(arq[19]),
                        'CST_IPI': self._formatar_str(arq[20]),
                        'COD_ENQ': self._formatar_str(arq[21]),
                        'VL_BC_IPI': self._formatar_valor(arq[22]),
                        'ALIQ_IPI': self._formatar_valor(arq[23]),
                        'VL_IPI': self._formatar_valor(arq[24]),
                        'CST_PIS': self._formatar_str(arq[25]),
                        'VL_BC_PIS': self._formatar_valor(arq[26]),
                        'ALIQ_PIS': self._formatar_valor(arq[27]),
                        'QUANT_BC_PIS': self._formatar_valor(arq[28]),
                        'ALIQ_PIS_R': self._formatar_valor(arq[29]),
                        'VL_PIS': self._formatar_valor(arq[30]),
                        'CST_COFINS': self._formatar_str(arq[31]),
                        'VL_BC_COFINS': self._formatar_valor(arq[32]),
                        'ALIQ_COFINS': self._formatar_valor(arq[33]),
                        'QUANT_BC_COFINS': self._formatar_valor(arq[34]),
                        'ALIQ_COFINS_R': self._formatar_valor(arq[35]),
                        'VL_COFINS': self._formatar_valor(arq[36]),
                        'COD_CTA': self._formatar_str(arq[37]),
                        'VL_ABAT_NT': self._formatar_valor(arq[38]),
                        'C171': {},
                        'C172': {},
                        'C173': {},
                        'C174': {},
                        'C175': {},
                        'C176': {},
                        'C177': {},
                        'C178': {},
                        'C179': {},
                        'C180': {},
                        'C181': {},

                    }
                    n_c170 += 1

                elif line[:6] == '|C171|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C171'][n_c171] = {
                        'NUM_TANQUE': self._formatar_str(arq[2]),
                        'QTDE': self._formatar_valor(arq[3])
                    }
                    n_c171 += 1

                elif line[:6] == '|C172|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C172'] = {
                        'VL_BC_ISSQN': self._formatar_valor(arq[2]),
                        'ALIQ_ISSQN': self._formatar_valor(arq[3]),
                        'VL_ISSQN': self._formatar_valor(arq[4])
                    }

                elif line[:6] == '|C173|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C173'][n_c173] = {
                        'LOTE_MED': self._formatar_str(arq[2]),
                        'QTD_ITEM': self._formatar_valor(arq[3]),
                        'DT_FAB': self._formatar_data(arq[4]),
                        'DT_VAL': self._formatar_data(arq[5]),
                        'IND_MED': self._formatar_str(arq[6]),
                        'TP_PROD': self._formatar_str(arq[7]),
                        'VL_TAB_MAX': self._formatar_valor(arq[8])
                    }
                    n_c173 += 1

                elif line[:6] == '|C174|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C174'][n_c174] = {
                        'IND_ARM': self._formatar_str(arq[2]),
                        'NUM_ARM': self._formatar_str(arq[3]),
                        'DESCR_COMPL': self._formatar_str(arq[4])
                    }
                    n_c174 += 1

                elif line[:6] == '|C175|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C175'][n_c175] = {
                        'IND_VEIC_OPER': self._formatar_str(arq[2]),
                        'CNPJ': self._formatar_str(arq[3]),
                        'UF': self._formatar_str(arq[4]),
                        'CHASSI_VEIC': self._formatar_str(arq[5])
                    }
                    n_c175 += 1

                elif line[:6] == '|C176|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C176'][n_c176] = {
                        'COD_MOD_ULT_E': self._formatar_str(arq[2]),
                        'NUM_DOC_ULT_E': self._formatar_str(arq[3]),
                        'SER_ULT_E': self._formatar_str(arq[4]),
                        'DT_ULT_E': self._formatar_data(arq[5]),
                        'COD_PART_ULT_E': self._formatar_str(arq[6], inteiro=False),
                        'QUANT_ULT_E': self._formatar_valor(arq[7]),
                        'VL_UNIT_ULT_E': self._formatar_valor(arq[8]),
                        'VL_UNIT_BC_ST': self._formatar_valor(arq[9]),
                        'CHAVE_NFE_ULT_E': self._formatar_str(arq[10]),
                        'NUM_ITEM_ULT_E': self._formatar_str(arq[11]),
                        'VL_UNIT_BC_ICMS_ULT_E': self._formatar_valor(arq[12]),
                        'ALIQ_ICMS_ULT_E': self._formatar_valor(arq[13]),
                        'VL_UNIT_LIMITE_BC_ICMS_ULT_E': self._formatar_valor(arq[14]),
                        'VL_UNIT_ICMS_ULT_E': self._formatar_valor(arq[15]),
                        'ALIQ_ST_ULT_E': self._formatar_valor(arq[16]),
                        'VL_UNIT_RES': self._formatar_valor(arq[17]),
                        'COD_RESP_RET': self._formatar_str(arq[18]),
                        'COD_MOT_RES': self._formatar_str(arq[19]),
                        'CHAVE_NFE_RET': self._formatar_str(arq[20]),
                        'COD_PART_NFE_RET': self._formatar_str(arq[21], inteiro=False),
                        'SER_NFE_RET': self._formatar_str(arq[22]),
                        'NUM_NFE_RET': self._formatar_str(arq[23]),
                        'ITEM_NFE_RET': self._formatar_str(arq[24]),
                        'COD_DA': self._formatar_str(arq[25]),
                        'NUM_DA': self._formatar_str(arq[26]),
                        'VL_UNIT_RES_FCP_ST': self._formatar_valor(arq[27])
                    }
                    n_c176 += 1

                elif line[:6] == '|C177|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C177'] = {
                        'COD_INF_ITEM': self._formatar_str(arq[2])
                    }

                elif line[:6] == '|C178|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C178'] = {
                        'CL_ENQ': self._formatar_str(arq[2]),
                        'VL_UNID': self._formatar_valor(arq[3]),
                        'QUANT_PAD': self._formatar_valor(arq[4]),
                    }

                elif line[:6] == '|C179|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C179'] = {
                        'BC_ST_ORIG_DEST': self._formatar_str(arq[2]),
                        'ICMS_ST_REP': self._formatar_valor(arq[3]),
                        'ICMS_ST_COMPL': self._formatar_valor(arq[4]),
                        'BC_RET': self._formatar_valor(arq[5]),
                        'ICMS_RET': self._formatar_valor(arq[6])
                    }

                elif line[:6] == '|C179|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C179'] = {
                        'BC_ST_ORIG_DEST': self._formatar_str(arq[2]),
                        'ICMS_ST_REP': self._formatar_valor(arq[3]),
                        'ICMS_ST_COMPL': self._formatar_valor(arq[4]),
                        'BC_RET': self._formatar_valor(arq[5]),
                        'ICMS_RET': self._formatar_valor(arq[6])
                    }

                elif line[:6] == '|C180|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C180'] = {
                        'COD_RESP_RET': self._formatar_str(arq[2]),
                        'QUANT_CONV': self._formatar_valor(arq[3]),
                        'UNID': self._formatar_str(arq[4]),
                        'VL_UNIT_CONV': self._formatar_valor(arq[5]),
                        'VL_UNIT_ICMS_OP_CONV': self._formatar_valor(arq[6]),
                        'VL_UNIT_BC_ICMS_ST_CONV': self._formatar_valor(arq[7]),
                        'VL_UNIT_ICMS_ST_CONV': self._formatar_valor(arq[8]),
                        'VL_UNIT_FCP_ST_CONV': self._formatar_valor(arq[9]),
                        'COD_DA': self._formatar_str(arq[10]),
                        'NUM_DA': self._formatar_str(arq[11])
                    }

                elif line[:6] == '|C181|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C170'][n_c170-1]['C181'][n_c181] = {
                        'COD_MOT_REST_COMPL': self._formatar_str(arq[2]),
                        'QUANT_CONV': self._formatar_valor(arq[3]),
                        'UNID': self._formatar_str(arq[4]),
                        'COD_MOD_SAIDA': self._formatar_str(arq[5]),
                        'SERIE_SAIDA': self._formatar_str(arq[6]),
                        'ECF_FAB_SAIDA': self._formatar_str(arq[7]),
                        'NUM_DOC_SAIDA': self._formatar_str(arq[8]),
                        'CHV_DFE_SAIDA': self._formatar_str(arq[9]),
                        'DT_DOC_SAIDA': self._formatar_data(arq[10]),
                        'NUM_ITEM_SAIDA': self._formatar_str(arq[11]),
                        'VL_UNIT_CONV_SAIDA': self._formatar_valor(arq[12]),
                        'VL_UNIT_ICMS_OP_ESTOQUE_CONV_SAIDA': self._formatar_valor(arq[13]),
                        'VL_UNIT_ICMS_ST_ESTOQUE_CONV_SAIDA': self._formatar_valor(arq[14]),
                        'VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV_SAIDA': self._formatar_valor(arq[15]),
                        'VL_UNIT_ICMS_NA_OPERACAO_CONV_SAIDA': self._formatar_valor(arq[16]),
                        'VL_UNIT_ICMS_OP_CONV_SAIDA': self._formatar_valor(arq[17]),
                        'VL_UNIT_ICMS_ST_CONV_REST': self._formatar_valor(arq[18]),
                        'VL_UNIT_FCP_ST_CONV_REST': self._formatar_valor(arq[19]),
                        'VL_UNIT_ICMS_ST_CONV_COMPL': self._formatar_valor(arq[20]),
                        'VL_UNIT_FCP_ST_CONV_COMPL': self._formatar_valor(arq[21])
                    }
                    n_c181 += 1

                elif line[:6] == '|C185|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C185'][n_c185] = {
                        'NUM_ITEM': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'CST_ICMS': self._formatar_str(arq[4]),
                        'CFOP': self._formatar_str(arq[5]),
                        'COD_MOT_REST_COMPL': self._formatar_str(arq[6]),
                        'QUANT_CONV': self._formatar_valor(arq[7]),
                        'UNID': self._formatar_str(arq[8]),
                        'VL_UNIT_CONV': self._formatar_valor(arq[9]),
                        'VL_UNIT_ICMS_NA_OPERACAO_CONV': self._formatar_valor(arq[10]),
                        'VL_UNIT_ICMS_OP_CONV': self._formatar_valor(arq[11]),
                        'VL_UNIT_ICMS_OP_ESTOQUE_CONV': self._formatar_valor(arq[12]),
                        'VL_UNIT_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[13]),
                        'VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[14]),
                        'VL_UNIT_ICMS_ST_CONV_REST': self._formatar_valor(arq[15]),
                        'VL_UNIT_FCP_ST_CONV_REST': self._formatar_valor(arq[16]),
                        'VL_UNIT_ICMS_ST_CONV_COMPL': self._formatar_valor(arq[17]),
                        'VL_UNIT_FCP_ST_CONV_COMPL': self._formatar_valor(arq[18])
                    }
                    n_c185 += 1

                elif line[:6] == '|C186|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C186'][n_c186] = {
                        'NUM_ITEM': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'CST_ICMS': self._formatar_str(arq[4]),
                        'CFOP': self._formatar_str(arq[5]),
                        'COD_MOT_REST_COMPL': self._formatar_str(arq[6]),
                        'QUANT_CONV': self._formatar_valor(arq[7]),
                        'UNID': self._formatar_str(arq[8]),
                        'COD_MOD_ENTRADA': self._formatar_str(arq[9]),
                        'SERIE_ENTRADA': self._formatar_str(arq[10]),
                        'NUM_DOC_ENTRADA': self._formatar_str(arq[11]),
                        'CHV_DFE_ENTRADA': self._formatar_str(arq[12]),
                        'DT_DOC_ENTRADA': self._formatar_data(arq[13]),
                        'NUM_ITEM_ENTRADA': self._formatar_str(arq[14]),
                        'VL_UNIT_CONV_ENTRADA': self._formatar_valor(arq[15]),
                        'VL_UNIT_ICMS_OP_CONV_ENTRADA': self._formatar_valor(arq[16]),
                        'VL_UNIT_BC_ICMS_ST_CONV_ENTRADA': self._formatar_valor(arq[17]),
                        'VL_UNIT_ICMS_ST_CONV_ENTRADA': self._formatar_valor(arq[18]),
                        'VL_UNIT_FCP_ST_CONV_ENTRADA': self._formatar_valor(arq[19])
                    }
                    n_c186 += 1

                elif line[:6] == '|C186|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C186'][n_c186] = {
                        'NUM_ITEM': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'CST_ICMS': self._formatar_str(arq[4]),
                        'CFOP': self._formatar_str(arq[5]),
                        'COD_MOT_REST_COMPL': self._formatar_str(arq[6]),
                        'QUANT_CONV': self._formatar_valor(arq[7]),
                        'UNID': self._formatar_str(arq[8]),
                        'COD_MOD_ENTRADA': self._formatar_str(arq[9]),
                        'SERIE_ENTRADA': self._formatar_str(arq[10]),
                        'NUM_DOC_ENTRADA': self._formatar_str(arq[11]),
                        'CHV_DFE_ENTRADA': self._formatar_str(arq[12]),
                        'DT_DOC_ENTRADA': self._formatar_data(arq[13]),
                        'NUM_ITEM_ENTRADA': self._formatar_str(arq[14]),
                        'VL_UNIT_CONV_ENTRADA': self._formatar_valor(arq[15]),
                        'VL_UNIT_ICMS_OP_CONV_ENTRADA': self._formatar_valor(arq[16]),
                        'VL_UNIT_BC_ICMS_ST_CONV_ENTRADA': self._formatar_valor(arq[17]),
                        'VL_UNIT_ICMS_ST_CONV_ENTRADA': self._formatar_valor(arq[18]),
                        'VL_UNIT_FCP_ST_CONV_ENTRADA': self._formatar_valor(arq[19])
                    }
                    n_c186 += 1

                elif line[:6] == '|C190|': # Nível 3
                    
                    n_c195 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C190'][n_c190] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[8]),
                        'VL_ICMS_ST': self._formatar_valor(arq[9]),
                        'VL_RED_BC': self._formatar_valor(arq[10]),
                        'VL_IPI': self._formatar_valor(arq[11]),
                        'COD_OBS': self._formatar_str(arq[12]),
                        'C191': {}
                    }
                    n_c190 += 1

                elif line[:6] == '|C191|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C190'][n_c190-1]['C191'] = {
                        'VL_FCP_OP': self._formatar_str(arq[2]),
                        'VL_FCP_ST': self._formatar_str(arq[3]),
                        'VL_FCP_RET': self._formatar_valor(arq[4])
                    }

                elif line[:6] == '|C195|': # Nível 3
                    
                    n_c197 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C195'][n_c195] = {
                        'COD_OBS': self._formatar_str(arq[2]),
                        'TXT_COMPL': self._formatar_str(arq[3]),
                        'C197': {}
                    }
                    n_c195 += 1

                elif line[:6] == '|C197|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['C001']['C100'][n_c100-1]['C195'][n_c195-1]['C197'][n_c197] = {
                        'COD_AJ': self._formatar_str(arq[2]),
                        'DESCR_COMPL_AJ': self._formatar_str(arq[3]),
                        'COD_ITEM': self._formatar_str(arq[4]),
                        'VL_BC_ICMS': self._formatar_valor(arq[5]),
                        'ALIQ_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_OUTROS': self._formatar_valor(arq[8])
                    }
                    n_c197 += 1

                elif line[:6] == '|C300|': # Nível 2
                    n_c310 = 0
                    n_c320 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C300'][n_c300] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'SER': self._formatar_str(arq[3]),
                        'SUB': self._formatar_str(arq[4]),
                        'NUM_DOC_INI': self._formatar_str(arq[5]),
                        'NUM_DOC_FIN': self._formatar_str(arq[6]),
                        'DT_DOC': self._formatar_data(arq[7]),
                        'VL_DOC': self._formatar_valor(arq[8]),
                        'VL_PIS': self._formatar_valor(arq[9]),
                        'VL_COFINS': self._formatar_valor(arq[10]),
                        'COD_CTA': self._formatar_str(arq[11]),
                        'C310': {},
                        'C320': {}
                    }
                    n_c300 += 1

                elif line[:6] == '|C310|': # Nível 3
                    arq = line.split('|')
                    self.efd['0000']['C001']['C300'][n_c300-1]['C310'][n_c310] = {
                        'NUM_DOC_CANC': self._formatar_str(arq[2])
                    }
                    n_c310 += 1

                elif line[:6] == '|C320|': # Nível 3
                    n_c321 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C300'][n_c300-1]['C320'][n_c320] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_RED_BC': self._formatar_valor(arq[8]),
                        'COD_OBS': self._formatar_str(arq[9]),
                        'C321': {}
                    }
                    n_c320 += 1

                elif line[:6] == '|C321|': # Nível 4
                    arq = line.split('|')
                    self.efd['0000']['C001']['C300'][n_c300-1]['C320'][n_c320-1]['C321'][n_c321] = {
                        'COD_ITEM': self._formatar_str(arq[2]),
                        'QTD': self._formatar_valor(arq[3]),
                        'UNID': self._formatar_str(arq[4]),
                        'VL_ITEM': self._formatar_valor(arq[5]),
                        'VL_DESC': self._formatar_valor(arq[6]),
                        'VL_BC_ICMS': self._formatar_valor(arq[7]),
                        'VL_ICMS': self._formatar_valor(arq[8]),
                        'VL_PIS': self._formatar_valor(arq[9]),
                        'VL_COFINS': self._formatar_valor(arq[10])
                    }
                    n_c321 += 1

                elif line[:6] == '|C330|': # Nível 4
                    arq = line.split('|')
                    self.efd['0000']['C001']['C300'][n_c300-1]['C320'][n_c320-1]['C321'][n_c321-1]['C330'] = {
                    'COD_MOT_REST_COMPL': self._formatar_str(arq[2]),
                    'QUANT_CONV': self._formatar_valor(arq[3]),
                    'UNID': self._formatar_str(arq[4]),
                    'VL_UNIT_CONV': self._formatar_valor(arq[5]),
                    'VL_UNIT_ICMS_NA_OPERACAO_CONV': self._formatar_valor(arq[6]),
                    'VL_UNIT_ICMS_OP_CONV': self._formatar_valor(arq[7]),
                    'VL_UNIT_ICMS_OP_ESTOQUE_CONV': self._formatar_valor(arq[8]),
                    'VL_UNIT_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[9]),
                    'VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[10]),
                    'VL_UNIT_ICMS_ST_CONV_REST': self._formatar_valor(arq[11]),
                    'VL_UNIT_FCP_ST_CONV_REST': self._formatar_valor(arq[12]),
                    'VL_UNIT_ICMS_ST_CONV_COMPL': self._formatar_valor(arq[13]),
                    'VL_UNIT_FCP_ST_CONV_COMPL': self._formatar_valor(arq[14])
                }

                elif line[:6] == '|C350|': # Nível 2
                    n_c370 = 0
                    n_c390 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C350'][n_c350] = {
                        'SER': self._formatar_str(arq[2]),
                        'SUB_SER': self._formatar_str(arq[3]),
                        'NUM_DOC': self._formatar_str(arq[4]),
                        'DT_DOC': self._formatar_data(arq[5]),
                        'CNPJ_CPF': self._formatar_str(arq[6]),
                        'VL_MERC': self._formatar_valor(arq[7]),
                        'VL_DOC': self._formatar_valor(arq[8]),
                        'VL_DESC': self._formatar_valor(arq[9]),
                        'VL_PIS': self._formatar_valor(arq[10]),
                        'VL_COFINS': self._formatar_valor(arq[11]),
                        'COD_CTA': self._formatar_str(arq[12]),
                        'C370': {},
                        'C390': {}
                    }

                    n_c350 += 1

                elif line[:6] == '|C370|': # Nível 3
                    arq = line.split('|')
                    self.efd['0000']['C001']['C350'][n_c350-1]['C370'][n_c370] = {
                        'NUM_ITEM': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'QTD': self._formatar_valor(arq[4]),
                        'UNID': self._formatar_str(arq[5]),
                        'VL_ITEM': self._formatar_valor(arq[6]),
                        'VL_DESC': self._formatar_valor(arq[7]),
                        'C380': {}
                    }
                    n_c370 += 1

                elif line[:6] == '|C380|': # Nível 4
                    arq = line.split('|')
                    self.efd['0000']['C001']['C350'][n_c350-1]['C370'][n_c370-1]['C380'] = {
                        'COD_MOT_REST_COMPL': self._formatar_str(arq[2]),
                        'QUANT_CONV': self._formatar_valor(arq[3]),
                        'UNID': self._formatar_str(arq[4]),
                        'VL_UNIT_CONV': self._formatar_valor(arq[5]),
                        'VL_UNIT_ICMS_NA_OPERACAO_CONV': self._formatar_valor(arq[6]),
                        'VL_UNIT_ICMS_OP_CONV': self._formatar_valor(arq[7]),
                        'VL_UNIT_ICMS_OP_ESTOQUE_CONV': self._formatar_valor(arq[8]),
                        'VL_UNIT_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[9]),
                        'VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[10]),
                        'VL_UNIT_ICMS_ST_CONV_REST': self._formatar_valor(arq[11]),
                        'VL_UNIT_FCP_ST_CONV_REST': self._formatar_valor(arq[12]),
                        'VL_UNIT_ICMS_ST_CONV_COMPL': self._formatar_valor(arq[13]),
                        'VL_UNIT_FCP_ST_CONV_COMPL': self._formatar_valor(arq[14]),
                        'CST_ICMS': self._formatar_str(arq[15]),
                        'CFOP': self._formatar_str(arq[16])
                    }

                    n_c370 += 1

                elif line[:6] == '|C390|': # Nível 3
                    arq = line.split('|')
                    self.efd['0000']['C001']['C350'][n_c350-1]['C390'][n_c390] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_RED_BC': self._formatar_valor(arq[8]),
                        'COD_OBS': self._formatar_str(arq[9])
                    }

                    n_c390 += 1

                elif line[:6] == '|C400|': # Nível 2
                    n_c405 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C400'][n_c400] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'ECF_MOD': self._formatar_str(arq[3]),
                        'ECF_FAB': self._formatar_str(arq[4]),
                        'ECF_CX': self._formatar_data(arq[5]),
                        'C405': {}
                    }
                    n_c400 += 1

                elif line[:6] == '|C405|': # Nível 3
                    n_c420 = 0
                    n_c460 = 0
                    n_c490 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C400'][n_c400-1]['C405'][n_c405] = {
                        'DT_DOC': self._formatar_data(arq[2]),
                        'CRO': self._formatar_str(arq[3]),
                        'CRZ': self._formatar_str(arq[4]),
                        'NUM_COO_FIN': self._formatar_str(arq[5]),
                        'GT_FIN': self._formatar_valor(arq[6]),
                        'VL_BRT': self._formatar_valor(arq[7]),
                        'C410': {},
                        'C420': {},
                        'C460': {},
                        'C490': {}
                    }

                    n_c405 += 1

                elif line[:6] == '|C410|': # Nível 4
                    arq = line.split('|')
                    self.efd['0000']['C001']['C400'][n_c400-1]['C405'][n_c405-1]['C410'] = {
                        'VL_PIS': self._formatar_valor(arq[2]),
                        'VL_valor': self._formatar_str(arq[3])
                    }

                elif line[:6] == '|C420|': # Nível 4
                    n_c425 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C400'][n_c400-1]['C405'][n_c405-1]['C420'][n_c420] = {
                        'COD_TOT_PAR': self._formatar_str(arq[2]),
                        'VLR_ACUM_TOT': self._formatar_valor(arq[3]),
                        'NR_TOT': self._formatar_str(arq[4]),
                        'DESCR_NR_TOT': self._formatar_str(arq[5]),
                        'C425': {}
                    }

                    n_c420 += 1

                elif line[:6] == '|C425|': # Nível 5
                    n_c430=0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C400'][n_c400-1]['C405'][n_c405-1]['C420'][n_c420-1]['C425'][n_c425] = {
                        'COD_ITEM': self._formatar_str(arq[2]),
                        'QTD': self._formatar_valor(arq[3]),
                        'UNID': self._formatar_str(arq[4]),
                        'VL_ITEM': self._formatar_valor(arq[5]),
                        'VL_PIS': self._formatar_valor(arq[6]),
                        'VL_COFINS': self._formatar_valor(arq[7])
                    }
                    n_c425 += 1

                elif line[:6] == '|C430|': # Nível 6
                    arq = line.split('|')
                    self.efd['0000']['C001']['C400'][n_c400-1]['C405'][n_c405-1]['C420'][n_c420-1]['C425'][n_c425-1]['C430'][n_c430] = {
                        'COD_MOT_REST_COMPL': self._formatar_str(arq[2]),
                        'QUANT_CONV': self._formatar_valor(arq[3]),
                        'UNID': self._formatar_str(arq[4]),
                        'VL_UNIT_CONV': self._formatar_valor(arq[5]),
                        'VL_UNIT_ICMS_NA_OPERACAO_CONV': self._formatar_valor(arq[6]),
                        'VL_UNIT_ICMS_OP_CONV': self._formatar_valor(arq[7]),
                        'VL_UNIT_ICMS_OP_ESTOQUE_CONV': self._formatar_valor(arq[8]),
                        'VL_UNIT_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[9]),
                        'VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[10]),
                        'VL_UNIT_ICMS_ST_CONV_REST': self._formatar_valor(arq[11]),
                        'VL_UNIT_FCP_ST_CONV_REST': self._formatar_valor(arq[12]),
                        'VL_UNIT_ICMS_ST_CONV_COMPL': self._formatar_valor(arq[13]),
                        'VL_UNIT_FCP_ST_CONV_COMPL': self._formatar_valor(arq[14]),
                        'CST_ICMS': self._formatar_str(arq[15]),
                        'CFOP': self._formatar_str(arq[16])
                    }

                    n_c430 += 1

                elif line[:6] == '|C460|': # Nível 4
                    n_c470 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C400'][n_c400-1]['C405'][n_c405-1]['C460'][n_c460] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'COD_SIT': self._formatar_str(arq[3]),
                        'NUM_DOC': self._formatar_str(arq[4]),
                        'DT_DOC': self._formatar_data(arq[5]),
                        'VL_DOC': self._formatar_valor(arq[6]),
                        'VL_PIS': self._formatar_valor(arq[7]),
                        'VL_COFINS': self._formatar_valor(arq[8]),
                        'CPF_CNPJ': self._formatar_str(arq[9]),
                        'NOM_ADQ': self._formatar_str(arq[10]),
                        'C465': {},
                        'C470': {}
                    }

                    n_c460 += 1

                elif line[:6] == '|C465|': # Nível 5
                    arq = line.split('|')
                    self.efd['0000']['C001']['C400'][n_c400-1]['C405'][n_c405-1]['C460'][n_c460-1]['C465'] = {
                        'CHV_CFE': self._formatar_str(arq[2]),
                        'NUM_CCF': self._formatar_str(arq[3])
                    }

                elif line[:6] == '|C470|': # Nível 5
                    arq = line.split('|')
                    self.efd['0000']['C001']['C400'][n_c400-1]['C405'][n_c405-1]['C460'][n_c460-1]['C470'][n_c470] = {
                        'COD_ITEM': self._formatar_str(arq[2]),
                        'QTD': self._formatar_valor(arq[3]),
                        'QTD_CANC': self._formatar_valor(arq[4]),
                        'UNID': self._formatar_str(arq[5]),
                        'VL_ITEM': self._formatar_valor(arq[6]),
                        'CST_ICMS': self._formatar_str(arq[7]),
                        'CFOP': self._formatar_str(arq[8]),
                        'ALIQ_ICMS': self._formatar_valor(arq[9]),
                        'VL_PIS': self._formatar_valor(arq[10]),
                        'VL_COFINS': self._formatar_valor(arq[11]),
                        'C480': {}
                    }

                    n_c470 += 1 

                elif line[:6] == '|C480|': # Nível 6
                    arq = line.split('|')
                    self.efd['0000']['C001']['C400'][n_c400-1]['C405'][n_c405-1]['C460'][n_c460-1]['C470'][n_c470-1]['C480'] = {
                        'COD_MOT_REST_COMPL': self._formatar_str(arq[2]),
                        'QUANT_CONV': self._formatar_valor(arq[3]),
                        'UNID': self._formatar_str(arq[4]),
                        'VL_UNIT_CONV': self._formatar_valor(arq[5]),
                        'VL_UNIT_ICMS_NA_OPERACAO_CONV': self._formatar_valor(arq[6]),
                        'VL_UNIT_ICMS_OP_CONV': self._formatar_valor(arq[7]),
                        'VL_UNIT_ICMS_OP_ESTOQUE_CONV': self._formatar_valor(arq[8]),
                        'VL_UNIT_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[9]),
                        'VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[10]),
                        'VL_UNIT_ICMS_ST_CONV_REST': self._formatar_valor(arq[11]),
                        'VL_UNIT_FCP_ST_CONV_REST': self._formatar_valor(arq[12]),
                        'VL_UNIT_ICMS_ST_CONV_COMPL': self._formatar_valor(arq[13]),
                        'VL_UNIT_FCP_ST_CONV_COMPL': self._formatar_valor(arq[14]),
                        'CST_ICMS': self._formatar_str(arq[15]),
                        'CFOP': self._formatar_str(arq[16])
                    }

                elif line[:6] == '|C490|': # Nível 4
                    n_c470 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C400'][n_c400-1]['C405'][n_c405-1]['C490'][n_c490] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'COD_OBS': self._formatar_str(arq[8])
                    }
                    n_c490 += 1
                    
                elif line[:6] == '|C495|': # Nível 2
                    n_c405 = 0
                    arq = line.split('|')
                    self.efd['0000']['C001']['C495'][n_c495] = {
                        'ALIQ_ICMS': self._formatar_valor(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'QTD': self._formatar_valor(arq[4]),
                        'QTD_CANC': self._formatar_valor(arq[5]),
                        'UNID': self._formatar_str(arq[6]),
                        'VL_ITEM': self._formatar_valor(arq[7]),
                        'VL_DESC': self._formatar_valor(arq[8]),
                        'VL_CANC': self._formatar_valor(arq[9]),
                        'VL_ACMO': self._formatar_valor(arq[10]),
                        'VL_BC_ICMS': self._formatar_valor(arq[11]),
                        'VL_ICMS': self._formatar_valor(arq[12]),
                        'VL_ISEN': self._formatar_valor(arq[13]),
                        'VL_NT': self._formatar_valor(arq[14]),
                        'VL_ICMS_ST': self._formatar_valor(arq[15])
                    }
                    n_c495 += 1

                elif line[:6] == '|C500|': # Nível 2

                    n_c510 = 0
                    n_c590 = 0
                    n_c595 = 0
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C500'][n_c500] = {
                        'IND_OPER': self._formatar_str(arq[2]),
                        'IND_EMIT': self._formatar_str(arq[3]),
                        'COD_PART': self._formatar_str(arq[4], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[5]),
                        'COD_SIT': self._formatar_str(arq[6]),
                        'SER': self._formatar_str(arq[7]),
                        'SUB': self._formatar_str(arq[8]),
                        'COD_CONS': self._formatar_str(arq[9]),
                        'NUM_DOC': self._formatar_str(arq[10]),
                        'DT_DOC': self._formatar_data(arq[11]),
                        'DT_E_S': self._formatar_data(arq[12]),
                        'VL_DOC': self._formatar_valor(arq[13]),
                        'VL_DESC': self._formatar_valor(arq[14]),
                        'VL_FORN': self._formatar_valor(arq[15]),
                        'VL_SERV_NT': self._formatar_valor(arq[16]),
                        'VL_TERC': self._formatar_valor(arq[17]),
                        'VL_DA': self._formatar_valor(arq[18]),
                        'VL_BC_ICMS': self._formatar_valor(arq[19]),
                        'VL_ICMS': self._formatar_valor(arq[20]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[21]),
                        'VL_ICMS_ST': self._formatar_valor(arq[22]),
                        'COD_INF': self._formatar_str(arq[23]),
                        'VL_PIS': self._formatar_valor(arq[24]),
                        'VL_COFINS': self._formatar_valor(arq[25]),
                        'TP_LIGACAO': self._formatar_str(arq[26]),
                        'COD_GRUPO_TENSAO': self._formatar_str(arq[27]),
                        
                        
                        'C510': {},
                        'C590': {},
                        'C595': {}}
                    
                    if self.efd['0000']['DT_INI'] >= dt(2020, 1, 1):
                        
                        dados_adicionados = {
                                            'CHV_DOCe': self._formatar_str(arq[28]),
                                            'FIN_DOCe': self._formatar_str(arq[29]),
                                            'CHV_DOCe_REF': self._formatar_str(arq[30]),
                                            'IND_DEST': self._formatar_str(arq[31]),
                                            'COD_MUN_DEST': self._formatar_str(arq[32]),
                                            'COD_CTA': self._formatar_str(arq[33])
                                            }
                        self.efd['0000']['C001']['C500'][n_c500] = self.efd['0000']['C001']['C500'][n_c500] | dados_adicionados
                        

                    if self.efd['0000']['DT_INI'] >= dt(2022, 1, 1): # Adicionado a partir de 2022
                        dados_adicionados = {
                                            'COD_MOD_DOC_REF': self._formatar_str(arq[34]),
                                            'HASH_DOC_REF': self._formatar_str(arq[35]),
                                            'SER_DOC_REF': self._formatar_str(arq[36]),
                                            'NUM_DOC_REF': self._formatar_str(arq[37]),
                                            'MES_DOC_REF': self._formatar_str(arq[38]),
                                            'ENER_INJET': self._formatar_valor(arq[39]),
                                            'OUTRAS_DED': self._formatar_valor(arq[40])
                                            }
                        
                        self.efd['0000']['C001']['C500'][n_c500] = self.efd['0000']['C001']['C500'][n_c500] | dados_adicionados


                    n_c500 += 1

                elif line[:6] == '|C510|': # Nível 3

                    
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C500'][n_c500-1]['C510'][n_c510] = {
                            'NUM_ITEM': self._formatar_str(arq[2]),
                            'COD_ITEM': self._formatar_str(arq[3]),
                            'COD_CLASS': self._formatar_str(arq[4]),
                            'QTD': self._formatar_valor(arq[5]),
                            'UNID': self._formatar_str(arq[6]),
                            'VL_ITEM': self._formatar_valor(arq[7]),
                            'VL_DESC': self._formatar_valor(arq[8]),
                            'CST_ICMS': self._formatar_str(arq[9]),
                            'CFOP': self._formatar_str(arq[10]),
                            'VL_BC_ICMS': self._formatar_valor(arq[11]),
                            'ALIQ_ICMS': self._formatar_valor(arq[12]),
                            'VL_ICMS': self._formatar_valor(arq[13]),
                            'VL_BC_ICMS_ST': self._formatar_valor(arq[14]),
                            'ALIQ_ST': self._formatar_valor(arq[15]),
                            'VL_ICMS_ST': self._formatar_valor(arq[16]),
                            'IND_REC': self._formatar_str(arq[17]),
                            'COD_PART': self._formatar_str(arq[18], inteiro=False),
                            'VL_PIS': self._formatar_valor(arq[19]),
                            'VL_COFINS': self._formatar_valor(arq[20]),
                            'COD_CTA': self._formatar_str(arq[21])
                        }


                    n_c510 += 1

                elif line[:6] == '|C590|': # Nível 3

                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C500'][n_c500-1]['C590'][n_c590] = {
                            'CST_ICMS': self._formatar_str(arq[2]),
                            'CFOP': self._formatar_str(arq[3]),
                            'ALIQ_ICMS': self._formatar_valor(arq[4]),
                            'VL_OPR': self._formatar_valor(arq[5]),
                            'VL_BC_ICMS': self._formatar_valor(arq[6]),
                            'VL_ICMS': self._formatar_valor(arq[7]),
                            'VL_BC_ICMS_ST': self._formatar_valor(arq[8]),
                            'VL_ICMS_ST': self._formatar_valor(arq[9]),
                            'VL_RED_BC': self._formatar_valor(arq[10]),
                            'COD_OBS': self._formatar_str(arq[11]),
                            'C591': {}
                        }

                    n_c590 += 1

                elif line[:6] == '|C591|': # Nível 4

                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C500'][n_c500-1]['C590'][n_c590-1]['C591'] = {
                            'VL_FCP_OP': self._formatar_valor(arq[2]),
                            'VL_FCP_ST': self._formatar_valor(arq[3])
                        }
                
                elif line[:6] == '|C595|': # Nível 3
                    
                    n_c597 = 0
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C500'][n_c500-1]['C595'][n_c595] = {
                            'COD_OBS': self._formatar_str(arq[2]),
                            'TXT_COMPL': self._formatar_str(arq[3]),
                            'C597': {}
                        }

                    n_c595 += 1

                elif line[:6] == '|C597|': # Nível 4
                    
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C500'][n_c500-1]['C595'][n_c595-1]['C597'][n_c597] = {
                        'COD_AJ': self._formatar_str(arq[2]),
                        'DESCR_COMPL_AJ': self._formatar_str(arq[3]),
                        'COD_ITEM': self._formatar_str(arq[4]),
                        'VL_BC_ICMS': self._formatar_valor(arq[5]),
                        'ALIQ_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_OUTROS': self._formatar_valor(arq[8])
                    }
                    n_c597 += 1

                elif line[:6] == '|C600|': # Nível 2

                    n_c601 = 0
                    n_c610 = 0
                    n_c690 = 0

                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C600'][n_c600] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'COD_MUN': self._formatar_str(arq[3]),
                        'SER': self._formatar_str(arq[4]),
                        'SUB': self._formatar_str(arq[5]),
                        'COD_CONS': self._formatar_str(arq[6]),
                        'QTD_CONS': self._formatar_valor(arq[7]),
                        'QTD_CANC': self._formatar_valor(arq[8]),
                        'DT_DOC': self._formatar_data(arq[9]),
                        'VL_DOC': self._formatar_valor(arq[10]),
                        'VL_DESC': self._formatar_valor(arq[11]),
                        'CONS': self._formatar_valor(arq[12]),
                        'VL_FORN': self._formatar_valor(arq[13]),
                        'VL_SERV_NT': self._formatar_valor(arq[14]),
                        'VL_TERC': self._formatar_valor(arq[15]),
                        'VL_DA': self._formatar_valor(arq[16]),
                        'VL_BC_ICMS': self._formatar_valor(arq[17]),
                        'VL_ICMS': self._formatar_valor(arq[18]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[19]),
                        'VL_ICMS_ST': self._formatar_valor(arq[20]),
                        'VL_PIS': self._formatar_valor(arq[21]),
                        'C601': {},
                        'C610': {},
                        'C690': {}
                    }

                    n_c600 += 1

                elif line[:6] == '|C601|': # Nível 3

                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C600'][n_c600-1]['C601'][n_c601] = {
                        'NUM_DOC_CANC': self._formatar_str(arq[2])
                    }

                    n_c601 += 1

                elif line[:6] == '|C610|': # Nível 3

                    arq = line.split('|')
                    self.efd['0000']['C001']['C600'][n_c600-1]['C610'][n_c610] = {
                        'COD_CLASS': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'QTD': self._formatar_valor(arq[4]),
                        'UNID': self._formatar_str(arq[5]),
                        'VL_ITEM': self._formatar_valor(arq[6]),
                        'VL_DESC': self._formatar_valor(arq[7]),
                        'CST_ICMS': self._formatar_str(arq[8]),
                        'CFOP': self._formatar_str(arq[9]),
                        'ALIQ_ICMS': self._formatar_valor(arq[10]),
                        'VL_BC_ICMS': self._formatar_valor(arq[11]),
                        'VL_ICMS': self._formatar_valor(arq[12]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[13]),
                        'VL_ICMS_ST': self._formatar_valor(arq[14]),
                        'VL_PIS': self._formatar_valor(arq[15]),
                        'VL_COFINS': self._formatar_valor(arq[16]),
                        'COD_CTA': self._formatar_str(arq[17])
                    }
                    n_c610 += 1

                elif line[:6] == '|C690|': # Nível 3

                    arq = line.split('|')
                    self.efd['0000']['C001']['C600'][n_c600-1]['C690'][n_c690] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_RED_BC': self._formatar_valor(arq[8]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[9]),
                        'VL_ICMS_ST': self._formatar_valor(arq[10]),
                        'COD_OBS': self._formatar_str(arq[11])
                    }
                    n_c690 += 1

                elif line[:6] == '|C700|': # Nível 2

                    n_c790 = 0
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C700'][n_c700] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'SER': self._formatar_str(arq[3]),
                        'NRO_ORD_INI': self._formatar_str(arq[4]),
                        'NRO_ORD_FIN': self._formatar_str(arq[5]),
                        'DT_DOC_INI': self._formatar_data(arq[6]),
                        'DT_DOC_FIN': self._formatar_data(arq[7]),
                        'NOM_MEST': self._formatar_str(arq[8]),
                        'CHV_COD_DIG': self._formatar_str(arq[9]),
                        'C790': {}
                    }
                    n_c700 += 1

                elif line[:6] == '|C790|': # Nível 3
                    n_c791 = 0
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C700'][n_c700-1]['C790'][n_c790] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[8]),
                        'VL_ICMS_ST': self._formatar_valor(arq[9]),
                        'VL_RED_BC': self._formatar_valor(arq[10]),
                        'COD_OBS': self._formatar_str(arq[11]),
                        'C791': {}
                    }

                    n_c790 += 1

                elif line[:6] == '|C791|': # Nível 4

                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C700'][n_c700-1]['C790'][n_c790-1]['C791'][n_c791] = {
                        'UF': self._formatar_str(arq[2]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[3]),
                        'VL_ICMS_ST': self._formatar_valor(arq[4])
                    }
                    n_c791 += 1

                elif line[:6] == '|C800|': # Nível 2

                    n_c810 = 0
                    n_c850 = 0
                    n_c855 = 0
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C800'][n_c800] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'COD_SIT': self._formatar_str(arq[3]),
                        'NUM_CFE': self._formatar_str(arq[4]),
                        'DT_DOC': self._formatar_data(arq[5]),
                        'VL_CFE': self._formatar_valor(arq[6]),
                        'VL_PIS': self._formatar_valor(arq[7]),
                        'VL_COFINS': self._formatar_valor(arq[8]),
                        'CNPJ_CPF': self._formatar_str(arq[9]),
                        'NR_SAT': self._formatar_str(arq[10]),
                        'CHV_CFE': self._formatar_str(arq[11]),
                        'VL_DESC': self._formatar_valor(arq[12]),
                        'VL_MERC': self._formatar_valor(arq[13]),
                        'VL_OUT_DA': self._formatar_valor(arq[14]),
                        'VL_ICMS': self._formatar_valor(arq[15]),
                        'VL_PIS_ST': self._formatar_valor(arq[16]),
                        'VL_COFINS_ST': self._formatar_valor(arq[17]),
                        'C810': {},
                        'C850': {},
                        'C855': {}
                    }

                    n_c800 += 1

                elif line[:6] == '|C810|': # Nível 3

                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C800'][n_c800-1]['C810'][n_c810] = {
                        'NUM_ITEM': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'QTD': self._formatar_valor(arq[4]),
                        'UNID': self._formatar_str(arq[5]),
                        'VL_ITEM': self._formatar_valor(arq[6]),
                        'CST_ICMS': self._formatar_str(arq[7]),
                        'CFOP': self._formatar_str(arq[8]),
                        'C815': {}
                    }

                    n_c810 += 1

                elif line[:6] == '|C815|': # Nível 4

                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C800'][n_c800-1]['C810'][n_c810-1]['C815'] = {
                        'COD_MOT_REST_COMPL': self._formatar_str(arq[2]),
                        'QUANT_CONV': self._formatar_valor(arq[3]),
                        'UNID': self._formatar_str(arq[4]),
                        'VL_UNIT_CONV': self._formatar_valor(arq[5]),
                        'VL_UNIT_ICMS_NA_OPERACAO_CONV': self._formatar_valor(arq[6]),
                        'VL_UNIT_ICMS_OP_CONV': self._formatar_valor(arq[7]),
                        'VL_UNIT_ICMS_OP_ESTOQUE_CONV': self._formatar_valor(arq[8]),
                        'VL_UNIT_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[9]),
                        'VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[10]),
                        'VL_UNIT_ICMS_ST_CONV_REST': self._formatar_valor(arq[11]),
                        'VL_UNIT_FCP_ST_CONV_REST': self._formatar_valor(arq[12]),
                        'VL_UNIT_ICMS_ST_CONV_COMPL': self._formatar_valor(arq[13]),
                        'VL_UNIT_FCP_ST_CONV_COMPL': self._formatar_valor(arq[14])
                    }

                elif line[:6] == '|C850|': # Nível 3

                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C800'][n_c800-1]['C850'][n_c850] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'COD_OBS': self._formatar_str(arq[8])
                    }

                    n_c850 += 1

                elif line[:6] == '|C855|': # Nível 3
                    
                    n_c857 = 0
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C800'][n_c800-1]['C855'][n_c855] = {
                        'COD_OBS': self._formatar_str(arq[2]),
                        'TXT_COMPL': self._formatar_str(arq[3]),
                        'C857': {}
                    }

                    n_c855 += 1

                elif line[:6] == '|C857|': # Nível 4

                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C800'][n_c800-1]['C855'][n_c855-1]['C857'][n_c857] = {
                        'COD_AJ': self._formatar_str(arq[2]),
                        'DESCR_COMPL_AJ': self._formatar_str(arq[3]),
                        'COD_ITEM': self._formatar_str(arq[4]),
                        'VL_BC_ICMS': self._formatar_valor(arq[5]),
                        'ALIQ_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_OUTROS': self._formatar_valor(arq[8])
                    }


                    n_c857 += 1

                elif line[:6] == '|C860|': # Nível 2

                    n_c870 = 0
                    n_c890 = 0
                    n_c895 = 0
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C860'][n_c860] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'NR_SAT': self._formatar_str(arq[3]),
                        'DT_DOC': self._formatar_data(arq[4]),
                        'DOC_INI': self._formatar_str(arq[5]),
                        'DOC_FIM': self._formatar_str(arq[6]),
                        'C870': {},
                        'C890': {},
                        'C895': {},
                    }

                    n_c860 += 1

                elif line[:6] == '|C870|': # Nível 3

                    
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C860'][n_c860-1]['C870'][n_c870] = {
                        'COD_ITEM': self._formatar_str(arq[2]),
                        'QTD': self._formatar_valor(arq[3]),
                        'UNID': self._formatar_str(arq[4]),
                        'CST_ICMS': self._formatar_str(arq[5]),
                        'CFOP': self._formatar_str(arq[6]),
                        'C880': {}
                    }


                    n_c870 += 1

                elif line[:6] == '|C880|': # Nível 4

                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C860'][n_c860-1]['C870'][n_c870-1]['C880'] = {
                        'COD_MOT_REST_COMPL': self._formatar_str(arq[2]),
                        'QUANT_CONV': self._formatar_valor(arq[3]),
                        'UNID': self._formatar_str(arq[4]),
                        'VL_UNIT_CONV': self._formatar_valor(arq[5]),
                        'VL_UNIT_ICMS_NA_OPERACAO_CONV': self._formatar_valor(arq[6]),
                        'VL_UNIT_ICMS_OP_CONV': self._formatar_valor(arq[7]),
                        'VL_UNIT_ICMS_OP_ESTOQUE_CONV': self._formatar_valor(arq[8]),
                        'VL_UNIT_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[9]),
                        'VL_UNIT_FCP_ICMS_ST_ESTOQUE_CONV': self._formatar_valor(arq[10]),
                        'VL_UNIT_ICMS_ST_CONV_REST': self._formatar_valor(arq[11]),
                        'VL_UNIT_FCP_ST_CONV_REST': self._formatar_valor(arq[12]),
                        'VL_UNIT_ICMS_ST_CONV_COMPL': self._formatar_valor(arq[13]),
                        'VL_UNIT_FCP_ST_CONV_COMPL': self._formatar_valor(arq[14])
                    }

                elif line[:6] == '|C890|': # Nível 3

                    n_c895 = 0
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C860'][n_c860-1]['C890'][n_c890] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_valor(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_str(arq[5]),
                        'VL_BC_ICMS': self._formatar_str(arq[6]),
                        'VL_ICMS': self._formatar_str(arq[7]),
                        'COD_OBS': self._formatar_str(arq[8]),
                        'C895': {}
                    }

                    n_c890 += 1

                elif line[:6] == '|C895|': # Nível 3
                    
                    n_c897 = 0
                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C860'][n_c860-1]['C895'][n_c890-1]['C895'][n_c895] = {
                        'COD_OBS': self._formatar_str(arq[2]),
                        'TXT_COMPL': self._formatar_valor(arq[3]),
                        'C897': {}
                    }
                    n_c895 += 1       

                elif line[:6] == '|C897|': # Nível 4

                    arq = line.split('|')
                    
                    self.efd['0000']['C001']['C860'][n_c860-1]['C890'][n_c890-1]['C895'][n_c895-1]['C897'][n_c897] = {
                        'COD_AJ': self._formatar_str(arq[2]),
                        'DESCR_COMPL_AJ': self._formatar_str(arq[3]),
                        'COD_ITEM': self._formatar_str(arq[4]),
                        'VL_BC_ICMS': self._formatar_valor(arq[5]),
                        'ALIQ_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_OUTROS': self._formatar_valor(arq[8]),
                    }
                    
                    n_c897 += 1

                elif line[:6] == '|C990|': # Nível 1
                    
                    arq = line.split('|')
                    self.efd['0000']['C990'] = {
                        'QTD_LIN_C': self._formatar_valor(arq[2])
                    }
                                   
                else:
                    None
                    raise ValueError(f"Registro em bloco {line[1]} inconsistente: {line[:6]}")

            elif line[:2] == '|D': # BLOCO D
                if line[:6] == '|D001|': # Nível 1
                    n_d100 = 0
                    n_d300 = 0
                    n_d350 = 0
                    n_d400 = 0
                    n_d500 = 0
                    n_d600 = 0
                    n_d695 = 0
                    n_d700 = 0
                    n_d750 = 0

                    arq = line.split('|')
                    self.efd['0000']['D001'] = {
                        'IND_MOV': self._formatar_str(arq[2]),
                        'D100': {},
                        'D300': {},
                        'D350': {},
                        'D400': {},
                        'D500': {},
                        'D600': {},
                        'D695': {},
                        'D700': {},
                        'D750': {}
                    }
                    
                
                elif line[:6] == '|D100|': # Nível 2
                    
                    n_d110 = 0
                    n_d130 = 0
                    n_d140 = 0
                    n_d150 = 0
                    n_d160 = 0
                    n_d170 = 0
                    n_d180 = 0
                    n_d190 = 0
                    n_d195 = 0

                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100] = {
                        'IND_OPER': self._formatar_str(arq[2]),
                        'IND_EMIT': self._formatar_str(arq[3]),
                        'COD_PART': self._formatar_str(arq[4], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[5]),
                        'COD_SIT': self._formatar_str(arq[6]),
                        'SER': self._formatar_str(arq[7]),
                        'SUB': self._formatar_str(arq[8]),
                        'NUM_DOC': self._formatar_str(arq[9]),
                        'CHV_CTE': self._formatar_str(arq[10]),
                        'DT_DOC': self._formatar_data(arq[11]),
                        'DT_A_P': self._formatar_data(arq[12]),
                        'TP_CTE': self._formatar_str(arq[13]),
                        'CHV_CTE_REF': self._formatar_str(arq[14]),
                        'VL_DOC': self._formatar_valor(arq[15]),
                        'VL_DESC': self._formatar_valor(arq[16]),
                        'IND_FRT': self._formatar_str(arq[17]),
                        'VL_SERV': self._formatar_valor(arq[18]),
                        'VL_BC_ICMS': self._formatar_valor(arq[19]),
                        'VL_ICMS': self._formatar_valor(arq[20]),
                        'VL_NT': self._formatar_valor(arq[21]),
                        'COD_INF': self._formatar_str(arq[22]),
                        'COD_CTA': self._formatar_str(arq[23]),
                        'COD_MUN_ORIG': self._formatar_str(arq[24]),
                        'COD_MUN_DEST': self._formatar_str(arq[25]),
                        'D101': {},
                        'D110': {}, 
                        'D130': {},
                        'D140': {}, 
                        'D150': {},
                        'D160': {}, 
                        'D170': {},
                        'D180': {}, 
                        'D190': {},
                        'D195': {},
                    }
                    n_d100 += 1

                elif line[:6] == '|D101|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D101'] = {
                        'VL_FCP_UF_DEST': self._formatar_valor(arq[2]),
                        'VL_ICMS_UF_DES': self._formatar_valor(arq[3]),
                        'VL_ICMS_UF_REM': self._formatar_valor(arq[4])
                    }

                elif line[:6] == '|D110|': # Nível 3
                    
                    n_d120 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D110'][n_d110] = {
                        'NUM_ITEM': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'VL_SERV': self._formatar_valor(arq[4]),
                        'VL_OUT': self._formatar_valor(arq[5]),
                        'D120': {}
                    }
                    n_d110 += 1

                elif line[:6] == '|D120|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D101']['D110'][n_d110-1]['D120'][n_d120] = {
                        'COD_MUN_ORIG': self._formatar_str(arq[2]),
                        'COD_MUN_DEST': self._formatar_str(arq[3]),
                        'VEIC_ID': self._formatar_str(arq[4]),
                        'UF_ID': self._formatar_str(arq[5]),
                    }
                    
                elif line[:6] == '|D130|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D130'][n_d130] = {
                        'COD_PART_CONSG': self._formatar_str(arq[2], inteiro=False),
                        'COD_PART_RED': self._formatar_str(arq[3], inteiro=False),
                        'IND_FRT_RED': self._formatar_str(arq[4]),
                        'COD_MUN_ORIG': self._formatar_str(arq[5]),
                        'COD_MUN_DEST': self._formatar_str(arq[6]),
                        'VEIC_ID': self._formatar_str(arq[7]),
                        'VL_LIQ_FRT': self._formatar_valor(arq[8]),
                        'VL_SEC_CAT': self._formatar_valor(arq[9]),
                        'VL_DESP': self._formatar_valor(arq[10]),
                        'VL_PEDG': self._formatar_valor(arq[11]),
                        'VL_OUT': self._formatar_valor(arq[12]),
                        'VL_FRT': self._formatar_valor(arq[13]),
                        'UF_ID': self._formatar_str(arq[14])
                    }

                    n_d130 += 1

                elif line[:6] == '|D140|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D140'] = {
                        'COD_PART_CONSG': self._formatar_str(arq[2], inteiro=False),
                        'COD_MUN_ORIG': self._formatar_str(arq[3]),
                        'COD_MUN_DEST': self._formatar_str(arq[4]),
                        'IND_VEIC': self._formatar_str(arq[5]),
                        'VEIC_ID': self._formatar_str(arq[6]),
                        'IND_NAV': self._formatar_str(arq[7]),
                        'VIAGEM': self._formatar_str(arq[8]),
                        'VL_FRT_LIQ': self._formatar_valor(arq[9]),
                        'VL_DESP_PORT': self._formatar_valor(arq[10]),
                        'VL_DESP_CAR_DESC': self._formatar_valor(arq[11]),
                        'VL_OUT': self._formatar_valor(arq[12]),
                        'VL_FRT_BRT': self._formatar_valor(arq[13]),
                        'VL_FRT_MM': self._formatar_valor(arq[14])
                    }

                elif line[:6] == '|D150|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D150'] = {
                        'COD_MUN_ORIG': self._formatar_str(arq[2]),
                        'COD_MUN_DEST': self._formatar_str(arq[3]),
                        'VEIC_ID': self._formatar_str(arq[4]),
                        'VIAGEM': self._formatar_str(arq[5]),
                        'IND_TFA': self._formatar_str(arq[6]),
                        'VL_PESO_TX': self._formatar_valor(arq[7]),
                        'VL_TX_TERR': self._formatar_valor(arq[8]),
                        'VL_TX_RED': self._formatar_valor(arq[9]),
                        'VL_OUT': self._formatar_valor(arq[10]),
                        'VL_TX_ADV': self._formatar_valor(arq[11])
                    }

                elif line[:6] == '|D160|': # Nível 3
                    
                    n_d162 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D160'][n_d160] = {
                        'DESPACHO': self._formatar_str(arq[2]),
                        'CNPJ_CPF_REM': self._formatar_str(arq[3]),
                        'IE_REM': self._formatar_str(arq[4]),
                        'COD_MUN_ORI': self._formatar_str(arq[5]),
                        'CNPJ_CPF_DEST': self._formatar_str(arq[6]),
                        'IE_DEST': self._formatar_str(arq[7]),
                        'COD_MUN_DEST': self._formatar_str(arq[8]),
                        'D161': {},
                        'D162': {}
                    }


                    n_d160 += 1
            
                elif line[:6] == '|D161|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D160'][n_d160-1]['D161'] = {
                        'IND_CARGA': self._formatar_str(arq[2]),
                        'CNPJ_CPF_COL': self._formatar_str(arq[3]),
                        'IE_COL': self._formatar_str(arq[4]),
                        'COD_MUN_COL': self._formatar_str(arq[5]),
                        'CNPJ_CPF_ENTG': self._formatar_str(arq[6]),
                        'IE_ENTG': self._formatar_str(arq[7]),
                        'COD_MUN_ENTG': self._formatar_str(arq[8])
                    }

                elif line[:6] == '|D162|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D160'][n_d160-1]['D162'][n_d162] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'SER': self._formatar_str(arq[3]),
                        'NUM_DOC': self._formatar_str(arq[4]),
                        'DT_DOC': self._formatar_str(arq[5]),
                        'VL_DOC': self._formatar_valor(arq[6]),
                        'VL_MERC': self._formatar_valor(arq[7]),
                        'QTD_VOL': self._formatar_valor(arq[8]),
                        'PESO_BRT': self._formatar_valor(arq[9]),
                        'PESO_LIQ': self._formatar_valor(arq[10])
                    }


                    n_d162 += 1

                elif line[:6] == '|D170|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D170'] = {
                        'COD_PART_CONSG': self._formatar_str(arq[2], inteiro=False),
                        'COD_PART_RED': self._formatar_str(arq[3], inteiro=False),
                        'COD_MUN_ORIG': self._formatar_valor(arq[4]),
                        'COD_MUN_DEST': self._formatar_valor(arq[5]),
                        'OTM': self._formatar_str(arq[6]),
                        'IND_NAT_FRT': self._formatar_str(arq[7]),
                        'VL_LIQ_FRT': self._formatar_valor(arq[8]),
                        'VL_GRIS': self._formatar_valor(arq[9]),
                        'VL_PDG': self._formatar_valor(arq[10]),
                        'VL_OUT': self._formatar_valor(arq[11]),
                        'VL_FRT': self._formatar_valor(arq[12]),
                        'VEIC_ID': self._formatar_str(arq[13]),
                        'UF_ID': self._formatar_str(arq[14])
                    }

                elif line[:6] == '|D180|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D180'][n_d180] = {
                        'NUM_SEQ': self._formatar_str(arq[2]),
                        'IND_EMIT': self._formatar_str(arq[3]),
                        'CNPJ_CPF_EMIT': self._formatar_str(arq[4]),
                        'UF_EMIT': self._formatar_str(arq[5]),
                        'IE_EMIT': self._formatar_str(arq[6]),
                        'COD_MUN_ORIG': self._formatar_str(arq[7]),
                        'CNPJ_CPF_TOM': self._formatar_str(arq[8]),
                        'UF_TOM': self._formatar_str(arq[9]),
                        'IE_TOM': self._formatar_str(arq[10]),
                        'COD_MUN_DEST': self._formatar_str(arq[11]),
                        'COD_MOD': self._formatar_str(arq[12]),
                        'SER': self._formatar_str(arq[13]),
                        'SUB': self._formatar_str(arq[14]),
                        'NUM_DOC': self._formatar_str(arq[15]),
                        'DT_DOC': self._formatar_data(arq[16]),
                        'VL_DOC': self._formatar_valor(arq[17])
                    }
                    n_d180 += 1
            
                elif line[:6] == '|D190|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D190'][n_d190] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_RED_BC': self._formatar_valor(arq[8]),
                        'COD_OBS': self._formatar_str(arq[9])
                    }

                    n_d190 += 1

                elif line[:6] == '|D195|': # Nível 3
                    
                    n_d197 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D195'][n_d195] = {
                        'COD_OBS': self._formatar_valor(arq[2]),
                        'TXT_COMPL': self._formatar_valor(arq[3]),
                        'D197': {}
                    }

                    n_d195 += 1

                elif line[:6] == '|D197|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D100'][n_d100-1]['D195'][n_d195-1]['D197'][n_d197] = {
                        'COD_AJ': self._formatar_str(arq[2]),
                        'DESCR_COMPL_AJ': self._formatar_str(arq[3]),
                        'COD_ITEM': self._formatar_str(arq[4]),
                        'VL_BC_ICMS': self._formatar_valor(arq[5]),
                        'ALIQ_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_OUTROS': self._formatar_valor(arq[8])
                    }
                    n_d197 += 1
        
                elif line[:6] == '|D300|': # Nível 2
                    
                    n_d301 = 0
                    n_d310 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D300'][n_d300] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'SER': self._formatar_str(arq[3]),
                        'SUB': self._formatar_str(arq[4]),
                        'NUM_DOC_INI': self._formatar_str(arq[5]),
                        'NUM_DOC_FIN': self._formatar_str(arq[6]),
                        'CST_ICMS': self._formatar_str(arq[7]),
                        'CFOP': self._formatar_str(arq[8]),
                        'ALIQ_ICMS': self._formatar_valor(arq[9]),
                        'DT_DOC': self._formatar_str(arq[10]),
                        'VL_OPR': self._formatar_valor(arq[11]),
                        'VL_DESC': self._formatar_valor(arq[12]),
                        'VL_SERV': self._formatar_valor(arq[13]),
                        'VL_SEG': self._formatar_valor(arq[14]),
                        'VL_OUT_DESP': self._formatar_valor(arq[15]),
                        'VL_BC_ICMS': self._formatar_valor(arq[16]),
                        'VL_ICMS': self._formatar_valor(arq[17]),
                        'VL_RED_BC': self._formatar_valor(arq[18]),
                        'COD_OBS': self._formatar_str(arq[19]),
                        'COD_CTA': self._formatar_str(arq[20]),
                        'D301': {},
                        'D310': {}
                    }

                    n_d300 += 1

                elif line[:6] == '|D301|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D300'][n_d300-1]['D301'][n_d301] = {
                        'NUM_DOC_CANC': self._formatar_str(arq[2])
                    }

                    n_d301 += 1

                elif line[:6] == '|D310|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D300'][n_d300-1]['D310'][n_d310] = {
                        'COD_MUN_ORIG': self._formatar_str(arq[2]),
                        'VL_SERV': self._formatar_valor(arq[3]),
                        'VL_BC_ICMS': self._formatar_valor(arq[4]),
                        'VL_ICMS': self._formatar_valor(arq[5])
                    }


                    n_d310 += 1

                elif line[:6] == '|D350|': # Nível 2
                    
                    n_d355 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D350'][n_d350] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'ECF_MOD': self._formatar_str(arq[3]),
                        'ECF_FAB': self._formatar_str(arq[4]),
                        'ECF_CX': self._formatar_valor(arq[5]),
                        'D355': {}
                    }

                    n_d350 += 1

                elif line[:6] == '|D355|': # Nível 3
                    
                    n_d365 = 0
                    n_d390 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D350'][n_d350-1]['D355'][n_d355] = {
                        'DT_DOC': self._formatar_data(arq[2]),
                        'CRO': self._formatar_valor(arq[3]),
                        'CRZ': self._formatar_valor(arq[4]),
                        'NUM_COO_FIN': self._formatar_valor(arq[5]),
                        'GT_FIN': self._formatar_valor(arq[6]),
                        'VL_BRT': self._formatar_valor(arq[7]),
                        'D360': {},
                        'D365': {},
                        'D390': {}
                    }

                    n_d355 += 1

                elif line[:6] == '|D360|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D350'][n_d350-1]['D355'][n_d355-1]['D360'] = {
                        'VL_PIS': self._formatar_valor(arq[2]),
                        'VL_COFINS': self._formatar_valor(arq[3])
                    }

                elif line[:6] == '|D365|': # Nível 4
                    
                    n_d370 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D350'][n_d350-1]['D355'][n_d355-1]['D365'][n_d365] = {
                        'COD_TOT_PAR': self._formatar_str(arq[2]),
                        'VLR_ACUM_TOT': self._formatar_valor(arq[3]),
                        'NR_TOT': self._formatar_valor(arq[4]),
                        'DESCR_NR_TOT': self._formatar_str(arq[5]),
                        'D370': {}
                    }

                    n_d365 += 1 

                elif line[:6] == '|D370|': # Nível 5
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D350'][n_d350-1]['D355'][n_d355-1]['D365'][n_d365-1]['D370'][n_d370] = {
                        'COD_MUN_ORIG': self._formatar_str(arq[2]),
                        'VL_SERV': self._formatar_valor(arq[3]),
                        'QTD_BILH': self._formatar_valor(arq[4]),
                        'VL_BC_ICMS': self._formatar_valor(arq[5]),
                        'VL_ICMS': self._formatar_valor(arq[6])
                    }

                    n_d370 += 1 

                elif line[:6] == '|D390|': # Nível 4
                    
                    n_d370 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D350'][n_d350-1]['D355'][n_d355-1]['D390'][n_d390] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ISSQN': self._formatar_valor(arq[6]),
                        'ALIQ_ISSQN': self._formatar_valor(arq[7]),
                        'VL_ISSQN': self._formatar_valor(arq[8]),
                        'VL_BC_ICMS': self._formatar_valor(arq[9]),
                        'VL_ICMS': self._formatar_valor(arq[10]),
                        'COD_OBS': self._formatar_str(arq[11])
                    }

                    n_d390 += 1 

                elif line[:6] == '|D400|': # Nível 2
                    
                    n_d410 = 0
                    n_d420 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D400'][n_d400] = {
                        'COD_PART': self._formatar_str(arq[2], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[3]),
                        'COD_SIT': self._formatar_str(arq[4]),
                        'SER': self._formatar_str(arq[5]),
                        'SUB': self._formatar_str(arq[6]),
                        'NUM_DOC': self._formatar_str(arq[7]),
                        'DT_DOC': self._formatar_data(arq[8]),
                        'VL_DOC': self._formatar_valor(arq[9]),
                        'VL_DESC': self._formatar_valor(arq[10]),
                        'VL_SERV': self._formatar_valor(arq[11]),
                        'VL_BC_ICMS': self._formatar_valor(arq[12]),
                        'VL_ICMS': self._formatar_valor(arq[13]),
                        'VL_PIS': self._formatar_valor(arq[14]),
                        'VL_COFINS': self._formatar_valor(arq[15]),
                        'COD_CTA': self._formatar_str(arq[16]),
                        'D410': {},
                        'D420': {}
                    }

                    n_d400 += 1

                elif line[:6] == '|D410|': # Nível 3
                    
                    n_d411 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D400'][n_d400-1]['D410'][n_d410] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'SER': self._formatar_str(arq[3]),
                        'SUB': self._formatar_str(arq[4]),
                        'NUM_DOC_INI': self._formatar_str(arq[5]),
                        'NUM_DOC_FIN': self._formatar_str(arq[6]),
                        'DT_DOC': self._formatar_data(arq[7]),
                        'CST_ICMS': self._formatar_str(arq[8]),
                        'CFOP': self._formatar_str(arq[9]),
                        'ALIQ_ICMS': self._formatar_valor(arq[10]),
                        'VL_OPR': self._formatar_valor(arq[11]),
                        'VL_DESC': self._formatar_valor(arq[12]),
                        'VL_SERV': self._formatar_valor(arq[13]),
                        'VL_BC_ICMS': self._formatar_valor(arq[14]),
                        'VL_ICMS': self._formatar_valor(arq[15]),
                        'D410': {}
                    }
                    n_d410 += 1

                elif line[:6] == '|D411|': # Nível 4
                    
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D400'][n_d400-1]['D410'][n_d410-1]['D411'][n_d411] = {
                        'NUM_DOC_CANC': self._formatar_str(arq[2])
                    }
                    n_d411 += 1

                elif line[:6] == '|D420|': # Nível 3
                    
                    n_d411 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D400'][n_d400-1]['D420'][n_d420] = {
                        'COD_MUN_ORIG': self._formatar_str(arq[2]),
                        'VL_SERV': self._formatar_valor(arq[3]),
                        'VL_BC_ICMS': self._formatar_valor(arq[4]),
                        'VL_ICMS': self._formatar_valor(arq[5])
                    }

                    n_d420 += 1

                elif line[:6] == '|D500|': # Nível 2
                    
                    n_d510 = 0
                    n_d530 = 0
                    n_d590 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D500'][n_d500] = {
                        'IND_OPER': self._formatar_str(arq[2]),
                        'IND_EMIT': self._formatar_str(arq[3]),
                        'COD_PART': self._formatar_str(arq[4], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[5]),
                        'COD_SIT': self._formatar_str(arq[6]),
                        'SER': self._formatar_str(arq[7]),
                        'SUB': self._formatar_str(arq[8]),
                        'NUM_DOC': self._formatar_str(arq[9]),
                        'DT_DOC': self._formatar_data(arq[10]),
                        'DT_A_P': self._formatar_data(arq[11]),
                        'VL_DOC': self._formatar_valor(arq[12]),
                        'VL_DESC': self._formatar_valor(arq[13]),
                        'VL_SERV': self._formatar_valor(arq[14]),
                        'VL_SERV_NT': self._formatar_valor(arq[15]),
                        'VL_TERC': self._formatar_valor(arq[16]),
                        'VL_DA': self._formatar_valor(arq[17]),
                        'VL_BC_ICMS': self._formatar_valor(arq[18]),
                        'VL_ICMS': self._formatar_valor(arq[19]),
                        'COD_INF': self._formatar_str(arq[20]),
                        'VL_PIS': self._formatar_valor(arq[21]),
                        'VL_COFINS': self._formatar_valor(arq[22]),
                        'COD_CTA': self._formatar_str(arq[23]),
                        'TP_ASSINANTE': self._formatar_str(arq[24]),
                        'D510': {},
                        'D530': {},
                        'D590': {}
                    }

                    n_d500 += 1

                elif line[:6] == '|D510|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D500'][n_d500-1]['D510'][n_d510] = {
                        'NUM_ITEM': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'COD_CLASS': self._formatar_str(arq[4]),
                        'QTD': self._formatar_valor(arq[5]),
                        'UNID': self._formatar_str(arq[6]),
                        'VL_ITEM': self._formatar_valor(arq[7]),
                        'VL_DESC': self._formatar_valor(arq[8]),
                        'CST_ICMS': self._formatar_str(arq[9]),
                        'CFOP': self._formatar_str(arq[10]),
                        'VL_BC_ICMS': self._formatar_valor(arq[11]),
                        'ALIQ_ICMS': self._formatar_valor(arq[12]),
                        'VL_ICMS': self._formatar_valor(arq[13]),
                        'VL_BC_ICMS_UF': self._formatar_valor(arq[14]),
                        'VL_ICMS_UF': self._formatar_valor(arq[15]),
                        'IND_REC': self._formatar_str(arq[16]),
                        'COD_PART': self._formatar_str(arq[17], inteiro=False),
                        'VL_PIS': self._formatar_valor(arq[18]),
                        'VL_COFINS': self._formatar_valor(arq[19]),
                        'COD_CTA': self._formatar_str(arq[20])
                    }
                    n_d510 += 1

                elif line[:6] == '|D530|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D500'][n_d500-1]['D530'][n_d530] = {
                        'IND_SERV': self._formatar_str(arq[2]),
                        'DT_INI_SERV': self._formatar_data(arq[3]),
                        'DT_FIN_SERV': self._formatar_data(arq[4]),
                        'PER_FISCAL': self._formatar_str(arq[5]),
                        'COD_AREA': self._formatar_str(arq[6]),
                        'TERMINAL': self._formatar_str(arq[7])
                    }

                    n_d530 += 1

                elif line[:6] == '|D590|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D500'][n_d500-1]['D590'][n_d590] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_BC_ICMS_UF': self._formatar_valor(arq[8]),
                        'VL_ICMS_UF': self._formatar_valor(arq[9]),
                        'VL_RED_BC': self._formatar_valor(arq[10]),
                        'COD_OBS': self._formatar_str(arq[11])
                    }
                    n_d590 += 1

                elif line[:6] == '|D600|': # Nível 2
                    
                    n_d610 = 0
                    n_d690 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D600'][n_d600] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'COD_MUN': self._formatar_str(arq[3]),
                        'SER': self._formatar_str(arq[4]),
                        'SUB': self._formatar_str(arq[5]),
                        'COD_CONS': self._formatar_str(arq[6]),
                        'QTD_CONS': self._formatar_valor(arq[7]),
                        'DT_DOC': self._formatar_data(arq[8]),
                        'VL_DOC': self._formatar_valor(arq[9]),
                        'VL_DESC': self._formatar_valor(arq[10]),
                        'VL_SERV': self._formatar_valor(arq[11]),
                        'VL_SERV_NT': self._formatar_valor(arq[12]),
                        'VL_TERC': self._formatar_valor(arq[13]),
                        'VL_DA': self._formatar_valor(arq[14]),
                        'VL_BC_ICMS': self._formatar_valor(arq[15]),
                        'VL_ICMS': self._formatar_valor(arq[16]),
                        'VL_PIS': self._formatar_valor(arq[17]),
                        'VL_COFINS': self._formatar_valor(arq[18]),
                        'D610': {},
                        'D690': {}
                    }

                    n_d600 += 1

                elif line[:6] == '|D610|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D600'][n_d600-1]['D610'][n_d610] = {
                        'COD_CLASS': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'QTD': self._formatar_valor(arq[4]),
                        'UNID': self._formatar_str(arq[5]),
                        'VL_ITEM': self._formatar_valor(arq[6]),
                        'VL_DESC': self._formatar_valor(arq[7]),
                        'CST_ICMS': self._formatar_str(arq[8]),
                        'CFOP': self._formatar_str(arq[9]),
                        'ALIQ_ICMS': self._formatar_valor(arq[10]),
                        'VL_BC_ICMS': self._formatar_valor(arq[11]),
                        'VL_ICMS': self._formatar_valor(arq[12]),
                        'VL_BC_ICMS_UF': self._formatar_valor(arq[13]),
                        'VL_ICMS_UF': self._formatar_valor(arq[14]),
                        'VL_RED_BC': self._formatar_valor(arq[15]),
                        'VL_PIS': self._formatar_valor(arq[16]),
                        'VL_COFINS': self._formatar_valor(arq[17]),
                        'COD_CTA': self._formatar_str(arq[18])
                    }

                    n_d610 += 1

                elif line[:6] == '|D690|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D600'][n_d600-1]['D690'][n_d690] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_BC_ICMS_UF': self._formatar_valor(arq[8]),
                        'VL_ICMS_UF': self._formatar_valor(arq[9]),
                        'VL_RED_BC': self._formatar_valor(arq[10]),
                        'COD_OBS': self._formatar_str(arq[11])
                    }

                    n_d690 += 1

                elif line[:6] == '|D695|': # Nível 2
                    
                    n_d696 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D695'][n_d695] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'SER': self._formatar_str(arq[3]),
                        'NRO_ORD_INI': self._formatar_valor(arq[4]),
                        'NRO_ORD_FIN': self._formatar_valor(arq[5]),
                        'DT_DOC_INI': self._formatar_data(arq[6]),
                        'DT_DOC_FIN': self._formatar_data(arq[7]),
                        'NOM_MEST': self._formatar_str(arq[8]),
                        'CHV_COD_DIG': self._formatar_str(arq[9]),
                        'D696': {}
                    }


                    n_d695 += 1

                elif line[:6] == '|D696|': # Nível 3
                    
                    n_d697 = 0
                    arq = line.split('|')
                    self.efd['0000']['D001']['D695'][n_d695-1]['D696'][n_d696] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_BC_ICMS_UF': self._formatar_valor(arq[8]),
                        'VL_ICMS_UF': self._formatar_valor(arq[9]),
                        'VL_RED_BC': self._formatar_valor(arq[10]),
                        'COD_OBS': self._formatar_str(arq[11]),
                        'D697': {}
                    }
                    n_d696 += 1

                elif line[:6] == '|D697|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D695'][n_d695-1]['D696'][n_d696-1]['D697'][n_d697] = {
                        'UF': self._formatar_str(arq[2]),
                        'VL_BC_ICMS': self._formatar_str(arq[3]),
                        'VL_ICMS': self._formatar_valor(arq[4])
                    }
                    n_d697 += 1

                elif line[:6] == '|D700|': # Nível 2
                    
                    n_d730 = 0
                    n_d735 = 0
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D700'][n_d700] = {
                        'IND_OPER': self._formatar_str(arq[2]),
                        'IND_EMIT': self._formatar_str(arq[3]),
                        'COD_PART': self._formatar_str(arq[4], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[5]),
                        'COD_SIT': self._formatar_str(arq[6]),
                        'SER': self._formatar_str(arq[7]),
                        'NUM_DOC': self._formatar_valor(arq[8]),
                        'DT_DOC': self._formatar_str(arq[9]),
                        'DT_E_S': self._formatar_str(arq[10]),
                        'VL_DOC': self._formatar_valor(arq[11]),
                        'VL_DESC': self._formatar_valor(arq[12]),
                        'VL_SERV': self._formatar_valor(arq[13]),
                        'VL_SERV_NT': self._formatar_valor(arq[14]),
                        'VL_TERC': self._formatar_valor(arq[15]),
                        'VL_DA': self._formatar_valor(arq[16]),
                        'VL_BC_ICMS': self._formatar_valor(arq[17]),
                        'VL_ICMS': self._formatar_valor(arq[18]),
                        'COD_INF': self._formatar_str(arq[19]),
                        'VL_PIS': self._formatar_valor(arq[20]),
                        'VL_COFINS': self._formatar_valor(arq[21]),
                        'CHV_DOCe': self._formatar_str(arq[22]),
                        'FIN_DOCe': self._formatar_str(arq[23]),
                        'TIP_FAT': self._formatar_str(arq[24]),
                        'COD_MOD_DOC_REF': self._formatar_str(arq[25]),
                        'CHV_DOCe_REF': self._formatar_str(arq[26]),
                        'HASH_DOC_REF': self._formatar_str(arq[27]),
                        'SER_DOC_REF': self._formatar_str(arq[28]),
                        'NUM_DOC_REF': self._formatar_valor(arq[29]),
                        'MES_DOC_REF': self._formatar_str(arq[30]),
                        'COD_MUN_DEST': self._formatar_str(arq[31]),
                        'D730': {},
                        'D735': {}
                    }
                    n_d700 += 1

                elif line[:6] == '|D730|': # Nível 3
                    

                    n_d731 = 0
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D700'][n_d700-1]['D730'][n_d730] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_valor(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_RED_BC': self._formatar_valor(arq[8]),
                        'COD_OBS': self._formatar_str(arq[9]),
                        'D731': {}
                    }

                    n_d730 += 1

                elif line[:6] == '|D731|': # Nível 4
                                
                    arq = line.split('|')
                    self.efd['0000']['D001']['D700'][n_d700-1]['D730'][n_d730-1]['D731'][n_d731] = {
                        'VL_FCP_OP': self._formatar_valor(arq[2])
                    }

                    n_d731 += 1

                elif line[:6] == '|D735|': # Nível 3

                    n_d737 = 0   
                    arq = line.split('|')
                    self.efd['0000']['D001']['D700'][n_d700-1]['D735'][n_d735] = {
                        'COD_OBS': self._formatar_str(arq[2]),
                        'TXT_COMPL': self._formatar_str(arq[3]),
                        'D737': {}
                    }

                    n_d735 += 1

                elif line[:6] == '|D737|': # Nível 4

                    arq = line.split('|')
                    self.efd['0000']['D001']['D700'][n_d700-1]['D735'][n_d735-1]['D737'][n_d737] = {
                        'COD_AJ': self._formatar_str(arq[2]),
                        'DESCR_COMPL_AJ': self._formatar_str(arq[3]),
                        'COD_ITEM': self._formatar_str(arq[4]),
                        'VL_BC_ICMS': self._formatar_valor(arq[5]),
                        'ALIQ_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_OUTROS': self._formatar_valor(arq[8])
                    }

                    n_d737 += 1

                elif line[:6] == '|D750|': # Nível 2
                    
                    n_d760 = 0
                    
                    arq = line.split('|')
                    self.efd['0000']['D001']['D750'][n_d750] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'SER': self._formatar_str(arq[3]),
                        'DT_DOC': self._formatar_str(arq[4]),
                        'QTD_CONS': self._formatar_valor(arq[5]),
                        'IND_PREPAGO': self._formatar_str(arq[6]),
                        'VL_DOC': self._formatar_valor(arq[7]),
                        'VL_SERV': self._formatar_valor(arq[8]),
                        'VL_SERV_NT': self._formatar_valor(arq[9]),
                        'VL_TERC': self._formatar_valor(arq[10]),
                        'VL_DESC': self._formatar_valor(arq[11]),
                        'VL_DA': self._formatar_valor(arq[12]),
                        'VL_BC_ICMS': self._formatar_valor(arq[13]),
                        'VL_ICMS': self._formatar_valor(arq[14]),
                        'VL_PIS': self._formatar_valor(arq[15]),
                        'VL_COFINS': self._formatar_valor(arq[16]),
                        'D760': {}
                    }

                    n_d750 += 1

                elif line[:6] == '|D760|': # Nível 3
                                
                    arq = line.split('|')
                    self.efd['0000']['D001']['D750'][n_d750-1]['D760'][n_d760] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'CFOP': self._formatar_str(arq[3]),
                        'ALIQ_ICMS': self._formatar_str(arq[4]),
                        'VL_OPR': self._formatar_valor(arq[5]),
                        'VL_BC_ICMS': self._formatar_valor(arq[6]),
                        'VL_ICMS': self._formatar_valor(arq[7]),
                        'VL_RED_BC': self._formatar_valor(arq[8]),
                        'COD_OBS': self._formatar_str(arq[9]),
                        'D761': {}
                    }

                    n_d760 += 1

                elif line[:6] == '|D761|': # Nível 4
                                
                    arq = line.split('|')
                    self.efd['0000']['D001']['D750'][n_d750-1]['D760'][n_d760-1]['D761'] = {
                        'VL_FCP_OP': self._formatar_valor(arq[2])
                    }

                elif line[:6] == '|D990|': # Nível 1

                    arq = line.split('|')
                    self.efd['0000']['D990'] = {
                        'QTD_LIN_D': self._formatar_valor(arq[2])
                    }
                    
                
                else:
                    raise ValueError(f"Registro em bloco {line[1]} inconsistente: {line[:6]}")

            elif line[:2] == '|E': # BLOCO E

                if line[:6] == '|E001|': # Nível 1
                    n_e100 = 0
                    n_e200 = 0
                    n_e300 = 0
                    n_e500 = 0
                    

                    arq = line.split('|')
                    self.efd['0000']['E001'] = {
                        'IND_MOV': self._formatar_str(arq[2]),
                        'E100': {},
                        'E200': {},
                        'E300': {},
                        'E500': {}
                    }
                    
                elif line[:6] == '|E100|': # Nível 2
                    
                    n_e110 = 0
                    arq = line.split('|')
                    self.efd['0000']['E001']['E100'][n_e100] = {
                        'DT_INI': self._formatar_data(arq[2]),
                        'DT_FIN': self._formatar_data(arq[3]),
                        'E110': {}
                    }
                    n_e100 += 1

                elif line[:6] == '|E110|': # Nível 3
                    
                    n_e111 = 0
                    n_e115 = 0
                    n_e116 = 0
                    arq = line.split('|')
                    self.efd['0000']['E001']['E100'][n_e100-1]['E110'][n_e110] = {
                        'VL_TOT_DEBITOS': self._formatar_valor(arq[2]),
                        'VL_AJ_DEBITOS': self._formatar_valor(arq[3]),
                        'VL_TOT_AJ_DEBITOS': self._formatar_valor(arq[4]),
                        'VL_ESTORNOS_CRED': self._formatar_valor(arq[5]),
                        'VL_TOT_CREDITOS': self._formatar_valor(arq[6]),
                        'VL_AJ_CREDITOS': self._formatar_valor(arq[7]),
                        'VL_TOT_AJ_CREDITOS': self._formatar_valor(arq[8]),
                        'VL_ESTORNOS_DEB': self._formatar_valor(arq[9]),
                        'VL_SLD_CREDOR_ANT': self._formatar_valor(arq[10]),
                        'VL_SLD_APURADO': self._formatar_valor(arq[11]),
                        'VL_TOT_DED': self._formatar_valor(arq[12]),
                        'VL_ICMS_RECOLHER': self._formatar_valor(arq[13]),
                        'VL_SLD_CREDOR_TRANSPORTAR': self._formatar_valor(arq[14]),
                        'DEB_ESP': self._formatar_valor(arq[15]),
                        'E111': {},
                        'E115': {}, 
                        'E116': {}
                    }


                    n_e110 += 1

                elif line[:6] == '|E111|': # Nível 4
                    
                    n_e112 = 0
                    n_e113 = 0
                    arq = line.split('|')
                    self.efd['0000']['E001']['E100'][n_e100-1]['E110'][n_e110-1]['E111'][n_e111] = {
                        'COD_AJ_APUR': self._formatar_str(arq[2]),
                        'DESCR_COMPL_AJ': self._formatar_str(arq[3]),
                        'VL_AJ_APUR': self._formatar_valor(arq[4]),
                        'E112': {},
                        'E113': {}
                    }


                    n_e111 += 1

                elif line[:6] == '|E112|': # Nível 5
                    
                    arq = line.split('|')
                    self.efd['0000']['E001']['E100'][n_e100-1]['E110'][n_e110-1]['E111'][n_e111-1]['E112'][n_e112] = {
                        'NUM_DA': self._formatar_str(arq[2]),
                        'NUM_PROC': self._formatar_str(arq[3]),
                        'IND_PROC': self._formatar_str(arq[4]),
                        'PROC': self._formatar_str(arq[5]),
                        'TXT_COMPL': self._formatar_str(arq[6])
                    }
                    n_e112 += 1

                elif line[:6] == '|E113|': # Nível 5
                    
                    arq = line.split('|')
                    self.efd['0000']['E001']['E100'][n_e100-1]['E110'][n_e110-1]['E111'][n_e111-1]['E113'][n_e113] = {
                        'COD_PART': self._formatar_str(arq[2], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[3]),
                        'SER': self._formatar_str(arq[4]),
                        'SUB': self._formatar_str(arq[5]),
                        'NUM_DOC': self._formatar_str(arq[6]),
                        'DT_DOC': self._formatar_str(arq[7]),
                        'COD_ITEM': self._formatar_str(arq[8]),
                        'VL_AJ_ITEM': self._formatar_valor(arq[9]),
                        'CHV_DOCe': self._formatar_str(arq[10])
                    }

                    n_e113 += 1

                elif line[:6] == '|E115|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['E001']['E100'][n_e100-1]['E110'][n_e110-1]['E115'][n_e115] = {
                        'COD_INF_ADIC': self._formatar_str(arq[2]),
                        'VL_INF_ADIC': self._formatar_str(arq[3]),
                        'DESCR_COMPL_AJ': self._formatar_valor(arq[4]),
                    }

                    n_e115 += 1

                elif line[:6] == '|E116|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['E001']['E100'][n_e100-1]['E110'][n_e110-1]['E116'][n_e116] = {
                        'COD_OR': self._formatar_str(arq[2]),
                        'VL_OR': self._formatar_valor(arq[3]),
                        'DT_VCTO': self._formatar_data(arq[4]),
                        'COD_REC': self._formatar_str(arq[5]),
                        'NUM_PROC': self._formatar_str(arq[6]),
                        'IND_PROC': self._formatar_str(arq[7]),
                        'PROC': self._formatar_str(arq[8]),
                        'TXT_COMPL': self._formatar_str(arq[9]),
                        'MES_REF': self._formatar_str(arq[10])
                    }


                    n_e116 += 1
                    
                elif line[:6] == '|E200|': # Nível 2
                    
                    n_e210 = 0

                    arq = line.split('|')
                    self.efd['0000']['E001']['E200'][n_e200] = {
                        'UF': self._formatar_str(arq[2]),
                        'DT_INI': self._formatar_data(arq[3]),
                        'DT_FIN': self._formatar_data(arq[4]),
                        'E210': {}
                    }
                    n_e200 += 1

                elif line[:6] == '|E210|': # Nível 3
                    
                    n_e220 = 0
                    n_e250 = 0
                    arq = line.split('|')
                    self.efd['0000']['E001']['E200'][n_e200-1]['E210'][n_e210] = {
                        'IND_MOV_ST': self._formatar_str(arq[2]),
                        'VL_SLD_CRED_ANT_ST': self._formatar_valor(arq[3]),
                        'VL_DEVOL_ST': self._formatar_valor(arq[4]),
                        'VL_RESSARC_ST': self._formatar_valor(arq[5]),
                        'VL_OUT_CRED_ST': self._formatar_valor(arq[6]),
                        'VL_AJ_CREDITOS_ST': self._formatar_valor(arq[7]),
                        'VL_RETENÇAO_ST': self._formatar_valor(arq[8]),
                        'VL_OUT_DEB_ST': self._formatar_valor(arq[9]),
                        'VL_AJ_DEBITOS_ST': self._formatar_valor(arq[10]),
                        'VL_SLD_DEV_ANT_ST': self._formatar_valor(arq[11]),
                        'VL_DEDUÇÕES_ST': self._formatar_valor(arq[12]),
                        'VL_ICMS_RECOL_ST': self._formatar_valor(arq[13]),
                        'VL_SLD_CRED_ST_TRANSPORTAR': self._formatar_valor(arq[14]),
                        'DEB_ESP_ST': self._formatar_valor(arq[15]),
                        'E220': {},
                        'E250': {}
                    }

                    n_e210 += 1

                elif line[:6] == '|E220|': # Nível 4
                    
                    n_e230 = 0
                    n_e240 = 0
                    arq = line.split('|')
                    self.efd['0000']['E001']['E200'][n_e200-1]['E210'][n_e210-1]['E220'][n_e220] = {
                        'COD_AJ_APUR': self._formatar_str(arq[2]),
                        'DESCR_COMPL_AJ': self._formatar_str(arq[3]),
                        'VL_AJ_APUR': self._formatar_valor(arq[4]),
                        'E230': {},
                        'E240': {}
                    }

                    n_e220 += 1

                elif line[:6] == '|E230|': # Nível 5
                    
                
                    arq = line.split('|')
                    self.efd['0000']['E001']['E200'][n_e200-1]['E210'][n_e210-1]['E220'][n_e220-1]['E230'][n_e230] = {
                        'NUM_DA': self._formatar_str(arq[2]),
                        'NUM_PROC': self._formatar_str(arq[3]),
                        'IND_PROC': self._formatar_str(arq[4]),
                        'PROC': self._formatar_str(arq[5]),
                        'TXT_COMPL': self._formatar_str(arq[6])
                    }
                    n_e230 += 1

                elif line[:6] == '|E240|': # Nível 5
                    
                
                    arq = line.split('|')
                    self.efd['0000']['E001']['E200'][n_e200-1]['E210'][n_e210-1]['E220'][n_e220-1]['E240'][n_e240] = {
                        'COD_PART': self._formatar_str(arq[2], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[3]),
                        'SER': self._formatar_str(arq[4]),
                        'SUB': self._formatar_str(arq[5]),
                        'NUM_DOC': self._formatar_str(arq[6]),
                        'DT_DOC': self._formatar_str(arq[7]),
                        'COD_ITEM': self._formatar_str(arq[8]),
                        'VL_AJ_ITEM': self._formatar_valor(arq[9]),
                        'CHV_DOCe': self._formatar_str(arq[10])
                    }
                    n_e240 += 1

                elif line[:6] == '|E250|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['E001']['E200'][n_e200-1]['E210'][n_e210-1]['E250'][n_e250] = {
                        'COD_OR': self._formatar_str(arq[2]),
                        'VL_OR': self._formatar_valor(arq[3]),
                        'DT_VCTO': self._formatar_data(arq[4]),
                        'COD_REC': self._formatar_str(arq[5]),
                        'NUM_PROC': self._formatar_str(arq[6]),
                        'IND_PROC': self._formatar_str(arq[7]),
                        'PROC': self._formatar_str(arq[8]),
                        'TXT_COMPL': self._formatar_str(arq[9]),
                        'MES_REF': self._formatar_str(arq[10])
                    }

                    n_e250 += 1

                elif line[:6] == '|E300|': # Nível 2
                    
                    n_e310 = 0
                    arq = line.split('|')
                    self.efd['0000']['E001']['E300'][n_e300] = {
                        'UF': self._formatar_str(arq[2]),
                        'DT_INI': self._formatar_data(arq[3]),
                        'DT_FIN': self._formatar_data(arq[4]),
                        'E310': {}
                    }
                    n_e300 += 1

                elif line[:6] == '|E310|': # Nível 3
                    
                    n_e311 = 0
                    n_e316 = 0

                    arq = line.split('|')
                    self.efd['0000']['E001']['E300'][n_e300-1]['E310'][n_e310] = {
                        'IND_MOV_DIFAL': self._formatar_str(arq[2]),
                        'VL_SLD_CRED_ANT_DIFAL': self._formatar_valor(arq[3]),
                        'VL_TOT_DEBITOS_DIFAL': self._formatar_valor(arq[4]),
                        'VL_OUT_DEB_DIFAL': self._formatar_valor(arq[5]),
                        'VL_TOT_DEB_FCP': self._formatar_valor(arq[6]),
                        'VL_TOT_CREDITOS_DIFAL': self._formatar_valor(arq[7]),
                        'VL_TOT_CRED_FCP': self._formatar_valor(arq[8]),
                        'VL_OUT_CRED_DIFAL': self._formatar_valor(arq[9]),
                        'VL_SLD_DEV_ANT_DIFAL': self._formatar_valor(arq[10]),
                        'VL_DEDUÇÕES_DIFAL': self._formatar_valor(arq[11]),
                        'VL_RECOL_FCP': self._formatar_valor(arq[12]),
                        'VL_SLD_CRED_TRANSPORTAR_FCP': self._formatar_valor(arq[13]),
                        'DEB_ESP_FCP': self._formatar_valor(arq[14]),
                        'E311': {},
                        'E316': {}
                    }
                    n_e310 += 1

                elif line[:6] == '|E311|': # Nível 4
                    
                    n_e312 = 0
                    n_e313 = 0

                    arq = line.split('|')
                    self.efd['0000']['E001']['E300'][n_e300-1]['E310'][n_e310-1]['E311'][n_e311] = {
                        'COD_AJ_APUR': self._formatar_str(arq[2]),
                        'DESCR_COMPL_AJ': self._formatar_str(arq[3]),
                        'VL_AJ_APUR': self._formatar_valor(arq[4]),
                        'E112': {},
                        'E113': {},

                    }
                    n_e311 += 1

                elif line[:6] == '|E312|': # Nível 5
                    
                    arq = line.split('|')
                    self.efd['0000']['E001']['E300'][n_e300-1]['E310'][n_e310-1]['E311'][n_e311-1]['E312'][n_e312] = {
                        'NUM_DA': self._formatar_str(arq[2]),
                        'NUM_PROC': self._formatar_str(arq[3]),
                        'IND_PROC': self._formatar_str(arq[4]),
                        'PROC': self._formatar_str(arq[5]),
                        'TXT_COMPL': self._formatar_str(arq[6])
                    }
                    n_e312 += 1

                elif line[:6] == '|E313|': # Nível 5
                    
                    arq = line.split('|')
                    self.efd['0000']['E001']['E300'][n_e300-1]['E310'][n_e310-1]['E311'][n_e311-1]['E313'][n_e313] = {
                        'COD_PART': self._formatar_str(arq[2], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[3]),
                        'SER': self._formatar_str(arq[4]),
                        'SUB': self._formatar_str(arq[5]),
                        'NUM_DOC': self._formatar_str(arq[6]),
                        'CHV_DOCe': self._formatar_str(arq[7]),
                        'DT_DOC': self._formatar_data(arq[8]),
                        'COD_ITEM': self._formatar_str(arq[9]),
                        'VL_AJ_ITEM': self._formatar_valor(arq[10])
                    }
                    n_e313 += 1
            
                elif line[:6] == '|E316|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['E001']['E300'][n_e300-1]['E310'][n_e310-1]['E316'][n_e316] = {
                        'COD_OR': self._formatar_str(arq[2]),
                        'VL_OR': self._formatar_valor(arq[3]),
                        'DT_VCTO': self._formatar_data(arq[4]),
                        'COD_REC': self._formatar_str(arq[5]),
                        'NUM_PROC': self._formatar_str(arq[6]),
                        'IND_PROC': self._formatar_str(arq[7]),
                        'PROC': self._formatar_str(arq[8]),
                        'TXT_COMPL': self._formatar_str(arq[9]),
                        'MES_REF': self._formatar_data(arq[10], formato="%m%Y"),
                        
                    }
                    n_e316 += 1

                elif line[:6] == '|E500|': # Nível 2
                    
                    n_e510 = 0
                    n_e520 = 0
            
                    arq = line.split('|')
                    self.efd['0000']['E001']['E500'][n_e500] = {
                        'IND_APUR': self._formatar_str(arq[2]),
                        'DT_INI': self._formatar_data(arq[3]),
                        'DT_FIN': self._formatar_data(arq[4]),
                        'E510': {},
                        'E520': {}
                    }
                    n_e500 += 1

                elif line[:6] == '|E510|': # Nível 3
                                
                    arq = line.split('|')
                    self.efd['0000']['E001']['E500'][n_e500-1]['E510'][n_e510] = {
                        'CFOP': self._formatar_str(arq[2]),
                        'CST_IPI': self._formatar_str(arq[3]),
                        'VL_CONT_IPI': self._formatar_valor(arq[4]),
                        'VL_BC_IPI': self._formatar_valor(arq[5]),
                        'VL_IPI': self._formatar_valor(arq[6])
                    }

                    n_e510 += 1

                elif line[:6] == '|E520|': # Nível 3
                    
                    n_e530 = 0
                    arq = line.split('|')
                    self.efd['0000']['E001']['E500'][n_e500-1]['E520'][n_e520] = {
                        'VL_SD_ANT_IPI': self._formatar_valor(arq[2]),
                        'VL_DEB_IPI': self._formatar_valor(arq[3]),
                        'VL_CRED_IPI': self._formatar_valor(arq[4]),
                        'VL_OD_IPI': self._formatar_valor(arq[5]),
                        'VL_OC_IPI': self._formatar_valor(arq[6]),
                        'VL_SC_IPI': self._formatar_valor(arq[7]),
                        'VL_SD_IPI': self._formatar_valor(arq[8])
                    }
                    n_e520 += 1

                elif line[:6] == '|E530|': # Nível 4
                    
                    n_e531 = 0
                    arq = line.split('|')
                    self.efd['0000']['E001']['E500'][n_e500-1]['E520'][n_e520-1]['E530'][n_e530] = {
                        'IND_AJ': self._formatar_str(arq[2]),
                        'VL_AJ': self._formatar_valor(arq[3]),
                        'COD_AJ': self._formatar_str(arq[4]),
                        'IND_DOC': self._formatar_str(arq[5]),
                        'NUM_DOC': self._formatar_str(arq[6]),
                        'DESCR_AJ': self._formatar_str(arq[6]),
                        'E531': {}
                    }

                    n_e530 += 1
                
                elif line[:6] == '|E531|': # Nível 5
                    
                    arq = line.split('|')
                    self.efd['0000']['E001']['E500'][n_e500-1]['E520'][n_e520-1]['E530'][n_e530-1]['E531'][n_e531] = {
                        'COD_PART': self._formatar_str(arq[2], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[3]),
                        'SER': self._formatar_str(arq[4]),
                        'SUB': self._formatar_str(arq[5]),
                        'NUM_DOC': self._formatar_str(arq[6]),
                        'DT_DOC': self._formatar_str(arq[7]),
                        'COD_ITEM': self._formatar_str(arq[8]),
                        'VL_AJ_ITEM': self._formatar_valor(arq[9]),
                        'CHV_NFE': self._formatar_str(arq[10])
                    }

                    n_e531 += 1

                elif line[:6] == '|E990|': # Nível 1
                    
                    arq = line.split('|')
                    self.efd['0000']['E990'] = {
                        'QTD_LIN_E': self._formatar_valor(arq[2])
                    }

                else:
                    raise ValueError(f"Registro em bloco {line[1]} inconsistente: {line[:6]}")

            elif line[:2] == '|G': # BLOCO G

                if line[:6] == '|G001|': # Nível 1
                    n_g110 = 0
                    
                    arq = line.split('|')
                    self.efd['0000']['G001'] = {
                        'IND_MOV': self._formatar_str(arq[2]),
                        'G110': {}
                    }

                elif line[:6] == '|G110|': # Nível 2
                    n_g125 = 0
                    
                    arq = line.split('|')
                    self.efd['0000']['G001']['G110'][n_g110] = {
                        'DT_INI': self._formatar_data(arq[2]),
                        'DT_FIN': self._formatar_data(arq[3]),
                        'SALDO_IN_ICMS': self._formatar_valor(arq[4]),
                        'SOM_PARC': self._formatar_valor(arq[5]),
                        'VL_TRIB_EXP': self._formatar_valor(arq[6]),
                        'VL_TOTAL': self._formatar_valor(arq[7]),
                        'IND_PER_SAI': self._formatar_valor(arq[8]),
                        'ICMS_APROP': self._formatar_valor(arq[9]),
                        'SOM_ICMS_OC': self._formatar_valor(arq[10]),
                        'G125': {}
                    }


                    n_g110 += 1
              
                elif line[:6] == '|G125|': # Nível 3
                    n_g126 = 0
                    n_g130 = 0
                    arq = line.split('|')
                    self.efd['0000']['G001']['G110'][n_g110-1]['G125'][n_g125] = {
                        'COD_IND_BEM': self._formatar_str(arq[2]),
                        'DT_MOV': self._formatar_data(arq[3]),
                        'TIPO_MOV': self._formatar_str(arq[4]),
                        'VL_IMOB_ICMS_OP': self._formatar_valor(arq[5]),
                        'VL_IMOB_ICMS_ST': self._formatar_valor(arq[6]),
                        'VL_IMOB_ICMS_FRT': self._formatar_valor(arq[7]),
                        'VL_IMOB_ICMS_DIF': self._formatar_valor(arq[8]),
                        'NUM_PARC': self._formatar_str(arq[9]),
                        'VL_PARC_PASS': self._formatar_valor(arq[10]),
                        'G126': {},
                        'G130': {}
                    }
                    n_g125 += 1

                elif line[:6] == '|G126|': # Nível 4
                    arq = line.split('|')
                    self.efd['0000']['G001']['G110'][n_g110-1]['G125'][n_g125-1]['G126'][n_g126] = {
                        'DT_INI': self._formatar_data(arq[2]),
                        'DT_FIM': self._formatar_data(arq[3]),
                        'NUM_PARC': self._formatar_str(arq[4]),
                        'VL_PARC_PASS': self._formatar_valor(arq[5]),
                        'VL_TRIB_OC': self._formatar_valor(arq[6]),
                        'VL_TOTAL': self._formatar_valor(arq[7]),
                        'IND_PER_SAI': self._formatar_valor(arq[8]),
                        'VL_PARC_APROP': self._formatar_valor(arq[9])
                    }

                    n_g126 += 1

                elif line[:6] == '|G130|': # Nível 4
                    
                    n_g140 = 0
                    arq = line.split('|')
                    self.efd['0000']['G001']['G110'][n_g110-1]['G125'][n_g125-1]['G130'][n_g130] = {
                        'IND_EMIT': self._formatar_str(arq[2]),
                        'COD_PART': self._formatar_str(arq[3], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[4]),
                        'SERIE': self._formatar_str(arq[5]),
                        'NUM_DOC': self._formatar_str(arq[6]),
                        'CHV_NFE_CTE': self._formatar_str(arq[7]),
                        'DT_DOC': self._formatar_data(arq[8]),
                        'NUM_DA': self._formatar_str(arq[9]),
                        'G140': {}
                    }

                    n_g130 += 1

                elif line[:6] == '|G140|': # Nível 5
                    
                    arq = line.split('|')
                    self.efd['0000']['G001']['G110'][n_g110-1]['G125'][n_g125-1]['G130'][n_g130-1]['G140'][n_g140] = {
                        'NUM_ITEM': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'QTDE': self._formatar_valor(arq[4]),
                        'UNID': self._formatar_str(arq[5]),
                        'VL_ICMS_OP_APLICADO': self._formatar_valor(arq[6]),
                        'VL_ICMS_ST_APLICADO': self._formatar_valor(arq[7]),
                        'VL_ICMS_FRT_APLICADO': self._formatar_valor(arq[8]),
                        'VL_ICMS_DIF_APLICADO': self._formatar_valor(arq[9])
                    }


                    n_g140 += 1

                elif line[:6] == '|G990|': # Nível 1         
                    arq = line.split('|')
                    self.efd['0000']['G990'] = {
                        'QTD_LIN_G': self._formatar_valor(arq[2])
                    }

                else:
                    raise ValueError(f"Registro em bloco {line[1]} inconsistente: {line[:6]}")

            elif line[:2] == '|H': # BLOCO H

                if line[:6] == '|H001|': # Nível 1
                    n_h005 = 0
                    
                    arq = line.split('|')
                    self.efd['0000']['H001'] = {
                        'IND_MOV': self._formatar_str(arq[2]),
                        'H005': {}
                    }

                elif line[:6] == '|H005|': # Nível 2

                    n_h010 = 0                    
                    arq = line.split('|')
                    self.efd['0000']['H001']['H005'][n_h005] = {
                        'DT_INV': self._formatar_data(arq[2]),
                        'VL_INV': self._formatar_valor(arq[3]),
                        'MOT_INV': self._formatar_str(arq[4]),
                        'H010': {}
                    }

                    n_h005 += 1
               
                elif line[:6] == '|H010|': # Nível 3

                    n_h020 = 0
                                        
                    arq = line.split('|')
                    self.efd['0000']['H001']['H005'][n_h005-1]['H010'][n_h010] = {
                        'COD_ITEM': self._formatar_str(arq[2]),
                        'UNID': self._formatar_str(arq[3]),
                        'QTD': self._formatar_valor(arq[4]),
                        'VL_UNIT': self._formatar_valor(arq[5]),
                        'VL_ITEM': self._formatar_valor(arq[6]),
                        'IND_PROP': self._formatar_str(arq[7]),
                        'COD_PART': self._formatar_str(arq[8], inteiro=False),
                        'TXT_COMPL': self._formatar_str(arq[9]),
                        'COD_CTA': self._formatar_str(arq[10]),
                        'VL_ITEM_IR': self._formatar_valor(arq[11]),
                        'H020': {},
                        'H030': {}
                    }

                    n_h010 += 1

                elif line[:6] == '|H020|': # Nível 4
                 
                    arq = line.split('|')
                    self.efd['0000']['H001']['H005'][n_h005-1]['H010'][n_h010-1]['H020'][n_h020] = {
                        'CST_ICMS': self._formatar_str(arq[2]),
                        'BC_ICMS': self._formatar_valor(arq[3]),
                        'VL_ICMS': self._formatar_valor(arq[4])
                    }

                    n_h020 += 1
               
                elif line[:6] == '|H030|': # Nível 4
                 
                    arq = line.split('|')
                    self.efd['0000']['H001']['H005'][n_h005-1]['H010'][n_h010-1]['H030'] = {
                        'VL_ICMS_OP': self._formatar_valor(arq[2]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[3]),
                        'VL_ICMS_ST': self._formatar_valor(arq[4]),
                        'VL_FCP': self._formatar_valor(arq[5])
                    }

                elif line[:6] == '|H990|': # Nível 1
                    
                    arq = line.split('|')
                    self.efd['0000']['H990'] = {
                        'QTD_LIN_H': self._formatar_valor(arq[2])
                        }
                    
                else:
                    raise ValueError(f"Registro em bloco {line[1]} inconsistente: {line[:6]}")

            elif line[:2] == '|K': # BLOCO K

                if line[:6] == '|K001|': # Nível 1
                    n_k100 = 0
                    
                    arq = line.split('|')
                    self.efd['0000']['K001'] = {
                        'IND_MOV': self._formatar_str(arq[2]),
                        'K010': {},
                        'K100': {}
                    }

                elif line[:6] == '|K010|': # Nível 2
                    
                    arq = line.split('|')
                    self.efd['0000']['K001']['K010'] = {
                        'IND_TP_LEIAUTE': self._formatar_str(arq[2])
                    }

                elif line[:6] == '|K100|': # Nível 2
                    
                    n_k200 = 0
                    n_k210 = 0
                    n_k220 = 0
                    n_k230 = 0
                    n_k250 = 0
                    n_k260 = 0
                    n_k270 = 0
                    n_k280 = 0
                    n_k290 = 0
                    n_k300 = 0

                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100] = {
                        'DT_INI': self._formatar_str(arq[2]),
                        'DT_FIN': self._formatar_str(arq[3]),
                        'K200': {},
                        'K210': {},
                        'K220': {},
                        'K230': {},
                        'K250': {},
                        'K260': {},
                        'K270': {},
                        'K280': {},
                        'K290': {},
                        'K300': {}
                    }
                    n_k100 += 1

                elif line[:6] == '|K200|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K200'][n_k200] = {
                        'DT_EST': self._formatar_data(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'QTD': self._formatar_valor(arq[4]),
                        'IND_EST': self._formatar_str(arq[5]),
                        'COD_PART': self._formatar_str(arq[6], inteiro=False)
                    }

                    n_k200 += 1

                elif line[:6] == '|K210|': # Nível 3
                    
                    n_k215 = 0
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K210'][n_k210] = {
                        'DT_INI_OS': self._formatar_data(arq[2]),
                        'DT_FIN_OS': self._formatar_data(arq[3]),
                        'COD_DOC_OS': self._formatar_str(arq[4]),
                        'COD_ITEM_ORI': self._formatar_str(arq[5]),
                        'QTD_ORI': self._formatar_valor(arq[6]),
                        'K215': {}
                    }
                    n_k210 += 1

                elif line[:6] == '|K215|': # Nível 4
                    
                    n_k215 = 0
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K210'][n_k210-1]['K215'][n_k215] = {
                        'COD_ITEM_DES': self._formatar_str(arq[2]),
                        'QTD_DES': self._formatar_valor(arq[3])
                    }
                    n_k215 += 1

                elif line[:6] == '|K220|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K220'][n_k220] = {
                        'DT_MOV': self._formatar_data(arq[2]),
                        'COD_ITEM_ORI': self._formatar_str(arq[3]),
                        'COD_ITEM_DEST': self._formatar_str(arq[4]),
                        'QTD_ORI': self._formatar_valor(arq[5]),
                        'QTD_DEST': self._formatar_valor(arq[6])
                    }

                    n_k220 += 1

                elif line[:6] == '|K230|': # Nível 3
                    
                    n_k235 = 0
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K230'][n_k230] = {
                        'DT_INI_OP': self._formatar_data(arq[2]),
                        'DT_FIN_OP': self._formatar_data(arq[3]),
                        'COD_DOC_OP': self._formatar_str(arq[4]),
                        'COD_ITEM': self._formatar_str(arq[5]),
                        'QTD_ENC': self._formatar_valor(arq[6]),
                        'K235': {}
                    }

                    n_k230 += 1

                elif line[:6] == '|K235|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K230'][n_k230-1]['K235'][n_k235] = {
                        'DT_SAÍDA': self._formatar_data(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'QTD': self._formatar_valor(arq[4]),
                        'COD_INS_SUBST': self._formatar_str(arq[5])
                    }

                    n_k235 += 1

                elif line[:6] == '|K250|': # Nível 3
                    
                    n_k255 = 0
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K250'][n_k250] = {
                        'DT_PROD': self._formatar_data(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'QTD': self._formatar_valor(arq[4]),
                        'K255': {}
                    }

                    n_k250 += 1

                elif line[:6] == '|K255|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K250'][n_k250-1]['K255'][n_k255] = {
                        'DT_CONS': self._formatar_data(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'QTD': self._formatar_valor(arq[4]),
                        'COD_INS_SUBST': self._formatar_str(arq[5]),
                        'K255': {}
                    }

                    n_k255 += 1

                elif line[:6] == '|K260|': # Nível 3
                    
                    n_k265 = 0
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K260'][n_k260] = {
                        'COD_OP_OS': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'DT_SAIDA': self._formatar_data(arq[4]),
                        'QTD_SAIDA': self._formatar_valor(arq[5]),
                        'DT_RET': self._formatar_data(arq[6]),
                        'QTD_RET': self._formatar_valor(arq[7]),
                        'K265': {}
                    }

                    n_k260 += 1
               
                elif line[:6] == '|K265|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K260'][n_k260-1]['K265'][n_k265] = {
                        'COD_ITEM': self._formatar_str(arq[2]),
                        'QTD_CONS': self._formatar_valor(arq[3]),
                        'QTD_RET': self._formatar_valor(arq[4])
                    }

                    n_k265 += 1

                elif line[:6] == '|K270|': # Nível 3
                    
                    n_k275 = 0
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K270'][n_k270] = {
                        'DT_INI_AP': self._formatar_data(arq[2]),
                        'DT_FIN_AP': self._formatar_data(arq[3]),
                        'COD_OP_OS': self._formatar_str(arq[4]),
                        'COD_ITEM': self._formatar_str(arq[5]),
                        'QTD_COR_POS': self._formatar_valor(arq[6]),
                        'QTD_COR_NEG': self._formatar_valor(arq[7]),
                        'ORIGEM': self._formatar_str(arq[8]),
                        'K275': {}
                    }
                    n_k270 += 1

                elif line[:6] == '|K275|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K270'][n_k270-1]['K275'][n_k275] = {
                        'COD_ITEM': self._formatar_str(arq[2]),
                        'QTD_COR_POS': self._formatar_valor(arq[3]),
                        'QTD_COR_NEG': self._formatar_valor(arq[4]),
                        'COD_INS_SUBST': self._formatar_str(arq[5])
                    }
                    n_k275 += 1

                elif line[:6] == '|K280|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K280'][n_k280] = {
                        'DT_EST': self._formatar_data(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'QTD_COR_POS': self._formatar_valor(arq[4]),
                        'QTD_COR_NEG': self._formatar_valor(arq[5]),
                        'IND_EST': self._formatar_str(arq[6]),
                        'COD_PART': self._formatar_str(arq[7], inteiro=False)
                    }
                    n_k280 += 1
               
                elif line[:6] == '|K290|': # Nível 3
                    
                    n_k291 = 0
                    n_k292 = 0

                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K290'][n_k290] = {
                        'DT_INI_OP': self._formatar_data(arq[2]),
                        'DT_FIN_OP': self._formatar_data(arq[3]),
                        'COD_DOC_OP': self._formatar_str(arq[4]),
                        'K291': {}, 
                        'K292': {}
                    }
                    n_k290 += 1
                
                elif line[:6] == '|K291|': # Nível 4
                    
                    n_k292 = 0

                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K290'][n_k290-1]['K291'][n_k291] = {
                        'COD_ITEM': self._formatar_data(arq[2]),
                        'QTD': self._formatar_data(arq[3])
                    }
                    n_k291 += 1

                elif line[:6] == '|K292|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K290'][n_k290-1]['K292'][n_k292] = {
                        'COD_ITEM': self._formatar_data(arq[2]),
                        'QTD': self._formatar_data(arq[3])
                    }
                    n_k292 += 1

                elif line[:6] == '|K300|': # Nível 3
                    
                    n_k301 = 0
                    n_k302 = 0

                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K300'][n_k300] = {
                        'DT_PROD': self._formatar_data(arq[2]),
                        'K301': {}, 
                        'K302': {}
                    }
                    n_k300 += 1

                elif line[:6] == '|K301|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K300'][n_k300-1]['K301'][n_k301] = {
                        'COD_ITEM': self._formatar_data(arq[2]),
                        'QTD': self._formatar_data(arq[3])
                    }
                    n_k301 += 1
               
                elif line[:6] == '|K302|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['K001']['K100'][n_k100-1]['K300'][n_k300-1]['K302'][n_k302] = {
                        'COD_ITEM': self._formatar_data(arq[2]),
                        'QTD': self._formatar_data(arq[3])
                    }
                    n_k302 += 1

                elif line[:6] == '|K990|': # Nível 1
                    n_k100 = 0
                    
                    arq = line.split('|')
                    self.efd['0000']['K990'] = {
                        'QTD_LIN_K': self._formatar_valor(arq[2])
                    }

                else:
                    raise ValueError(f"Registro em bloco {line[1]} inconsistente: {line[:6]}")

            elif line[:2] == '|1': # BLOCO 1

                if line[:6] == '|1001|': # Nível 1
                    n_1100 = 0
                    n_1200 = 0
                    n_1250 = 0
                    n_1300 = 0
                    n_1350 = 0
                    n_1390 = 0
                    n_1400 = 0
                    n_1500 = 0
                    n_1600 = 0
                    n_1601 = 0
                    n_1700 = 0
                    n_1800 = 0
                    n_1900 = 0
                    n_1960 = 0
                    n_1970 = 0
                    n_1980 = 0

                    arq = line.split('|')
                    self.efd['0000']['1001'] = {
                        'IND_MOV': self._formatar_str(arq[2]),
                        '1010': {},
                        '1100': {},
                        '1200': {},
                        '1250': {},
                        '1300': {},
                        '1350': {},
                        '1390': {},
                        '1400': {},
                        '1500': {},
                        '1600': {},
                        '1601': {},
                        '1700': {},
                        '1800': {},
                        '1900': {},
                        '1960': {},
                        '1970': {},
                        '1980': {}
                    }

                elif line[:6] == '|1010|': # Nível 2

                    arq = line.split('|')
                    self.efd['0000']['1001']['1010'] = {
                        'IND_EXP': self._formatar_str(arq[2]),
                        'IND_CCRF': self._formatar_str(arq[3]),
                        'IND_COMB': self._formatar_str(arq[4]),
                        'IND_USINA': self._formatar_str(arq[5]),
                        'IND_VA': self._formatar_str(arq[6]),
                        'IND_EE': self._formatar_str(arq[7]),
                        'IND_CART': self._formatar_str(arq[8]),
                        'IND_FORM': self._formatar_str(arq[9]),
                        'IND_AER': self._formatar_str(arq[10]),
                        'IND_GIAF1': self._formatar_str(arq[11]),
                        'IND_GIAF3': self._formatar_str(arq[12]),
                        'IND_GIAF4': self._formatar_str(arq[13]),
                        'IND_REST_RESSARC_COMPL_ICMS': self._formatar_str(arq[14])
                    }

                elif line[:6] == '|1100|': # Nível 2
                    
                    n_1105 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1100'][n_1100] = {
                        'IND_DOC': self._formatar_str(arq[2]),
                        'NRO_DE': self._formatar_str(arq[3]),
                        'DT_DE': self._formatar_data(arq[4]),
                        'NAT_EXP': self._formatar_str(arq[5]),
                        'NRO_RE': self._formatar_str(arq[6]),
                        'DT_RE': self._formatar_data(arq[7]),
                        'CHC_EMB': self._formatar_str(arq[8]),
                        'DT_CHC': self._formatar_data(arq[9]),
                        'DT_AVB': self._formatar_data(arq[10]),
                        'TP_CHC': self._formatar_str(arq[11]),
                        'PAIS': self._formatar_str(arq[12]),
                        '1105': {}
                    }
                    n_1100 += 1
               
                elif line[:6] == '|1105|': # Nível 3
                    
                    n_1110 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1100'][n_1100-1]['1105'][n_1105] = {
                        'COD_MOD': self._formatar_str(arq[2]),
                        'SERIE': self._formatar_str(arq[3]),
                        'NUM_DOC': self._formatar_str(arq[4]),
                        'CHV_NFE': self._formatar_str(arq[5]),
                        'DT_DOC': self._formatar_data(arq[6]),
                        'COD_ITEM': self._formatar_str(arq[7]),
                        '1100': {}
                    }

                    n_1105 += 1

                elif line[:6] == '|1110|': # Nível 4
                    
                    arq = line.split('|')
                    self.efd['0000']['1001']['1100'][n_1100-1]['1105'][n_1105-1]['1110'][n_1110] = {
                        'COD_PART': self._formatar_str(arq[2], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[3]),
                        'SER': self._formatar_str(arq[4]),
                        'NUM_DOC': self._formatar_str(arq[5]),
                        'DT_DOC': self._formatar_data(arq[6]),
                        'CHV_NFE': self._formatar_str(arq[7]),
                        'NR_MEMO': self._formatar_str(arq[8]),
                        'QTD': self._formatar_valor(arq[9]),
                        'UNID': self._formatar_str(arq[10])
                    }

                    n_1110 += 1

                elif line[:6] == '|1200|': # Nível 2

                    n_1210 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1200'][n_1200] = {
                        'COD_AJ_APUR': self._formatar_str(arq[2]),
                        'SLD_CRED': self._formatar_valor(arq[3]),
                        'CRED_APR': self._formatar_valor(arq[4]),
                        'CRED_RECEB': self._formatar_valor(arq[5]),
                        'CRED_UTIL': self._formatar_valor(arq[6]),
                        'SLD_CRED_FIM': self._formatar_valor(arq[7]),
                        '1210': {}
                    }
                    n_1200 += 1

                elif line[:6] == '|1210|': # Nível 3

                    arq = line.split('|')
                    self.efd['0000']['1001']['1200'][n_1200-1]['1210'][n_1210] = {
                        'TIPO_UTIL': self._formatar_str(arq[2]),
                        'NR_DOC': self._formatar_str(arq[3]),
                        'VL_CRED_UTIL': self._formatar_valor(arq[4]),
                        'CHV_DOCe': self._formatar_str(arq[5])
                    }

                    n_1210 += 1

                elif line[:6] == '|1250|': # Nível 2

                    n_1255 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1250'] = {
                        'VL_CREDITO_ICMS_OP': self._formatar_valor(arq[2]),
                        'VL_ICMS_ST_REST': self._formatar_valor(arq[3]),
                        'VL_FCP_ST_REST': self._formatar_valor(arq[4]),
                        'VL_ICMS_ST_COMPL': self._formatar_valor(arq[5]),
                        'VL_FCP_ST_COMPL': self._formatar_valor(arq[6]),
                        '1255': {}
                    }

                elif line[:6] == '|1255|': # Nível 3

                    arq = line.split('|')
                    self.efd['0000']['1001']['1250']['1255'][n_1255] = {
                        'COD_MOT_REST_COMPL': self._formatar_str(arq[2]),
                        'VL_CREDITO_ICMS_OP_MOT': self._formatar_valor(arq[3]),
                        'VL_ICMS_ST_REST_MOT': self._formatar_valor(arq[4]),
                        'VL_FCP_ST_REST_MOT': self._formatar_valor(arq[5]),
                        'VL_ICMS_ST_COMPL_MOT': self._formatar_valor(arq[6]),
                        'VL_FCP_ST_COMPL_MOT': self._formatar_valor(arq[7])
                    }

                    n_1255 += 1 
                
                elif line[:6] == '|1300|': # Nível 2

                    n_1310 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1300'][n_1300] = {
                        'COD_ITEM': self._formatar_str(arq[2]),
                        'DT_FECH': self._formatar_data(arq[3]),
                        'ESTQ_ABERT': self._formatar_valor(arq[4]),
                        'VOL_ENTR': self._formatar_valor(arq[5]),
                        'VOL_DISP': self._formatar_valor(arq[6]),
                        'VOL_SAIDAS': self._formatar_valor(arq[7]),
                        'ESTQ_ESCR': self._formatar_valor(arq[8]),
                        'VAL_AJ_PERDA': self._formatar_valor(arq[9]),
                        'VAL_AJ_GANHO': self._formatar_valor(arq[10]),
                        'FECH_FISICO': self._formatar_valor(arq[11]),
                        '1310': {}
                    }

                    n_1300 += 1 

                elif line[:6] == '|1310|': # Nível 3

                    n_1320 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1300'][n_1300-1]['1310'][n_1310] = {
                        'NUM_TANQUE': self._formatar_str(arq[2]),
                        'ESTQ_ABERT': self._formatar_valor(arq[3]),
                        'VOL_ENTR': self._formatar_valor(arq[4]),
                        'VOL_DISP': self._formatar_valor(arq[5]),
                        'VOL_SAIDAS': self._formatar_valor(arq[6]),
                        'ESTQ_ESCR': self._formatar_valor(arq[7]),
                        'VAL_AJ_PERDA': self._formatar_valor(arq[8]),
                        'VAL_AJ_GANHO': self._formatar_valor(arq[9]),
                        'FECH_FISICO': self._formatar_valor(arq[10]),
                        '1320': {}
                    }

                    n_1310 += 1 

                elif line[:6] == '|1320|': # Nível 4

                    arq = line.split('|')
                    self.efd['0000']['1001']['1300'][n_1300-1]['1310'][n_1310-1]['1320'][n_1320] = {
                        'NUM_BICO': self._formatar_valor(arq[2]),
                        'NR_INTERV': self._formatar_str(arq[3]),
                        'MOT_INTERV': self._formatar_str(arq[4]),
                        'NOM_INTERV': self._formatar_str(arq[5]),
                        'CNPJ_INTERV': self._formatar_str(arq[6]),
                        'CPF_INTERV': self._formatar_str(arq[7]),
                        'VAL_FECHA': self._formatar_valor(arq[8]),
                        'VAL_ABERT': self._formatar_valor(arq[9]),
                        'VOL_AFERI': self._formatar_valor(arq[10]),
                        'VOL_VENDAS': self._formatar_valor(arq[11])
                    }
                    n_1320 += 1 

                elif line[:6] == '|1350|': # Nível 2

                    n_1360 = 0
                    n_1370 = 0

                    arq = line.split('|')
                    self.efd['0000']['1001']['1350'][n_1350] = {
                        'SERIE': self._formatar_str(arq[2]),
                        'FABRICANTE': self._formatar_str(arq[3]),
                        'MODELO': self._formatar_str(arq[4]),
                        'TIPO_MEDICAO': self._formatar_str(arq[5]),
                        '1360': {},
                        '1370': {}
                    }

                    n_1350 += 1 

                elif line[:6] == '|1360|': # Nível 3

                    arq = line.split('|')
                    self.efd['0000']['1001']['1350'][n_1350-1]['1360'][n_1360] = {
                        'NUM_LACRE': self._formatar_str(arq[2]),
                        'DT_APLICACAO': self._formatar_data(arq[3])
                    }

                    n_1360 += 1   

                elif line[:6] == '|1370|': # Nível 3

                    arq = line.split('|')
                    self.efd['0000']['1001']['1350'][n_1350-1]['1370'][n_1370] = {
                        'NUM_BICO': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'NUM_TANQUE': self._formatar_str(arq[4])
                    }

                    n_1370 += 1 

                elif line[:6] == '|1390|': # Nível 2

                    n_1391 = 0

                    arq = line.split('|')
                    self.efd['0000']['1001']['1390'][n_1390] = {
                        'COD_PROD': self._formatar_str(arq[2]),
                        '1391': {}
                    }

                    n_1390 += 1 

                elif line[:6] == '|1391|': # Nível 3

                    n_1391 = 0

                    arq = line.split('|')
                    self.efd['0000']['1001']['1390'][n_1390-1]['1391'][n_1391] = {
                        'DT_REGISTRO': self._formatar_data(arq[2]),
                        'QTD_MOID': self._formatar_valor(arq[3]),
                        'ESTQ_INI': self._formatar_valor(arq[4]),
                        'QTD_PRODUZ': self._formatar_valor(arq[5]),
                        'ENT_ANID_HID': self._formatar_valor(arq[6]),
                        'OUTR_ENTR': self._formatar_valor(arq[7]),
                        'PERDA': self._formatar_valor(arq[8]),
                        'CONS': self._formatar_valor(arq[9]),
                        'SAI_ANI_HID': self._formatar_valor(arq[10]),
                        'SAÍDAS': self._formatar_valor(arq[11]),
                        'ESTQ_FIN': self._formatar_valor(arq[12]),
                        'ESTQ_INI_MEL': self._formatar_valor(arq[13]),
                        'PROD_DIA_MEL': self._formatar_valor(arq[14]),
                        'UTIL_MEL': self._formatar_valor(arq[15]),
                        'PROD_ALC_MEL': self._formatar_valor(arq[16]),
                        'OBS': self._formatar_str(arq[17]),
                        'COD_ITEM': self._formatar_str(arq[18]),
                        'TP_RESIDUO': self._formatar_str(arq[19]),
                        'QTD_RESIDUO': self._formatar_valor(arq[20]),
                        'QTD_RESIDUO_DDG': self._formatar_valor(arq[21]),
                        'QTD_RESIDUO_WDG': self._formatar_valor(arq[22]),
                        'QTD_RESIDUO_CANA': self._formatar_valor(arq[23])
                    }

                    n_1391 += 1 

                elif line[:6] == '|1400|': # Nível 2

                    arq = line.split('|')
                    self.efd['0000']['1001']['1400'][n_1400] = {
                        'COD_ITEM_IPM': self._formatar_str(arq[2]),
                        'MUN': self._formatar_str(arq[3]),
                        'VALOR': self._formatar_valor(arq[4])
                    }

                    n_1400 += 1 

                elif line[:6] == '|1500|': # Nível 2

                    n_1510 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1500'][n_1500] = {
                        'IND_OPER': self._formatar_str(arq[2]),
                        'IND_EMIT': self._formatar_str(arq[3]),
                        'COD_PART': self._formatar_str(arq[4], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[5]),
                        'COD_SIT': self._formatar_str(arq[6]),
                        'SER': self._formatar_str(arq[7]),
                        'SUB': self._formatar_str(arq[8]),
                        'COD_CONS': self._formatar_str(arq[9]),
                        'NUM_DOC': self._formatar_str(arq[10]),
                        'DT_DOC': self._formatar_data(arq[11]),
                        'DT_E_S': self._formatar_data(arq[12]),
                        'VL_DOC': self._formatar_valor(arq[13]),
                        'VL_DESC': self._formatar_valor(arq[14]),
                        'VL_FORN': self._formatar_valor(arq[15]),
                        'VL_SERV_NT': self._formatar_valor(arq[16]),
                        'VL_TERC': self._formatar_valor(arq[17]),
                        'VL_DA': self._formatar_valor(arq[18]),
                        'VL_BC_ICMS': self._formatar_valor(arq[19]),
                        'VL_ICMS': self._formatar_valor(arq[20]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[21]),
                        'VL_ICMS_ST': self._formatar_valor(arq[22]),
                        'COD_INF': self._formatar_str(arq[23]),
                        'VL_PIS': self._formatar_valor(arq[24]),
                        'VL_COFINS': self._formatar_valor(arq[25]),
                        'TP_LIGACAO': self._formatar_str(arq[26]),
                        'COD_GRUPO_TENSAO': self._formatar_str(arq[27]),
                        '1510': {}
                    }
                    n_1500 += 1 

                elif line[:6] == '|1510|': # Nível 3

                    n_1510 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1500'][n_1500-1]['1510'][n_1510] = {
                        'NUM_ITEM': self._formatar_str(arq[2]),
                        'COD_ITEM': self._formatar_str(arq[3]),
                        'COD_CLASS': self._formatar_str(arq[4]),
                        'QTD': self._formatar_valor(arq[5]),
                        'UNID': self._formatar_str(arq[6]),
                        'VL_ITEM': self._formatar_valor(arq[7]),
                        'VL_DESC': self._formatar_valor(arq[8]),
                        'CST_ICMS': self._formatar_str(arq[9]),
                        'CFOP': self._formatar_str(arq[10]),
                        'VL_BC_ICMS': self._formatar_valor(arq[11]),
                        'ALIQ_ICMS': self._formatar_valor(arq[12]),
                        'VL_ICMS': self._formatar_valor(arq[13]),
                        'VL_BC_ICMS_ST': self._formatar_valor(arq[14]),
                        'ALIQ_ST': self._formatar_valor(arq[15]),
                        'VL_ICMS_ST': self._formatar_valor(arq[16]),
                        'IND_REC': self._formatar_str(arq[17]),
                        'COD_PART': self._formatar_str(arq[18], inteiro=False),
                        'VL_PIS': self._formatar_valor(arq[19]),
                        'VL_COFINS': self._formatar_valor(arq[20]),
                        'COD_CTA': self._formatar_str(arq[21])
                    }

                    n_1510 += 1 

                elif line[:6] == '|1600|': # Nível 2

                    n_1510 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1600'][n_1600] = {
                        'COD_PART': self._formatar_str(arq[2], inteiro=False),
                        'TOT_CREDITO': self._formatar_str(arq[3]),
                        'TOT_DEBITO': self._formatar_str(arq[4])
                    }
                    n_1600 += 1 

                elif line[:6] == '|1601|': # Nível 2

                    arq = line.split('|')
                    self.efd['0000']['1001']['1601'][n_1601] = {
                        'COD_PART_IP': self._formatar_str(arq[2], inteiro=False),
                        'COD_PART_IT': self._formatar_str(arq[3], inteiro=False),
                        'TOT_VS': self._formatar_valor(arq[4]),
                        'TOT_ISS': self._formatar_valor(arq[5]),
                        'TOT_OUTROS': self._formatar_valor(arq[6])
                    }

                    n_1601 += 1 

                elif line[:6] == '|1700|': # Nível 2
                    
                    n_1710 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1700'][n_1700] = {
                        'COD_DISP': self._formatar_str(arq[2]),
                        'COD_MOD': self._formatar_str(arq[3]),
                        'SER': self._formatar_str(arq[4]),
                        'SUB': self._formatar_str(arq[5]),
                        'NUM_DOC_INI': self._formatar_valor(arq[6]),
                        'NUM_DOC_FIN': self._formatar_valor(arq[7]),
                        'NUM_AUT': self._formatar_str(arq[8]),
                        '1710': {}
                    }
                    n_1700 += 1 
                
                elif line[:6] == '|1710|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['1001']['1700'][n_1700-1]['1710'][n_1710] = {
                        'NUM_DOC_INI': self._formatar_valor(arq[2]),
                        'NUM_DOC_FIN': self._formatar_valor(arq[3])
                    }
                    n_1710 += 1 

                elif line[:6] == '|1800|': # Nível 2
                    
                    arq = line.split('|')
                    self.efd['0000']['1001']['1800'][n_1800] = {
                        'VL_CARGA': self._formatar_valor(arq[2]),
                        'VL_PASS': self._formatar_valor(arq[3]),
                        'VL_FAT': self._formatar_valor(arq[4]),
                        'IND_RAT': self._formatar_valor(arq[5]),
                        'VL_ICMS_ANT': self._formatar_valor(arq[6]),
                        'VL_BC_ICMS': self._formatar_valor(arq[7]),
                        'VL_ICMS_APUR': self._formatar_valor(arq[8]),
                        'VL_BC_ICMS_APUR': self._formatar_valor(arq[9]),
                        'VL_DIF': self._formatar_valor(arq[10])
                    }

                    n_1800 += 1

                elif line[:6] == '|1900|': # Nível 2
                    
                    n_1910 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1900'][n_1900] = {
                        'IND_APUR_ICMS': self._formatar_str(arq[2]),
                        'DESCR_COMPL_OUT_APUR': self._formatar_str(arq[3]),
                        '1910': {}
                    }

                    n_1900 += 1

                elif line[:6] == '|1910|': # Nível 3
                    
                    arq = line.split('|')
                    self.efd['0000']['1001']['1900'][n_1900-1]['1910'][n_1910] = {
                        'DT_INI': self._formatar_data(arq[2]),
                        'DT_FIN': self._formatar_data(arq[3]),
                        '1920': {}
                    }

                    n_1910 += 1

                elif line[:6] == '|1920|': # Nível 4
                    
                    n_1921 = 0
                    n_1925 = 0
                    n_1926 = 0

                    arq = line.split('|')
                    self.efd['0000']['1001']['1900'][n_1900-1]['1910'][n_1910-1]['1920'] = {
                        'VL_TOT_TRANSF_DEBITOS_OA': self._formatar_valor(arq[2]),
                        'VL_TOT_AJ_DEBITOS_OA': self._formatar_valor(arq[3]),
                        'VL_ESTORNOS_CRED_OA': self._formatar_valor(arq[4]),
                        'VL_TOT_TRANSF_CREDITOS_OA': self._formatar_valor(arq[5]),
                        'VL_TOT_AJ_CREDITOS_OA': self._formatar_valor(arq[6]),
                        'VL_ESTORNOS_DEB_OA': self._formatar_valor(arq[7]),
                        'VL_SLD_CREDOR_ANT_OA': self._formatar_valor(arq[8]),
                        'VL_SLD_APURADO_OA': self._formatar_valor(arq[9]),
                        'VL_TOT_DED': self._formatar_valor(arq[10]),
                        'VL_ICMS_RECOLHER_OA': self._formatar_valor(arq[11]),
                        'VL_SLD_CREDOR_TRANSP_OA': self._formatar_valor(arq[12]),
                        'DEB_ESP_OA': self._formatar_valor(arq[13]),
                        '1921': {},
                        '1925': {},
                        '1926': {} 
                    }

                elif line[:6] == '|1921|': # Nível 5
                    
                    n_1922 = 0
                    n_1923 = 0

                    arq = line.split('|')
                    self.efd['0000']['1001']['1900'][n_1900-1]['1910'][n_1910-1]['1920']['1921'][n_1921] = {
                        'COD_AJ_APUR': self._formatar_str(arq[2]),
                        'DESCR_COMPL_AJ': self._formatar_str(arq[3]),
                        'VL_AJ_APUR': self._formatar_valor(arq[4])
                    }


                    n_1921 += 1

                elif line[:6] == '|1922|': # Nível 6
                    
                    arq = line.split('|')
                    self.efd['0000']['1001']['1900'][n_1900-1]['1910'][n_1910-1]['1920']['1921'][n_1921-1]['1922'][n_1922] = {
                        'NUM_DA': self._formatar_str(arq[2]),
                        'NUM_PROC': self._formatar_str(arq[3]),
                        'IND_PROC': self._formatar_str(arq[4]),
                        'PROC': self._formatar_str(arq[5]),
                        'TXT_COMPL': self._formatar_str(arq[6])
                    }
                    n_1922 += 1

                elif line[:6] == '|1923|': # Nível 6
                    
                    arq = line.split('|')
                    self.efd['0000']['1001']['1900'][n_1900-1]['1910'][n_1910-1]['1920']['1921'][n_1921-1]['1923'][n_1923] = {
                        'COD_PART': self._formatar_str(arq[2], inteiro=False),
                        'COD_MOD': self._formatar_str(arq[3]),
                        'SER': self._formatar_str(arq[4]),
                        'SUB': self._formatar_str(arq[5]),
                        'NUM_DOC': self._formatar_str(arq[6]),
                        'DT_DOC': self._formatar_data(arq[7]),
                        'COD_ITEM': self._formatar_str(arq[8]),
                        'VL_AJ_ITEM': self._formatar_valor(arq[9]),
                        'CHV_DOCe': self._formatar_str(arq[10])
                    }
                    n_1923 += 1

                elif line[:6] == '|1925|': # Nível 5
                    
                    arq = line.split('|')
                    self.efd['0000']['1001']['1900'][n_1900-1]['1910'][n_1910-1]['1920']['1925'][n_1925] = {
                        'COD_INF_ADIC': self._formatar_str(arq[2]),
                        'VL_INF_ADIC': self._formatar_valor(arq[3]),
                        'DESCR_COMPL_AJ': self._formatar_str(arq[4])
                    }


                    n_1925 += 1

                elif line[:6] == '|1926|': # Nível 5
                    
                    arq = line.split('|')
                    self.efd['0000']['1001']['1900'][n_1900-1]['1910'][n_1910-1]['1920']['1926'][n_1926] = {
                        'COD_OR': self._formatar_str(arq[2]),
                        'VL_OR': self._formatar_valor(arq[3]),
                        'DT_VCTO': self._formatar_data(arq[4]),
                        'COD_REC': self._formatar_str(arq[5]),
                        'NUM_PROC': self._formatar_str(arq[6]),
                        'IND_PROC': self._formatar_str(arq[7]),
                        'PROC': self._formatar_str(arq[8]),
                        'TXT_COMPL': self._formatar_str(arq[9]),
                        'MES_REF': self._formatar_str(arq[10])
                    }
                    n_1926 += 1

                elif line[:6] == '|1960|': # Nível 2
                
                    arq = line.split('|')
                    self.efd['0000']['1001']['1960'][n_1960] = {
                        'IND_AP': self._formatar_str(arq[2]),
                        'G1_01': self._formatar_valor(arq[3]),
                        'G1_02': self._formatar_valor(arq[4]),
                        'G1_03': self._formatar_valor(arq[5]),
                        'G1_04': self._formatar_valor(arq[6]),
                        'G1_05': self._formatar_valor(arq[7]),
                        'G1_06': self._formatar_valor(arq[8]),
                        'G1_07': self._formatar_valor(arq[9]),
                        'G1_08': self._formatar_valor(arq[10]),
                        'G1_09': self._formatar_valor(arq[11]),
                        'G1_10': self._formatar_valor(arq[12]),
                        'G1_11': self._formatar_valor(arq[13])
                    }
                    n_1960 += 1

                elif line[:6] == '|1970|': # Nível 2
                    
                    n_1975 = 0
                    arq = line.split('|')
                    self.efd['0000']['1001']['1970'][n_1970] = {
                        'IND_AP': self._formatar_str(arq[2]),
                        'G3_01': self._formatar_valor(arq[3]),
                        'G3_02': self._formatar_valor(arq[4]),
                        'G3_03': self._formatar_valor(arq[5]),
                        'G3_04': self._formatar_valor(arq[6]),
                        'G3_05': self._formatar_valor(arq[7]),
                        'G3_06': self._formatar_valor(arq[8]),
                        'G3_07': self._formatar_valor(arq[9]),
                        'G3_T': self._formatar_valor(arq[10]),
                        'G3_08': self._formatar_valor(arq[11]),
                        'G3_09': self._formatar_valor(arq[12])
                    }
                    n_1970 += 1

                elif line[:6] == '|1975|': # Nível 2
                    
                    arq = line.split('|')
                    self.efd['0000']['1001']['1970'][n_1970-1]['1975'][n_1975] = {
                        'ALIQ_IMP_BASE': self._formatar_valor(arq[2]),
                        'G3_10': self._formatar_valor(arq[3]),
                        'G3_11': self._formatar_valor(arq[4]),
                        'G3_12': self._formatar_valor(arq[5])
                    }
                    n_1975 += 1

                elif line[:6] == '|1980|': # Nível 2
                    
                    arq = line.split('|')
                    self.efd['0000']['1001']['1980'][n_1980] = {
                        'IND_AP': self._formatar_str(arq[2]),
                        'G4_01': self._formatar_valor(arq[3]),
                        'G4_02': self._formatar_valor(arq[4]),
                        'G4_03': self._formatar_valor(arq[5]),
                        'G4_04': self._formatar_valor(arq[6]),
                        'G4_05': self._formatar_valor(arq[7]),
                        'G4_06': self._formatar_valor(arq[8]),
                        'G4_07': self._formatar_valor(arq[9]),
                        'G4_08': self._formatar_valor(arq[10]),
                        'G4_09': self._formatar_valor(arq[11]),
                        'G4_10': self._formatar_valor(arq[12]),
                        'G4_11': self._formatar_valor(arq[13]),
                        'G4_12': self._formatar_valor(arq[14])
                    }
                    n_1980 += 1

                elif line[:6] == '|1990|': # Nível 1
                    
                    arq = line.split('|')
                    self.efd['0000']['1001'] = {
                        'QTD_LIN_1': self._formatar_valor(arq[2])
                    }

                else:
                    raise ValueError(f"Registro em bloco {line[1]} inconsistente: {line[:6]}")

            elif line[:2] == '|9': # BLOCO 9

                if line[:6] == '|9001|': # Nível 1

                    n_9900 = 0                    
                    arq = line.split('|')
                    self.efd['0000']['9001'] = {
                        'IND_MOV': self._formatar_str(arq[2]),
                        '9900': {}
                    }
                    
                elif line[:6] == '|9900|': # Nível 2

                    n_9900 = 0                    
                    arq = line.split('|')
                    self.efd['0000']['9001']['9900'][n_9900] = {
                        'REG_BLC': self._formatar_str(arq[2]),
                        'QTD_REG_BLC': self._formatar_str(arq[3]),
                        '9900': {}
                    }

                elif line[:6] == '|9990|': # Nível 1

                    arq = line.split('|')
                    self.efd['0000']['9990'] = {
                        'QTD_LIN_9': self._formatar_valor(arq[2])
                    }

                elif line[:6] == '|9990|': # Nível 0

                    arq = line.split('|')
                    self.efd['0000']['9999'] = {
                        'QTD_LIN': self._formatar_valor(arq[2])
                    }

            else:
                None

    def _formatar_str(self, variavel, inteiro=True):
        """ Função interna para confirmar valores como string """
        if variavel and variavel != '' and variavel != ' ':
            variavel = " ".join(variavel.split())
            if inteiro:
                try:
                    variavel = int(variavel)
                except:
                    pass 
        return str(variavel)
    
    def _formatar_data(self, variavel, formato=None):
        """ Função interna para formatar datas e retornar datetime format """
        

        if variavel and variavel != '' and variavel != ' ':
            if formato:
                data = dt.strptime(variavel, formato)
            else:
                data = variavel[:2] + '/' + variavel[2:4] + '/' + variavel[4:]
                data = dt.strptime(data, "%d/%m/%Y")
        else:
            data=variavel
        return data

    def _formatar_hora(self, variavel):
        """ Função interna para formatar horas e retornar datetime format """
        if variavel and variavel != '' and variavel != ' ':
            data = variavel[-2] + ':' + variavel[2:4] + ':' + variavel[4:6]
            data = dt.strptime(data, "%H:%M:%S")
        else:
            data = variavel 
        return data

    def _formatar_valor(self, variavel):
        """ 
        Função interna para formatar valor fracionado BR para float, substituindo vírgula por ponto. 
        Aplicável apenas para campos numéricos que não sejam variáveis categóricas 
        """
        if variavel and variavel != '' and variavel != ' ':
            valor = variavel.replace(',', '.')
            try:
                valor = float(valor)
            except:
                valor = 0 
        else:
            valor = variavel
        return valor
    
    def _formatar_codigo(self, variavel):
        if variavel and variavel != '' and variavel != ' ':
            return str(int(variavel))  
        else:
            return variavel 
