from .connection.efd_block_0_import import import_efd_title, import_efd_0000, import_efd_0001, import_efd_0002, \
    import_efd_0005, import_efd_0015, import_efd_0100, import_efd_0150, import_efd_0175, import_efd_0190, \
    import_efd_0200, import_efd_0205, import_efd_0206, import_efd_0210, import_efd_0220, import_efd_0221, \
    import_efd_0300, import_efd_0305, \
    import_efd_0400, import_efd_0450, import_efd_0460, import_efd_0500, import_efd_0600, import_efd_0990

from .connection.efd_block_C_import import (
    import_efd_C001,
    import_efd_C100, import_efd_C101, import_efd_C105, import_efd_C110, import_efd_C111, import_efd_C112, import_efd_C113, import_efd_C114, 
    import_efd_C115, import_efd_C116, import_efd_C120, import_efd_C130, import_efd_C160, 
    import_efd_C165, import_efd_C170, import_efd_C171, import_efd_C172, import_efd_C173, import_efd_C174, import_efd_C175, import_efd_C176, import_efd_C177, 
    import_efd_C178, import_efd_C179, import_efd_C180, import_efd_C181, import_efd_C185, 
    import_efd_C186, import_efd_C190, import_efd_C191, import_efd_C195, import_efd_C197, import_efd_C500, import_efd_C510, 
    import_efd_C590, import_efd_C591, import_efd_C595, import_efd_C597, import_efd_C600, 
    import_efd_C601, import_efd_C610, import_efd_C690, import_efd_C700, import_efd_C790, 
    import_efd_C791, import_efd_C990
)

from .connection.efd_block_D_import import (
    import_efd_D001, import_efd_D100, import_efd_D101, import_efd_D110, import_efd_D190, import_efd_D195, import_efd_D197,
    import_efd_D500, import_efd_D510, import_efd_D530, import_efd_D590,
    import_efd_D600, import_efd_D610, import_efd_D690, import_efd_D695, import_efd_D696, import_efd_D697,
    import_efd_D700, import_efd_D730, import_efd_D731, import_efd_D735, import_efd_D737, import_efd_D750, import_efd_D760, import_efd_D761,
    import_efd_D990
)


from .connection.efd_block_E_import import (
    import_efd_E001, 
    import_efd_E100, import_efd_E110, import_efd_E111, import_efd_E112, import_efd_E113, import_efd_E115, import_efd_E116,
    import_efd_E200, import_efd_E210, import_efd_E220, import_efd_E230, import_efd_E240, import_efd_E250,
    import_efd_E300, import_efd_E310, import_efd_E311, import_efd_E312, import_efd_E313, import_efd_E316, 
    import_efd_E500, import_efd_E510, import_efd_E520, import_efd_E530, import_efd_E531, import_efd_E990
)


from .connection.efd_block_G_import import (
    import_efd_G001,
    import_efd_G110, import_efd_G125, import_efd_G126, import_efd_G130, import_efd_G140,
    import_efd_G990
)


from .connection.efd_block_H_import import (
    import_efd_H001,
    import_efd_H005, import_efd_H010, import_efd_H020, import_efd_H030, 
    import_efd_H990
)

from .connection.efd_block_K_import import (
    import_efd_K001, import_efd_K010,
    import_efd_K100, 
    import_efd_K200, import_efd_K210, import_efd_K215, import_efd_K220, import_efd_K230, import_efd_K235, import_efd_K250, import_efd_K255, import_efd_K260, import_efd_K265, import_efd_K270,
    import_efd_K275, import_efd_K280, import_efd_K290, import_efd_K291, import_efd_K292, import_efd_K300, import_efd_K301, import_efd_K302, import_efd_K990
)

from .connection.efd_block_1_import import (
    import_efd_1001, import_efd_1010,
    import_efd_1100, import_efd_1105, import_efd_1110, 
    import_efd_1200, import_efd_1210, import_efd_1250, import_efd_1255,
    import_efd_1300, import_efd_1310, import_efd_1320, import_efd_1350, import_efd_1360, import_efd_1370, import_efd_1390, import_efd_1391, 
    import_efd_1400, 
    import_efd_1500, import_efd_1510,
    import_efd_1600, import_efd_1601,
    import_efd_1700, import_efd_1710,
    import_efd_1800,
    import_efd_1900, import_efd_1910, import_efd_1920, import_efd_1921, import_efd_1922, import_efd_1923, import_efd_1925, import_efd_1926, import_efd_1960, import_efd_1970, import_efd_1975, import_efd_1980, import_efd_1990
)

from .connection.efd_block_9_import import (
    import_efd_9001, import_efd_9900, import_efd_9990, import_efd_9999
)

# Third-party modules import
import numpy as np
import pandas as pd 
from datetime import datetime as dt 
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from .local_database_maker.processar_efd import processar_efd



# Package attributes
normal_doc_enterprises = [('01', '12'), ('01', '36'), ('01', '24'), ('01', '90'), ('05', '12'), ('05', '24')]



class EFDLocal:
    """ 
    Classe para processamento de arquivos EFD locais 
    ------------------------------------------------

    Para instanciar a classe, deve-se repassar:

    cnpj: str
        CNPJ sem máscara da empresa a ser fiscalizada

    mes_ref: str
        Mês de referência no formato "MM/YYYY"
    
    
    """
    def __init__(self, cnpj, mes_ref, local) -> None:


        try:
            processar_efd(local)
        except:
            print('Banco de dados já existe!')
            

        efd_title = import_efd_title(mes_ref=mes_ref, cnpj=cnpj)

        # Creating num_months and date_ref from the object
        self.mes_ref = mes_ref
        self.cnpj = cnpj 

        
        efd_title.rename(columns = {'CABE_NR_CNPJ':'cnpj'}, inplace = True)

        # Extracting the foreign keys
        self._keys = efd_title[[ 
                                    'cnpj',
                                    'ID_0000',
                                    'ID_0001',
                                    'ID_C001', 
                                    'ID_D001',
                                    'ID_E001',
                                    'ID_G001',
                                    'ID_H001',
                                    'ID_K001',
                                    'ID_1001',
                                    'ID_9001'
                                ]]
        
        # Creating defaultdicts to receive already downloaded registers and also control downloaded registers
        self.data = dict()
        self.control = dict()

        
        
    def __str__(self) -> str:
        mensage = f"""
        CNPJ: {self.cnpj}
        ------------------------------
        
        """
        return mensage


    def __getattr__(self, name):
        if name in self.data:
            return self.data[name]
        elif name in self._registers:
            reg_info = self._registers[name]
            pk = reg_info['pk']
            fk = reg_info['fk']
            reg_name = reg_info['reg']
            new_reg = reg_info['new_reg']
            func = reg_info['func']

            reg = self._keys if reg_name == '_keys' else getattr(self, reg_name)
            return self.__download_efd(pk=pk, fk=fk, reg=reg, new_reg=new_reg, func=func)
            return self.data[new_reg]
        else:
            raise AttributeError(f"'EFDDatabase' object has no attribute '{name}'")
        

# ------------ REGISTROS ------------- #
    ##################### BLOCK 0 ######################
    @property
    def INFO0000(self):
        if 'INFO0000' in self.data.keys():
            return self.data['INFO0000']
        else:
            return self.__download_efd(pk='ID_0000', fk = 'ID_0000', reg=self._keys, new_reg = 'INFO0000', func = import_efd_0000)
            return self.data['INFO0000']

    @property
    def INFO0001(self):
        if 'INFO0001' in self.data.keys():
            return self.data['INFO0001']
        else:
            return self.__download_efd(pk='ID_0000', fk = 'ID_0001', reg=self.INFO0000, new_reg = 'INFO0001', func = import_efd_0001)
            return self.data['INFO0001']

    @property
    def INFO0002(self):
        if 'INFO0002' in self.data.keys():
            return self.data['INFO0002']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0002', reg=self.INFO0001, new_reg = 'INFO0002', func = import_efd_0002)
            return self.data['INFO0002']

    @property 
    def INFO0005(self):
        if 'INFO0005' in self.data.keys():
            return self.data['INFO0005']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0005', reg=self.INFO0001, new_reg = 'INFO0005', func = import_efd_0005)
            return self.data['INFO0005']

    @property 
    def INFO0015(self):
        if 'INFO0015' in self.data.keys():
            return self.data['INFO0015']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0015', reg=self.INFO0001, new_reg = 'INFO0015', func = import_efd_0015)
            return self.data['INFO0015']

    @property 
    def INFO0100(self):
        if 'INFO0100' in self.data.keys():
            return self.data['INFO0100']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0100', reg=self.INFO0001, new_reg = 'INFO0100', func = import_efd_0100)
            return self.data['INFO0100']
    
    @property 
    def INFO0150(self):
        if 'INFO0150' in self.data.keys():
            return self.data['INFO0150']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0150', reg=self.INFO0001, new_reg = 'INFO0150', func = import_efd_0150)
            return self.data['INFO0150']
        
    @property 
    def INFO0175(self):
        if 'INFO0175' in self.data.keys():
            return self.data['INFO0175']
        else:
            return self.__download_efd(pk='ID_0150', fk = 'ID_0175', reg=self.INFO0150, new_reg = 'INFO0175', func = import_efd_0175)
            return self.data['INFO0175']
    
    @property 
    def INFO0190(self):
        if 'INFO0190' in self.data.keys():
            return self.data['INFO0190']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0190', reg=self.INFO0001, new_reg = 'INFO0190', func = import_efd_0190)
            return self.data['INFO0190']
    
    @property 
    def INFO0200(self):
        if 'INFO0200' in self.data.keys():
            return self.data['INFO0200']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0200', reg=self.INFO0001, new_reg = 'INFO0200', func = import_efd_0200)
            return self.data['INFO0200']
    
    @property 
    def INFO0205(self):
        if 'INFO0205' in self.data.keys():
            return self.data['INFO0205']
        else:
            return self.__download_efd(pk='ID_0200', fk = 'ID_0205', reg=self.INFO0200, new_reg = 'INFO0205', func = import_efd_0205)
            return self.data['INFO0205']
        
    @property 
    def INFO0206(self):
        if 'INFO0206' in self.data.keys():
            return self.data['INFO0206']
        else:
            return self.__download_efd(pk='ID_0200', fk = 'ID_0206', reg=self.INFO0206, new_reg = 'INFO0206', func = import_efd_0206)
            return self.data['INFO0206']

    @property 
    def INFO0210(self):
        if 'INFO0210' in self.data.keys():
            return self.data['INFO0210']
        else:
            return self.__download_efd(pk='ID_0200', fk = 'ID_0210', reg=self.INFO0200, new_reg = 'INFO0210', func = import_efd_0210)
            return self.data['INFO0210']

    @property 
    def INFO0220(self):
        if 'INFO0220' in self.data.keys():
            return self.data['INFO0220']
        else:
            return self.__download_efd(pk='ID_0200', fk = 'ID_0220', reg=self.INFO0200, new_reg = 'INFO0220', func = import_efd_0220)
            return self.data['INFO0220']
    
    @property 
    def INFO0221(self):
        if 'INFO0221' in self.data.keys():
            return self.data['INFO0221']
        else:
            return self.__download_efd(pk='ID_0200', fk = 'ID_0221', reg=self.INFO0200, new_reg = 'INFO0221', func = import_efd_0221)
            return self.data['INFO0221']

    @property 
    def INFO0300(self):
        if 'INFO0300' in self.data.keys():
            return self.data['INFO0300']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0300', reg=self.INFO0001, new_reg = 'INFO0300', func = import_efd_0300)
            return self.data['INFO0300']
        
    @property 
    def INFO0305(self):
        if 'INFO0305' in self.data.keys():
            return self.data['INFO0305']
        else:
            return self.__download_efd(pk='ID_0300', fk = 'ID_0305', reg=self.INFO0300, new_reg = 'INFO0305', func = import_efd_0305)
            return self.data['INFO0305']
        
    @property 
    def INFO0400(self):
        if 'INFO0400' in self.data.keys():
            return self.data['INFO0400']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0400', reg=self.INFO0001, new_reg = 'INFO0400', func = import_efd_0400)
            return self.data['INFO0400']

    @property 
    def INFO0450(self):
        if 'INFO0450' in self.data.keys():
            return self.data['INFO0450']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0450', reg=self.INFO0001, new_reg = 'INFO0450', func = import_efd_0450)
            return self.data['INFO0450']

    @property 
    def INFO0460(self):
        if 'INFO0460' in self.data.keys():
            return self.data['INFO0460']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0460', reg=self.INFO0001, new_reg = 'INFO0460', func = import_efd_0460)
            return self.data['INFO0460']
        
    @property 
    def INFO0500(self):
        if 'INFO0500' in self.data.keys():
            return self.data['INFO0500']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0500', reg=self.INFO0001, new_reg = 'INFO0500', func = import_efd_0500)
            return self.data['INFO0500']
        
    @property 
    def INFO0600(self):
        if 'INFO0600' in self.data.keys():
            return self.data['INFO0600']
        else:
            return self.__download_efd(pk='ID_0001', fk = 'ID_0600', reg=self.INFO0001, new_reg = 'INFO0600', func = import_efd_0600)
            return self.data['INFO0600']
    
    @property 
    def INFO0990(self):
        if 'INFO0990' in self.data.keys():
            return self.data['INFO0990']
        else:
            return self.__download_efd(pk='ID_0000', fk = 'ID_0990', reg=self.INFO0000, new_reg = 'INFO0990', func = import_efd_0990)
            return self.data['INFO0990']


    ##################### BLOCK C ###################### 
    @property 
    def C001(self):
        if 'C001' in self.data.keys():
            return self.data['C001']
        else:
            return self.__download_efd(pk='ID_C001', fk = 'ID_C001', reg=self._keys, new_reg = 'C001', func = import_efd_C001)
            return self.data['C001']

    @property 
    def C100(self):
        if 'C100' in self.data.keys():
            return self.data['C100']
        else:
            return self.__download_efd(pk='ID_C001', fk = 'ID_C100', reg=self._keys, new_reg = 'C100', func = import_efd_C100)
            return self.data['C100']
        
    @property 
    def C101(self):
        if 'C101' in self.data.keys():
            return self.data['C101']
        else:
            return self.__download_efd(pk='ID_C100', fk = 'ID_C101', reg=self.C100, new_reg = 'C101', func = import_efd_C101)
            return self.data['C101']
    
    @property 
    def C105(self):
        if 'C105' in self.data.keys():
            return self.data['C105']
        else:
            return self.__download_efd(pk='ID_C100', fk = 'ID_C105', reg=self.C100, new_reg = 'C105', func = import_efd_C105)
            return self.data['C105']

    @property 
    def C110(self):
        if 'C110' in self.data.keys():
            return self.data['C110']
        else:
            return self.__download_efd(pk='ID_C100', fk = 'ID_C110', reg=self.C100, new_reg = 'C110', func = import_efd_C110)
            return self.data['C110']
    
    @property 
    def C111(self):
        if 'C111' in self.data.keys():
            return self.data['C111']
        else:
            return self.__download_efd(pk='ID_C110', fk = 'ID_C111', reg=self.C110, new_reg = 'C111', func = import_efd_C111)
            return self.data['C111']
    
    @property 
    def C112(self):
        if 'C112' in self.data.keys():
            return self.data['C112']
        else:
            return self.__download_efd(pk='ID_C110', fk = 'ID_C112', reg=self.C110, new_reg = 'C112', func = import_efd_C112)
            return self.data['C112']
    
    @property 
    def C113(self):
        if 'C113' in self.data.keys():
            return self.data['C113']
        else:
            return self.__download_efd(pk='ID_C110', fk = 'ID_C113', reg=self.C110, new_reg = 'C113', func = import_efd_C113)
            return self.data['C113']
    
    @property 
    def C114(self):
        if 'C114' in self.data.keys():
            return self.data['C114']
        else:
            return self.__download_efd(pk='ID_C110', fk='ID_C114', reg=self.C110, new_reg='C114', func=import_efd_C114)
            return self.data['C114']

    @property 
    def C115(self):
        if 'C115' in self.data.keys():
            return self.data['C115']
        else:
            return self.__download_efd(pk='ID_C110', fk='ID_C115', reg=self.C110, new_reg='C115', func=import_efd_C115)
            return self.data['C115']

    @property 
    def C116(self):
        if 'C116' in self.data.keys():
            return self.data['C116']
        else:
            return self.__download_efd(pk='ID_C110', fk='ID_C116', reg=self.C110, new_reg='C116', func=import_efd_C116)
            return self.data['C116']

    @property 
    def C120(self):
        if 'C120' in self.data.keys():
            return self.data['C120']
        else:
            return self.__download_efd(pk='ID_C100', fk='ID_C120', reg=self.C100, new_reg='C120', func=import_efd_C120)
            return self.data['C120']

    @property 
    def C130(self):
        if 'C130' in self.data.keys():
            return self.data['C130']
        else:
            return self.__download_efd(pk='ID_C100', fk='ID_C130', reg=self.C100, new_reg='C130', func=import_efd_C130)
            return self.data['C130']

    @property 
    def C160(self):
        if 'C160' in self.data.keys():
            return self.data['C160']
        else:
            return self.__download_efd(pk='ID_C100', fk='ID_C160', reg=self.C100, new_reg='C160', func=import_efd_C160)
            return self.data['C160']

    @property 
    def C165(self):
        if 'C165' in self.data.keys():
            return self.data['C165']
        else:
            return self.__download_efd(pk='ID_C100', fk='ID_C165', reg=self.C100, new_reg='C165', func=import_efd_C165)
            return self.data['C165']

    @property 
    def C170(self):
        if 'C170' in self.data.keys():
            return self.data['C170']
        else:
            return self.__download_efd(pk='ID_C100', fk = 'ID_C170', reg=self.C100, new_reg = 'C170', func = import_efd_C170)
            return self.data['C170']
    
    @property 
    def C171(self):
        if 'C171' in self.data.keys():
            return self.data['C171']
        else:
            return self.__download_efd(pk='ID_C170', fk = 'ID_C171', reg=self.C170, new_reg = 'C171', func = import_efd_C171)
            return self.data['C171']

    @property 
    def C172(self):
        if 'C172' in self.data.keys():
            return self.data['C172']
        else:
            return self.__download_efd(pk='ID_C170', fk='ID_C172', reg=self.C170, new_reg='C172', func=import_efd_C172)
            return self.data['C172']
        
    @property 
    def C173(self):
        if 'C173' in self.data.keys():
            return self.data['C173']
        else:
            return self.__download_efd(pk='ID_C170', fk = 'ID_C173', reg=self.C170, new_reg = 'C173', func = import_efd_C173)
            return self.data['C173']

    @property 
    def C174(self):
        if 'C174' in self.data.keys():
            return self.data['C174']
        else:
            return self.__download_efd(pk='ID_C170', fk = 'ID_C174', reg=self.C170, new_reg = 'C174', func = import_efd_C174)
            return self.data['C174']

    @property 
    def C175(self):
        if 'C175' in self.data.keys():
            return self.data['C175']
        else:
            return self.__download_efd(pk='ID_C170', fk = 'ID_C175', reg=self.C170, new_reg = 'C175', func = import_efd_C175)
            return self.data['C175']

    @property 
    def C176(self):
        if 'C176' in self.data.keys():
            return self.data['C176']
        else:
            return self.__download_efd(pk='ID_C170', fk='ID_C176', reg=self.C170, new_reg='C176', func=import_efd_C176)
            return self.data['C176']

    @property 
    def C177(self):
        if 'C177' in self.data.keys():
            return self.data['C177']
        else:
            return self.__download_efd(pk='ID_C170', fk='ID_C177', reg=self.C170, new_reg='C177', func=import_efd_C177)
            return self.data['C177']

    @property 
    def C178(self):
        if 'C178' in self.data.keys():
            return self.data['C178']
        else:
            return self.__download_efd(pk='ID_C170', fk='ID_C178', reg=self.C170, new_reg='C178', func=import_efd_C178)
            return self.data['C178']

    @property 
    def C179(self):
        if 'C179' in self.data.keys():
            return self.data['C179']
        else:
            return self.__download_efd(pk='ID_C170', fk='ID_C179', reg=self.C170, new_reg='C179', func=import_efd_C179)
            return self.data['C179']
        
    @property 
    def C180(self):
        if 'C180' in self.data.keys():
            return self.data['C180']
        else:
            return self.__download_efd(pk='ID_C170', fk='ID_C180', reg=self.C170, new_reg='C180', func=import_efd_C180)
            return self.data['C180']

    @property 
    def C181(self):
        if 'C181' in self.data.keys():
            return self.data['C181']
        else:
            return self.__download_efd(pk='ID_C170', fk='ID_C181', reg=self.C170, new_reg='C181', func=import_efd_C181)
            return self.data['C181']

    @property 
    def C185(self):
        if 'C185' in self.data.keys():
            return self.data['C185']
        else:
            return self.__download_efd(pk='ID_C100', fk='ID_C185', reg=self.C100, new_reg='C185', func=import_efd_C185)
            return self.data['C185']

    @property 
    def C186(self):
        if 'C186' in self.data.keys():
            return self.data['C186']
        else:
            return self.__download_efd(pk='ID_C100', fk='ID_C186', reg=self.C100, new_reg='C186', func=import_efd_C186)
            return self.data['C186']

    @property 
    def C190(self):
        """ 
            Registro C190, em que há os valores de ICMS/ BC ICMS/ ICMS-ST/ BC ICMS-ST agregado, por:
            - CST
            - CFOP
            - Alíquota

            Guia Prático
            ------------
            Este registro tem por objetivo representar a escrituração dos documentos fiscais totalizados por CST, CFOP e Alíquota 
            de ICMS. 
            ------------

            Args
            ----------------------
            None
            
            Return
            ----------------------
            efd_C190 pandas.DataFrame

            
            Colunas
            -------
            1.  CST_ICMS: Código da Situação Tributária, conforme a Tabela indicada no item 4.3.1
            2.  CFOP: "Código Fiscal de Operação e Prestação do agrupamento de itens"
            3.  ALIQ_ICMS: Alíquota do ICMS
            4.  VL_OPR: Valor da operação na combinação de CST_ICMS, CFOP e alíquota do ICMS, correspondente ao somatório do valor das mercadorias, despesas acessorias (frete, seguros e outras despesas acessórias), ICMS_ST e IPI.
            5.  VL_BC_ICMS: Parcela correspondente ao Valor da base de cálculo do ICMS" referente a combinação de CST_ICMS, CFOP e alíquota do ICMS."
            6.  VL_ICMS: "Parcela correspondente ao ""Valor do ICMS"", incluindo o FCP, quando aplicável, referente à combinação de CST_ICMS, CFOP e alíquota do ICMS"
            7.  VL_BC_ICMS_ST: "Parcela correspondente ao ""Valor da base de cálculo do ICMS"" da substituição tributária referente à combinação de CST_ICMS, CFOP e alíquota do ICMS."
            8.  VL_ICMS_ST: "Parcela correspondente ao valor creditado/debitado do ICMS da substituição tributária, incluindo o FCP_ ST, quando aplicável, referente à combinação de CST_ICMS, CFOP, e alíquota do ICMS."
            9.  VL_RED_BC: Valor não tributado em função da redução da base de cálculo do ICMS, referente a combinação de CST_ICMS, CFOP e alíquota do ICMS.
            10. COD_OBS: "Código da observação do lancamento fiscal (campo 02 do Registro 0460)"

            """
        if 'C190' in self.data.keys():
            return self.data['C190']
        else:
            return self.__download_efd(pk='ID_C100', fk = 'ID_C190', reg=self.C100, new_reg = 'C190', func = import_efd_C190)
            return self.data['C190']
    
    @property 
    def C191(self):
        if 'C191' in self.data.keys():
            return self.data['C191']
        else:
            return self.__download_efd(pk='ID_C190', fk='ID_C191', reg=self.C190, new_reg='C191', func=import_efd_C191)
            return self.data['C191']
        
    @property 
    def C195(self):
        if 'C195' in self.data.keys():
            return self.data['C195']
        else:
            return self.__download_efd(pk='ID_C100', fk = 'ID_C195', reg=self.C100, new_reg = 'C195', func = import_efd_C195)
            return self.data['C195']
    
    @property 
    def C197(self):
        if 'C197' in self.data.keys():
            return self.data['C197']
        else:
            return self.__download_efd(pk='ID_C195', fk = 'ID_C197', reg=self.C195, new_reg = 'C197', func = import_efd_C197)
            return self.data['C197']

    @property 
    def C500(self):
        if 'C500' in self.data.keys():
            return self.data['C500']
        else:
            return self.__download_efd(pk='ID_C001', fk = 'ID_C500', reg=self._keys, new_reg = 'C500', func = import_efd_C500)
            return self.data['C500']
    
    @property 
    def C510(self):
        if 'C510' in self.data.keys():
            return self.data['C510']
        else:
            return self.__download_efd(pk='ID_C500', fk='ID_C510', reg=self.C500, new_reg='C510', func=import_efd_C510)
            return self.data['C510']

    @property 
    def C590(self):
        if 'C590' in self.data.keys():
            return self.data['C590']
        else:
            return self.__download_efd(pk='ID_C500', fk = 'ID_C590', reg=self.C500, new_reg = 'C590', func = import_efd_C590)
            return self.data['C590']

    @property 
    def C591(self):
        if 'C591' in self.data.keys():
            return self.data['C591']
        else:
            return self.__download_efd(pk='ID_C590', fk='ID_C591', reg=self.C590, new_reg='C591', func=import_efd_C591)
            return self.data['C591']

    @property 
    def C595(self):
        if 'C595' in self.data.keys():
            return self.data['C595']
        else:
            return self.__download_efd(pk='ID_C591', fk='ID_C595', reg=self.C591, new_reg='C595', func=import_efd_C595)
            return self.data['C595']

    @property 
    def C597(self):
        if 'C597' in self.data.keys():
            return self.data['C597']
        else:
            return self.__download_efd(pk='ID_C595', fk='ID_C597', reg=self.C595, new_reg='C597', func=import_efd_C597)
            return self.data['C597']
        
    @property 
    def C600(self):
        if 'C600' in self.data.keys():
            return self.data['C600']
        else:
            return self.__download_efd(pk='ID_C001', fk = 'ID_C600', reg=self._keys, new_reg = 'C600', func = import_efd_C600)
            return self.data['C600']

    @property 
    def C601(self):
        if 'C601' in self.data.keys():
            return self.data['C601']
        else:
            return self.__download_efd(pk='ID_C600', fk='ID_C601', reg=self.C600, new_reg='C601', func=import_efd_C601)
            return self.data['C601']
        
    @property 
    def C610(self):
        if 'C610' in self.data.keys():
            return self.data['C610']
        else:
            return self.__download_efd(pk='ID_C600', fk = 'ID_C610', reg=self.C600, new_reg = 'C610', func = import_efd_C610)
            return self.data['C610']

    @property 
    def C690(self):
        if 'C690' in self.data.keys():
            return self.data['C690']
        else:
            return self.__download_efd(pk='ID_C600', fk = 'ID_C690', reg=self.C600, new_reg = 'C690', func = import_efd_C690)
            return self.data['C690']

    @property 
    def C700(self):
        if 'C700' in self.data.keys():
            return self.data['C700']
        else:
            return self.__download_efd(pk='ID_C001', fk = 'ID_C700', reg=self._keys, new_reg = 'C700', func = import_efd_C700)
            return self.data['C700']

    @property 
    def C790(self):
        if 'C790' in self.data.keys():
            return self.data['C790']
        else:
            return self.__download_efd(pk='ID_C700', fk = 'ID_C790', reg=self.C700, new_reg = 'C790', func = import_efd_C790)
            return self.data['C790']
        
    @property 
    def C791(self):
        if 'C791' in self.data.keys():
            return self.data['C791']
        else:
            return self.__download_efd(pk='ID_C790', fk='ID_C791', reg=self.C790, new_reg='C791', func=import_efd_C791)
            return self.data['C791']

    @property 
    def C990(self):
        if 'C990' in self.data.keys():
            return self.data['C990']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_C990', reg=self._keys, new_reg='C990', func=import_efd_C990)
            return self.data['C990']


    ##################### BLOCK D #####################

    @property 
    def D001(self):
        if 'D001' in self.data.keys():
            return self.data['D001']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_D001', reg=self._keys, new_reg='D001', func=import_efd_D001)
            return self.data['D001']

    @property 
    def D100(self):
        if 'D100' in self.data.keys():
            return self.data['D100']
        else:
            return self.__download_efd(pk='ID_D001', fk='ID_D100', reg=self.D001, new_reg='D100', func=import_efd_D100)
            return self.data['D100']
        
    @property 
    def D101(self):
        if 'D101' in self.data.keys():
            return self.data['D101']
        else:
            return self.__download_efd(pk='ID_D100', fk='ID_D101', reg=self.D100, new_reg='D101', func=import_efd_D101)
            return self.data['D101']
        
    @property 
    def D190(self):
        if 'D190' in self.data.keys():
            return self.data['D190']
        else:
            return self.__download_efd(pk='ID_D100', fk='ID_D190', reg=self.D100, new_reg='D190', func=import_efd_D190)
            return self.data['D190']
    
    @property 
    def D195(self):
        if 'D195' in self.data.keys():
            return self.data['D195']
        else:
            return self.__download_efd(pk='ID_D100', fk='ID_D195', reg=self.D100, new_reg='D195', func=import_efd_D195)
            return self.data['D195']
        
    @property 
    def D197(self):
        if 'D197' in self.data.keys():
            return self.data['D197']
        else:
            return self.__download_efd(pk='ID_D195', fk='ID_D197', reg=self.D195, new_reg='D197', func=import_efd_D197)
            return self.data['D197']

    @property 
    def D500(self):
        if 'D500' in self.data.keys():
            return self.data['D500']
        else:
            return self.__download_efd(pk='ID_D001', fk='ID_D500', reg=self.D001, new_reg='D500', func=import_efd_D500)
            return self.data['D500']

    @property 
    def D510(self):
        if 'D510' in self.data.keys():
            return self.data['D510']
        else:
            return self.__download_efd(pk='ID_D500', fk='ID_D510', reg=self.D500, new_reg='D510', func=import_efd_D510)
            return self.data['D510']

    @property 
    def D530(self):
        if 'D530' in self.data.keys():
            return self.data['D530']
        else:
            return self.__download_efd(pk='ID_D500', fk='ID_D530', reg=self.D500, new_reg='D530', func=import_efd_D530)
            return self.data['D530']

    @property 
    def D590(self):
        if 'D590' in self.data.keys():
            return self.data['D590']
        else:
            return self.__download_efd(pk='ID_D500', fk='ID_D590', reg=self.D500, new_reg='D590', func=import_efd_D590)
            return self.data['D590']
        
    @property 
    def D600(self):
        if 'D600' in self.data.keys():
            return self.data['D600']
        else:
            return self.__download_efd(pk='ID_D001', fk='ID_D600', reg=self.D001, new_reg='D600', func=import_efd_D600)
            return self.data['D600']

    @property 
    def D610(self):
        if 'D610' in self.data.keys():
            return self.data['D610']
        else:
            return self.__download_efd(pk='ID_D600', fk='ID_D610', reg=self.D600, new_reg='D610', func=import_efd_D610)
            return self.data['D610']
    
    @property 
    def D690(self):
        if 'D690' in self.data.keys():
            return self.data['D690']
        else:
            return self.__download_efd(pk='ID_D600', fk='ID_D690', reg=self.D600, new_reg='D690', func=import_efd_D690)
            return self.data['D690']

    @property 
    def D695(self):
        if 'D695' in self.data.keys():
            return self.data['D695']
        else:
            return self.__download_efd(pk='ID_D001', fk='ID_D695', reg=self.D001, new_reg='D695', func=import_efd_D695)
            return self.data['D695']

    @property 
    def D696(self):
        if 'D696' in self.data.keys():
            return self.data['D696']
        else:
            return self.__download_efd(pk='ID_D695', fk='ID_D696', reg=self.D695, new_reg='D696', func=import_efd_D696)
            return self.data['D696']
        
    @property 
    def D697(self):
        if 'D697' in self.data.keys():
            return self.data['D697']
        else:
            return self.__download_efd(pk='ID_D696', fk='ID_D697', reg=self.D696, new_reg='D697', func=import_efd_D697)
            return self.data['D697']

    @property 
    def D700(self):
        if 'D700' in self.data.keys():
            return self.data['D700']
        else:
            return self.__download_efd(pk='ID_D001', fk='ID_D700', reg=self.D001, new_reg='D700', func=import_efd_D700)
            return self.data['D700']

    @property 
    def D730(self):
        if 'D730' in self.data.keys():
            return self.data['D730']
        else:
            return self.__download_efd(pk='ID_D700', fk='ID_D730', reg=self.D700, new_reg='D730', func=import_efd_D730)
            return self.data['D730']

    @property 
    def D731(self):
        if 'D731' in self.data.keys():
            return self.data['D731']
        else:
            return self.__download_efd(pk='ID_D730', fk='ID_D731', reg=self.D730, new_reg='D731', func=import_efd_D731)
            return self.data['D731']

    @property 
    def D735(self):
        if 'D735' in self.data.keys():
            return self.data['D735']
        else:
            return self.__download_efd(pk='ID_D700', fk='ID_D735', reg=self.D700, new_reg='D735', func=import_efd_D735)
            return self.data['D735']
            
    @property 
    def D737(self):
        if 'D737' in self.data.keys():
            return self.data['D737']
        else:
            return self.__download_efd(pk='ID_D735', fk='ID_D737', reg=self.D735, new_reg='D737', func=import_efd_D737)
            return self.data['D737']
        
    @property 
    def D750(self):
        if 'D750' in self.data.keys():
            return self.data['D750']
        else:
            return self.__download_efd(pk='ID_D001', fk='ID_D750', reg=self.D001, new_reg='D750', func=import_efd_D750)
            return self.data['D750']

    @property 
    def D760(self):
        if 'D760' in self.data.keys():
            return self.data['D760']
        else:
            return self.__download_efd(pk='ID_D750', fk='ID_D760', reg=self.D750, new_reg='D760', func=import_efd_D760)
            return self.data['D760']

    @property 
    def D761(self):
        if 'D761' in self.data.keys():
            return self.data['D761']
        else:
            return self.__download_efd(pk='ID_D760', fk='ID_D761', reg=self.D760, new_reg='D761', func=import_efd_D761)
            return self.data['D761']

    @property 
    def D990(self):
        if 'D990' in self.data.keys():
            return self.data['D990']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_D990', reg=self._keys, new_reg='D990', func=import_efd_D990)
            return self.data['D990']

    ##################### BLOCK E ######################

    @property 
    def E001(self):
        if 'E001' in self.data.keys():
            return self.data['E001']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_E001', reg=self._keys, new_reg='E001', func=import_efd_E001)
            return self.data['E001']

    @property 
    def E100(self):
        if 'E100' in self.data.keys():
            return self.data['E100']
        else:
            return self.__download_efd(pk='ID_E001', fk='ID_E100', reg=self.E001, new_reg='E100', func=import_efd_E100)
            return self.data['E100']

    @property 
    def E110(self):
        if 'E110' in self.data.keys():
            return self.data['E110']
        else:
            return self.__download_efd(pk='ID_E100', fk='ID_E110', reg=self.E100, new_reg='E110', func=import_efd_E110)
            return self.data['E110']

    @property 
    def E111(self):
        if 'E111' in self.data.keys():
            return self.data['E111']
        else:
            return self.__download_efd(pk='ID_E110', fk='ID_E111', reg=self.E110, new_reg='E111', func=import_efd_E111)
            return self.data['E111']

    @property 
    def E112(self):
        if 'E112' in self.data.keys():
            return self.data['E112']
        else:
            return self.__download_efd(pk='ID_E111', fk='ID_E112', reg=self.E111, new_reg='E112', func=import_efd_E112)
            return self.data['E112']

    @property 
    def E113(self):
        if 'E113' in self.data.keys():
            return self.data['E113']
        else:
            return self.__download_efd(pk='ID_E111', fk='ID_E113', reg=self.E111, new_reg='E113', func=import_efd_E113)
            return self.data['E113']

    @property 
    def E115(self):
        if 'E115' in self.data.keys():
            return self.data['E115']
        else:
            return self.__download_efd(pk='ID_E110', fk='ID_E115', reg=self.E110, new_reg='E115', func=import_efd_E115)
            return self.data['E115']
        
    @property 
    def E116(self):
        if 'E116' in self.data.keys():
            return self.data['E116']
        else:
            return self.__download_efd(pk='ID_E110', fk='ID_E116', reg=self.E110, new_reg='E116', func=import_efd_E116)
            return self.data['E116']

    @property 
    def E200(self):
        if 'E200' in self.data.keys():
            return self.data['E200']
        else:
            return self.__download_efd(pk='ID_E001', fk='ID_E200', reg=self.E001, new_reg='E200', func=import_efd_E200)
            return self.data['E200']

    @property 
    def E210(self):
        if 'E210' in self.data.keys():
            return self.data['E210']
        else:
            return self.__download_efd(pk='ID_E200', fk='ID_E210', reg=self.E200, new_reg='E210', func=import_efd_E210)
            return self.data['E210']
        
    @property 
    def E220(self):
        if 'E220' in self.data.keys():
            return self.data['E220']
        else:
            return self.__download_efd(pk='ID_E210', fk='ID_E220', reg=self.E210, new_reg='E220', func=import_efd_E220)
            return self.data['E220']
        
    @property 
    def E230(self):
        if 'E230' in self.data.keys():
            return self.data['E230']
        else:
            return self.__download_efd(pk='ID_E220', fk='ID_E230', reg=self.E220, new_reg='E230', func=import_efd_E230)
            return self.data['E230']
        
    @property 
    def E240(self):
        if 'E240' in self.data.keys():
            return self.data['E240']
        else:
            return self.__download_efd(pk='ID_E220', fk='ID_E240', reg=self.E220, new_reg='E240', func=import_efd_E240)
            return self.data['E240']
        
    @property 
    def E250(self):
        if 'E250' in self.data.keys():
            return self.data['E250']
        else:
            return self.__download_efd(pk='ID_E210', fk='ID_E250', reg=self.E210, new_reg='E250', func=import_efd_E250)
            return self.data['E250']

    @property 
    def E300(self):
        if 'E300' in self.data.keys():
            return self.data['E300']
        else:
            return self.__download_efd(pk='ID_E001', fk='ID_E300', reg=self.E001, new_reg='E300', func=import_efd_E300)
            return self.data['E300']
    
    @property 
    def E310(self):
        if 'E310' in self.data.keys():
            return self.data['E310']
        else:
            return self.__download_efd(pk='ID_E300', fk='ID_E310', reg=self.E300, new_reg='E310', func=import_efd_E310)
            return self.data['E310']

    @property 
    def E311(self):
        if 'E311' in self.data.keys():
            return self.data['E311']
        else:
            return self.__download_efd(pk='ID_E310', fk='ID_E311', reg=self.E310, new_reg='E311', func=import_efd_E311)
            return self.data['E311']
        
    @property 
    def E312(self):
        if 'E312' in self.data.keys():
            return self.data['E312']
        else:
            return self.__download_efd(pk='ID_E311', fk='ID_E312', reg=self.E311, new_reg='E312', func=import_efd_E312)
            return self.data['E312']
        
    @property 
    def E313(self):
        if 'E313' in self.data.keys():
            return self.data['E313']
        else:
            return self.__download_efd(pk='ID_E311', fk='ID_E313', reg=self.E311, new_reg='E313', func=import_efd_E313)
            return self.data['E313']
        
    @property 
    def E316(self):
        if 'E316' in self.data.keys():
            return self.data['E316']
        else:
            return self.__download_efd(pk='ID_E310', fk='ID_E316', reg=self.E310, new_reg='E316', func=import_efd_E316)
            return self.data['E316']

    @property 
    def E500(self):
        if 'E500' in self.data.keys():
            return self.data['E500']
        else:
            return self.__download_efd(pk='ID_E001', fk='ID_E500', reg=self.E001, new_reg='E500', func=import_efd_E500)
            return self.data['E500']
    
    @property 
    def E510(self):
        if 'E510' in self.data.keys():
            return self.data['E510']
        else:
            return self.__download_efd(pk='ID_E500', fk='ID_E510', reg=self.E500, new_reg='E510', func=import_efd_E510)
            return self.data['E510']
          
    @property 
    def E520(self):
        if 'E520' in self.data.keys():
            return self.data['E520']
        else:
            return self.__download_efd(pk='ID_E500', fk='ID_E520', reg=self.E500, new_reg='E520', func=import_efd_E520)
            return self.data['E520']
        
    @property 
    def E530(self):
        if 'E530' in self.data.keys():
            return self.data['E530']
        else:
            return self.__download_efd(pk='ID_E520', fk='ID_E530', reg=self.E520, new_reg='E530', func=import_efd_E530)
            return self.data['E530']

    @property 
    def E531(self):
        if 'E531' in self.data.keys():
            return self.data['E531']
        else:
            return self.__download_efd(pk='ID_E530', fk='ID_E531', reg=self.E530, new_reg='E531', func=import_efd_E531)
            return self.data['E531']

    @property 
    def E990(self):
        if 'E990' in self.data.keys():
            return self.data['E990']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_E990', reg=self._keys, new_reg='E990', func=import_efd_E990)
            return self.data['E990']



    ##################### BLOCK G ######################
    @property 
    def G001(self):
        if 'G001' in self.data.keys():
            return self.data['G001']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_G001', reg=self._keys, new_reg='G001', func=import_efd_G001)
            return self.data['G001']
        
    @property 
    def G110(self):
        if 'G110' in self.data.keys():
            return self.data['G110']
        else:
            return self.__download_efd(pk='ID_G001', fk='ID_G110', reg=self.G001, new_reg='G110', func=import_efd_G110)
            return self.data['G110']

    @property 
    def G125(self):
        if 'G125' in self.data.keys():
            return self.data['G125']
        else:
            return self.__download_efd(pk='ID_G110', fk='ID_G125', reg=self.G110, new_reg='G125', func=import_efd_G125)
            return self.data['G125']
        
    @property 
    def G126(self):
        if 'G126' in self.data.keys():
            return self.data['G126']
        else:
            return self.__download_efd(pk='ID_G125', fk='ID_G126', reg=self.G125, new_reg='G126', func=import_efd_G126)
            return self.data['G126']
        
    @property 
    def G130(self):
        if 'G130' in self.data.keys():
            return self.data['G130']
        else:
            return self.__download_efd(pk='ID_G125', fk='ID_G130', reg=self.G125, new_reg='G130', func=import_efd_G130)
            return self.data['G130']

    @property 
    def G140(self):
        if 'G140' in self.data.keys():
            return self.data['G140']
        else:
            return self.__download_efd(pk='ID_G130', fk='ID_G140', reg=self.G130, new_reg='G140', func=import_efd_G140)
            return self.data['G140']

    @property 
    def G990(self):
        if 'G990' in self.data.keys():
            return self.data['G990']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_G990', reg=self._keys, new_reg='G990', func=import_efd_G990)
            return self.data['G990']


    ##################### BLOCK H ######################

    @property 
    def H001(self):
        if 'H001' in self.data.keys():
            return self.data['H001']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_H001', reg=self._keys, new_reg='H001', func=import_efd_H001)
            return self.data['H001']
        
    @property 
    def H005(self):
        if 'H005' in self.data.keys():
            return self.data['H005']
        else:
            return self.__download_efd(pk='ID_H001', fk='ID_H005', reg=self.H001, new_reg='H005', func=import_efd_H005)
            return self.data['H005']
        
    @property 
    def H010(self):
        if 'H010' in self.data.keys():
            return self.data['H010']
        else:
            return self.__download_efd(pk='ID_H005', fk='ID_H010', reg=self.H005, new_reg='H010', func=import_efd_H010)
            return self.data['H010']
        
    @property 
    def H020(self):
        if 'H020' in self.data.keys():
            return self.data['H020']
        else:
            return self.__download_efd(pk='ID_H010', fk='ID_H020', reg=self.H010, new_reg='H020', func=import_efd_H020)
            return self.data['H020']
        
    @property 
    def H030(self):
        if 'H030' in self.data.keys():
            return self.data['H030']
        else:
            return self.__download_efd(pk='ID_H010', fk='ID_H030', reg=self.H010, new_reg='H030', func=import_efd_H030)
            return self.data['H030']
        
    @property 
    def H990(self):
        if 'H990' in self.data.keys():
            return self.data['H990']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_H990', reg=self._keys, new_reg='H990', func=import_efd_H990)
            return self.data['H990']


    ##################### BLOCK K ######################

    @property 
    def K001(self):
        if 'K001' in self.data.keys():
            return self.data['K001']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_K001', reg=self._keys, new_reg='K001', func=import_efd_K001)
            return self.data['K001']
        
    @property 
    def K010(self):
        if 'K010' in self.data.keys():
            return self.data['K010']
        else:
            return self.__download_efd(pk='ID_K001', fk='ID_K010', reg=self.K001, new_reg='K010', func=import_efd_K010)
            return self.data['K010']
        
    @property 
    def K100(self):
        if 'K100' in self.data.keys():
            return self.data['K100']
        else:
            return self.__download_efd(pk='ID_K001', fk='ID_K100', reg=self.K001, new_reg='K100', func=import_efd_K100)
            return self.data['K100']
        
    @property 
    def K200(self):
        if 'K200' in self.data.keys():
            return self.data['K200']
        else:
            return self.__download_efd(pk='ID_K100', fk='ID_K200', reg=self.K100, new_reg='K200', func=import_efd_K200)
            return self.data['K200']
    
    @property 
    def K210(self):
        if 'K210' in self.data.keys():
            return self.data['K210']
        else:
            return self.__download_efd(pk='ID_K100', fk='ID_K210', reg=self.K100, new_reg='K210', func=import_efd_K210)
            return self.data['K210']
        
    @property 
    def K215(self):
        if 'K215' in self.data.keys():
            return self.data['K215']
        else:
            return self.__download_efd(pk='ID_K210', fk='ID_K215', reg=self.K210, new_reg='K215', func=import_efd_K215)
            return self.data['K215']

    @property 
    def K220(self):
        if 'K220' in self.data.keys():
            return self.data['K220']
        else:
            return self.__download_efd(pk='ID_K100', fk='ID_K220', reg=self.K100, new_reg='K220', func=import_efd_K220)
            return self.data['K220']
        
    @property 
    def K230(self):
        if 'K230' in self.data.keys():
            return self.data['K230']
        else:
            return self.__download_efd(pk='ID_K100', fk='ID_K230', reg=self.K100, new_reg='K230', func=import_efd_K230)
            return self.data['K230']
        
    @property 
    def K235(self):
        if 'K235' in self.data.keys():
            return self.data['K235']
        else:
            return self.__download_efd(pk='ID_K230', fk='ID_K235', reg=self.K230, new_reg='K235', func=import_efd_K235)
            return self.data['K235']
        
    @property 
    def K250(self):
        if 'K250' in self.data.keys():
            return self.data['K250']
        else:
            return self.__download_efd(pk='ID_K100', fk='ID_K250', reg=self.K100, new_reg='K250', func=import_efd_K250)
            return self.data['K250']
        
    @property 
    def K255(self):
        if 'K255' in self.data.keys():
            return self.data['K255']
        else:
            return self.__download_efd(pk='ID_K250', fk='ID_K255', reg=self.K250, new_reg='K255', func=import_efd_K255)
            return self.data['K255']

    @property 
    def K260(self):
        if 'K260' in self.data.keys():
            return self.data['K260']
        else:
            return self.__download_efd(pk='ID_K100', fk='ID_K260', reg=self.K100, new_reg='K260', func=import_efd_K260)
            return self.data['K260']
           
    @property 
    def K265(self):
        if 'K265' in self.data.keys():
            return self.data['K265']
        else:
            return self.__download_efd(pk='ID_K260', fk='ID_K265', reg=self.K260, new_reg='K265', func=import_efd_K265)
            return self.data['K265']
        
    @property 
    def K270(self):
        if 'K270' in self.data.keys():
            return self.data['K270']
        else:
            return self.__download_efd(pk='ID_K100', fk='ID_K270', reg=self.K100, new_reg='K270', func=import_efd_K270)
            return self.data['K270']
        
    @property 
    def K275(self):
        if 'K275' in self.data.keys():
            return self.data['K275']
        else:
            return self.__download_efd(pk='ID_K270', fk='ID_K275', reg=self.K270, new_reg='K275', func=import_efd_K275)
            return self.data['K275']
        
    @property 
    def K280(self):
        if 'K280' in self.data.keys():
            return self.data['K280']
        else:
            return self.__download_efd(pk='ID_K100', fk='ID_K280', reg=self.K100, new_reg='K280', func=import_efd_K280)
            return self.data['K280']
        
    @property 
    def K290(self):
        if 'K290' in self.data.keys():
            return self.data['K290']
        else:
            return self.__download_efd(pk='ID_K100', fk='ID_K290', reg=self.K100, new_reg='K290', func=import_efd_K290)
            return self.data['K290']

    @property 
    def K291(self):
        if 'K291' in self.data.keys():
            return self.data['K291']
        else:
            return self.__download_efd(pk='ID_K290', fk='ID_K291', reg=self.K290, new_reg='K291', func=import_efd_K291)
            return self.data['K291']
        
    @property 
    def K292(self):
        if 'K292' in self.data.keys():
            return self.data['K292']
        else:
            return self.__download_efd(pk='ID_K290', fk='ID_K292', reg=self.K290, new_reg='K292', func=import_efd_K292)
            return self.data['K292']
        
    @property 
    def K300(self):
        if 'K300' in self.data.keys():
            return self.data['K300']
        else:
            return self.__download_efd(pk='ID_K100', fk='ID_K300', reg=self.K100, new_reg='K300', func=import_efd_K300)
            return self.data['K300']

    @property 
    def K301(self):
        if 'K301' in self.data.keys():
            return self.data['K301']
        else:
            return self.__download_efd(pk='ID_K300', fk='ID_K301', reg=self.K300, new_reg='K301', func=import_efd_K301)
            return self.data['K301']
        
    @property 
    def K302(self):
        if 'K302' in self.data.keys():
            return self.data['K302']
        else:
            return self.__download_efd(pk='ID_K300', fk='ID_K302', reg=self.K300, new_reg='K302', func=import_efd_K302)
            return self.data['K302']

    @property 
    def K990(self):
        if 'K990' in self.data.keys():
            return self.data['K990']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_K990', reg=self._keys, new_reg='K990', func=import_efd_K990)
            return self.data['K990']



    ##################### BLOCK 1 ######################
    @property 
    def INFO1001(self):
        if '1001' in self.data.keys():
            return self.data['1001']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_1001', reg=self._keys, new_reg='1001', func=import_efd_1001)
            return self.data['1001']
        
    @property 
    def INFO1010(self):
        if '1010' in self.data.keys():
            return self.data['1010']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1010', reg=self.INFO1001, new_reg='1010', func=import_efd_1010)
            return self.data['1010']

    @property 
    def INFO1100(self):
        if '1100' in self.data.keys():
            return self.data['1100']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1100', reg=self.INFO1001, new_reg='1100', func=import_efd_1100)
            return self.data['1100']
        
    @property 
    def INFO1105(self):
        if '1105' in self.data.keys():
            return self.data['1105']
        else:
            return self.__download_efd(pk='ID_1100', fk='ID_1105', reg=self.INFO1100, new_reg='1105', func=import_efd_1105)
            return self.data['1105']
        
    @property 
    def INFO1110(self):
        if '1110' in self.data.keys():
            return self.data['1110']
        else:
            return self.__download_efd(pk='ID_1105', fk='ID_1110', reg=self.INFO1105, new_reg='1110', func=import_efd_1110)
            return self.data['1110']
        
    @property 
    def INFO1200(self):
        if '1200' in self.data.keys():
            return self.data['1200']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1200', reg=self.INFO1001, new_reg='1200', func=import_efd_1200)
            return self.data['1200']
        
    @property 
    def INFO1210(self):
        if '1210' in self.data.keys():
            return self.data['1210']
        else:
            return self.__download_efd(pk='ID_1200', fk='ID_1210', reg=self.INFO1200, new_reg='1210', func=import_efd_1210)
            return self.data['1210']
        
    @property 
    def INFO1250(self):
        if '1250' in self.data.keys():
            return self.data['1250']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1250', reg=self.INFO1001, new_reg='1250', func=import_efd_1250)
            return self.data['1250']
        
    @property 
    def INFO1255(self):
        if '1255' in self.data.keys():
            return self.data['1255']
        else:
            return self.__download_efd(pk='ID_1250', fk='ID_1255', reg=self.INFO1250, new_reg='1255', func=import_efd_1255)
            return self.data['1255']


    @property 
    def INFO1300(self):
        if '1300' in self.data.keys():
            return self.data['1300']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1300', reg=self.INFO1001, new_reg='1300', func=import_efd_1300)
            return self.data['1300']
        
    @property 
    def INFO1310(self):
        if '1310' in self.data.keys():
            return self.data['1310']
        else:
            return self.__download_efd(pk='ID_1300', fk='ID_1310', reg=self.INFO1300, new_reg='1310', func=import_efd_1310)
            return self.data['1310']
    
    @property 
    def INFO1320(self):
        if '1320' in self.data.keys():
            return self.data['1320']
        else:
            return self.__download_efd(pk='ID_1310', fk='ID_1320', reg=self.INFO1310, new_reg='1320', func=import_efd_1320)
            return self.data['1320']
        
    
    @property 
    def INFO1350(self):
        if '1350' in self.data.keys():
            return self.data['1350']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1350', reg=self.INFO1001, new_reg='1350', func=import_efd_1350)
            return self.data['1350']
        
    @property 
    def INFO1360(self):
        raise NotImplementedError("Erro no relacionamento entre tabelas! Verifique o relatório 07 de homologação!")
        if '1360' in self.data.keys():
            return self.data['1360']
        else:
            return self.__download_efd(pk='ID_1350', fk='ID_1360', reg=self.INFO1350, new_reg='1360', func=import_efd_1360)
            return self.data['1360']

    @property 
    def INFO1370(self):
        if '1370' in self.data.keys():
            return self.data['1370']
        else:
            return self.__download_efd(pk='ID_1350', fk='ID_1370', reg=self.INFO1350, new_reg='1370', func=import_efd_1370)
            return self.data['1370']
        
    @property 
    def INFO1390(self):
        if '1390' in self.data.keys():
            return self.data['1390']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1390', reg=self.INFO1001, new_reg='1390', func=import_efd_1390)
            return self.data['1390']
        
    @property 
    def INFO1391(self):
        if '1391' in self.data.keys():
            return self.data['1391']
        else:
            return self.__download_efd(pk='ID_1390', fk='ID_1391', reg=self.INFO1390, new_reg='1391', func=import_efd_1391)
            return self.data['1391']
    
    @property 
    def INFO1400(self):
        if '1400' in self.data.keys():
            return self.data['1400']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1400', reg=self.INFO1001, new_reg='1400', func=import_efd_1400)
            return self.data['1400']
        
    @property 
    def INFO1500(self):
        if '1500' in self.data.keys():
            return self.data['1500']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1500', reg=self.INFO1001, new_reg='1500', func=import_efd_1500)
            return self.data['1500']

    @property 
    def INFO1510(self):
        if '1510' in self.data.keys():
            return self.data['1510']
        else:
            return self.__download_efd(pk='ID_1500', fk='ID_1510', reg=self.INFO1500, new_reg='1510', func=import_efd_1510)
            return self.data['1510']
        
    @property 
    def INFO1600(self):
        if '1600' in self.data.keys():
            return self.data['1600']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1600', reg=self.INFO1001, new_reg='1600', func=import_efd_1600)
            return self.data['1600']
        

    @property 
    def INFO1601(self):
        if '1601' in self.data.keys():
            return self.data['1601']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1601', reg=self.INFO1001, new_reg='1601', func=import_efd_1601)
            return self.data['1601']
        
    @property 
    def INFO1700(self):
        if '1700' in self.data.keys():
            return self.data['1700']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1700', reg=self.INFO1001, new_reg='1700', func=import_efd_1700)
            return self.data['1700']
        
    @property 
    def INFO1710(self):
        if '1710' in self.data.keys():
            return self.data['1710']
        else:
            return self.__download_efd(pk='ID_1700', fk='ID_1710', reg=self.INFO1700, new_reg='1710', func=import_efd_1710)
            return self.data['1710']
        
    @property 
    def INFO1800(self):
        if '1800' in self.data.keys():
            return self.data['1800']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1800', reg=self.INFO1001, new_reg='1800', func=import_efd_1800)
            return self.data['1800']
        
    @property 
    def INFO1900(self):
        if '1900' in self.data.keys():
            return self.data['1900']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1900', reg=self.INFO1001, new_reg='1900', func=import_efd_1900)
            return self.data['1900']
        
    @property 
    def INFO1910(self):
        if '1910' in self.data.keys():
            return self.data['1910']
        else:
            return self.__download_efd(pk='ID_1900', fk='ID_1910', reg=self.INFO1900, new_reg='1910', func=import_efd_1910)
            return self.data['1910']
        
    @property 
    def INFO1920(self):
        if '1920' in self.data.keys():
            return self.data['1920']
        else:
            return self.__download_efd(pk='ID_1910', fk='ID_1920', reg=self.INFO1910, new_reg='1920', func=import_efd_1920)
            return self.data['1920']
        
    @property 
    def INFO1921(self):
        if '1921' in self.data.keys():
            return self.data['1921']
        else:
            return self.__download_efd(pk='ID_1920', fk='ID_1921', reg=self.INFO1920, new_reg='1921', func=import_efd_1921)
            return self.data['1921']
        
    @property 
    def INFO1922(self):
        if '1922' in self.data.keys():
            return self.data['1922']
        else:
            return self.__download_efd(pk='ID_1921', fk='ID_1922', reg=self.INFO1921, new_reg='1922', func=import_efd_1922)
            return self.data['1922']
        
    @property 
    def INFO1923(self):
        if '1923' in self.data.keys():
            return self.data['1923']
        else:
            return self.__download_efd(pk='ID_1921', fk='ID_1923', reg=self.INFO1921, new_reg='1923', func=import_efd_1923)
            return self.data['1923']
        

    @property 
    def INFO1925(self):
        if '1925' in self.data.keys():
            return self.data['1925']
        else:
            return self.__download_efd(pk='ID_1920', fk='ID_1925', reg=self.INFO1920, new_reg='1925', func=import_efd_1925)
            return self.data['1925']
        
    @property 
    def INFO1926(self):
        if '1926' in self.data.keys():
            return self.data['1926']
        else:
            return self.__download_efd(pk='ID_1920', fk='ID_1926', reg=self.INFO1920, new_reg='1926', func=import_efd_1926)
            return self.data['1926']

    @property 
    def INFO1960(self):
        if '1960' in self.data.keys():
            return self.data['1960']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1960', reg=self.INFO1001, new_reg='1960', func=import_efd_1960)
            return self.data['1960']

    @property 
    def INFO1970(self):
        if '1970' in self.data.keys():
            return self.data['1970']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1970', reg=self.INFO1001, new_reg='1970', func=import_efd_1970)
            return self.data['1970']

    @property 
    def INFO1975(self):
        if '1975' in self.data.keys():
            return self.data['1975']
        else:
            return self.__download_efd(pk='ID_1970', fk='ID_1975', reg=self.INFO1970, new_reg='1975', func=import_efd_1975)
            return self.data['1975']
        
    @property 
    def INFO1980(self):
        if '1980' in self.data.keys():
            return self.data['1980']
        else:
            return self.__download_efd(pk='ID_1001', fk='ID_1980', reg=self.INFO1001, new_reg='1980', func=import_efd_1980)
            return self.data['1980']


    @property 
    def INFO1990(self):
        if '1990' in self.data.keys():
            return self.data['1990']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_1990', reg=self._keys, new_reg='1990', func=import_efd_1990)
            return self.data['1990']



    ##################### BLOCK 9 ######################
    @property 
    def INFO9001(self):
        if '9001' in self.data.keys():
            return self.data['9001']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_9001', reg=self._keys, new_reg='9001', func=import_efd_9001)
            return self.data['9001']
        
    @property 
    def INFO9900(self):
        if '9900' in self.data.keys():
            return self.data['9900']
        else:
            return self.__download_efd(pk='ID_9001', fk='ID_9900', reg=self.INFO9001, new_reg='9900', func=import_efd_9900)
            return self.data['9900']

    @property 
    def INFO9990(self):
        if '9990' in self.data.keys():
            return self.data['9990']
        else:
            return self.__download_efd(pk='ID_0000', fk='ID_9990', reg=self._keys, new_reg='9990', func=import_efd_9990)
            return self.data['9990']



    def gerar_gim(self):
        """
        Função para gerar GIM por meio dos objetos EFD

        Args
        ----------------------
        None
            
        Return
        ----------------------
        gim_gerada pandas.DataFrame
            DataFrame contendo GIM simplificada com os valores gerais de operações de entrada e saída, apuração de ICMS e movimentações de créditos.
        
        """
        # It isn't necessary to filter by EFD with movement, as the merging operations with C190 will naturally eliminate EFD without movement
        entries_cfop = ('1', '2', '3')
        leaves_cfop = ('5', '6', '7')
        intern_cfop = ('1', '5')
        interstate_cfop = ('2', '6')
        international_cfop = ('3', '7')

        # Generating divisions (grouping it by the first digit of CFOP and taxed value)
        gim_gerada = self.C190.copy()
        gim_gerada['MAIN_DIGIT'] = [x[0] for x in gim_gerada['CFOP']]                     # To divide operations
        gim_gerada['TAXED'] = ['1' if x != 0 else '0' for x in gim_gerada['VL_ICMS']]         # To divide tax situation

        gim_gerada = gim_gerada.merge(self.C100[['ID_C001', 'ID_C100']].drop_duplicates(subset = 'ID_C100'), on = 'ID_C100', how = 'left', validate = 'many_to_one')
        gim_gerada = gim_gerada.groupby(['MAIN_DIGIT', 'TAXED', 'ID_C001'], as_index = False)[['VL_OPR', 'VL_BC_ICMS', 'VL_ICMS']].sum()

        # Adding CNPJ and DATE_REF values
        gim_gerada = gim_gerada.merge(self._keys[['CONINSCNPJ2', 'ID_0000','ID_C001']].drop_duplicates(subset = 'ID_C001'), on = 'ID_C001', how = 'left', validate = 'many_to_one')
        gim_gerada = gim_gerada.merge(self.cdco_id[['CONINSEST', 'ID_0000', 'DATE_REF']], on = 'ID_0000', how = 'left', validate = 'many_to_one')

        gim_gerada['COD_OPER'] = ['ENTRADA' if x in entries_cfop else 'SAIDA' for x in gim_gerada['MAIN_DIGIT']]
        gim_gerada['RANGE'] = ['INTERNO' if x in intern_cfop 
                                        else 'INTERESTADUAL' if x in interstate_cfop 
                                                else 'INTERNACIONAL'
                                for x in gim_gerada['MAIN_DIGIT']] 
        
        gim_gerada = gim_gerada.pivot(index = ['CONINSCNPJ2', 'DATE_REF', 'CONINSEST', 'ID_0000', 'MAIN_DIGIT'], columns = ['COD_OPER','RANGE','TAXED'], values = ['VL_OPR', 'VL_BC_ICMS', 'VL_ICMS'])

        gim_gerada.fillna(0, inplace = True)     # Removing NaN
        
        gim_gerada = gim_gerada.groupby(['CONINSCNPJ2','CONINSEST', 'DATE_REF', 'ID_0000']).sum()
        
        list_of_gim_names = {('VL_OPR', 'ENTRADA', 'INTERNO', '0'): 'ENTISE2',
                             ('VL_OPR', 'ENTRADA', 'INTERNO', '1'): 'ENTVAL7',
                             ('VL_OPR', 'ENTRADA', 'INTERESTADUAL', '0'): 'ENTISE4',
                             ('VL_OPR', 'ENTRADA', 'INTERESTADUAL', '1'): 'ENTVAL1',
                             ('VL_OPR', 'ENTRADA', 'INTERNACIONAL', '0'): 'ENTISE5',
                             ('VL_OPR', 'ENTRADA', 'INTERNACIONAL', '1'): 'ENTVAL2',
                             ('VL_OPR', 'SAIDA', 'INTERNO', '0'): 'SAIISE9',
                             ('VL_OPR', 'SAIDA', 'INTERNO', '1'): 'SAIVAL6',
                             ('VL_OPR', 'SAIDA', 'INTERESTADUAL', '0'): 'SAIISE2',
                             ('VL_OPR', 'SAIDA', 'INTERESTADUAL', '1'): 'SAIVAL7',
                             ('VL_OPR', 'SAIDA', 'INTERNACIONAL', '0'): 'SAIISE4',
                             ('VL_OPR', 'SAIDA', 'INTERNACIONAL', '1'): 'SAIVAL1',   

                                # BC Values     PS.: It is senseless to have VL_BC_ICMS/VL_ICMS from non-taxed operation (it will be 0)
                             ('VL_BC_ICMS', 'ENTRADA', 'INTERNO', '1'): 'ENTBAS5',
                             ('VL_BC_ICMS', 'ENTRADA', 'INTERESTADUAL', '1'): 'ENTBAS8',
                             ('VL_BC_ICMS', 'ENTRADA', 'INTERNACIONAL', '1'): 'ENTBAS9',
                             ('VL_BC_ICMS', 'SAIDA', 'INTERNO', '1'): 'SAIBAS4',
                             ('VL_BC_ICMS', 'SAIDA', 'INTERESTADUAL', '1'): 'SAIBAS5',
                             ('VL_BC_ICMS', 'SAIDA', 'INTERNACIONAL', '1'): 'SAIBAS8',  

                                # ICMS Values  
                             ('VL_ICMS', 'ENTRADA', 'INTERNO', '1'): 'ENTIMP3',
                             ('VL_ICMS', 'ENTRADA', 'INTERESTADUAL', '1'): 'ENTIMP6',
                             ('VL_ICMS', 'ENTRADA', 'INTERNACIONAL', '1'): 'ENTIMP7',
                             ('VL_ICMS', 'SAIDA', 'INTERNO', '1'): 'SAIIMP2',
                             ('VL_ICMS', 'SAIDA', 'INTERESTADUAL', '1'): 'SAIIMP3',
                             ('VL_ICMS', 'SAIDA', 'INTERNACIONAL', '1'): 'SAIIMP6'
                         }
        previous_columns = list(gim_gerada.columns)
      

        new_columns = [x for x in map(list_of_gim_names.get, previous_columns, previous_columns)]
        gim_gerada.columns = new_columns

        # Account values should include non-taxed values!
        gim_gerada.fillna(0, inplace = True)

        account_columns = [['ENTVAL7', 'ENTISE2'], ['ENTVAL1', 'ENTISE4'], ['ENTVAL2', 'ENTISE5'], ['SAIVAL6', 'SAIISE9'],\
                        ['SAIVAL7', 'SAIISE2'], ['SAIVAL1', 'SAIISE4']]
        for column1, column2 in account_columns:
            if column1 not in gim_gerada.columns:
                gim_gerada[column1] = 0
            if column2 not in gim_gerada.columns:
                gim_gerada[column2] = 0
            
            gim_gerada[column1] += gim_gerada[column2]
        

        # Fullfilling the missing columns
        for column in list_of_gim_names.values():
            if column not in gim_gerada.columns:
                gim_gerada[column] = 0
        
        gim_gerada = gim_gerada[list_of_gim_names.values()]

        # Creating columsn with total informations
        gim_gerada['ENTTOT3'] = gim_gerada['ENTVAL7'] + gim_gerada['ENTVAL1'] + gim_gerada['ENTVAL2']
        gim_gerada['ENTTOT2'] = gim_gerada['ENTBAS5'] + gim_gerada['ENTBAS8'] + gim_gerada['ENTBAS9']
        gim_gerada['ENTTOT1'] = gim_gerada['ENTIMP3'] + gim_gerada['ENTIMP6'] + gim_gerada['ENTIMP7']
        gim_gerada['ENTTOT8'] = gim_gerada['ENTISE2'] + gim_gerada['ENTISE4'] + gim_gerada['ENTISE5']

        gim_gerada['SAITOT2'] = gim_gerada['SAIVAL6'] + gim_gerada['SAIVAL7'] + gim_gerada['SAIVAL1']
        gim_gerada['SAITOT9'] = gim_gerada['SAIBAS4'] + gim_gerada['SAIBAS5'] + gim_gerada['SAIBAS8']
        gim_gerada['SAITOT7'] = gim_gerada['SAIIMP2'] + gim_gerada['SAIIMP3'] + gim_gerada['SAIIMP6']
        gim_gerada['SAITOT5'] = gim_gerada['SAIISE9'] + gim_gerada['SAIISE2'] + gim_gerada['SAIISE4']


        # Generating credit and debit calculation columns
        temp = self.E110.copy()
        temp = temp.groupby(['ID_E001'], as_index = False).sum()

        # Adding ID_0000
        temp = temp.merge(self._keys[['ID_0000', 'ID_E001']].drop_duplicates(subset = 'ID_E001'), on = 'ID_E001', how = 'left', validate = 'one_to_one')
        
        # Creating and renaming the columns (in order to extract GIM columns)
        temp['VL_OUTROS_CREDITOS'] = temp['VL_AJ_CREDITOS'] + temp['VL_TOT_AJ_CREDITOS']
        temp['VL_OUTROS_DEBITOS'] = temp['VL_AJ_DEBITOS'] + temp['VL_TOT_AJ_DEBITOS']

        gim_icms_apuration_names = {
                'VL_SLD_CREDOR_ANT': 'CREDVAL7',
                'VL_TOT_CREDITOS': 'CREDVAL1',
                'VL_ESTORNOS_DEB': 'CREDVAL2',
                'VL_OUTROS_CREDITOS': 'CREDVAL3',
                'VL_SLD_APURADO': 'CREDVAL6',
                'VL_TOT_DEBITOS': 'DEBVAL4',
                'VL_ESTORNOS_CRED': 'DEBVAL7',
                'VL_OUTROS_DEBITOS': 'DEBVAL2',
                'VL_SLD_CREDOR_TRANSPORTAR': 'DEBVAL5'
                }

        temp.rename(columns = gim_icms_apuration_names, inplace = True)

        # Filtering columns
        list_of_columns = list(gim_icms_apuration_names.values())
        list_of_columns = ['ID_0000'] + list_of_columns
        temp = temp[list_of_columns]

        gim_gerada = gim_gerada.reset_index().merge(temp, on = 'ID_0000', how = 'left', validate = 'one_to_one')

        return gim_gerada


    def __download_efd(self, pk, fk, reg, new_reg, func):
        """
        Hidden function to download registers

        Args
        ------
        pk: str
            Refers to the column name used to import the table

        fk: str
            Refers to the new primary key (a foreign key for _keys attribute) from the new table, to be added to _keys

        reg: EFD attribute
            Refers to the table (EFD attribute) used to extract the values to the import

        new_reg: EFD attribute
            Refers to the new table to be created (imported)

        func: function
            Refers to the function to import the new register

        Return
        ------
        None
        """
        assert isinstance(pk, str), f"The pk isn't string! Check it!"
        assert isinstance(reg, pd.DataFrame), f"The reg isn't a dataframe! Check it!"
        

        # Allowing imports with only one primary key
        if len(reg[pk].unique()) > 1:
            ID_tuples = tuple(reg[pk].unique())
        elif len(reg[pk].unique()) == 1:
            ID_tuples = [reg[pk].values[0], 0]      # Inserting '-1' only to the SQL script be the same (using IN clause)
        else:
            ID_tuples = tuple([0, 0])   # This avoids SQL error, although there are no values with ID 0



        if len(ID_tuples) >= 1000:
            n = 0
            new_reg_downloaded = pd.DataFrame()
            
            while n < len(ID_tuples):
                
                if len(ID_tuples) - n < 1000:
                    chunk_size = len(ID_tuples) - n
                else:
                    chunk_size = 1000
                
                chunk = func(ID_tuples[n:n+chunk_size])
                new_reg_downloaded = pd.concat([new_reg_downloaded, chunk])
                n += chunk_size

        else:
            new_reg_downloaded = func(ID_tuples)
        
        return new_reg_downloaded   # Removed the cache for RAM optimize.
    
        # new_reg_downloaded.flags.writeable = False
        # self.data[new_reg] = new_reg_downloaded
        # self.control[new_reg] = True
        
        # if pk == fk:
        #     self._keys = self._keys.merge(new_reg_downloaded[[pk]].drop_duplicates(), on = pk, how = 'left')
        # else:
        #     self._keys = self._keys.merge(new_reg_downloaded[[pk, fk]].drop_duplicates(), on = pk, how = 'left', suffixes=['', '_repetido'])