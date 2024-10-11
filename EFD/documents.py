# User-defined modules
from MalhasFiscais.documents.utils import folder_manager


# Third-party modules import
import dill
import numpy as np
import pandas as pd 
from datetime import datetime as dt 
from pandas.api.types import is_datetime64_any_dtype as is_datetime



class Documents(object):
    """ Class to represent documents in general (NF-es, NFC-es, EFD, GIM, PGDAS, ...)"""


    @classmethod
    @folder_manager('Working')
    def save(cls, instance, filename: str) -> None:
        """ 
        Função para salvar trabalho (para evitar baixar novamente os dados)
        
        Args
        -----
        instance: EFD
            Refere-se à instância da EFD (trabalho atual que está sendo realizado)

        filename: str
            Refere-se ao nome do trabalho a ser salvo

        Return
        -------
        None


        Example
        -------
        > trabalho_atual = EFD(cdco, dt(2022, 1, 1), num_months = 3)
        > trabalho_atual.C100
        > ... # Working on
        > EFD.save(instance = trabalho_atual, filename = 'OS_20102_2023')
        
        """
        with open(filename, 'wb') as file:
            dill.dump(instance, file)



    @classmethod
    @folder_manager('Working')
    def load(cls, filename):
        """ 
        Função para retomar um trabalho em andamento (para evitar baixar novamente os dados)
        
        Args
        -----
        filename: str
            Refere-se ao nome do trabalho salvo

        Return
        -------
        None


        Example
        -------
        > trabalho_atual = EFD.load(filename = 'OS_20102_2023')
        > trabalho_atual.C100
        > ...
        """
        with open(filename, 'rb') as file:
            return dill.load(file)
        

    def __init__(self):
        pd.set_option('display.max_columns', None)  # There is too much columns in each document. So, the max_columns setting will be removed (changed to None)