import pandas as pd 


class PreprocessamentoEFD:

    def Gerar0200Extendido(efd):
        """ Função para estender o 0200, enriquecendo com GTIN e NCM baseado na moda das notas fiscais """
        # Preparando o 0200
        INFO0200 = pd.DataFrame()
        for mes_ref in efd.keys():
            
            INFO0200 = pd.concat([INFO0200, pd.DataFrame(efd[mes_ref].RetornarCampo('0200'))])
        INFO0200.drop_duplicates(subset=['COD_ITEM', 'DESCR_ITEM'], inplace=True)


        if INFO0200['COD_ITEM'].duplicated().sum() > 0:
            inconsistencia_0200 = INFO0200[INFO0200['COD_ITEM'].duplicated()]
            relatorio.append('Foi encontrada inconsistência no registro 0200 com mesmo COD_ITEM de descrições distintas.')

