import pandas as pd 
import numpy as np 
from ..conexoes.connection import DadosParametrizados

class PreprocessamentoEFD:
    """ Classe para pré processar EFD a fim de permitir auditorias sobre itens """

    def _preencher_coluna(df, nova_coluna, coluna_maior, coluna_menor):
        """
        Preenche a nova coluna com os valores da coluna_maior, ou da coluna_menor se houver valores NaN na coluna_maior.

        Parâmetros:
        df (pd.DataFrame): DataFrame a ser modificado.
        nova_coluna (str): Nome da nova coluna a ser preenchida.
        coluna_maior (str): Coluna prioritária para preencher os valores.
        coluna_menor (str): Coluna alternativa para preencher caso a coluna_maior tenha NaN.

        Retorna:
        pd.DataFrame: DataFrame com a nova coluna preenchida.
        """
        df[nova_coluna] = np.where(df[coluna_maior].isna(), df[coluna_menor], df[coluna_maior])
        return df

    def _padronizar_coluna_extrair_apenas_numeros(df, column):
        ''' Padroniza coluna para manter apenas dígitos numéricos e transformar "espaços" em None'''
        def padronizar_gtin_ncm(value):
            
            # Utiliza expressão regular para manter apenas os dígitos
            return ''.join(filter(str.isdigit, str(value)))
        
        df[column] = df[column].apply(padronizar_gtin_ncm)
        
        df.loc[df[column] == "", column] = None
        
        return df
    
    def _adicionar_coluna_fiscal(df):

        df_copy = df.copy()
        """
        Função para adicionar a coluna 'FISCAL' com base nas colunas 'PARAM_GTIN', 
        'CLASSE_FRONTEIRA' e 'PARAM_NCM', em ordem de preferência.
        Desconsidera o valor de 'CLASSE_FRONTEIRA' se ele começar com '40' e
        dá preferência a 'PARAM_NCM' >= 4300 sobre 'CLASSE_FRONTEIRA'.
        
        Parâmetros:
        df (pd.DataFrame): DataFrame com as colunas 'PARAM_GTIN', 'CLASSE_FRONTEIRA' e 'PARAM_NCM'.
        
        Retorno:
        pd.DataFrame: DataFrame com a nova coluna 'FISCAL'.
        """
        
        # Função para buscar os valores em ordem de preferência
        def obter_valor_fiscal(row):
            # 1. Preferência máxima para PARAM_GTIN se não for nulo
            if pd.notnull(row['PARAM_GTIN']):
                return pd.Series([row['PARAM_GTIN'], "GTIN"])
            
            # 2. Verifica se PARAM_NCM é um número e se for maior ou igual a 4300, tem prioridade sobre CLASSE_FRONTEIRA
            elif pd.notnull(row['PARAM_NCM']) and str(row['PARAM_NCM']).isdigit() and int(row['PARAM_NCM']) >= 4300:
                return pd.Series([row['PARAM_NCM'], "NCM"])
            
            # 3. Se CLASSE_FRONTEIRA não for nulo e não começar com "40", tem prioridade
            elif pd.notnull(row['MODA_CLASSE_FRONTEIRA']) and not str(row['MODA_CLASSE_FRONTEIRA']).startswith('40') and not str(row['MODA_CLASSE_FRONTEIRA']).startswith('4205'):
                return pd.Series([row['MODA_CLASSE_FRONTEIRA'], "FRONTEIRA"])
            
            # 4. Se PARAM_NCM for menor que 4300 e válido, será considerado como último critério
            elif pd.notnull(row['PARAM_NCM']) and str(row['PARAM_NCM']).isdigit():
                return pd.Series([row['PARAM_NCM'], "NCM"])
            
            # 5. Caso todos os critérios falhem, retorna None
            else:
                return pd.Series([None, None])
        
        # Aplica a função linha por linha
        df_copy[['FISCAL', 'FISCAL_CRITERIO']] = df_copy.apply(obter_valor_fiscal, axis=1)
        
        return df_copy

    # Função para calcular a prioridade, assegurando que somas menores não ultrapassem prioridades maiores
    def _calcular_prioridade(row):
        prioridade = 0
        # 1. Prioridade máxima para PARAM_GTIN
        if pd.notnull(row['PARAM_GTIN']):
            return 1000  # Use a very high number to ensure it always takes precedence
        
        # 2. Prioridade para PARAM_NCM >= 4300
        
        if pd.notnull(row['PARAM_NCM']) and str(row['PARAM_NCM']).isdigit() and int(row['PARAM_NCM']) >= 4300:
            prioridade += 100  # High priority but less than PARAM_GTIN
        
        # 3. Prioridade para MODA_FRONTEIRA que não começa com "40"
        if pd.notnull(row['MODA_CLASSE_FRONTEIRA']) and not str(row['MODA_CLASSE_FRONTEIRA']).startswith('40'):
            prioridade += 10  # Smaller increment
        
        # 4. Prioridade para PARAM_NCM válido (< 4300)
        if pd.notnull(row['PARAM_NCM']) and str(row['PARAM_NCM']).isdigit() and int(row['PARAM_NCM']) < 4300:
            prioridade += 1  # Smallest increment
        
        return prioridade
    
    def C170_completo(self, efd):
        """ Retorna C170 completado com os registros 0000, 0200 e C100 """

        # Obter C170 completo
        INFO0000 = efd.INFO0000[['ID_0000', 'DT_INI', 'DT_FIN']]
        C100 = efd.C100[['ID_0000', 'ID_C100', 'IND_OPER', 'CHV_NFE', 'NUM_DOC', 'DT_DOC']]
        INFO0200 = efd.INFO0200[['ID_0000', 'COD_ITEM', 'DESCR_ITEM', 'COD_BARRA', 'UNID_INV', 'COD_NCM', 'CEST', 'ALIQ_ICMS']]
        C170 = efd.C170[['ID_0000', 'ID_C100', 'NUM_ITEM', 'COD_ITEM', 'CFOP', 'CST_ICMS', 'VL_ITEM', 'VL_DESC', 'VL_BC_ICMS', 'ALIQ_ICMS', 'VL_ICMS', 'VL_BC_ICMS_ST', 'ALIQ_ST', 'VL_ICMS_ST']]

        C170_completo = INFO0000.merge(C100, on = 'ID_0000', how='inner', validate='one_to_many')
        C170_completo = C170_completo.merge(C170, on = ['ID_0000', 'ID_C100'], how='inner', validate='one_to_many')
        C170_completo = C170_completo.merge(INFO0200, on = ['ID_0000', 'COD_ITEM'], how='inner', validate='many_to_one')
        
        # Padroniza colunas
        C170_completo = self._padronizar_coluna_extrair_apenas_numeros(C170_completo, 'COD_BARRA')
        C170_completo = self._padronizar_coluna_extrair_apenas_numeros(C170_completo, 'COD_NCM')
        return C170_completo
    
    def TB_NFE(self, efd, NFeEntrada):
        """ Retorna a tabela FTMPASP (referente aos produtos desembaraçados nos Postos Fiscais) """
        # Obter dados de passe no posto fiscal 
        C100 = efd.C100
        chaves = C100[(C100['COD_MOD']=='55') & (C100['IND_OPER']=='0') & (C100['COD_SIT'].isin(['1', '2']))]['CHV_NFE']
        tb_nfe = NFeEntrada.BuscarProdutosPorNotas(chaves['CHV_NFE'])
        
        # Padroniza colunas
        tb_nfe = self._padronizar_coluna_extrair_apenas_numeros(tb_nfe, 'GTIN_PRD')
        tb_nfe = self._padronizar_coluna_extrair_apenas_numeros(tb_nfe, 'NCM_PRD')
        return tb_nfe 
    
    def Gerar0200Parametrizado(self, efd, NFeEntrada):
        self.efd = efd 
        self.NFeEntrada = NFeEntrada

        tb_C170 = self.C170_completo(efd) 
        tb_pasp = self.TB_NFE(efd, NFeEntrada)


        # Verifica se a nota escriturada no registro C170 possui passe fiscal e adiciona coluna de flag
        if 'PASSOU_POSTO' in tb_C170.columns:
            tb_C170 = tb_C170.drop('PASSOU_POSTO', axis=1)

        tb_C170 = tb_C170.merge(tb_pasp[['CHAVE', 'NUM_ITEM', 'CLASSE_FRONTEIRA', 'GTIN_PRD', 'NCM_PRD']], 
                                how='left', left_on=['CHV_NFE', 'NUM_ITEM'], right_on=['CHAVE', 'NUM_ITEM'], 
                                indicator='PASSOU_POSTO', sufixes = ['_C170', '_PASP'])

        tb_C170['PASSOU_POSTO'] = tb_C170['PASSOU_POSTO'].astype('category')

        tb_C170['PASSOU_POSTO'] = tb_C170['PASSOU_POSTO'].cat.rename_categories({'left_only': 0, 'both': 1})

        tb_C170 = tb_C170.drop('CHAVE', axis=1)

        # Busca a moda do GTIN e do NCM com base na tabela FTMPASP (fronteira) 
        moda_gtin_por_cod_item = tb_C170.groupby(['ID_0000', 'COD_ITEM'], as_index=False)['GTIN_PRD'].mode()
        moda_gtin_por_cod_item.columns = ['ID_0000', 'COD_ITEM', 'MODA_GTIN_PRD']

        moda_ncm_por_cod_item = tb_C170.groupby(['ID_0000', 'COD_ITEM'], as_index=False)['NUM_PRD'].mode()
        moda_ncm_por_cod_item.columns = ['ID_0000', 'COD_ITEM', 'MODA_NCM_PRD']
        
        modas = moda_gtin_por_cod_item.merge(moda_ncm_por_cod_item, on = ['ID_0000', 'COD_ITEM'], how='outer', validate='one_to_one')

        INFO0000 = efd.INFO0000 

        INFO0000_extendida = INFO0000.merge(modas, on = ['ID_0000', 'COD_ITEM'], how='left', validate = 'one_to_one')

        INFO0000_extendida = self._preencher_coluna(INFO0000_extendida, 'GTIN_ADOTADO', 'MODA_GTIN_PRD', 'COD_BARRA')
        INFO0000_extendida = self._preencher_coluna(INFO0000_extendida, 'NCM_ADOTADO', 'MODA_NCM_PRD', 'COD_NCM')


        INFO0000_extendida['GTIN_ADOTADO'] = INFO0000_extendida['GTIN_ADOTADO'].str.strip()
        filtro_gtin = INFO0000_extendida.query('GTIN_ADOTADO.notna()')['GTIN_ADOTADO'].unique()

        parametrizacao_gtin = DadosParametrizados.BuscarParametrizacaoGTIN(tuple(filtro_gtin))

        parametrizacao_gtin = self._padronizar_coluna_extrair_apenas_numeros(parametrizacao_gtin,'TMEANCOD')
        parametrizacao_gtin = self._padronizar_coluna_extrair_apenas_numeros(parametrizacao_gtin,'PARAM_GTIN')


        INFO0000_extendida['NCM_ADOTADO'] = INFO0000_extendida['NCM_ADOTADO'].str.strip()
        filtro_ncm = INFO0000_extendida.query('NCM_ADOTADO.notna()')['NCM_ADOTADO'].unique()

        parametrizacao_ncm = DadosParametrizados.BuscarParametrizacaoNCM(tuple(filtro_ncm))
        parametrizacao_ncm = self._padronizar_coluna_extrair_apenas_numeros(parametrizacao_ncm,'NCM_COD_NC')
        parametrizacao_ncm = self._padronizar_coluna_extrair_apenas_numeros(parametrizacao_ncm,'PARAM_NCM')


        
        INFO0000_extendida = INFO0000_extendida.merge(tb_gtin[['TMEANCOD', 'PARAM_GTIN']], how='left', left_on='GTIN_ADOTADO', right_on='TMEANCOD')
        INFO0000_extendida = INFO0000_extendida.drop(['TMEANCOD'], axis=1)
        INFO0000_extendida = INFO0000_extendida.merge(tb_ncm[['NCM_COD_NC', 'PARAM_NCM']], how='left', left_on='NCM_ADOTADO', right_on='NCM_COD_NC')
        INFO0000_extendida = INFO0000_extendida.drop(['NCM_COD_NC'], axis=1)
        
        moda_classe_fronteira_por_cod_item = tb_C170.groupby(['ID_0000', 'COD_ITEM'], as_index=False)['CLASSE_FRONTEIRA'].mode()
        
        moda_classe_fronteira_por_cod_item = self._padronizar_coluna_extrair_apenas_numeros(moda_classe_fronteira_por_cod_item, 'MODA_CLASSE_FRONTEIRA')
        INFO0000_extendida = INFO0000_extendida.merge(moda_classe_fronteira_por_cod_item, on='COD_ITEM', how='left')


        tb_0200_parametrizado = self._adicionar_coluna_fiscal(INFO0000_extendida).copy()
        tb_0200_parametrizado['FISCAL'] = tb_0200_parametrizado['FISCAL'].str.strip()



        # Aplicar a nova lógica de prioridade ao DataFrame
        tb_0200_parametrizado['prioridade'] = tb_0200_parametrizado.apply(calcular_prioridade, axis=1)

        # Ordenar o dataframe pela coluna 'prioridade' (do maior para o menor) para garantir que as linhas com maior prioridade apareçam primeiro
        tb_0200_parametrizado_sorted = tb_0200_parametrizado.sort_values(by=['COD_ITEM', 'prioridade'], ascending=[True, False])

        # Remova as duplicatas, mantendo apenas a primeira linha (que terá a maior prioridade)
        tb_0200_parametrizado = tb_0200_parametrizado_sorted.drop_duplicates(subset=['COD_ITEM'], keep='first')

        # Remover a coluna de prioridade, se desejado
        tb_0200_parametrizado = tb_0200_parametrizado.drop(columns=['prioridade'])

        # Resultado final
        tb_0200_parametrizado.reset_index(drop=True, inplace=True)

        tb_0200_parametrizado.sort_values(by='DESCR_ITEM', inplace=True)

        return tb_0200_parametrizado


