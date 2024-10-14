import oracledb
import pandas as pd 
from dados import user, pw, dns_tns
from dateutil.relativedelta import relativedelta
import warnings
import functools


oracledb.init_oracle_client()

def extrair_df_de_lobs(query):
    with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        
        # Fetch all rows and read LOBs
        rows = []
        for row in cursor:
            row = list(row)
            # Convert any LOBs to strings
            for i, col in enumerate(row):
                if isinstance(col, oracledb.LOB):
                    row[i] = col.read()  # Read the LOB content
            rows.append(row)
        
        # Get column names
        column_names = [desc[0] for desc in cursor.description]
        
        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=column_names)
        
    return df

def extrair_df(query):
    with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
        df = pd.read_sql_query(query, con = connection)
    return df


class DocumentacaoAuditoria:
    """ 
    Classe para servir de base para qualquer documento extraído para auditoria 
    =========================================================================
    Por meio desse documento, são adicionados métodos a serem usados em todos os documentos
    
    """

    def __init__(self):
        self.query = None 

    def retrieve(self):
        with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
            doc = pd.read_sql_query(self.query, con=connection)
            
        return doc
    
    def save(self, path):
        doc = pd.ExcelWriter(path=path)
        doc_usado = self.retrieve()
        doc_usado.to_excel(doc, sheet_name='GIM', index=False)
        doc.close()




class DadosEscriturais:
    
    class GIM(DocumentacaoAuditoria):
        """ Classe que retorna a GIM da empresa fiscalizada """
        def __init__(self, cgf, data_inicio, data_fim):

            data_fim = data_fim + relativedelta(months=2)
            self.query = f""" 
                    WITH input AS (SELECT '{cgf}' AS cgf, '{data_inicio.strftime("%d/%m/%Y")}' AS dt_ini, '{data_fim.strftime("%d/%m/%Y")}' AS dt_fin FROM DUAL),
                        os AS (SELECT * FROM SIATE.FIOS WHERE FIOSINSEST in (SELECT cgf FROM input))
                    SELECT 

                        GIM.CONINSEST           AS CGF,
                        GIM.REFGIM              AS PERÍODO,
                        GIM.DTALGGIM            AS DT_GIM,
                        GIM.DTARECGIM           AS DT_REC,
                        GIM.DTAREF              AS DT_REF_GIM,
                        
                        CASE 
                            WHEN EXISTS (SELECT * FROM os WHERE GIM.DTARECGIM BETWEEN os.FIOSDTEMI AND os.FIOSDTEMI + 90 AND GIM.DTAREF BETWEEN os.FIOSPFINI AND os.FIOSPFFIM)
                                THEN 'EMITIDO DURANTE FISCALIZAÇÃO'
                            WHEN EXISTS (SELECT * FROM os WHERE GIM.DTARECGIM > os.FIOSDTEMI + 90 AND GIM.DTAREF BETWEEN os.FIOSPFINI AND os.FIOSPFFIM)
                                THEN 'EMITIDO APÓS FISCALIZAÇÃO'
                            ELSE 'REGULAR'
                        END AS "SITUAÇÃO DE EMISSÃO",
                        
                        GIM.NATSOLGIM           AS "NAT.SOLIC",
                        --GIM.ENTVAL7        AS "Ent Estado",
                        --GIM.ENTVAL1        AS "Ent Outr Estado",
                        --GIM.ENTVAL2        AS "Ent Exterior",
                        GIM.ESTTOT8             AS "ESTOQ.TOTAL",
                        GIM.ENTTOT3             AS "ENT.TOTAL",
                        GIM.ENTTOT8             AS "ENT.ISEN",
                    --GIM.ENTBAS5        AS "Bs Cal Ent Estado",
                    --GIM.ENTBAS8        AS "Bs Cal Ent Outr Est",
                    --GIM.ENTBAS9        AS "Bs Cal Ent Exterior",
                    GIM.ENTTOT2               AS "BC.ENTRADA",
                    --GIM.ENTIMP3        AS "Cred Entr Estado",
                    --GIM.ENTIMP6        AS "Cred Ent Outr Est",
                    --GIM.ENTIMP7        AS "Cred Ent Exterior",
                    GIM.ENTTOT1               AS "CRÉD.ENT",
                    --GIM.ENTISE2        AS "Ent Isen Estado",
                    --GIM.ENTISE4        AS "Ent Isen Outr Est",
                    --GIM.ENTISE5        AS "Ent Isen Exterior",
                    --GIM.ENTTOT8        AS "Ent Isen Total",
                    --GIM.SAIVAL6        AS "Saida Estado",
                    --GIM.SAIVAL7        AS "Saida Outr Estado",
                    --GIM.SAIVAL1        AS "Saida Exterior",
                    GIM.SAITOT2               AS "SAÍDA.TOTAL",
                    GIM.SAITOT5               AS "SAÍ.ISEN",
                    --GIM.SAIBAS4        AS "Bs Cal Sai Est",
                    --GIM.SAIBAS5        AS "Bs Cal Sai Outr Est",
                    --GIM.SAIBAS8        AS "Bs Cal Sai Exterior",
                    GIM.SAITOT9               AS "BC.SAÍDA",
                    --GIM.SAIIMP2        AS "Deb Sai Est",
                    --GIM.SAIIMP3        AS "Deb Sai Outr Est",
                    --GIM.SAIIMP6        AS "Deb Sai Exterior",
                    GIM.SAITOT7               AS "DÉB.SAÍDA",
                    --GIM.SAIISE9        AS "Sai Isen Estado",
                    --GIM.SAIISE2        AS "Sai Isen Outr Est",
                    --GIM.SAIISE4        AS "Sai Isen Exterior",
                    --GIM.SAITOT5        AS "Sai Isen Total",
                    GIM.CREDVAL1       AS "CRÉD.ENTRADAS",
                    GIM.CREDVAL2       AS "ESTORNO.DÉB",
                    GIM.CREDVAL3       AS "OUTROS.CRÉD",
                    GIM.CREDVAL6       AS "SD.DEVEDOR",
                    GIM.CREDVAL7       AS "SD.CRED.MÊS.ANT",
                    GIM.CREDTOT7       AS "TOTAL.CRÉDITOS",
                    GIM.DEBVAL4        AS "DÉB.SAÍDAS",
                    GIM.DEBVAL7        AS "ESTORNO.CRÉ",
                    GIM.DEBVAL2        AS "OUTROS.DÉB",
                    GIM.DEBVAL5        AS "SD.CRED.MÊS.SEG",
                    GIM.DEBTOT3        AS "TOTAL.DEBITOS",
                    GIM.DISCRREC4      AS "SD.DEV.NORMAL",
                    GIM.DISCRREC2      AS "SD.DEV.ANT.P",
                    GIM.DISCRREC5      AS "SD.DEV.ST.ENT",
                    GIM.DISCRREC1      AS "SD.DEV.ST.SAI",
                    GIM.DISCRREC3      AS "SD.DEV.DIFER",
                    GIM.DISCRREC7      AS "SD.DEV.ANTEC",
                    GIM.DISCRREC6      AS "SD.DEV.IMPORT.1",
                    GIM.DISCRREC9      AS "SD.DEV.IMPORT.2",
                    GIM.DISCRREC8      AS "SD.DEV.IMPORT.3",
                    GIM.DISCRTOT4      AS "SD.DEV.TOTAL",
                    --GIM.ENTATI7        AS "ENT.IMOB.EST",
                    --GIM.ENTATI3        AS "ENT.IMOB.OUTR.EST",
                    --GIM.ENTATI8        AS "ENT.IMOB.EXTER",
                    GIM.ENTTOT9        AS "ENT IMOB TOTAL"
                    --GIM.*
                    FROM 

                    SIATE.CDGIM GIM 

                    WHERE 

                        GIM.CONINSEST = (SELECT cgf FROM input)
                    AND GIM.DTAREF BETWEEN (SELECT dt_ini FROM input) AND (SELECT dt_fin FROM input)

                    UNION 


                    SELECT 

                        HGIM.HGIMCGF           AS CGF,
                        HGIM.HREFGIM              AS PERÍODO,
                        HGIM.GHDTALGGIM            AS DT_GIM,
                        HGIM.GHDTARCGIM           AS DT_REC,
                        TO_DATE(HGIM.HREFGIM, 'MM/YYYY') AS DT_REF_GIM, 
                        CASE 
                            WHEN EXISTS (SELECT * FROM os WHERE HGIM.GHDTARCGIM BETWEEN os.FIOSDTEMI AND os.FIOSDTEMI + 90 AND TO_DATE(HGIM.HREFGIM, 'MM/YYYY') BETWEEN os.FIOSPFINI AND os.FIOSPFFIM)
                                THEN 'RETIFICADO E EMITIDO DURANTE FISCALIZAÇÃO'
                            WHEN EXISTS (SELECT * FROM os WHERE HGIM.GHDTARCGIM > os.FIOSDTEMI + 90 AND TO_DATE(HGIM.HREFGIM, 'MM/YYYY') BETWEEN os.FIOSPFINI AND os.FIOSPFFIM)
                                THEN 'RETIFICADO E EMITIDO APÓS FISCALIZAÇÃO'
                            ELSE 'RETIFICADO'
                        END AS "SITUAÇÃO DE EMISSÃO",
                        HGIM.GHNATSOLGI      AS "NAT.SOLIC",
                        --HGIM.GHENTVAL7        AS "Ent Estado",
                        --HGIM.GHENTVAL1        AS "Ent Outr Estado",
                        --HGIM.GHENTVAL2        AS "Ent Exterior",
                        HGIM.GHESTTOT8             AS "ESTOQ.TOTAL",
                        HGIM.GHENTTOT3             AS "ENT.TOTAL",
                        HGIM.GHENTTOT8             AS "ENT.ISEN",
                    --HGIM.GHENTBAS5        AS "Bs Cal Ent Estado",
                    --HGIM.GHENTBAS8        AS "Bs Cal Ent Outr Est",
                    --HGIM.GHENTBAS9        AS "Bs Cal Ent Exterior",
                    HGIM.GHENTTOT2               AS "BC.ENTRADA",
                    --HGIM.GHENTIMP3        AS "Cred Entr Estado",
                    --HGIM.GHENTIMP6        AS "Cred Ent Outr Est",
                    --HGIM.GHENTIMP7        AS "Cred Ent Exterior",
                    HGIM.GHENTTOT1               AS "CRÉD.ENT",
                    --HGIM.GHENTISE2        AS "Ent Isen Estado",
                    --HGIM.GHENTISE4        AS "Ent Isen Outr Est",
                    --HGIM.GHENTISE5        AS "Ent Isen Exterior",
                    --HGIM.GHENTTOT8        AS "Ent Isen Total",
                    --HGIM.GHSAIVAL6        AS "Saida Estado",
                    --HGIM.GHSAIVAL7        AS "Saida Outr Estado",
                    --HGIM.GHSAIVAL1        AS "Saida Exterior",
                    HGIM.GHSAITOT2               AS "SAÍDA.TOTAL",
                    HGIM.GHSAITOT5               AS "SAÍ.ISEN",
                    --HGIM.GHSAIBAS4        AS "Bs Cal Sai Est",
                    --HGIM.GHSAIBAS5        AS "Bs Cal Sai Outr Est",
                    --HGIM.GHSAIBAS8        AS "Bs Cal Sai Exterior",
                    HGIM.GHSAITOT9               AS "BC.SAÍDA",
                    --HGIM.GHSAIIMP2        AS "Deb Sai Est",
                    --HGIM.GHSAIIMP3        AS "Deb Sai Outr Est",
                    --HGIM.GHSAIIMP6        AS "Deb Sai Exterior",
                    HGIM.GHSAITOT7               AS "DÉB.SAÍDA",
                    --HGIM.GHSAIISE9        AS "Sai Isen Estado",
                    --HGIM.GHSAIISE2        AS "Sai Isen Outr Est",
                    --HGIM.GHSAIISE4        AS "Sai Isen Exterior",
                    --HGIM.GHSAITOT5        AS "Sai Isen Total",
                    HGIM.GHCREDVAL1       AS "CRÉD.ENTRADAS",
                    HGIM.GHCREDVAL2       AS "ESTORNO.DÉB",
                    HGIM.GHCREDVAL3       AS "OUTROS.CRÉD",
                    HGIM.GHCREDVAL6       AS "SD.DEVEDOR",
                    HGIM.GHCREDVAL7       AS "SD.CRED.MÊS.ANT",
                    HGIM.GHCREDTOT7       AS "TOTAL.CRÉDITOS",
                    HGIM.GHDEBVAL4        AS "DÉB.SAÍDAS",
                    HGIM.GHDEBVAL7        AS "ESTORNO.CRÉ",
                    HGIM.GHDEBVAL2        AS "OUTROS.DÉB",
                    HGIM.GHDEBVAL5        AS "SD.CRED.MÊS.SEG",
                    HGIM.GHDEBTOT3        AS "TOTAL.DEBITOS",
                    HGIM.GHDISCRRC4      AS "SD.DEV.NORMAL",
                    HGIM.GHDISCRRC2      AS "SD.DEV.ANT.P",
                    HGIM.GHDISCRRC5      AS "SD.DEV.ST.ENT",
                    HGIM.GHDISCRRC1      AS "SD.DEV.ST.SAI",
                    HGIM.GHDISCRRC3      AS "SD.DEV.DIFER",
                    HGIM.GHDISCRRC7      AS "SD.DEV.ANTEC",
                    HGIM.GHDISCRRC6      AS "SD.DEV.IMPORT.1",
                    HGIM.GHDISCRRC9      AS "SD.DEV.IMPORT.2",
                    HGIM.GHDISCRRC8      AS "SD.DEV.IMPORT.3",
                    HGIM.GHDISCRTT4      AS "SD.DEV.TOTAL",
                    --HGIM.GHENTATI7        AS "ENT.IMOB.EST",
                    --HGIM.GHENTATI3        AS "ENT.IMOB.OUTR.EST",
                    --HGIM.GHENTATI8        AS "ENT.IMOB.EXTER",
                    HGIM.GHENTTOT9        AS "ENT IMOB TOTAL"
                    --HGIM.*
                    FROM 

                        SIATE.GIHSGIM HGIM

                    WHERE 

                    HGIM.HGIMCGF = (SELECT cgf FROM input)
                    AND TO_DATE(HREFGIM, 'MM/YYYY') BETWEEN (SELECT dt_ini FROM input) AND (SELECT dt_fin FROM input)


                    ORDER BY DT_REF_GIM DESC, DT_REC

                """

        def retrieve(self):
            with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
                gim = pd.read_sql_query(self.query, con=connection)
            
            return gim 
    

    def BuscarObrigatoriedade(cgf, data_inicio, data_fim):
        
        dtype = {'OB_CNPJ': str, 'OB_DATA': 'datetime64[ns]'}
        query = f"""
                SELECT
                    OB_CNPJ,
                    OB_DATA,
                    OB_TIPO
                FROM
                    SIATE.TB_OBRIGADO
                WHERE
                    OB_IE = '{cgf}'
                    AND OB_DATA BETWEEN '{data_inicio.strftime("%d/%m/%Y")}' AND '{data_fim.strftime("%d/%m/%Y")}'
                """
        with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
            gim = pd.read_sql_query(query, con=connection, dtype=dtype)
        
        return gim 
    

class DadosCadastrais(DocumentacaoAuditoria):
    
    def __init__(self, cnpj):
        self.cnpj = cnpj 
        self.cadastro = self.CadastroAtualEstadual(cnpj)
        self.historico_cadastro = self.HistoricoCadastroEstadual(cnpj)

    class CadastroAtualEstadual(DocumentacaoAuditoria):
        """ Classe que retorna o Cadastro Geral Fazendário Estadual da empresa """
        def __init__(self, cnpj):
            self.query = f""" SELECT
                            CONCODSEQ AS "SequenciaHistorico",
                                CONDTAATU AS "DataAlteracao",
                                CONINSCNPJ2 AS "CNPJ",
                                CONINSEST AS "CGF",
                                CONRAZSOC AS "RazaoSocial",
                                CONCAPSOC AS "CapitalSocial",
                                CASE WHEN NATJURCOD = '05' AND CCOCOD = '12' THEN 1 ELSE 0 END AS "OptanteSimples",
                                CONNOMFAN AS "NomeFantasia",
                                CONSITCON AS "SituacaoCadastral",
                                CONENDRUA AS "Rua",
                                CONENDNUM AS "Numero",
                                CONENDBAI AS "Bairro",
                                CONENDMUNN AS "Cidade",
                                CONENDUFC AS "Estado",
                                CONENDCOM AS "Complemento",
                                CONENDCEP AS "CEP",
                                CONMAIL AS "Email",
                                CONTEL AS "Telefone",
                                CONATVC01 AS "CNAEPrincipal",
                                CONATVN01 AS "DescricaoCnaePrincipal",
                                CONATVC02 AS "CNAESecundario",
                                CONATVN02 AS "DescricaoCnaeSecundario",
                                CONENDLAT AS "Latitude",
                                CONENDLON AS "Longitude"
                                
                            FROM
                                SIATE.CDCO
                            WHERE
                                CONINSCNPJ2 = '{cnpj}'"""
            
        def retrieve(self):
            return extrair_df(self.query)
    
    class HistoricoCadastroEstadual(DocumentacaoAuditoria):
        """ Classe que retorna o Histórico do Cadastro Geral Fazendário Estadual da empresa """
        def __init__(self, cnpj):

            self.query = f""" SELECT
                            HCONCODSEQ AS "SequenciaHistorico",
                            HINFDTA AS "DataAlteracao",
                            HINFCPF AS "CPFResponsavelAlteracao",
                            HPARFISC AS "MotivoAlteracao",
                            HCONINSCNP2 AS "CNPJ",
                            HCONINSEST AS "CGF",
                            HCONRAZSOC AS "RazaoSocial",
                            HCONCAPSOC AS "CapitalSocial",
                            CASE WHEN HNATJURCOD = '05' AND HCCOCOD = '12' THEN 1 ELSE 0 END AS "OptanteSimples",
                            HCONNOMFAN AS "NomeFantasia",
                            HCONSITCON AS "SituacaoCadastral",
                            HCONENDRUA AS "Rua",
                            HCONENDNUM AS "Numero",
                            HCONENDBAI AS "Bairro",
                            HCONENDMN AS "Cidade",
                            HCONENDUFC AS "Estado",
                            HCONENDCOM AS "Complemento",
                            HCONENDCEP AS "CEP",
                            HCONMAIL AS "Email",
                            HCONMAIL AS "Telefone",
                            HCONATVC01 AS "CNAEPrincipal",
                            '' AS "DescricaoCnaePrincipal",
                            HCONATVC02 AS "CNAESecundario",
                            '' AS "DescricaoCnaeSecundario",
                            NULL AS "Latitude",
                            NULL AS "Longitude"
                            FROM
                                SIATE.CDCOHSF
                            WHERE 1=1
                                AND HCONINSCNP2 = '{cnpj}'"""
        
        def retrieve(self):
            cadastro_historico = extrair_df(self.query)
            cadastro_historico.sort_values(by='SequenciaHistorico', inplace=True)
            cadastro_historico['DataAlteracaoAnterior'] = cadastro_historico['DataAlteracao'].shift(1)
            cadastro_historico['SituacaoCadastralAnterior'] = cadastro_historico['SituacaoCadastral'].shift(1)
            cadastro_historico['OptanteSimplesAnterior'] = cadastro_historico['OptanteSimples'].astype(int).shift(1)
            return cadastro_historico


class DadosDebitos:

    class DSOT(DocumentacaoAuditoria):
        """ Classe para retornar os valores de débitos estaduais da empresa """
        def __init__(self, cgf, data_inicio, data_fim):

            self.query = f"""
                    SELECT
                        DS_CBDBINSCRI AS "CGF",
                        DS_CTDISEQDEB AS "SequenciaDebito",
                        DS_CTDIREFERE AS "MesReferencia",
                        DS_CTDIVLRPRI AS "ValorPrincipal",
                        DS_CTDIVLRMUL AS "ValorMulta",
                        DS_CTDIVLRJUR AS "ValorJuros",
                        DS_CTDIVLRCOR AS "ValorCorrecao",
                        DS_CTDIEXCECA AS "Suspensao",
                        DS_CTDIPASSE AS "PasseFiscal",
                        DS_CTDISEQUEN AS "SequenciaPasse",
                        DS_CTDIDTVENC AS "DataVencimento",
                        DS_CTDIORIDEB AS "OrigemDebito"
                        
                    FROM
                        SIATE.VW_CBTPDBIT
                    WHERE
                        DS_CBDBINSCRI = '{cgf}'
                        AND DS_CTDIDTVENC BETWEEN '{data_inicio.strftime("%d/%m/%Y")}' AND '{data_fim.strftime("%d/%m/%Y")}'
                    """
            
        def retrieve(self):
            return extrair_df(self.query)

        def BuscarJustificativa(codigos: tuple):

            justificativas = f""" SELECT
                            DEEX_CBTDCODIGO AS "CodigoDebito",
                            DEEX_DT_INICIO AS "DataInicioSuspensao",
                            DEEX_DT_FIM AS "DataFimSuspensao",
                            DEEX_PARECER AS "ParecerSuspensao"
                        FROM
                            SIATE.TB_DEBITO_EXCECAO
                        WHERE
                            DEEX_CBTDCODIGO IN {codigos}

                    
                    """
            return extrair_df_de_lobs(justificativas)
    
    class DebitosAntecipacaoParcialPagos(DocumentacaoAuditoria):
        """ Classe para retornar valores de débitos de antecipação parcial pagos """
        def __init__(self, cgf, data_inicio, data_fim):
            self.query = f"""    
                SELECT 
                    y.CONINSEST       AS CGF,
                    y.CONRAZSOC       AS "Razao Social",
                    EXTRACT(YEAR FROM z.ARDTAPAG) ANO,
                    EXTRACT(MONTH FROM z.ARDTAPAG) MES,
                    SUM(z.ARVALOR)    AS "Ant.Pacial.Pago"

                FROM 
                    
                    siate.cdco y,
                    SIATE.ararq1 z

                WHERE 
                    
                    z.ARCOINEST = y.CONINSEST
                    and y.CONINSEST = '{cgf}'
                    and SUBSTR(z.ARTRIBUTO, 3) in ('45')
                    AND z.ARDTAPAG BETWEEN '{data_inicio.strftime("%d/%m/%Y")}' AND '{data_fim.strftime("%d/%m/%Y")}'


                GROUP BY y.CONINSEST, y.CONRAZSOC, EXTRACT(MONTH FROM z.ARDTAPAG), EXTRACT(YEAR FROM z.ARDTAPAG)

                ORDER BY ANO desc, MES desc"""
        
        def retrieve(self):
            return extrair_df(self.query)


class DadosOperacoes:
    """ Classe para armazenar subclasses (documentos fiscais) e eventos, extraídos do banco de dados """
    



    class NotasFiscaisEntrada(DocumentacaoAuditoria):
        """ Classe para importar apenas notas fiscais de entrada """
        def __init__(self, cnpj, data_inicio, data_fim):
            self.cnpj = cnpj 
            self.data_inicio = data_inicio 
            self.data_fim = data_fim
            self.queries = []
            self.queries.append(f""" 
                    SELECT 
                        PASM.NFERDOCN AS "CNPJ.EMIT",
                        PASM.NFERRAZ AS "RAZ.EMITENTE",
                        PASM.NFERIEST AS "IE.ST.EMIT",
                        PASM.NFERUF AS "UF.EMIT",
                        CASE
                            WHEN PASM.NFERCRT = '1'
                            THEN 'SIMPLES'
                            WHEN PASM.NFERCRT = '2'
                            THEN 'SN.SUBLI.EXC'
                            WHEN PASM.NFERCRT = '3'
                            THEN 'NORMAL'
                            ELSE 'N/A'
                            END AS REGIME,
                        PASM.NFEDUF     AS "UF.DEST/REMET",
                        PASM.NFEDRAZ    AS "RAZ.DEST/REMET",
                        PASM.NFEDDOCN   AS "DOC.DEST/REMET",
                        PASM.NFEDINSC   AS "IE.DEST/REMET",
                        PASM.NFESUFRAMA AS SUFRAMA,
                        PASM.NFENNOT    AS "Nº.NF",
                        CASE
                            WHEN PASM.NFECANC = 'S'
                            THEN 'CANCELADA'
                            WHEN PASM.NFECANC = 'D'
                            THEN 'DENEGADA'
                            WHEN PASM.NFECANC = 'N'
                            THEN 'ATIVA'
                        END           AS "ATIVA/CANC",
                        PASM.DANF     AS "ChaveAcesso",
                        PASM.NFECFOP  AS "CFOP.NF",
                        PRD.NFEPCFOP  AS "CFOP.ITEM",
                        PASM.NFEDTEMI AS "DT.EMISSÃO",
                        CASE
                            WHEN PASM.NFESIT = '0'
                            THEN 'NÃO.DESEMB'
                            WHEN PASM.NFESIT = '1'
                            THEN 'DESEMBARAÇADA'
                            WHEN PASM.NFESIT = '2'
                            THEN 'OP.DESCONH'
                            WHEN PASM.NFESIT = '3'
                            THEN 'ALT.FISCAL'
                            WHEN PASM.NFESIT = '4'
                            THEN 'INTERMUNIC'
                        END                           AS SITUAÇÃO,
                        RPAD(PASM.NFENATOP, 30)       AS "NAT.OPER",
                        PASP.TMPCLASSE                AS "CLASSE.FRONT",
                        PRD.NFEPEAN                   AS GTIN,
                        PRD.NFEPCODITEM               AS "COD.PROD.EMIT",
                        PRD.NFEPNCM                   AS NCM,
                        PRD.NFEPNITEM                 AS "Nº.ITEM",
                        PRD.NFEPNDESC                 AS "DESC.ITEM",
                        PRD.NFEPQTRIB                 AS QNT,
                        PRD.NFEPUTRIB                 AS UND,
                        PRD.NFEPVUNTRIB               AS "VLR.UNT",
                        PRD.NFEPVPROD                 AS "VLR.BRUTO.ITEM",
                        PRD.NFEPVDESC                 AS "DESCON.ITEM",
                        PRD.NFEPVPROD - PRD.NFEPVDESC AS "VLR.LIQ.ITEM",
                        PRD.NFEPCST                   AS CST,
                        CASE
                            WHEN PRD.NFEPORIG IN ('1', '2', '3', '8')
                            THEN 'IMPORT'
                            ELSE 'NACIONAL'
                        END                AS ORIGEM,
                        PRD.NFEPPREDBC     AS "%.RED.BC",
                        PRD.NFEPVBC        AS "BC.ICMS.ITEM",
                        PRD.NFEPPICMS      AS ALIQ,
                        PRD.NFEPVICMS      AS "ICMS.ITEM",
                        PRD.NFEPVICMSDESON AS "ICMS.DESON.ITEM",
                        PASM.NFEVICMSD     AS "ICMS.DESON.NF",
                        PASM.NFEVNICMS     AS "ICMS.NF",
                        PRD.NFEPMDBCST     AS "MOD.BC.ST",
                        PRD.NFEPVBCST      AS "BC.ST.ITEM",
                        PRD.NFEPPMVAST     AS "MVA.ST",
                        PRD.NFEPPST        AS "ALIQ.ST.ITEM",
                        PRD.NFEPVST        AS "ST.RET.ITEM",
                        PRD.NFEPVSTR       AS "ST.RET.ANT.ITEM",
                        PRD.VICMSUFDE      AS "DIFAL.ITEM",
                        PASM.VICMSUFDE     AS "DIFAL.NF",
                        PRD.PICMSUFDE      AS "ALIQ.DEST",
                        PASM.NFEBNICMS     AS "BC.ICMS.NF",
                        PASM.NFEBSICMS     AS "BC.ST.NF",
                        PASM.NFEVSICMS     AS "ICMS.ST.NF",
                        PASM.NFETVPROD     AS "VLR.BR.ITENS.NF",
                        PASM.NFETVFRET     AS "FRETE.NF",
                        PASM.NFEVSEGU      AS "SEGURO.NF",
                        PASM.NFEVSICMS     AS "ICMS.ST.NF.2",
                        PASM.NFENVDESC     AS "DESCONTO.NF",
                        PASM.NFEVNOTA      AS "VALOR.FINAL.NF",
                        CASE
                            WHEN PASM.NFETPNF = '0'
                            THEN 'ENTRADA'
                            WHEN PASM.NFETPNF = '1'
                            THEN 'SAÍDA'
                            ELSE 'N/A'
                        END AS "TIPO.OPER",
                        CASE
                            WHEN PASM.NFEFINEMI = '1'
                            THEN 'NORMAL'
                            WHEN PASM.NFEFINEMI = '2'
                            THEN 'COMPLEMENTAR'
                            WHEN PASM.NFEFINEMI = '3'
                            THEN 'AJUSTE'
                            WHEN PASM.NFEFINEMI = '4'
                            THEN 'DEVOLUÇÃO'
                            ELSE 'N/A'
                        END AS FINALIDADE,
                        CASE
                            WHEN PASM.NFEIDDEST = '1'
                            THEN 'INTERNA'
                            WHEN PASM.NFEIDDEST = '2'
                            THEN 'INTERESTADUAL'
                            WHEN PASM.NFEIDDEST = '3'
                            THEN 'EXTERIOR'
                            ELSE 'N/A'
                        END               AS DESTINO,
                        PASM.NFEREFNFE    AS "NF-e.REF",
                        PASM.NFEINFADC    AS "DADOS.AD.P1",
                        PASM.NFEINFADCLOB AS "DADOS.AD.P2",
                        PASM.NFEDLOGRAD,
                        PASM.NFEDBAI,
                        PASM.NFEDNUM
                    FROM 
                        SIATE.NFEPASM PASM,
                        SIATE.NFEPRD PRD,
                        SIATE.FTMPASN PASN,
                        SIATE.FTMPASP PASP
                    WHERE 
                        PASM.DANF      = PASN.TMDANF(+)
                        AND PASM.DANF        = PRD.DANF
                        AND PASN.TMPASSE     = PASP.TMPASSE(+)
                        AND PASN.TMNSEQ      = PASP.TMNSEQ(+)
                        AND PASP.TMPNITEM(+) = PRD.NFEPNITEM
                        AND ((PASM.NFEDDOCN  = '{cnpj}'
                            AND PASM.NFETPNF     = '1')
                            OR (PASM.NFERDOCN    = '{cnpj}'
                            AND PASM.NFETPNF     = '0'))
                        AND PASM.NFEDTEMI BETWEEN '{data_inicio.strftime("%d/%m/%Y")}' AND '{data_fim.strftime("%d/%m/%Y")}'
                        AND PASM.NFESIT  <> '2'
                        AND PASM.NFECANC  = 'N'
                        AND PASM.NFECANC <> 'D'
                
                """)
        
        def AdicionarNotasFiscaisExtemporaneas(self, chaves_acesso):
            inicial = 0

            while inicial < len(chaves_acesso):
                final = min(inicial+1000, len(chaves_acesso))
                query = f""" 
                    SELECT 
                        PASM.NFERDOCN AS "CNPJ.EMIT",
                        PASM.NFERRAZ AS "RAZ.EMITENTE",
                        PASM.NFERIEST AS "IE.ST.EMIT",
                        PASM.NFERUF AS "UF.EMIT",
                        CASE
                            WHEN PASM.NFERCRT = '1'
                            THEN 'SIMPLES'
                            WHEN PASM.NFERCRT = '2'
                            THEN 'SN.SUBLI.EXC'
                            WHEN PASM.NFERCRT = '3'
                            THEN 'NORMAL'
                            ELSE 'N/A'
                            END AS REGIME,
                        PASM.NFEDUF     AS "UF.DEST/REMET",
                        PASM.NFEDRAZ    AS "RAZ.DEST/REMET",
                        PASM.NFEDDOCN   AS "DOC.DEST/REMET",
                        PASM.NFEDINSC   AS "IE.DEST/REMET",
                        PASM.NFESUFRAMA AS SUFRAMA,
                        PASM.NFENNOT    AS "Nº.NF",
                        CASE
                            WHEN PASM.NFECANC = 'S'
                            THEN 'CANCELADA'
                            WHEN PASM.NFECANC = 'D'
                            THEN 'DENEGADA'
                            WHEN PASM.NFECANC = 'N'
                            THEN 'ATIVA'
                        END           AS "ATIVA/CANC",
                        PASM.DANF     AS "ChaveAcesso",
                        PASM.NFECFOP  AS "CFOP.NF",
                        PRD.NFEPCFOP  AS "CFOP.ITEM",
                        PASM.NFEDTEMI AS "DT.EMISSÃO",
                        CASE
                            WHEN PASM.NFESIT = '0'
                            THEN 'NÃO.DESEMB'
                            WHEN PASM.NFESIT = '1'
                            THEN 'DESEMBARAÇADA'
                            WHEN PASM.NFESIT = '2'
                            THEN 'OP.DESCONH'
                            WHEN PASM.NFESIT = '3'
                            THEN 'ALT.FISCAL'
                            WHEN PASM.NFESIT = '4'
                            THEN 'INTERMUNIC'
                        END                           AS SITUAÇÃO,
                        RPAD(PASM.NFENATOP, 30)       AS "NAT.OPER",
                        PASP.TMPCLASSE                AS "CLASSE.FRONT",
                        PRD.NFEPEAN                   AS GTIN,
                        PRD.NFEPCODITEM               AS "COD.PROD.EMIT",
                        PRD.NFEPNCM                   AS NCM,
                        PRD.NFEPNITEM                 AS "Nº.ITEM",
                        PRD.NFEPNDESC                 AS "DESC.ITEM",
                        PRD.NFEPQTRIB                 AS QNT,
                        PRD.NFEPUTRIB                 AS UND,
                        PRD.NFEPVUNTRIB               AS "VLR.UNT",
                        PRD.NFEPVPROD                 AS "VLR.BRUTO.ITEM",
                        PRD.NFEPVDESC                 AS "DESCON.ITEM",
                        PRD.NFEPVPROD - PRD.NFEPVDESC AS "VLR.LIQ.ITEM",
                        PRD.NFEPCST                   AS CST,
                        CASE
                            WHEN PRD.NFEPORIG IN ('1', '2', '3', '8')
                            THEN 'IMPORT'
                            ELSE 'NACIONAL'
                        END                AS ORIGEM,
                        PRD.NFEPPREDBC     AS "%.RED.BC",
                        PRD.NFEPVBC        AS "BC.ICMS.ITEM",
                        PRD.NFEPPICMS      AS ALIQ,
                        PRD.NFEPVICMS      AS "ICMS.ITEM",
                        PRD.NFEPVICMSDESON AS "ICMS.DESON.ITEM",
                        PASM.NFEVICMSD     AS "ICMS.DESON.NF",
                        PASM.NFEVNICMS     AS "ICMS.NF",
                        PRD.NFEPMDBCST     AS "MOD.BC.ST",
                        PRD.NFEPVBCST      AS "BC.ST.ITEM",
                        PRD.NFEPPMVAST     AS "MVA.ST",
                        PRD.NFEPPST        AS "ALIQ.ST.ITEM",
                        PRD.NFEPVST        AS "ST.RET.ITEM",
                        PRD.NFEPVSTR       AS "ST.RET.ANT.ITEM",
                        PRD.VICMSUFDE      AS "DIFAL.ITEM",
                        PASM.VICMSUFDE     AS "DIFAL.NF",
                        PRD.PICMSUFDE      AS "ALIQ.DEST",
                        PASM.NFEBNICMS     AS "BC.ICMS.NF",
                        PASM.NFEBSICMS     AS "BC.ST.NF",
                        PASM.NFEVSICMS     AS "ICMS.ST.NF",
                        PASM.NFETVPROD     AS "VLR.BR.ITENS.NF",
                        PASM.NFETVFRET     AS "FRETE.NF",
                        PASM.NFEVSEGU      AS "SEGURO.NF",
                        PASM.NFEVSICMS     AS "ICMS.ST.NF.2",
                        PASM.NFENVDESC     AS "DESCONTO.NF",
                        PASM.NFEVNOTA      AS "VALOR.FINAL.NF",
                        CASE
                            WHEN PASM.NFETPNF = '0'
                            THEN 'ENTRADA'
                            WHEN PASM.NFETPNF = '1'
                            THEN 'SAÍDA'
                            ELSE 'N/A'
                        END AS "TIPO.OPER",
                        CASE
                            WHEN PASM.NFEFINEMI = '1'
                            THEN 'NORMAL'
                            WHEN PASM.NFEFINEMI = '2'
                            THEN 'COMPLEMENTAR'
                            WHEN PASM.NFEFINEMI = '3'
                            THEN 'AJUSTE'
                            WHEN PASM.NFEFINEMI = '4'
                            THEN 'DEVOLUÇÃO'
                            ELSE 'N/A'
                        END AS FINALIDADE,
                        CASE
                            WHEN PASM.NFEIDDEST = '1'
                            THEN 'INTERNA'
                            WHEN PASM.NFEIDDEST = '2'
                            THEN 'INTERESTADUAL'
                            WHEN PASM.NFEIDDEST = '3'
                            THEN 'EXTERIOR'
                            ELSE 'N/A'
                        END               AS DESTINO,
                        PASM.NFEREFNFE    AS "NF-e.REF",
                        PASM.NFEINFADC    AS "DADOS.AD.P1",
                        PASM.NFEINFADCLOB AS "DADOS.AD.P2",
                        PASM.NFEDLOGRAD,
                        PASM.NFEDBAI,
                        PASM.NFEDNUM
                    FROM 
                        SIATE.NFEPASM PASM,
                        SIATE.NFEPRD PRD,
                        SIATE.FTMPASN PASN,
                        SIATE.FTMPASP PASP
                    WHERE 
                        PASM.DANF      = PASN.TMDANF(+)
                        AND PASM.DANF        = PRD.DANF
                        AND PASN.TMPASSE     = PASP.TMPASSE(+)
                        AND PASN.TMNSEQ      = PASP.TMNSEQ(+)
                        AND PASP.TMPNITEM(+) = PRD.NFEPNITEM
                        AND PASM.DANF IN {chaves_acesso[inicial:final]}
                
                """
                inicial += 1000
                self.queries.append(query)    
                
        def retrieve(self):
            dtypes = {'CFOP.ITEM': str}
            df = pd.DataFrame()
            for query in self.queries:
                temp = extrair_df_de_lobs(query)
                df = pd.concat([df, temp])

            for col, tipo in dtypes.items():
                df[col] = df[col].astype(tipo)

            return df
    
        def quantidade(self):
            query = f""" SELECT 
                            COUNT(*) AS quantidade
                        FROM 
                            SIATE.NFEPASM PASM
                        WHERE 
                            AND ((PASM.NFEDDOCN  = '{self.cnpj}'
                                AND PASM.NFETPNF     = '1')
                                OR (PASM.NFERDOCN    = '{self.cnpj}'
                                AND PASM.NFETPNF     = '0'))
                            AND PASM.NFEDTEMI BETWEEN '{self.data_inicio.strftime("%d/%m/%Y")}' AND '{self.data_fim.strftime("%d/%m/%Y")}'
                            AND PASM.NFESIT  <> '2'
                            AND PASM.NFECANC  = 'N'
                            AND PASM.NFECANC <> 'D'
                    """
            return extrair_df(query)



    class NotasFiscaisSaida(DocumentacaoAuditoria):
        """ Classe para importar Notas Fiscais de Saída. Não inclui notas fiscais ao consumidor (NFC-e)"""
        def __init__(self, cnpj, data_inicio, data_fim):
            self.cnpj = cnpj
            self.data_inicio = data_inicio 
            self.data_fim = data_fim 
            self.query = f""" 
                    SELECT
                        PASM.NFERDOCN                       AS "CNPJ.EMIT",            -- CNPJ do Remetente
                        PASM.NFERRAZ                        AS "RAZ.EMITENTE",             -- 
                        PASM.NFERIEST                       AS "IE.ST.EMIT",           -- Inscrição Estadual do Rementente em ST
                        PASM.NFERUF                         AS "UF.EMIT",              -- UF Remetente

                        CASE                                WHEN PASM.NFERCRT = '1' THEN 'SIMPLES'
                                                            WHEN PASM.NFERCRT = '2' THEN 'SN.SUBLI.EXC'
                                                            WHEN PASM.NFERCRT = '3' THEN 'NORMAL'
                                                            ELSE 'N/A' END AS "REGIME.EMIT",              -- Código de Regime Tributario: 1=Simples Nacional; 2=Simples Nacional, excesso sublimite de receita bruta; 3=Regime Normal. (v2.0).
                        
                        PASM.NFEDUF                         AS "UF.DEST/REMET",             -- UF Destino
                        PASM.NFEDRAZ                        AS "RAZ.DEST/REMET",
                        PASM.NFEDDOCN                       AS "DOC.DEST/REMET",
                        PASM.NFEDINSC                       AS "IE.DEST/REMET",
                        PASM.NFESUFRAMA                     AS "SUFRAMA",
                        PASM.NFENNOT                        AS "Nº.NF",
                        CASE                                WHEN  pasm.NFECANC = 'S' THEN 'CANCELADA'
                                                            WHEN  pasm.NFECANC = 'D' THEN 'DENEGADA'
                                                            WHEN  pasm.NFECANC = 'N' THEN 'ATIVA' END AS "ATIVA/CANC", 
                        PASM.DANF                           AS "CHAVE.NF",
                        PASM.NFECFOP                        AS "CFOP.NF",            -- CFOP da Nota
                        PASM.NFEDTEMI                       AS "DT.EMISSÃO",             -- Data da Emissão da NF-e 
                        --PASM.NFEHORA                        AS "HORA",                -- Hora da Emissão da NF-e 
                        CASE                                WHEN  PASM.NFESIT = '0' THEN 'NÃO.DESEMB'
                                                            WHEN  PASM.NFESIT = '1' THEN 'DESEMBARAÇADA'
                                                            WHEN  PASM.NFESIT = '2' THEN 'OP.DESCONH'
                                                            WHEN  PASM.NFESIT = '3' THEN 'ALT.FISCAL'
                                                            WHEN  PASM.NFESIT = '4' THEN 'INTERMUNIC' END AS "SITUAÇÃO", 
                        RPAD(PASM.NFENATOP, 30)             AS "NAT.OPER",
                        --CASE                                WHEN PASM.NFECANC = 'S' THEN 'Cancelada' 
                                                            --WHEN PASM.NFECANC = 'D' THEN 'Denegada' 
                                                            --ELSE 'ATIVA' END AS "NF.CANC?",
                        PASP.TMPCLASSE                      AS "CLASSE.FRONT",
                        PRD.NFEPEAN                         AS GTIN,
                        PRD.NFEPCODITEM                     AS "COD.PROD.EMIT",             
                        --PRD.NFEPCPRODANP                    AS ANP,                     
                        PRD.NFEPNCM                         AS NCM,          
                        PRD.NFEPCFOP                        AS "CFOP.ITEM",           -- CFOP do Produto
                        PRD.NFEPNITEM                       AS "Nº.ITEM",              -- nº Item
                        PRD.NFEPNDESC                       AS "DESC.ITEM",             -- Descrição do Produto
                        PRD.NFEPQTRIB                       AS "QNT",                 -- Quantidade Tributável
                        PRD.NFEPUTRIB                       AS "UND",                 -- Unidade
                        PRD.NFEPVUNTRIB                     AS "VLR.UNT",
                        PRD.NFEPVPROD                       AS "VLR.BRUTO.ITEM",               -- Valor do Produto
                        PRD.NFEPVDESC                       AS "DESCON.ITEM",
                        PRD.NFEPVPROD - PRD.NFEPVDESC       AS "VLR.LIQ.ITEM", 
                        PRD.NFEPPREDBC                      as "%.RED.BC",
                        PRD.NFEPVBC                         AS "BC.ICMS.ITEM",
                        PRD.NFEPPICMS                       AS "ALIQ",
                        PRD.NFEPVICMS                       AS "ICMS.ITEM",
                        PRD.NFEPCST                         AS "CST",
                        CASE                                WHEN  PRD.NFEPORIG IN ('1','2','3','8') THEN 'IMPORT'
                                                            ELSE 'NACIONAL' END AS "ORIGEM", 
                        PRD.NFEPMDBCST                      AS "MOD.BC.ST",
                        PRD.NFEPPMVAST                      AS "MVA.ST",
                        PRD.NFEPVBCST                       AS "BC.ST.ITEM",
                        PRD.NFEPPST                         AS "ALIQ.ST.ITEM", 
                        PRD.NFEPVST                         AS "ST.RET.ITEM",       -- Valor do ICMS ST (NFe N23)
                        PRD.NFEPVICMSDESON                  AS "ICMS.DESON.ITEM",
                        PRD.PICMSUFDE                       AS "ALIQ.DEST",           -- ALIQUOTA ICMS DESTINO - DIFAL
                        PRD.VICMSUFDE                       AS "DIFAL.ITEM",     -- ICMS DESTINO por produto - DIFAL
                        PASM.VICMSUFDE                      AS "DIFAL.NF",           -- Total de ICMS DESTINO - DIFAL
                        PASM.NFETVPROD                      AS "VLR.BR.ITENS.NF",
                        PASM.NFEBNICMS                      AS "BC.ICMS.NF",
                        PASM.NFEVNICMS                      AS "ICMS.NF",
                        PASM.NFEVICMSD                      AS "ICMS.DESON.NF",
                        PASM.NFEBSICMS                      AS "BC.ST.NF",
                        PASM.NFEVSICMS                      AS "ICMS.ST.NF",
                        PASM.NFETVFRET                      AS "FRETE.NF",
                        PASM.NFEVSEGU                       AS "SEGURO.NF",
                        PASM.NFEVSICMS                      AS "ICMS.ST.NF",
                        PASM.NFENVDESC                      AS "DESCONTO.NF",
                        PASM.NFEVNOTA                       AS "VALOR.FINAL.NF",
                                
                        CASE                                WHEN PASM.NFETPNF = '0' THEN 'ENTRADA'
                                                            WHEN PASM.NFETPNF = '1' THEN 'SAÍDA'
                                                            ELSE 'N/A' END AS "TIPO.OPER",                -- Tipo de Operacao: 0 - Entrada e 1- Saida
                        
                        CASE                                WHEN PASM.NFEFINEMI = '1' THEN 'NORMAL'
                                                            WHEN PASM.NFEFINEMI = '2' THEN 'COMPLEMENTAR'
                                                            WHEN PASM.NFEFINEMI = '3' THEN 'AJUSTE'
                                                            WHEN PASM.NFEFINEMI = '4' THEN 'DEVOLUÇÃO'
                                                            ELSE 'N/A' END AS "FINALIDADE",                
                            
                        CASE                                WHEN PASM.NFEIDDEST = '1' THEN 'INTERNA'
                                                            WHEN PASM.NFEIDDEST = '2' THEN 'INTERESTADUAL'
                                                            WHEN PASM.NFEIDDEST = '3' THEN 'EXTERIOR'
                                                            ELSE 'N/A' END AS "DESTINO",
                        PASM.NFEREFNFE                      AS "NF-e.REF",
                        PASM.NFEINFADC                      AS "DADOS.AD.P1",
                        PASM.NFEINFADCLOB                   AS "DADOS.AD.P2"
                                


                    FROM
                        
                        SIATE.NFEPASM PASM,
                        SIATE.NFEPRD PRD,
                        SIATE.FTMPASN PASN,
                        SIATE.FTMPASP PASP
                        

                    WHERE
                        /*JOINS*/
                                
                                PASM.DANF            = PASN.TMDANF(+)   
                            AND PASM.DANF            = PRD.DANF
                            AND PASN.TMPASSE         = PASP.TMPASSE(+)
                            AND PASN.TMNSEQ          = PASP.TMNSEQ(+)
                            AND PASP.TMPNITEM(+)     = PRD.NFEPNITEM
                            
                            AND PASM.NFEDTEMI BETWEEN '{data_inicio.strftime("%d/%m/%Y")}' AND '{data_fim.strftime("%d/%m/%Y")}'
                            AND ((PASM.NFERDOCN = '{cnpj}' AND PASM.NFETPNF = '1') or (PASM.NFEDDOCN = '{cnpj}' AND PASM.NFETPNF = '0')) --EMITENTE E SAÍDA / REMETENTE DE DEVOLUÇÃO
                            and pasm.NFECANC = 'N'
                            and pasm.NFECANC <> 'D'
                    
                    ORDER BY 
                        PASM.NFEDTEMI DESC, 
                        PASM.NFEHORA desc, 
                        PASM.DANF
                """
        
        def AdicionarNotasFiscaisExtemporaneas(self, chaves_acesso):
            inicial = 0

            while inicial < len(chaves_acesso):
                final = min(inicial+1000, len(chaves_acesso))
                query = f""" 
                    SELECT 
                        PASM.NFERDOCN AS "CNPJ.EMIT",
                        PASM.NFERRAZ AS "RAZ.EMITENTE",
                        PASM.NFERIEST AS "IE.ST.EMIT",
                        PASM.NFERUF AS "UF.EMIT",
                        CASE
                            WHEN PASM.NFERCRT = '1'
                            THEN 'SIMPLES'
                            WHEN PASM.NFERCRT = '2'
                            THEN 'SN.SUBLI.EXC'
                            WHEN PASM.NFERCRT = '3'
                            THEN 'NORMAL'
                            ELSE 'N/A'
                            END AS REGIME,
                        PASM.NFEDUF     AS "UF.DEST/REMET",
                        PASM.NFEDRAZ    AS "RAZ.DEST/REMET",
                        PASM.NFEDDOCN   AS "DOC.DEST/REMET",
                        PASM.NFEDINSC   AS "IE.DEST/REMET",
                        PASM.NFESUFRAMA AS SUFRAMA,
                        PASM.NFENNOT    AS "Nº.NF",
                        CASE
                            WHEN PASM.NFECANC = 'S'
                            THEN 'CANCELADA'
                            WHEN PASM.NFECANC = 'D'
                            THEN 'DENEGADA'
                            WHEN PASM.NFECANC = 'N'
                            THEN 'ATIVA'
                        END           AS "ATIVA/CANC",
                        PASM.DANF     AS "ChaveAcesso",
                        PASM.NFECFOP  AS "CFOP.NF",
                        PRD.NFEPCFOP  AS "CFOP.ITEM",
                        PASM.NFEDTEMI AS "DT.EMISSÃO",
                        CASE
                            WHEN PASM.NFESIT = '0'
                            THEN 'NÃO.DESEMB'
                            WHEN PASM.NFESIT = '1'
                            THEN 'DESEMBARAÇADA'
                            WHEN PASM.NFESIT = '2'
                            THEN 'OP.DESCONH'
                            WHEN PASM.NFESIT = '3'
                            THEN 'ALT.FISCAL'
                            WHEN PASM.NFESIT = '4'
                            THEN 'INTERMUNIC'
                        END                           AS SITUAÇÃO,
                        RPAD(PASM.NFENATOP, 30)       AS "NAT.OPER",
                        PASP.TMPCLASSE                AS "CLASSE.FRONT",
                        PRD.NFEPEAN                   AS GTIN,
                        PRD.NFEPCODITEM               AS "COD.PROD.EMIT",
                        PRD.NFEPNCM                   AS NCM,
                        PRD.NFEPNITEM                 AS "Nº.ITEM",
                        PRD.NFEPNDESC                 AS "DESC.ITEM",
                        PRD.NFEPQTRIB                 AS QNT,
                        PRD.NFEPUTRIB                 AS UND,
                        PRD.NFEPVUNTRIB               AS "VLR.UNT",
                        PRD.NFEPVPROD                 AS "VLR.BRUTO.ITEM",
                        PRD.NFEPVDESC                 AS "DESCON.ITEM",
                        PRD.NFEPVPROD - PRD.NFEPVDESC AS "VLR.LIQ.ITEM",
                        PRD.NFEPCST                   AS CST,
                        CASE
                            WHEN PRD.NFEPORIG IN ('1', '2', '3', '8')
                            THEN 'IMPORT'
                            ELSE 'NACIONAL'
                        END                AS ORIGEM,
                        PRD.NFEPPREDBC     AS "%.RED.BC",
                        PRD.NFEPVBC        AS "BC.ICMS.ITEM",
                        PRD.NFEPPICMS      AS ALIQ,
                        PRD.NFEPVICMS      AS "ICMS.ITEM",
                        PRD.NFEPVICMSDESON AS "ICMS.DESON.ITEM",
                        PASM.NFEVICMSD     AS "ICMS.DESON.NF",
                        PASM.NFEVNICMS     AS "ICMS.NF",
                        PRD.NFEPMDBCST     AS "MOD.BC.ST",
                        PRD.NFEPVBCST      AS "BC.ST.ITEM",
                        PRD.NFEPPMVAST     AS "MVA.ST",
                        PRD.NFEPPST        AS "ALIQ.ST.ITEM",
                        PRD.NFEPVST        AS "ST.RET.ITEM",
                        PRD.NFEPVSTR       AS "ST.RET.ANT.ITEM",
                        PRD.VICMSUFDE      AS "DIFAL.ITEM",
                        PASM.VICMSUFDE     AS "DIFAL.NF",
                        PRD.PICMSUFDE      AS "ALIQ.DEST",
                        PASM.NFEBNICMS     AS "BC.ICMS.NF",
                        PASM.NFEBSICMS     AS "BC.ST.NF",
                        PASM.NFEVSICMS     AS "ICMS.ST.NF",
                        PASM.NFETVPROD     AS "VLR.BR.ITENS.NF",
                        PASM.NFETVFRET     AS "FRETE.NF",
                        PASM.NFEVSEGU      AS "SEGURO.NF",
                        PASM.NFEVSICMS     AS "ICMS.ST.NF.2",
                        PASM.NFENVDESC     AS "DESCONTO.NF",
                        PASM.NFEVNOTA      AS "VALOR.FINAL.NF",
                        CASE
                            WHEN PASM.NFETPNF = '0'
                            THEN 'ENTRADA'
                            WHEN PASM.NFETPNF = '1'
                            THEN 'SAÍDA'
                            ELSE 'N/A'
                        END AS "TIPO.OPER",
                        CASE
                            WHEN PASM.NFEFINEMI = '1'
                            THEN 'NORMAL'
                            WHEN PASM.NFEFINEMI = '2'
                            THEN 'COMPLEMENTAR'
                            WHEN PASM.NFEFINEMI = '3'
                            THEN 'AJUSTE'
                            WHEN PASM.NFEFINEMI = '4'
                            THEN 'DEVOLUÇÃO'
                            ELSE 'N/A'
                        END AS FINALIDADE,
                        CASE
                            WHEN PASM.NFEIDDEST = '1'
                            THEN 'INTERNA'
                            WHEN PASM.NFEIDDEST = '2'
                            THEN 'INTERESTADUAL'
                            WHEN PASM.NFEIDDEST = '3'
                            THEN 'EXTERIOR'
                            ELSE 'N/A'
                        END               AS DESTINO,
                        PASM.NFEREFNFE    AS "NF-e.REF",
                        PASM.NFEINFADC    AS "DADOS.AD.P1",
                        PASM.NFEINFADCLOB AS "DADOS.AD.P2",
                        PASM.NFEDLOGRAD,
                        PASM.NFEDBAI,
                        PASM.NFEDNUM
                    FROM 
                        SIATE.NFEPASM PASM,
                        SIATE.NFEPRD PRD,
                        SIATE.FTMPASN PASN,
                        SIATE.FTMPASP PASP
                    WHERE 
                        PASM.DANF      = PASN.TMDANF(+)
                        AND PASM.DANF        = PRD.DANF
                        AND PASN.TMPASSE     = PASP.TMPASSE(+)
                        AND PASN.TMNSEQ      = PASP.TMNSEQ(+)
                        AND PASP.TMPNITEM(+) = PRD.NFEPNITEM
                        AND PASM.DANF IN {chaves_acesso[inicial:final]}
                
                """
                inicial += 1000
                self.queries.append(query)    
                
        def retrieve(self):
            dtypes = {'CFOP.ITEM': str}
            df = pd.DataFrame()
            for query in self.queries:
                temp = extrair_df_de_lobs(query)
                df = pd.concat([df, temp])

            for col, tipo in dtypes.items():
                df[col] = df[col].astype(tipo)

            return df

        def quantidade(self):
            query = f""" SELECT 
                            COUNT(*) AS quantidade
                        FROM 
                            SIATE.NFEPASM PASM
                        WHERE 
                            PASM.NFEDTEMI BETWEEN '{self.data_inicio.strftime("%d/%m/%Y")}' AND '{self.data_fim.strftime("%d/%m/%Y")}'
                            AND ((PASM.NFERDOCN = '{self.cnpj}' AND PASM.NFETPNF = '1') or (PASM.NFEDDOCN = '{self.cnpj}' AND PASM.NFETPNF = '0')) --EMITENTE E SAÍDA / REMETENTE DE DEVOLUÇÃO
                            and pasm.NFECANC = 'N'
                            and pasm.NFECANC <> 'D'
                    """
            return extrair_df(query)
        

    class NotasFiscaisConsumidor(DocumentacaoAuditoria):
        
        def __init__(self, cnpj, data_inicio, data_fim):
            self.cnpj = cnpj
            self.data_inicio = data_inicio
            self.data_fim = data_fim
            self.queries = []
            self.queries.append(f""" 
                    SELECT 
                        
                        X.NFCE_NR_DOC_REM       AS "CNPJ.EMIT",
                        x.NFCE_NM_RAZ_REM       as "EMIT",
                        x.NFCE_NM_RAZ_DES       AS "DEST",
                        X.NFCE_NR_DOC_DES       AS "CNPJ.DEST",
                        X.NFCE_DT_EMISSAO       AS "DT.EMISSAO",
                        X.NFCE_NR_DANF          AS "CHAVE",
                        X.NFCE_NR_NOTA          AS "Nº.NFC-e",
                        x.NFCE_NAT_OPER         as "NAT.OPER",
                        x.NFCE_CD_CFOP          as "CFOP.NFC-e",
                        Y.NFCP_CD_EAN           AS "GTIN",
                        Y.NFCP_CODIGO_ITEM      AS "COD.ITEM",
                        Y.NFCP_CD_NCM           AS "NCM",
                        Y.NFCP_CD_CFOP          AS "CFOP.ITEM",
                        Y.NFCP_CLAS_TRIB        AS "CLASS.TRIB",
                        Y.NFCP_CD_OP_SN         AS "SIT.OPE",
                        Y.NFCP_NR_ITEM          AS "Nº.ITEM",
                        Y.NFCP_DESC             AS "DESC",
                        Y.NFCP_UN_TRIB          AS "UND",
                        Y.NFCP_QT_TRIB          AS "QNT",
                        y.NFCP_VL_UNIT_TRIB     AS "VLR.UNT.TRIB",
                        Y.NFCP_TT_PROD          AS "VLR.BR.PRODUTOS",
                        Y.NFCP_VL_DESC          AS "DESCONTO",
                        Y.NFCP_TT_PROD - Y.NFCP_VL_DESC as "VLR.LIQ.PRODUTOS",
                        Y.NFCP_VL_BC            AS "BC.ICMS.ITEM",
                        Y.NFCP_ALIQ_ICMS        AS "ALIQ",
                        Y.NFCP_VL_ICMS          AS "ICMS.ITEM",
                        Y.NFCP_VL_FRETE         AS "FRETE",
                        Y.NFCP_VL_SEGURO        AS "SEGURO",
                        Y.NFCP_VL_OUTROS        AS "OUTROS",
                        Y.NFCP_CLAS_TRIB        AS "CST",
                        Y.NFCP_ORIGEM           AS "ORIGEM",
                        CASE                                WHEN  Y.NFCP_ORIGEM IN ('1','2','3','8') THEN 'IMPORT'
                                                            ELSE 'NACIONAL' END AS "ORIGEM", 
                        X.NFCE_BS_ICMS          AS "BC.NFC-e",
                        x.NFCE_VL_ICMS          as "ICMS.NFC-e",
                        Y.NFCP_PC_MVAST         AS "MVA",
                        Y.NFCP_VL_BCST          AS "BC.ST",
                        Y.NFCP_ALIQ_ICMS_ST     AS "ALIQ.ST",
                        Y.NFCP_VL_ICMS_ST       AS "ICMS.ST",
                        Y.NFCP_VL_BCSTR         AS "BC.ICMS.Ret",
                        Y.NFCP_VL_ICMS_STR      AS "VLR.ICMS.Ret",
                        X.NFCE_TT_NOTA          AS "TOTAL.LIQ.NFC-e"
                        

                    FROM 

                        NFCE_OWNER.TB_NFCE X,
                        NFCE_OWNER.TB_NFCEPRD Y

                    WHERE 
                        X.NFCE_NR_DANF   = Y.NFCP_NR_DANF
                    AND X.NFCE_DT_EMISSAO between '{data_inicio.strftime("%d/%m/%Y")}' AND '{data_fim.strftime("%d/%m/%Y")}'
                    AND X.NFCE_NR_DOC_REM  = '{cnpj}'
                    AND X.NFCE_CANC        = 'N'

                    ORDER BY 
                        X.NFCE_DT_EMISSAO desc, X.NFCE_NR_DANF, Y.NFCP_NR_ITEM
                """)
        
        def AdicionarNotasFiscaisConsumidorExtemporaneas(self, chaves_acesso):

            query = f""" 
                    SELECT 
                        
                        X.NFCE_NR_DOC_REM       AS "CNPJ.EMIT",
                        x.NFCE_NM_RAZ_REM       as "EMIT",
                        x.NFCE_NM_RAZ_DES       AS "DEST",
                        X.NFCE_NR_DOC_DES       AS "CNPJ.DEST",
                        X.NFCE_DT_EMISSAO       AS "DT.EMISSAO",
                        X.NFCE_NR_DANF          AS "CHAVE",
                        X.NFCE_NR_NOTA          AS "Nº.NFC-e",
                        x.NFCE_NAT_OPER         as "NAT.OPER",
                        x.NFCE_CD_CFOP          as "CFOP.NFC-e",
                        Y.NFCP_CD_EAN           AS "GTIN",
                        Y.NFCP_CODIGO_ITEM      AS "COD.ITEM",
                        Y.NFCP_CD_NCM           AS "NCM",
                        Y.NFCP_CD_CFOP          AS "CFOP.ITEM",
                        Y.NFCP_CLAS_TRIB        AS "CLASS.TRIB",
                        Y.NFCP_CD_OP_SN         AS "SIT.OPE",
                        Y.NFCP_NR_ITEM          AS "Nº.ITEM",
                        Y.NFCP_DESC             AS "DESC",
                        Y.NFCP_UN_TRIB          AS "UND",
                        Y.NFCP_QT_TRIB          AS "QNT",
                        y.NFCP_VL_UNIT_TRIB     AS "VLR.UNT.TRIB",
                        Y.NFCP_TT_PROD          AS "VLR.BR.PRODUTOS",
                        Y.NFCP_VL_DESC          AS "DESCONTO",
                        Y.NFCP_TT_PROD - Y.NFCP_VL_DESC as "VLR.LIQ.PRODUTOS",
                        Y.NFCP_VL_BC            AS "BC.ICMS.ITEM",
                        Y.NFCP_ALIQ_ICMS        AS "ALIQ",
                        Y.NFCP_VL_ICMS          AS "ICMS.ITEM",
                        Y.NFCP_VL_FRETE         AS "FRETE",
                        Y.NFCP_VL_SEGURO        AS "SEGURO",
                        Y.NFCP_VL_OUTROS        AS "OUTROS",
                        Y.NFCP_CLAS_TRIB        AS "CST",
                        Y.NFCP_ORIGEM           AS "ORIGEM",
                        CASE                                WHEN  Y.NFCP_ORIGEM IN ('1','2','3','8') THEN 'IMPORT'
                                                            ELSE 'NACIONAL' END AS "ORIGEM", 
                        X.NFCE_BS_ICMS          AS "BC.NFC-e",
                        x.NFCE_VL_ICMS          as "ICMS.NFC-e",
                        Y.NFCP_PC_MVAST         AS "MVA",
                        Y.NFCP_VL_BCST          AS "BC.ST",
                        Y.NFCP_ALIQ_ICMS_ST     AS "ALIQ.ST",
                        Y.NFCP_VL_ICMS_ST       AS "ICMS.ST",
                        Y.NFCP_VL_BCSTR         AS "BC.ICMS.Ret",
                        Y.NFCP_VL_ICMS_STR      AS "VLR.ICMS.Ret",
                        X.NFCE_TT_NOTA          AS "TOTAL.LIQ.NFC-e"
                        

                    FROM 

                        NFCE_OWNER.TB_NFCE X,
                        NFCE_OWNER.TB_NFCEPRD Y

                    WHERE 
                        X.NFCE_NR_DANF   = Y.NFCP_NR_DANF
                    AND NFCE_NR_DANF IN {chaves_acesso}

                    ORDER BY 
                        X.NFCE_DT_EMISSAO desc, X.NFCE_NR_DANF, Y.NFCP_NR_ITEM
                """

            self.queries.append(query)
     
        def retrieve(self):
            df = pd.DataFrame()
            for query in self.queries:
                temp = extrair_df_de_lobs(query)
                df = pd.concat([df, temp])
            return df
    
        def quantidade(self):
            query = f""" SELECT 
                            COUNT(*) AS quantidade
                        FROM 
                            NFCE_OWNER.TB_NFCE NFCE
                        WHERE 
                            AND NFCE.NFCE_DT_EMISSAO between '{self.data_inicio.strftime("%d/%m/%Y")}' AND '{self.data_fim.strftime("%d/%m/%Y")}'
                            AND NFCE.NFCE_NR_DOC_REM  = '{self.cnpj}'
                            AND NFCE.NFCE_CANC        = 'N'
                    """
            return extrair_df(query)
    
    class EventosNotas(DocumentacaoAuditoria):
        
        def __init__(self, chaves_acesso):
        
            inicial = 0
            self.queries = []
            while inicial < len(chaves_acesso):
                final = min(inicial+1000, len(chaves_acesso))
                query = f"""
                            SELECT
                                DANF AS "ChaveAcesso", 
                                NFEEVSEQ AS "SequenciaEvento",
                                NFEEVDATA AS "DataEvento",
                                NFEEVHORA AS "HoraEvento",
                                NFEEVCOD AS "CodigoEvento",
                                NFEEVDESC AS "DescricaoEvento",
                                NFEEVJUST AS "Justificativa"
                            FROM
                                SIATE.NFEEVEN
                            WHERE
                                DANF IN {chaves_acesso[inicial:final]}
                        """
                
                self.queries.append(query)
                inicial += 1000
        
        def retrieve(self):
            df = pd.DataFrame()
            for query in self.queries:
                temp = extrair_df_de_lobs(query)
                df = pd.concat([df, temp])
            return df 
    
    class EventosNotasConsumidor(DocumentacaoAuditoria):
        
        def __init__(self, chaves_acesso):
        
            inicial = 0
            self.queries = []
            
            while inicial < len(chaves_acesso):
                final = min(inicial+1000, len(chaves_acesso))
                query = f"""
                        SELECT
                            SELECT
                                NFCE_NR_DANF AS "ChaveAcesso", 
                                NFCE_EVSEQ AS "SequenciaEvento",
                                NFCE_EVDATA AS "DataEvento",
                                NFCE_EVHORA AS "HoraEvento",
                                NFCE_EVCOD AS "CodigoEvento",
                                NFCE_EVDESC AS "DescricaoEvento",
                                NFCE_EVJUST AS "Justificativa"
                            FROM
                                NFCE_OWNER.TB_NFCEEVEN
                            WHERE
                                NFCE_NR_DANF IN {chaves_acesso[inicial:final]}
                        """
                
                self.queries.append(query)
                inicial += 1000
        
        def retrieve(self):
            df = pd.DataFrame()
            for query in self.queries:
                temp = extrair_df_de_lobs(query)
                df = pd.concat([df, temp])
            return df 
             
    class EventosDesembaraco(DocumentacaoAuditoria):
        
        def __init__(self, chaves_acesso):
            inicial = 0
            self.queries = []
            while inicial < len(chaves_acesso):
                final = min(inicial+1000, len(chaves_acesso))
                query = f""" 
                            SELECT
                                TMDANF AS "CHV_ACC",
                                TMDTPAS AS "DATA_PASSAGEM",
                                TMHRPAS AS "HORA_PASSAGEM"
                            FROM
                                SIATE.FTMPASL L INNER JOIN SIATE.FTMPASN N ON L.TMPASSE = N.TMPASSE
                            WHERE
                                TMDANF IN {chaves_acesso[inicial:final]}
                        """
                self.queries.append(query)
                inicial += 1000
        
        def retrieve(self):
            df = pd.DataFrame()
            for query in self.queries:
                temp = extrair_df_de_lobs(query)
                df = pd.concat([df, temp])
            return df 


class DadosParametrizados:
    
    def BuscarCFOPTributados():
        endereco = r'parametrizacoes\CFOPs.xlsx'
        CFOP = pd.read_excel(endereco, dtype={'CFOP': str, 'GERA_CREDITO': bool})
        CFOP['TRIBUTADO'] = [True if linha['TMCLOE']=='TRIB' else False for idx, linha in CFOP.iterrows()]
        return CFOP[['CFOP', 'GERA_CREDITO', 'TRIBUTADO']]
    
    def BuscarGtin():
        endereco = r'parametrizacoes\GTIN.csv'
        temp1 = pd.read_csv(endereco, dtype={'GTIN': str, 'CLASSE': str})
        return temp1 
    
    def BuscarNcmParametrizados():
        endereco = r'parametrizacoes\NCMs PARAMETRIZADAS.csv'
        df = pd.read_csv(endereco, usecols = ['NCM', 'PARAMETRIZAÇÃO'], dtype={'NCM': str, 'PARAMETRIZAÇÃO': str})
        return df
    

    def BuscarClassesFronteira():
        endereco = r'parametrizacoes\CLASSE.xlsx'
        try:
            return pd.read_excel(endereco, dtypes={'CLASSE': str})
        except:

            query = """ 
                    SELECT 
                        CASE                                   
                            WHEN  c.TMCTOP = '1'     THEN 'ENTRADA'
                            WHEN  c.TMCTOP = '2'     THEN 'Saída.Interest'
                            WHEN  c.TMCTOP = '3'     THEN 'Saida.Interna'
                            WHEN  c.TMCTOP = '4'     THEN 'Importação'
                            WHEN  c.TMCTOP = '5'     THEN 'Exportação' END AS "Tipo Operação", 
                        
                        C.TMCLFPAG                     as "forma.pag",          
                        CASE                                   
                            WHEN  C.TMCLFPAG = '  '   THEN 'Não Tributada'
                            WHEN  C.TMCLFPAG = '40' THEN 'Exportação.'
                            WHEN  C.TMCLFPAG = '46' THEN 'DIFAL Remet.'
                            WHEN  C.TMCLFPAG = '47' THEN 'DIFAL Consumo'
                            WHEN  C.TMCLFPAG = '48' THEN 'DIFAL Imobilizado'
                            WHEN  C.TMCLFPAG = '35' THEN 'Import.'
                            WHEN  C.TMCLFPAG = '55' THEN 'Comp. Govern.'
                            WHEN  C.TMCLFPAG = '60' THEN 'Diferimento'
                            WHEN  C.TMCLFPAG = '25' THEN 'Sub. Tributária'
                            WHEN  C.TMCLFPAG = '45' THEN 'Ant. Parcial'
                            WHEN  C.TMCLFPAG = '20' THEN 'Ant. Total' END AS "FORMA PGTO?", 
                        C.TMCLCOD  AS CLASSE,
                        c.TMCSEQ as seq,
                        --c.TMCLDTFI as DT_fim,
                        A.TMCLDESC AS DESCRICAO,
                        C.TMCLAMPL AS "AMPARO LEGAL",
                        CASE                                  WHEN  A.TMCSIT = 'S'     THEN 'Sim' else 'Não' END AS "Ativo?",
                        c.TMCLDTIN as "Dt.Início",
                        CASE
                            WHEN  C.TMCLGDEB = 'S' THEN 'Sim' 
                            ELSE 'Não' END AS "GERA DEBITO?",
                        
                        CASE                                 WHEN  C.TMCLCRED = 'S'     THEN 'Sim' ELSE 'Não' END AS "GERA CRÉDITO?",
                        
                        CASE                                 WHEN  C.TMCMVA1 > 0 THEN TO_CHAR(C.TMCMVA1) else 'Sem MVA' END AS "MVA(S/SD-ES)",
                        
                        CASE                                 WHEN  C.TMCMVA1 > 0 THEN TO_CHAR(C.TMCMVA1) else 'Sem MVA' END AS "MVA(N/NE/CO+ES)",
                        
                        CASE                                 WHEN  C.TMCLALIQ in (12,17,20,25) THEN TO_CHAR(C.TMCLALIQ) else 'Não.Trib' END AS "Aliq?",
                        
                        CASE                                  WHEN  c.TMCLTPALIQ = '1'     THEN 'Básica'
                                                                WHEN  c.TMCLTPALIQ = '2'     THEN 'Modal'
                                                                WHEN  c.TMCLTPALIQ = '3'     THEN 'Supérflua'
                                                                WHEN  c.TMCLTPALIQ = '4'     THEN 'Não.Trib' 
                                                                WHEN  c.TMCLTPALIQ not in ('1','2','3') THEN 'Não.Trib' else 'Não.Trib' END AS "Tipo Aliq?",
                        
                        CASE                                 WHEN  C.TMCLRBCL = 'S' THEN 'Sim' else 'Não' END AS "RED BC DEB?",
                        
                        CASE                                 WHEN  C.TMCLRBCC = 'S' THEN 'Sim' else 'Não' END AS "RED BC CRED?",
                        
                        CASE                                  WHEN  c.TMPMPF = 'S'     THEN 'Sim'
                                                                WHEN  c.TMPMPF is null   THEN 'Não'
                                                                WHEN  c.TMPMPF = 'N'     THEN 'Não' END AS "É PMPF?",
                        CASE                                 WHEN  c.TMCLUNID is not null THEN c.TMCLUNID else 'Inaplicável' END AS "Und Med?",
                                                                
                        CASE                                 WHEN  (c.TMCLPMPF is not null AND c.TMCLPMPF != 0) THEN TO_CHAR(c.TMCLPMPF) else 'Inaplicável' END AS "Vlr PMPF?",
                        
                        
                        CASE                                 WHEN  c.TMCLIMB = 'S' THEN 'Sim' else 'Não' END AS "Imobilizado?"
                                                                
                        
                        
                        
                        FROM 
                        SIATE.FTMCL a, siate.ftmcli c 

                        WHERE 
                        C.TMCLCOD = A.TMCLCOD
                        and c.TMCTOP is not null
                        AND A.TMCSIT = 'S'
                        AND (C.TMCLDTFI = '01/01/0001' OR C.TMCLDTFI IS NULL)
                        --and c.TMCTOP = '2'
                        --and C.TMCLFPAG IN ('25')
                        --and A.TMCLDESC like '%CERVEJA%'
                        --and c.TMPMPF = 'S'
                        --and C.TMCLFPAG IN ('45','46','47','48')
                        and a.TMCLCOD >= '4000'


                        ORDER BY c.TMCTOP, C.TMCLCOD, c.TMCSEQ desc

                    """
            df = extrair_df(query)

            df.to_excel(endereco, index=False)
            return df