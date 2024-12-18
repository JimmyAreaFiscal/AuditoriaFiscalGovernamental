import oracledb
import pandas as pd 
from dados import user, pw, dns_tns
from dateutil.relativedelta import relativedelta
import warnings
import functools
from sqlalchemy import create_engine




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
        
        print(f'Recuperado {len(doc)} registros!')
        return doc
    
    def save(self, path, sheet_name='GIM'):
        doc = pd.ExcelWriter(path=path)
        doc_usado = self.retrieve()
        doc_usado.to_excel(doc, sheet_name=sheet_name, index=False)
        doc.close()

        # Adicionando "cache" de memória secundária
        self.path = path 


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
            try:    # Verificando se há dado importado na memória secundária
                return pd.read_excel(self.path)
            except:
                with oracledb.connect(user=user, password=pw, dsn=dns_tns) as connection:
                    gim = pd.read_sql_query(self.query, con=connection)
                print(f'Recuperado {len(gim)} registros!')
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
    

    class DEFIS(DocumentacaoAuditoria):
        # Parâmetros de conexão com o PostgreSQL
        DATABASE_URL = "postgresql+psycopg2://auditor:1234@localhost:5432/simples_nacional"
        engine = create_engine(DATABASE_URL)

        def __init__(self, cnpj, data_inicio, data_fim):
            
            self.cnpj = cnpj 
            ano_inicial = int(data_inicio.strftime('%Y'))
            ano_final = int(data_fim.strftime('%Y'))
            lista_anos = [x for x in range(ano_inicial, ano_final+1)]
            self.query = f"""

            with defis_valida as (
                select 
                    ULTIMO."id"
                from 
                    "defis"."D1000" ULTIMO
                where 
                    not exists (select 
                                    1 
                                from 
                                    "defis"."D1000" ANTERIORES 
                                where 
                                    SUBSTR(ANTERIORES."DEFIS_ID_DECLARACAO", 0, 12) = SUBSTR(ULTIMO."DEFIS_ID_DECLARACAO", 0, 12)
                                    and ANTERIORES."DEFIS_DT_TRANSMISSAO" > ULTIMO."DEFIS_DT_TRANSMISSAO")
            )

            select
                D1000."DEFIS_DT_TRANSMISSAO" AS "DATA_DECLARACAO",
                D1000."DEFIS_ID_DECLARACAO" AS "DEFIS_ID_DECLARACAO",
                D1000."TDSN_ID_TIPO" AS "TIPO_DECLARACAO",
                D1000."DEFIS_EXERCICIO" AS "DEFIS_EXERCICIO",
                D5300."DESN_CNPJ_ESTABELECIMENTO" AS "CNPJ",
                D5300."DESN_VL_ESTOQUE_INICIAL" AS "ESTOQUE_INICIAL",
                D5300."DESN_VL_ESTOQUE_FINAL" AS "ESTOQUE_FINAL",
                D5300."DESN_VL_SALDO_CX_BANCO_INICIAL" AS "CAIXA_INICIAL",
                D5300."DESN_VL_SALDO_CX_BANCO_FINAL" AS "CAIXA_FINAL",
                D5300."DESN_VL_TOT_ENTRADAS" AS "TOTAL_ENTRADA_DECLARADO",
                D5300."DESN_VL_TOT_AQUIS_MERC_COM_IND" AS "TOTAL_ENTRADA_MERCADORIA_DECLARADO"
            from 
                "defis"."D1000" D1000
                    inner join "defis_valida" on (D1000.id = defis_valida."id")
                    left join "defis"."D5300" D5300 on (D1000.id = D5300."D1000_id")
            WHERE 
                D5300."DESN_CNPJ_ESTABELECIMENTO" = '{cnpj}'
                AND D1000."DEFIS_EXERCICIO" IN {tuple(lista_anos)}
            
        """

        def retrieve(self):
            with self.engine.connect() as connection:
                defis = pd.read_sql_query(self.query, con=connection, dtype={'DESN_CNPJ_ESTABELECIMENTO': str, 'TDSN_ID_TIPO': str, 'DEFIS_ID_DECLARACAO': str})
            print(f'Recuperado {len(defis)} registros!')
            return defis


    class PGDAS(DocumentacaoAuditoria):
        # Parâmetros de conexão com o PostgreSQL
        DATABASE_URL = "postgresql+psycopg2://auditor:1234@localhost:5432/simples_nacional"
        engine = create_engine(DATABASE_URL)

        def __init__(self, cnpj, data_inicio, data_fim):
            
            self.cnpj = cnpj 
            self.data_inicio = data_inicio
            self.data_fim = data_fim
            
            self.query = f"""

            with PGDAS_VALIDOS as 
                (SELECT 
                    NORMAL."Pgdasd_ID_Declaracao" 
                FROM 
                    "pgdas"."00000" NORMAL
                WHERE 
                    NOT EXISTS (
                        SELECT 
                            1
                        FROM 
                            "pgdas"."00000" RETIF
                        WHERE 
                            NORMAL."Cnpjmatriz" = RETIF."Cnpjmatriz" 
                            AND NORMAL."PA" = RETIF."PA"
                            AND RETIF."Operacao" = 'R'  -- Literal de string corrigido
                            AND NORMAL."Pgdasd_Dt_Transmissao" < RETIF."Pgdasd_Dt_Transmissao"
                    )),
            
            faturamento_beneficiados AS (
                SELECT 
                    COALESCE("03111"."03110_id", "03112"."03110_id") AS "03110_id",
                    SUM(COALESCE("03111"."Valor", 0)) AS "Valor de Isenção",
                    SUM(COALESCE("03112"."Valor", 0)) AS "Valor de Redução de BC"
                FROM 
                    "pgdas"."03111"
                        FULL OUTER JOIN "pgdas"."03112" ON ("03111"."03110_id" = "03112"."03110_id")
                GROUP BY
                    COALESCE("03111"."03110_id", "03112"."03110_id")
            )

            SELECT 
               "00000"."Pgdasd_ID_Declaracao",
                "00000"."Pgdasd_Dt_Transmissao",
                "00000"."Operacao",
                "00000"."Regime",
                "AAAAA"."DT_INI",
                "03000"."CNPJ",
                "03000"."ImpedidoIcmsIss",
                "03100"."Tipo",
                'A' AS "Faixa",
                "03100"."Vltotal",
                "03110"."UF",
                "03110"."ICMS",
                "03110"."Aliq_ICMS" as "Alíquota apurada de ICMS",
                "03110"."Valor_ICMS" as "Valor apurado de ICMS",
                faturamento_beneficiados."Valor de Isenção",
                faturamento_beneficiados."Valor de Redução de BC"
                
            FROM 
                "pgdas"."AAAAA" INNER JOIN "pgdas"."00000" ON ("AAAAA".id = "00000"."AAAAA_id")
                    LEFT JOIN "pgdas"."03000" ON ("00000".id = "03000"."00000_id")
                    LEFT JOIN "pgdas"."03100" ON ("03000".id = "03100"."03000_id")
                    LEFT JOIN "pgdas"."03110" ON ("03100".id = "03110"."03100_id")
                    LEFT JOIN faturamento_beneficiados ON (faturamento_beneficiados."03110_id" = "03110"."id")
            WHERE 
                "03000"."CNPJ" = '{cnpj}'
                AND "AAAAA"."DT_INI" BETWEEN '{data_inicio.strftime("%Y-%m-%d")}' AND '{data_fim.strftime("%Y-%m-%d")}'
                AND "00000"."Pgdasd_ID_Declaracao" IN (SELECT PGDAS_VALIDOS."Pgdasd_ID_Declaracao" FROM PGDAS_VALIDOS)
            UNION ALL
            
            SELECT  
                "00000"."Pgdasd_ID_Declaracao",
                "00000"."Pgdasd_Dt_Transmissao",
                "00000"."Operacao",
                "00000"."Regime",
                "AAAAA"."DT_INI",
                "03000"."CNPJ",
                "03000"."ImpedidoIcmsIss",
                "03100"."Tipo",
                'B' AS "Faixa",
                "03100"."Vltotal",
                "03000"."Uf",
                '00' AS "ICMS",
                "03120"."Aliq_ICMS" as "Alíquota apurada de ICMS",
                "03120"."Valor_ICMS" as "Valor apurado de ICMS",
                '0' AS "Valor de Isenção",
                '0' AS "Valor de Redução de BC"
            FROM 
                "pgdas"."AAAAA" INNER JOIN "pgdas"."00000" ON ("AAAAA".id = "00000"."AAAAA_id")
                    LEFT JOIN "pgdas"."03000" ON ("00000".id = "03000"."00000_id")
                    LEFT JOIN "pgdas"."03100" ON ("03000".id = "03100"."03000_id")
                    LEFT JOIN "pgdas"."03120" ON ("03100".id = "03120"."03100_id")

            WHERE 
                "03000"."CNPJ" = '{cnpj}'
                AND "AAAAA"."DT_INI" BETWEEN '{data_inicio.strftime("%Y-%m-%d")}' AND '{data_fim.strftime("%Y-%m-%d")}'
                AND "00000"."Pgdasd_ID_Declaracao" IN (SELECT PGDAS_VALIDOS."Pgdasd_ID_Declaracao" FROM PGDAS_VALIDOS)

            UNION ALL
            
            SELECT 

                 
                "00000"."Pgdasd_ID_Declaracao",
                "00000"."Pgdasd_Dt_Transmissao",
                "00000"."Operacao",
                "00000"."Regime",
                "AAAAA"."DT_INI",
                "03000"."CNPJ",
                "03000"."ImpedidoIcmsIss",
                "03100"."Tipo",
                'B' AS "Faixa",
                "03100"."Vltotal",
                "03000"."Uf",
                '00' AS "ICMS",
                "03130"."Aliq_ICMS" as "Alíquota apurada de ICMS",
                "03130"."Valor_ICMS" as "Valor apurado de ICMS",
                '0' AS "Valor de Isenção",
                '0' AS "Valor de Redução de BC"
            FROM 
                "pgdas"."AAAAA" INNER JOIN "pgdas"."00000" ON ("AAAAA".id = "00000"."AAAAA_id")
                    LEFT JOIN "pgdas"."03000" ON ("00000".id = "03000"."00000_id")
                    LEFT JOIN "pgdas"."03100" ON ("03000".id = "03100"."03000_id")
                    LEFT JOIN "pgdas"."03130" ON ("03100".id = "03130"."03100_id")

            WHERE 
                "03000"."CNPJ" = '{cnpj}'
                AND "AAAAA"."DT_INI" BETWEEN '{data_inicio.strftime("%Y-%m-%d")}' AND '{data_fim.strftime("%Y-%m-%d")}'
                AND "00000"."Pgdasd_ID_Declaracao" IN (SELECT PGDAS_VALIDOS."Pgdasd_ID_Declaracao" FROM PGDAS_VALIDOS)
            
        """

        def retrieve(self):
            with self.engine.connect() as connection:
                pgdas = pd.read_sql_query(self.query, con=connection)
            print(f'Recuperado {len(pgdas)} registros!')
            return pgdas
    

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
            try:    # Verificando se há dado importado na memória secundária
                return pd.read_excel(self.path)
            except:
                doc = extrair_df(self.query)
                print(f'Recuperado {len(doc)} registros!')
                return doc
    
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
            try:    # Verificando se há dado importado na memória secundária
                cadastro_historico = pd.read_excel(self.path)
            except:
                cadastro_historico = extrair_df(self.query)

            cadastro_historico.sort_values(by='SequenciaHistorico', inplace=True)
            cadastro_historico['DataAlteracaoAnterior'] = cadastro_historico['DataAlteracao'].shift(1)
            cadastro_historico['SituacaoCadastralAnterior'] = cadastro_historico['SituacaoCadastral'].shift(1)
            cadastro_historico['OptanteSimplesAnterior'] = cadastro_historico['OptanteSimples'].astype(int).shift(1)

            print(f'Recuperado {len(cadastro_historico)} registros!')
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
            try:    # Verificando se há dado importado na memória secundária
                return pd.read_excel(self.path)
            except:
                doc = extrair_df(self.query)
                print(f'Recuperado {len(doc)} registros!')
                return doc

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
            try:    # Verificando se há dado importado na memória secundária
                return pd.read_excel(self.path)
            except:
                doc = extrair_df(self.query)
                print(f'Recuperado {len(doc)} registros!')
                return doc


class DadosOperacoes:
    """ Classe para armazenar subclasses (documentos fiscais) e eventos, extraídos do banco de dados """
    
    NOMENCLATURA = {
            'CNPJ_REM' : 'CNPJ do remetente',
            'REMETENTE': 'Razão Social do Remetente/Emitente',
            'IE_REM': 'Inscrição Estadual do Remetente',
            'UF_REM': 'UF do Remetente',
            "REGIME": "Regime do Remetente",
            "UF_DEST": "UF do Destinatário",
            "DESTINATARIO": "Razão Social do Destinatário",
            'CNPJ_DEST': 'CNPJ do Destinatário',
            'IE_DEST': 'Inscrição Estadual do Destinatário',
            'SUFRAMA': 'Nº SUFRAMA',
            'NUM_NOTA': 'Número da Nota Fiscal',
            'SERIE_NOTA': 'Série da Nota Fiscal',
            'SITUACAO': 'Situação da Nota Fiscal',
            'CHAVE_ACESSO': 'Chave de Acesso da Nota Fiscal',
            'CFOP': 'CFOP do item da Nota Fiscal',
            'DT_EMISSAO': 'Data de Emissão da Nota Fiscal',
            'NAT_OPER': 'Natureza da Operação',
            'CLASSE_FRONTEIRA': 'Classificação dada por Auditores Fiscais no Posto Fiscal ou Sistema',
            'GTIN': 'Código GTIN do Produto',
            'COD_PROD_EMIT': 'Código do Produto, conforme emitente da Nota Fiscal',
            'NCM': 'NCM do item da nota fiscal',
            'NUM_ITEM': 'Número do item da Nota Fiscal',
            'DESCRICAO_PROD': 'Descrição do Produto',
            'QTDE': 'Quantidade de produto tributável',
            'UND': 'Unidade tributável do produto',
            'VALOR_UNIT': 'Valor unitário do produto',
            'VALOR_BRUTO_ITEM': 'Valor bruto do item',
            'VALOR_DESCONTO_ITEM': 'Valor desconto do item',
            'VALOR_LIQUIDO_ITEM': 'Valor líquido do item, considerando o valor bruto menos o desconto',
            'CST': 'CST do produto, na visão do emissor',
            'ORIGEM': 'Origem do item - Nacional ou Importado',
            'REDUC_BC': 'Percentual da redução da Base de Cálculo do item',
            'BC_ICMS': 'Base de Cálculo do ICMS do item',
            'ALIQ': 'Alíquota do item',
            'ICMS_ITEM': 'ICMS do item da Nota Fiscal',
            'ICMS_DESON_ITEM': 'ICMS Desonerado do item',
            'ICMS_DESON_NOTA': 'ICMS Desonerado da Nota Fiscal',
            'ICMS_NOTA': 'ICMS Nota Fiscal'
        }

    TIPOS_NOTAS = {
            'CNPJ_REM': str, 
            'IE_REM': str, 
            'REGIME_REM': str,
            'CNPJ_DEST': str,
            'IE_DEST': str,
            'SUFRAMA': str,
            'NUM_NOTA': str,
            'CHAVE_ACESSO': str,
            'CFOP_ITEM': str, 
            'DT_EMISSAO': 'datetime64[ns]',
            'CLASSE_FRONTEIRA': str, 
            'GTIN_ITEM': str,
            'COD_PROD_EMIT': str, 
            'NCM_ITEM': str,
            'NUM_ITEM': str, 
            'CST_ITEM': str, 
            'CHAVE_ACESSO_REF': str
        }
    
    TIPOS_PRODUTOS = {
            'CHAVE_ACESSO': str,
            'NUM_ITEM': str,
            'CLASSE_FRONTEIRA': str,
            'GTIN_ITEM': str,
            'NCM_ITEM': str,
            'CFOP_ITEM': str,
            'CST_ITEM': str, 
        }
    
    TIPO_EVENTOS = {
            'CHAVE_ACESSO': str, 
            'DATA_EVENTO': 'datetime64[ns]',
            'CODIGO_EVENTO': str
            
        }

    class NotasFiscaisEntrada(DocumentacaoAuditoria):
        """ Classe para importar apenas notas fiscais de entrada """
        def __init__(self, cnpj, data_inicio, data_fim):
            self.cnpj = cnpj 
            self.data_inicio = data_inicio 
            self.data_fim = data_fim
            self.queries = []
            self.queries.append(f""" 
                    SELECT 
                        PASM.NFERDOCN AS "CNPJ_REM",
                        PASM.NFERRAZ AS "REMETENTE",
                        PASM.NFERIEST AS "IE_REM",
                        PASM.NFERUF AS "UF_REM",
                        CASE
                            WHEN PASM.NFERCRT = '1'
                            THEN 'SIMPLES'
                            WHEN PASM.NFERCRT = '2'
                            THEN 'SN.SUBLI.EXC'
                            WHEN PASM.NFERCRT = '3'
                            THEN 'NORMAL'
                            ELSE 'N/A'
                            END AS "REGIME_REM",
                        PASM.NFEDUF     AS "UF_DEST",
                        PASM.NFEDRAZ    AS "DESTINATARIO",
                        PASM.NFEDDOCN   AS "CNPJ_DEST",
                        PASM.NFEDINSC   AS "IE_DEST",
                        PASM.NFESUFRAMA AS "SUFRAMA",
                        PASM.NFENNOT    AS "NUM_NOTA",
                        PASM.NFENSER AS "SERIE_NOTA",
                        CASE
                            WHEN PASM.NFECANC = 'S'
                            THEN 'CANCELADA'
                            WHEN PASM.NFECANC = 'D'
                            THEN 'DENEGADA'
                            WHEN PASM.NFECANC = 'N'
                            THEN 'ATIVA'
                        END           AS "SITUACAO",
                        PASM.DANF     AS "CHAVE_ACESSO",
                        PRD.NFEPCFOP  AS "CFOP_ITEM",
                        PASM.NFEDTEMI AS "DT_EMISSAO",
                        
                        RPAD(PASM.NFENATOP, 30)       AS "NAT_OPER",
                        PASP.TMPCLASSE                AS "CLASSE_FRONTEIRA",
                        PRD.NFEPEAN                   AS "GTIN_ITEM",
                        PRD.NFEPCODITEM               AS "COD_PROD_EMIT",
                        PRD.NFEPNCM                   AS "NCM_ITEM",
                        PRD.NFEPNITEM                 AS "NUM_ITEM",
                        PRD.NFEPNDESC                 AS "DESCRICAO_PROD",
                        PRD.NFEPQTRIB                 AS "QTDE_ITEM",
                        PRD.NFEPUTRIB                 AS "UND_ITEM",
                        PRD.NFEPVUNTRIB               AS "VALOR_UNIT_ITEM",
                        PRD.NFEPVPROD                 AS "VALOR_BRUTO_ITEM",
                        PRD.NFEPVDESC                 AS "VALOR_DESCONTO_ITEM",
                        PRD.NFEPVPROD - COALESCE(PRD.NFEPVDESC, 0) AS "VALOR_LIQUIDO_ITEM",
                        PRD.NFEPCST                   AS "CST_ITEM",
                        CASE
                            WHEN PRD.NFEPORIG IN ('1', '2', '3', '8')
                            THEN 'IMPORT'
                            ELSE 'NACIONAL'
                        END                AS "ORIGEM",
                        PRD.NFEPPREDBC     AS "REDUC_BC",
                        PRD.NFEPVBC        AS "BC_ICMS_ITEM",
                        PRD.NFEPPICMS      AS "ALIQ_ITEM",
                        PRD.NFEPVICMS      AS "ICMS_ITEM",
                        PRD.NFEPVICMSDESON AS "ICMS_DESON_ITEM",
                        PASM.NFEVICMSD     AS "ICMS_DESON_NOTA",
                        PASM.NFEVNICMS     AS "ICMS_NOTA",
                        --PRD.NFEPMDBCST     AS "MOD.BC.ST",
                        --PRD.NFEPVBCST      AS "BC.ST.ITEM",
                        --PRD.NFEPPMVAST     AS "MVA.ST",
                        --PRD.NFEPPST        AS "ALIQ.ST.ITEM",
                        --PRD.NFEPVST        AS "ST.RET.ITEM",
                        --PRD.NFEPVSTR       AS "ST.RET.ANT.ITEM",
                        --PRD.VICMSUFDE      AS "DIFAL.ITEM",
                        --PASM.VICMSUFDE     AS "DIFAL.NF",
                        --PRD.PICMSUFDE      AS "ALIQ.DEST",
                        --PASM.NFEBNICMS     AS "BC.ICMS.NF",
                        --PASM.NFEBSICMS     AS "BC.ST.NF",
                        --PASM.NFEVSICMS     AS "ICMS.ST.NF",
                        --PASM.NFETVPROD     AS "VLR.BR.ITENS.NF",
                        PASM.NFETVFRET     AS "FRETE_NOTA",
                        PASM.NFEVSEGU      AS "SEGURO_NOTA",
                        PASM.NFENVDESC     AS "DESCONTO_NOTA",
                        PASM.NFEVNOTA      AS "VALOR_FINAL_NOTA",
                        CASE
                            WHEN PASM.NFETPNF = '0'
                            THEN 'ENTRADA'
                            WHEN PASM.NFETPNF = '1'
                            THEN 'SAÍDA'
                            ELSE 'N/A'
                        END AS "TIPO_OPER",
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
                        END AS "FINALIDADE",
                        CASE
                            WHEN PASM.NFEIDDEST = '1'
                            THEN 'INTERNA'
                            WHEN PASM.NFEIDDEST = '2'
                            THEN 'INTERESTADUAL'
                            WHEN PASM.NFEIDDEST = '3'
                            THEN 'EXTERIOR'
                            ELSE 'N/A'
                        END               AS "DESTINO",
                        PASM.NFEREFNFE    AS "CHAVE_ACESSO_REF",
                        PASM.NFEINFADC    AS "INFORMACOES_ADC",
                        PASM.NFEINFADCLOB AS "INFORMACOES_ADC_CLOB"
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
                        PASM.NFERDOCN AS "CNPJ_REM",
                        PASM.NFERRAZ AS "REMETENTE",
                        PASM.NFERIEST AS "IE_REM",
                        PASM.NFERUF AS "UF_REM",
                        CASE
                            WHEN PASM.NFERCRT = '1'
                            THEN 'SIMPLES'
                            WHEN PASM.NFERCRT = '2'
                            THEN 'SN.SUBLI.EXC'
                            WHEN PASM.NFERCRT = '3'
                            THEN 'NORMAL'
                            ELSE 'N/A'
                            END AS "REGIME_REM",
                        PASM.NFEDUF     AS "UF_DEST",
                        PASM.NFEDRAZ    AS "DESTINATARIO",
                        PASM.NFEDDOCN   AS "CNPJ_DEST",
                        PASM.NFEDINSC   AS "IE_DEST",
                        PASM.NFESUFRAMA AS "SUFRAMA",
                        PASM.NFENNOT    AS "NUM_NOTA",
                        PASM.NFENSER AS "SERIE_NOTA",
                        CASE
                            WHEN PASM.NFECANC = 'S'
                            THEN 'CANCELADA'
                            WHEN PASM.NFECANC = 'D'
                            THEN 'DENEGADA'
                            WHEN PASM.NFECANC = 'N'
                            THEN 'ATIVA'
                        END           AS "SITUACAO",
                        PASM.DANF     AS "CHAVE_ACESSO",
                        PRD.NFEPCFOP  AS "CFOP_ITEM",
                        PASM.NFEDTEMI AS "DT_EMISSAO",
                        
                        RPAD(PASM.NFENATOP, 30)       AS "NAT_OPER",
                        PASP.TMPCLASSE                AS "CLASSE_FRONTEIRA",
                        PRD.NFEPEAN                   AS "GTIN_ITEM",
                        PRD.NFEPCODITEM               AS "COD_PROD_EMIT",
                        PRD.NFEPNCM                   AS "NCM_ITEM",
                        PRD.NFEPNITEM                 AS "NUM_ITEM",
                        PRD.NFEPNDESC                 AS "DESCRICAO_PROD",
                        PRD.NFEPQTRIB                 AS "QTDE_ITEM",
                        PRD.NFEPUTRIB                 AS "UND_ITEM",
                        PRD.NFEPVUNTRIB               AS "VALOR_UNIT_ITEM",
                        PRD.NFEPVPROD                 AS "VALOR_BRUTO_ITEM",
                        PRD.NFEPVDESC                 AS "VALOR_DESCONTO_ITEM",
                        PRD.NFEPVPROD - COALESCE(PRD.NFEPVDESC, 0) AS "VALOR_LIQUIDO_ITEM",
                        PRD.NFEPCST                   AS CST_ITEM,
                        CASE
                            WHEN PRD.NFEPORIG IN ('1', '2', '3', '8')
                            THEN 'IMPORT'
                            ELSE 'NACIONAL'
                        END                AS "ORIGEM",
                        PRD.NFEPPREDBC     AS "REDUC_BC",
                        PRD.NFEPVBC        AS "BC_ICMS_ITEM",
                        PRD.NFEPPICMS      AS "ALIQ_ITEM",
                        PRD.NFEPVICMS      AS "ICMS_ITEM",
                        PRD.NFEPVICMSDESON AS "ICMS_DESON_ITEM",
                        PASM.NFEVICMSD     AS "ICMS_DESON_NOTA",
                        PASM.NFEVNICMS     AS "ICMS_NOTA",
                        --PRD.NFEPMDBCST     AS "MOD.BC.ST",
                        --PRD.NFEPVBCST      AS "BC.ST.ITEM",
                        --PRD.NFEPPMVAST     AS "MVA.ST",
                        --PRD.NFEPPST        AS "ALIQ.ST.ITEM",
                        --PRD.NFEPVST        AS "ST.RET.ITEM",
                        --PRD.NFEPVSTR       AS "ST.RET.ANT.ITEM",
                        --PRD.VICMSUFDE      AS "DIFAL.ITEM",
                        --PASM.VICMSUFDE     AS "DIFAL.NF",
                        --PRD.PICMSUFDE      AS "ALIQ.DEST",
                        --PASM.NFEBNICMS     AS "BC.ICMS.NF",
                        --PASM.NFEBSICMS     AS "BC.ST.NF",
                        --PASM.NFEVSICMS     AS "ICMS.ST.NF",
                        --PASM.NFETVPROD     AS "VLR.BR.ITENS.NF",
                        PASM.NFETVFRET     AS "FRETE_NOTA",
                        PASM.NFEVSEGU      AS "SEGURO_NOTA",
                        PASM.NFENVDESC     AS "DESCONTO_NOTA",
                        PASM.NFEVNOTA      AS "VALOR_FINAL_NOTA",
                        CASE
                            WHEN PASM.NFETPNF = '0'
                            THEN 'ENTRADA'
                            WHEN PASM.NFETPNF = '1'
                            THEN 'SAÍDA'
                            ELSE 'N/A'
                        END AS "TIPO_OPER",
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
                        PASM.NFEREFNFE    AS "CHAVE_ACESSO_REF",
                        PASM.NFEINFADC    AS "INFORMACOES_ADC",
                        PASM.NFEINFADCLOB AS "INFORMACOES_ADC_CLOB"
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
            try:    # Verificando se há dado importado na memória secundária
                return pd.read_excel(self.path, dtype=DadosOperacoes.TIPOS_NOTAS)
            except:
                df = pd.DataFrame()
                for query in self.queries:
                    temp = extrair_df_de_lobs(query)
                    df = pd.concat([df, temp])

                for col, tipo in DadosOperacoes.TIPOS_NOTAS.items():
                    df[col] = df[col].astype(tipo)

                print(f'Recuperado {len(df)} registros!')
                return df
    
        def quantidade(self):
            query = f""" SELECT 
                            COUNT(*) AS quantidade
                        FROM 
                            SIATE.NFEPASM PASM
                        WHERE 1=1
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

        def BuscarProdutosPorNotas(self, chv_acc):
            
            self.queries_fronteira = []
            inicial = 0

            while inicial < len(chv_acc):
                final = min(inicial+1000, len(chv_acc))
                query = f"""        
                            SELECT
                                PRD.DANF          "CHAVE_ACESSO",
                                PRD.NFEPNITEM     "NUM_ITEM",
                                PRD.NFEPORIG      "ORIGEM",
                                PASP.TMPCLASSE    "CLASSE_FRONTEIRA",
                                PRD.NFEPEAN       "GTIN_ITEM",
                                PRD.NFEPNCM       "NCM_ITEM",  
                                PRD.NFEPCFOP      "CFOP_ITEM",
                                PRD.NFEPCST       "CST_ITEM",
                                PRD.NFEPVICMS     "ICMS_NOTA",
                                PASP.TMPVICMSD    "ICMS_DESON_NOTA",
                                PASP.TMPVST       "VALOR_ICMS_ST_NOTA" 
                                 

                            FROM 
                                SIATE.NFEPRD PRD 
                                    LEFT JOIN SIATE.FTMPASN PASN ON (PRD.DANF = PASN.TMDANF),
                                    LEFT JOIN SIATE.FTMPASP PASP ON (PASN.TMPASSE = PASN.TMPASSE AND PASN.TMNSEQ  = PASP.TMNSEQ AND PRD.NFEPNITEM = PASP.TMPNITEM)

                            WHERE 
                                1=1

                                AND PRD.DANF IN ({tuple(chv_acc[inicial:final])})
                        """
                inicial += 1000
                self.queries_fronteira.append(query)  
            
            for query in self.queries_fronteira:
                
                df = pd.DataFrame()
                for query in self.queries:
                    temp = extrair_df_de_lobs(query)
                    df = pd.concat([df, temp])
                for col, tipo in DadosOperacoes.TIPOS_PRODUTOS.items():
                    df[col] = df[col].astype(tipo)

                return df
            
    class NotasFiscaisSaida(DocumentacaoAuditoria):
        
        """ Classe para importar Notas Fiscais de Saída. Não inclui notas fiscais ao consumidor (NFC-e)"""
        
        def __init__(self, cnpj, data_inicio, data_fim):
            self.cnpj = cnpj
            self.data_inicio = data_inicio 
            self.data_fim = data_fim 
            self.queries = []
            self.queries.append(f"""
                    SELECT 
                        PASM.NFERDOCN AS "CNPJ_REM",
                        PASM.NFERRAZ AS "REMETENTE",
                        PASM.NFERIEST AS "IE_REM",
                        PASM.NFERUF AS "UF_REM",
                        CASE
                            WHEN PASM.NFERCRT = '1'
                            THEN 'SIMPLES'
                            WHEN PASM.NFERCRT = '2'
                            THEN 'SN.SUBLI.EXC'
                            WHEN PASM.NFERCRT = '3'
                            THEN 'NORMAL'
                            ELSE 'N/A'
                            END AS "REGIME_REM",
                        PASM.NFEDUF     AS "UF_DEST",
                        PASM.NFEDRAZ    AS "DESTINATARIO",
                        PASM.NFEDDOCN   AS "CNPJ_DEST",
                        PASM.NFEDINSC   AS "IE_DEST",
                        PASM.NFESUFRAMA AS "SUFRAMA",
                        PASM.NFENNOT    AS "NUM_NOTA",
                        PASM.NFENSER AS "SERIE_NOTA",
                        CASE
                            WHEN PASM.NFECANC = 'S'
                            THEN 'CANCELADA'
                            WHEN PASM.NFECANC = 'D'
                            THEN 'DENEGADA'
                            WHEN PASM.NFECANC = 'N'
                            THEN 'ATIVA'
                        END           AS "SITUACAO",
                        PASM.DANF     AS "CHAVE_ACESSO",
                        
                        PRD.NFEPCFOP  AS "CFOP_ITEM",
                        PASM.NFEDTEMI AS "DT_EMISSAO",
                        
                        RPAD(PASM.NFENATOP, 30)       AS "NAT_OPER",
                        PASP.TMPCLASSE                AS "CLASSE_FRONTEIRA",
                        PRD.NFEPEAN                   AS "GTIN_ITEM",
                        PRD.NFEPCODITEM               AS "COD_PROD_EMIT",
                        PRD.NFEPNCM                   AS "NCM_ITEM",
                        PRD.NFEPNITEM                 AS "NUM_ITEM",
                        PRD.NFEPNDESC                 AS "DESCRICAO_PROD",
                        PRD.NFEPQTRIB                 AS "QTDE_ITEM",
                        PRD.NFEPUTRIB                 AS "UND_ITEM",
                        PRD.NFEPVUNTRIB               AS "VALOR_UNIT_ITEM",
                        PRD.NFEPVPROD                 AS "VALOR_BRUTO_ITEM",
                        PRD.NFEPVDESC                 AS "VALOR_DESCONTO_ITEM",
                        PRD.NFEPVPROD - COALESCE(PRD.NFEPVDESC, 0) AS "VALOR_LIQUIDO_ITEM",
                        PRD.NFEPCST                   AS CST_ITEM,
                        CASE
                            WHEN PRD.NFEPORIG IN ('1', '2', '3', '8')
                            THEN 'IMPORT'
                            ELSE 'NACIONAL'
                        END                AS "ORIGEM",
                        PRD.NFEPPREDBC     AS "REDUC_BC",
                        PRD.NFEPVBC        AS "BC_ICMS_ITEM",
                        PRD.NFEPPICMS      AS "ALIQ_ITEM",
                        PRD.NFEPVICMS      AS "ICMS_ITEM",
                        PRD.NFEPVICMSDESON AS "ICMS_DESON_ITEM",
                        PASM.NFEVICMSD     AS "ICMS_DESON_NOTA",
                        PASM.NFEVNICMS     AS "ICMS_NOTA",
                        --PRD.NFEPMDBCST     AS "MOD.BC.ST",
                        --PRD.NFEPVBCST      AS "BC.ST.ITEM",
                        --PRD.NFEPPMVAST     AS "MVA.ST",
                        --PRD.NFEPPST        AS "ALIQ.ST.ITEM",
                        --PRD.NFEPVST        AS "ST.RET.ITEM",
                        --PRD.NFEPVSTR       AS "ST.RET.ANT.ITEM",
                        --PRD.VICMSUFDE      AS "DIFAL.ITEM",
                        --PASM.VICMSUFDE     AS "DIFAL.NF",
                        --PRD.PICMSUFDE      AS "ALIQ.DEST",
                        --PASM.NFEBNICMS     AS "BC.ICMS.NF",
                        --PASM.NFEBSICMS     AS "BC.ST.NF",
                        --PASM.NFEVSICMS     AS "ICMS.ST.NF",
                        --PASM.NFETVPROD     AS "VLR.BR.ITENS.NF",
                        PASM.NFETVFRET     AS "FRETE_NOTA",
                        PASM.NFEVSEGU      AS "SEGURO_NOTA",
                        PASM.NFENVDESC     AS "DESCONTO_NOTA",
                        PASM.NFEVNOTA      AS "VALOR_FINAL_NOTA",
                        CASE
                            WHEN PASM.NFETPNF = '0'
                            THEN 'ENTRADA'
                            WHEN PASM.NFETPNF = '1'
                            THEN 'SAÍDA'
                            ELSE 'N/A'
                        END AS "TIPO_OPER",
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
                        PASM.NFEREFNFE    AS "CHAVE_ACESSO_REF",
                        PASM.NFEINFADC    AS "INFORMACOES_ADC",
                        PASM.NFEINFADCLOB AS "INFORMACOES_ADC_CLOB"

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
                """)
        
        def AdicionarNotasFiscaisExtemporaneas(self, chaves_acesso):
            inicial = 0

            while inicial < len(chaves_acesso):
                final = min(inicial+1000, len(chaves_acesso))
                query = f""" 
                    SELECT 
                        PASM.NFERDOCN AS "CNPJ_REM",
                        PASM.NFERRAZ AS "REMETENTE",
                        PASM.NFERIEST AS "IE_REM",
                        PASM.NFERUF AS "UF_REM",
                        CASE
                            WHEN PASM.NFERCRT = '1'
                            THEN 'SIMPLES'
                            WHEN PASM.NFERCRT = '2'
                            THEN 'SN.SUBLI.EXC'
                            WHEN PASM.NFERCRT = '3'
                            THEN 'NORMAL'
                            ELSE 'N/A'
                            END AS "REGIME_REM",
                        PASM.NFEDUF     AS "UF_DEST",
                        PASM.NFEDRAZ    AS "DESTINATARIO",
                        PASM.NFEDDOCN   AS "CNPJ_DEST",
                        PASM.NFEDINSC   AS "IE_DEST",
                        PASM.NFESUFRAMA AS "SUFRAMA",
                        PASM.NFENNOT    AS "NUM_NOTA",
                        PASM.NFENSER AS "SERIE_NOTA",
                        CASE
                            WHEN PASM.NFECANC = 'S'
                            THEN 'CANCELADA'
                            WHEN PASM.NFECANC = 'D'
                            THEN 'DENEGADA'
                            WHEN PASM.NFECANC = 'N'
                            THEN 'ATIVA'
                        END           AS "SITUACAO",
                        PASM.DANF     AS "CHAVE_ACESSO",
                        PRD.NFEPCFOP  AS "CFOP_ITEM",
                        PASM.NFEDTEMI AS "DT_EMISSAO",
                        
                        RPAD(PASM.NFENATOP, 30)       AS "NAT_OPER",
                        PASP.TMPCLASSE                AS "CLASSE_FRONTEIRA",
                        PRD.NFEPEAN                   AS "GTIN_ITEM",
                        PRD.NFEPCODITEM               AS "COD_PROD_EMIT",
                        PRD.NFEPNCM                   AS "NCM_ITEM",
                        PRD.NFEPNITEM                 AS "NUM_ITEM",
                        PRD.NFEPNDESC                 AS "DESCRICAO_PROD",
                        PRD.NFEPQTRIB                 AS "QTDE_ITEM",
                        PRD.NFEPUTRIB                 AS "UND_ITEM",
                        PRD.NFEPVUNTRIB               AS "VALOR_UNIT_ITEM",
                        PRD.NFEPVPROD                 AS "VALOR_BRUTO_ITEM",
                        PRD.NFEPVDESC                 AS "VALOR_DESCONTO_ITEM",
                        PRD.NFEPVPROD - COALESCE(PRD.NFEPVDESC, 0) AS "VALOR_LIQUIDO_ITEM",
                        PRD.NFEPCST                   AS CST_ITEM,
                        CASE
                            WHEN PRD.NFEPORIG IN ('1', '2', '3', '8')
                            THEN 'IMPORT'
                            ELSE 'NACIONAL'
                        END                AS "ORIGEM",
                        PRD.NFEPPREDBC     AS "REDUC_BC",
                        PRD.NFEPVBC        AS "BC_ICMS_ITEM",
                        PRD.NFEPPICMS      AS "ALIQ_ITEM",
                        PRD.NFEPVICMS      AS "ICMS_ITEM",
                        PRD.NFEPVICMSDESON AS "ICMS_DESON_ITEM",
                        PASM.NFEVICMSD     AS "ICMS_DESON_NOTA",
                        PASM.NFEVNICMS     AS "ICMS_NOTA",
                        --PRD.NFEPMDBCST     AS "MOD.BC.ST",
                        --PRD.NFEPVBCST      AS "BC.ST.ITEM",
                        --PRD.NFEPPMVAST     AS "MVA.ST",
                        --PRD.NFEPPST        AS "ALIQ.ST.ITEM",
                        --PRD.NFEPVST        AS "ST.RET.ITEM",
                        --PRD.NFEPVSTR       AS "ST.RET.ANT.ITEM",
                        --PRD.VICMSUFDE      AS "DIFAL.ITEM",
                        --PASM.VICMSUFDE     AS "DIFAL.NF",
                        --PRD.PICMSUFDE      AS "ALIQ.DEST",
                        --PASM.NFEBNICMS     AS "BC.ICMS.NF",
                        --PASM.NFEBSICMS     AS "BC.ST.NF",
                        --PASM.NFEVSICMS     AS "ICMS.ST.NF",
                        --PASM.NFETVPROD     AS "VLR.BR.ITENS.NF",
                        PASM.NFETVFRET     AS "FRETE_NOTA",
                        PASM.NFEVSEGU      AS "SEGURO_NOTA",
                        PASM.NFENVDESC     AS "DESCONTO_NOTA",
                        PASM.NFEVNOTA      AS "VALOR_FINAL_NOTA",
                        CASE
                            WHEN PASM.NFETPNF = '0'
                            THEN 'ENTRADA'
                            WHEN PASM.NFETPNF = '1'
                            THEN 'SAÍDA'
                            ELSE 'N/A'
                        END AS "TIPO_OPER",
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
                        PASM.NFEREFNFE    AS "CHAVE_ACESSO_REF",
                        PASM.NFEINFADC    AS "INFORMACOES_ADC",
                        PASM.NFEINFADCLOB AS "INFORMACOES_ADC_CLOB"
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
            try:    # Verificando se há dado importado na memória secundária
                return pd.read_excel(self.path, dtype=DadosOperacoes.TIPOS_PRODUTOS)
            except:
                df = pd.DataFrame()
                for query in self.queries:
                    temp = extrair_df_de_lobs(query)
                    df = pd.concat([df, temp])

                for col, tipo in DadosOperacoes.TIPOS_PRODUTOS.items():
                    df[col] = df[col].astype(tipo)
                
                print(f'Recuperado {len(df)} registros!')
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
        
        TIPOS_NOTAS_CONSUMIDOR = {
            'CNPJ_REM': str, 
            'CNPJ_DEST': str,
            'NUM_NOTA': str,
            'CHAVE_ACESSO': str,
            'CFOP_ITEM': str, 
            'DT_EMISSAO': 'datetime64[ns]',
            'GTIN_ITEM': str,
            'COD_PROD_EMIT': str, 
            'NCM_ITEM': str,
            'NUM_ITEM': str, 
            'CST_ITEM': str, 
        }

        def __init__(self, cnpj, data_inicio, data_fim):
            self.cnpj = cnpj
            self.data_inicio = data_inicio
            self.data_fim = data_fim
            self.queries = []
            self.queries.append(f""" 
                    SELECT 
                        
                        X.NFCE_NR_DOC_REM       AS "CNPJ_REM",
                        x.NFCE_NM_RAZ_REM       as "REMETENTE",
                        x.NFCE_NM_RAZ_DES       AS "DESTINATARIO",
                        X.NFCE_NR_DOC_DES       AS "CNPJ_DEST",
                        X.NFCE_DT_EMISSAO       AS "DT_EMISSAO",
                        X.NFCE_NR_DANF          AS "CHAVE_ACESSO",
                        X.NFCE_NR_NOTA          AS "NUM_NOTA",
                        X.NFCE_NR_SERIE         AS "SERIE_NOTA",
                        x.NFCE_NAT_OPER         as "NAT_OPER",
                        Y.NFCP_CD_EAN           AS "GTIN_ITEM",
                        Y.NFCP_CODIGO_ITEM      AS "COD_PROD_EMIT",
                        Y.NFCP_CD_NCM           AS "NCM_ITEM",
                        Y.NFCP_CD_CFOP          AS "CFOP_ITEM",
                        Y.NFCP_CD_OP_SN         AS "SITUACAO",
                        Y.NFCP_NR_ITEM          AS "NUM_ITEM",
                        Y.NFCP_DESC             AS "DESCRICAO_PROD",
                        Y.NFCP_UN_TRIB          AS "UND_ITEM",
                        Y.NFCP_QT_TRIB          AS "QTDE_ITEM",
                        y.NFCP_VL_UNIT_TRIB     AS "VALOR_UNIT_ITEM",
                        Y.NFCP_TT_PROD          AS "VALOR_BRUTO_ITEM",
                        Y.NFCP_VL_DESC          AS "VALOR_DESCONTO_ITEM",
                        Y.NFCP_TT_PROD - COALESCE(Y.NFCP_VL_DESC, 0) as "VALOR_LIQUIDO_ITEM",
                        Y.NFCP_VL_BC            AS "BC_ICMS_ITEM",
                        Y.NFCP_ALIQ_ICMS        AS "ALIQ_ITEM",
                        Y.NFCP_VL_ICMS          AS "ICMS_ITEM",
                        Y.NFCP_VL_FRETE         AS "FRETE_ITEM",
                        Y.NFCP_VL_SEGURO        AS "SEGURO_ITEM",
                        Y.NFCP_VL_OUTROS        AS "OUTROS_ITEM",
                        Y.NFCP_CLAS_TRIB        AS "CST_ITEM",
                        CASE 
                            WHEN  Y.NFCP_ORIGEM IN ('1','2','3','8') THEN 'IMPORT'
                            ELSE 'NACIONAL' 
                        END                     AS "ORIGEM", 
                        --X.NFCE_BS_ICMS          AS "BC.NFC-e",
                        --x.NFCE_VL_ICMS          as "ICMS.NFC-e",
                       -- Y.NFCP_PC_MVAST         AS "MVA",
                       -- Y.NFCP_VL_BCST          AS "BC.ST",
                        --Y.NFCP_ALIQ_ICMS_ST     AS "ALIQ.ST",
                        --Y.NFCP_VL_ICMS_ST       AS "ICMS.ST",
                        --Y.NFCP_VL_BCSTR         AS "BC.ICMS.Ret",
                        --Y.NFCP_VL_ICMS_STR      AS "VLR.ICMS.Ret",
                        X.NFCE_TT_NOTA          AS "VALOR_FINAL_NOTA"
                        

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
                        
                        X.NFCE_NR_DOC_REM       AS "CNPJ_REM",
                        x.NFCE_NM_RAZ_REM       as "REMETENTE",
                        x.NFCE_NM_RAZ_DES       AS "DESTINATARIO",
                        X.NFCE_NR_DOC_DES       AS "CNPJ_DEST",
                        X.NFCE_DT_EMISSAO       AS "DT_EMISSAO",
                        X.NFCE_NR_DANF          AS "CHAVE_ACESSO",
                        X.NFCE_NR_NOTA          AS "NUM_NOTA",
                        x.NFCE_NAT_OPER         as "NAT_OPER",
                        Y.NFCP_CD_EAN           AS "GTIN_ITEM",
                        Y.NFCP_CODIGO_ITEM      AS "COD_PROD_EMIT",
                        Y.NFCP_CD_NCM           AS "NCM_ITEM",
                        Y.NFCP_CD_CFOP          AS "CFOP_ITEM",
                        Y.NFCP_CD_OP_SN         AS "SITUACAO",
                        Y.NFCP_NR_ITEM          AS "NUM_ITEM",
                        Y.NFCP_DESC             AS "DESCRICAO_PROD",
                        Y.NFCP_UN_TRIB          AS "UND_ITEM",
                        Y.NFCP_QT_TRIB          AS "QTDE_ITEM",
                        y.NFCP_VL_UNIT_TRIB     AS "VALOR_UNIT_ITEM",
                        Y.NFCP_TT_PROD          AS "VALOR_BRUTO_ITEM",
                        Y.NFCP_VL_DESC          AS "VALOR_DESCONTO_ITEM",
                        Y.NFCP_TT_PROD - COALESCE(Y.NFCP_VL_DESC, 0) as "VALOR_LIQUIDO_ITEM",
                        Y.NFCP_VL_BC            AS "BC_ICMS_ITEM",
                        Y.NFCP_ALIQ_ICMS        AS "ALIQ_ITEM",
                        Y.NFCP_VL_ICMS          AS "ICMS_ITEM",
                        Y.NFCP_VL_FRETE         AS "FRETE_ITEM",
                        Y.NFCP_VL_SEGURO        AS "SEGURO_ITEM",
                        Y.NFCP_VL_OUTROS        AS "OUTROS_ITEM",
                        Y.NFCP_CLAS_TRIB        AS "CST_ITEM",
                        CASE 
                            WHEN  Y.NFCP_ORIGEM IN ('1','2','3','8') THEN 'IMPORT'
                            ELSE 'NACIONAL' 
                        END                     AS "ORIGEM", 
                        --X.NFCE_BS_ICMS          AS "BC.NFC-e",
                        --x.NFCE_VL_ICMS          as "ICMS.NFC-e",
                       -- Y.NFCP_PC_MVAST         AS "MVA",
                       -- Y.NFCP_VL_BCST          AS "BC.ST",
                        --Y.NFCP_ALIQ_ICMS_ST     AS "ALIQ.ST",
                        --Y.NFCP_VL_ICMS_ST       AS "ICMS.ST",
                        --Y.NFCP_VL_BCSTR         AS "BC.ICMS.Ret",
                        --Y.NFCP_VL_ICMS_STR      AS "VLR.ICMS.Ret",
                        X.NFCE_TT_NOTA          AS "VALOR_FINAL_NOTA"
                        
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
            try:    # Verificando se há dado importado na memória secundária
                return pd.read_excel(self.path, dtype=self.TIPOS_NOTAS_CONSUMIDOR)
            except:
                df = pd.DataFrame()
                for query in self.queries:
                    temp = extrair_df_de_lobs(query)
                    df = pd.concat([df, temp])

                for col, dtype in self.TIPOS_NOTAS_CONSUMIDOR.items():
                    df[col] = df[col].astype(dtype)

                print(f'Recuperado {len(df)} registros!')
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
                                DANF AS "CHAVE_ACESSO", 
                                NFEEVSEQ AS "SEQUENCIA_EVENTO",
                                NFEEVDATA AS "DATA_EVENTO",
                                NFEEVHORA AS "HORA_EVENTO",
                                NFEEVCOD AS "CODIGO_EVENTO",
                                NFEEVDESC AS "DESCRICAO_EVENTO",
                                NFEEVJUST AS "JUSTIFICATIVA"
                            FROM
                                SIATE.NFEEVEN
                            WHERE
                                DANF IN {chaves_acesso[inicial:final]}
                        """
                
                self.queries.append(query)
                inicial += 1000
        
        def retrieve(self):
            try:    # Verificando se há dado importado na memória secundária
                return pd.read_excel(self.path)
            except:
                df = pd.DataFrame()
                for query in self.queries:
                    temp = extrair_df_de_lobs(query)
                    df = pd.concat([df, temp])

                for col, dtype in DadosOperacoes.TIPO_EVENTOS.items():
                    df[col] = df[col].astype(dtype)

                print(f'Recuperado {len(df)} registros!')
                return df 
    
    class EventosNotasConsumidor(DocumentacaoAuditoria):
        
        def __init__(self, chaves_acesso):
        
            inicial = 0
            self.queries = []
            
            while inicial < len(chaves_acesso):
                final = min(inicial+1000, len(chaves_acesso))
                query = f"""
                            SELECT
                                NFCE_NR_DANF AS "CHAVE_ACESSO", 
                                NFCE_EVSEQ AS "SEQUENCIA_EVENTO",
                                NFCE_EVDATA AS "DATA_EVENTO",
                                NFCE_EVHORA AS "HORA_EVENTO",
                                NFCE_EVCOD AS "CODIGO_EVENTO",
                                NFCE_EVDESC AS "DESCRICAO_EVENTO",
                                NFCE_EVJUST AS "JUSTIFICATIVA"
                            FROM
                                NFCE_OWNER.TB_NFCEEVEN
                            WHERE
                                NFCE_NR_DANF IN {chaves_acesso[inicial:final]}
                        """
                
                self.queries.append(query)
                inicial += 1000
        
        def retrieve(self):
            try:    # Verificando se há dado importado na memória secundária
                return pd.read_excel(self.path)
            except:
                df = pd.DataFrame()
                for query in self.queries:
                    temp = extrair_df_de_lobs(query)
                    df = pd.concat([df, temp])
                
                for col, dtype in DadosOperacoes.TIPO_EVENTOS.items():
                    df[col] = df[col].astype(dtype)

                print(f'Recuperado {len(df)} registros!')
                return df 
             
    class EventosDesembaraco(DocumentacaoAuditoria):
        
        TIPO_DESEMBARACO = {
            'CHAVE_ACESSO': str, 
            'DATA_PASSAGEM': 'datetime64[ns]'

        }

        def __init__(self, chaves_acesso):
            inicial = 0
            self.queries = []
            while inicial < len(chaves_acesso):
                final = min(inicial+1000, len(chaves_acesso))
                query = f""" 
                            SELECT
                                TMDANF AS "CHAVE_ACESSO",
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
            try:    # Verificando se há dado importado na memória secundária
                return pd.read_excel(self.path)
            except:
                df = pd.DataFrame()
                for query in self.queries:
                    temp = extrair_df_de_lobs(query)
                    df = pd.concat([df, temp])
                
                for col, dtype in self.TIPO_DESEMBARACO.items():
                    df[col] = df[col].astype(dtype)

                print(f'Recuperado {len(df)} registros!')
                return df 


class DadosParametrizados:
    
    def BuscarCFOPTributados():
        endereco = r'.\parametrizacoes\CFOPs.xlsx'
        CFOP = pd.read_excel(endereco, dtype={'CFOP': str, 'GERA_CREDITO': bool})
        CFOP['TRIBUTADO'] = [True if linha['TMCLOE']=='TRIB' else False for idx, linha in CFOP.iterrows()]
        return CFOP[['CFOP', 'GERA_CREDITO', 'TRIBUTADO']]
    
    def BuscarGtin():
        endereco = r'.\parametrizacoes\GTIN.csv'
        temp1 = pd.read_csv(endereco, dtype={'GTIN': str, 'CLASSE': str})
        return temp1 
    
    def BuscarNcmParametrizados():
        endereco = r'.\parametrizacoes\NCMs PARAMETRIZADAS.csv'
        df = pd.read_csv(endereco, usecols = ['NCM', 'PARAMETRIZAÇÃO'], dtype={'NCM': str, 'PARAMETRIZAÇÃO': str})
        return df
    
    def BuscarParametrizacaoGTIN(tupla_gtin):
        
        inicial = 0
        queries = []
        while inicial < len(tupla_gtin):
            final = min(inicial+1000, len(tupla_gtin))
            query = f"""
                        SELECT
                            DISTINCT
                            GTIN.TMEANCOD,
                            GTIN.TMEANDESC,
                            GTIN.TMEANCLENT "PARAM_GTIN"

                        FROM 
                            SIATE.FTMEAN GTIN

                        WHERE 
                        1=1
                            AND GTIN.TMEANSITU = 'A'
                            AND GTIN.TMEANCOD IN {query[inicial:final]}
                                    
                        """
            inicial += 1000
            queries.append(query)
            
        metadados_gtin = {'TMEANCOD':str, 'TMEANDESC': str, 'PARAM_GTIN': str}
        df = pd.DataFrame()
        
        for query in queries:
            with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
                temp = pd.read_sql_query(query, con = connection, dtype=metadados_gtin)
            
            df = pd.concat([df, temp])

        return df
    
    def BuscarParametrizacaoNCM(tupla_ncm):
        
        inicial = 0
        queries = []
        while inicial < len(tupla_ncm):
            final = min(inicial+1000, len(tupla_ncm))
            query = f"""
                        SELECT
                            DISTINCT
                            NCM.NCM_COD_NC,
                            NCM.NCM_NOME_N,
                            NCM.NCM_ANENTR PARAM_NCM

                        FROM 
                            SIATE.NCM NCM

                        WHERE 
                        1=1
                            AND NCM.NCM_SIT = 'A'
                            AND NCM.NCM_COD_NC IN {tupla_ncm[inicial:final]}       
                        """
            inicial += 1000
            queries.append(query)
            
        metadados_ncm = {'NCM_COD_NC':str, 'NCM_NOME_N': str, 'PARAM_NCM': str}
        df = pd.DataFrame()
        
        for query in queries:
            with oracledb.connect(user = user, password = pw, dsn = dns_tns) as connection:
                temp = pd.read_sql_query(query, con = connection, dtype=metadados_ncm)
            
            df = pd.concat([df, temp])

        return df


    def BuscarClassesFronteira():
        endereco = r'.\parametrizacoes\CLASSE.xlsx'
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