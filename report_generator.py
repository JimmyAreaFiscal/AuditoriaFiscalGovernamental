import datetime
import json
import os
import string
import tempfile
from docx import Document
from docx.shared import Inches
from docx.oxml.ns import qn
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import nsmap
from docx.enum.section import WD_ORIENT
from docx.enum.section import WD_SECTION_START
from docx.enum.style import WD_STYLE_TYPE
from docx.shared import Pt
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.table import WD_ALIGN_VERTICAL
from docx.oxml.ns import qn

# Este código assume:
# - Python 3.10 ou superior
# - Biblioteca python-docx instalada
# - Uma LLM local (ollama) está disponível, mas aqui deixaremos apenas a estrutura para posterior implementação.
# - O JSON com os textos do planejamento deverá existir em um caminho conhecido, por exemplo "planejamento.json".
#   Esse arquivo conterá um dicionário com key = nome do método e value = texto a ser inserido no capítulo "Do Planejamento".
#   Exemplo de planejamento.json:
#   {
#     "Verificação de Obrigações Acessórias": "Este texto para 'Verificação de Obrigações Acessórias' estará num JSON",
#     "Verificação de Obrigações Principais": "Este texto para 'Verificação de Obrigações Principais' estará num JSON"
#   }

# Requisitos não funcionais e detalhes de formatação:
# - Cabeçalho com o símbolo da SEFAZ-RR no início do documento.
#   Supondo que exista um arquivo "sefaz_rr_logo.png" disponível em algum path.
# - Nota de rodapé em todas as páginas.
# - Formatação padrão ABNT (aqui faremos algo básico, considerando fontes Times New Roman, tamanho 12, etc.)

# As variáveis de texto fixo da classe, conforme o enunciado:
# (Essas podem ser sobrescritas conforme necessidade, mas aqui deixaremos fixas)
# Note: O enunciado demonstra prints desses atributos de classe:
# print(RelatorioAuditoria.texto_fixo_da_auditoria)
# print(RelatorioAuditoria.texto_fixo_do_planejamento)
# print(RelatorioAuditoria.texto_fixo_da_matriz_achados)
# Aqui deixaremos como atributos de classe (variáveis de classe):

class RelatorioAuditoria:
    texto_fixo_da_auditoria = "Este texto fixo será informado na classe"
    texto_fixo_do_planejamento = "Este texto fixo para o planejamento será informado na classe"
    texto_fixo_da_matriz_achados = "Este texto fixo para a matriz de achados será informado na classe"
    texto_resultado = "Por meio dos procedimentos acima, chegou-se às seguintes conclusões: \n"

    def __init__(self, cnpj:str, cgf:str, ordem_servico:str, proc_sei:str, data_inicio:datetime.datetime, 
                 data_fim:datetime.datetime, nome_arquivo:str, 
                 caminho_logo:str="sefaz_rr_logo.png", caminho_json_planejamento:str="planejamento.json"):
        
        # Atributos principais
        self.cnpj = cnpj
        self.cgf = cgf
        self.ordem_servico = ordem_servico
        self.proc_sei = proc_sei
        self.data_inicio = data_inicio
        self.data_fim = data_fim
        self.nome_arquivo = nome_arquivo
        self.caminho_logo = caminho_logo
        self.caminho_json_planejamento = caminho_json_planejamento
        
        # Leitura do json de planejamento
        self.dict_planejamento = self._ler_textos_planejamento()
        
        # Armazenamento de procedimentos (capítulo II e III, bem como dados para a matriz do IV)
        # Cada procedimento: {"nome_metodo":..., "texto":..., "imagem":..., "tabela":..., "resultado":...}
        self.lista_procedimentos = []
        
        self.document = None
        

    def _ler_textos_planejamento(self):
        if os.path.exists(self.caminho_json_planejamento):
            with open(self.caminho_json_planejamento, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
        

    def _configurar_estilos_documento(self):
        # Ajustar fonte padrão para Times New Roman, tamanho 12
        style = self.document.styles['Normal']
        font = style.font
        font.name = 'Times New Roman'
        font.size = Pt(12)
        

    def _inserir_cabecalho(self):
        # Inserir Logo da SEFAZ-RR no início do documento, seguido do título
        # Aqui, inserir como primeira coisa no documento
        if os.path.exists(self.caminho_logo):
            self.document.add_picture(self.caminho_logo, width=Inches(1.5))
        
        titulo_SEFAZ = self.document.add_paragraph("SECRETARIA DE ESTADO DA FAZENDA DE RORAIMA")
        titulo_SEFAZ.alignment = WD_ALIGN_PARAGRAPH.CENTER
        titulo_SEFAZ.runs[0].bold = True

        titulo_DIFIS = self.document.add_paragraph("DIVISÃO DE FISCALIZAÇÃO DE ESTABELECIMENTOS")
        titulo_DIFIS.alignment = WD_ALIGN_PARAGRAPH.CENTER
        titulo_DIFIS.runs[0].bold = True

        self.document.add_paragraph('')
        self.document.add_paragraph('')
        titulo = self.document.add_paragraph("RELATÓRIO PRELIMINAR DE AUDITORIA")
        titulo.alignment = WD_ALIGN_PARAGRAPH.CENTER
        titulo.runs[0].bold = True
        
        self.document.add_paragraph("") # espaço
        
        
    def _criar_capitulo_da_auditoria(self):
        # Dar espaço em relação ao capítulo anterior
        for _ in range(3):
            self.document.add_paragraph("")

        # Capítulo: DA AUDITORIA
        p = self.document.add_paragraph("DA AUDITORIA")
        p.style = self.document.styles['Normal']
        p.runs[0].bold = True
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        # Texto fixo da auditoria
        self.document.add_paragraph(self.texto_fixo_da_auditoria)
        
        # Quadro: Informações do Processo
        self.document.add_paragraph("")
        self.document.add_paragraph("INFORMAÇÕES DO PROCESSO").runs[0].bold = True
        tabela_processo = self.document.add_table(rows=4, cols=2)
        tabela_processo.style = 'Table Grid'
        tabela_processo.alignment = WD_TABLE_ALIGNMENT.CENTER
        
        # Preenchimento da tabela de Processo
        tabela_processo.cell(0,0).text = "Ordem de Serviço"
        tabela_processo.cell(0,1).text = self.ordem_servico
        
        tabela_processo.cell(1,0).text = "Processo SEI"
        tabela_processo.cell(1,1).text = self.proc_sei
        
        tabela_processo.cell(2,0).text = "Data de Relatório"
        tabela_processo.cell(2,1).text = datetime.datetime.now().strftime("%d/%m/%Y")
        
        tabela_processo.cell(3,0).text = "Período de Fiscalização"
        tabela_processo.cell(3,1).text = f"{self.data_inicio.strftime('%d/%m/%Y')} a {self.data_fim.strftime('%d/%m/%Y')}"
        
        self.document.add_paragraph("")
        
        # Quadro: Informações da Empresa
        self.document.add_paragraph("INFORMAÇÕES DA EMPRESA").runs[0].bold = True
        tabela_empresa = self.document.add_table(rows=2, cols=2)
        tabela_empresa.style = 'Table Grid'
        tabela_empresa.alignment = WD_TABLE_ALIGNMENT.CENTER
        
        tabela_empresa.cell(0,0).text = "Número CNPJ"
        tabela_empresa.cell(0,1).text = self.cnpj
        
        tabela_empresa.cell(1,0).text = "Número de Cadastro Geral da Fazenda (CGF)"
        tabela_empresa.cell(1,1).text = self.cgf
        
        self.document.add_paragraph("")
        
        # Dar espaço em relação ao capítulo posterior
        self.document.add_page_break()
        
    def _criar_capitulo_do_planejamento(self):

        

        # Capítulo: DO PLANEJAMENTO
        p = self.document.add_paragraph("DO PLANEJAMENTO")
        p.runs[0].bold = True
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        self.document.add_paragraph(self.texto_fixo_do_planejamento)
        
        # Aqui no segundo capítulo, posteriormente iremos inserir subtítulos e textos do JSON
        # conforme o método inserir_procedimento for chamado.
        # Porém, a especificação diz que o segundo capítulo terá texto pré-definido (em JSON) para cada nome de método.
        # Este texto deverá ser inserido quando inserirmos o procedimento, antes do capítulo dos procedimentos executados.
        # Mas, a partir da leitura do enunciado, entende-se que o Capítulo II (Do Planejamento)
        # deverá conter os textos do JSON para cada método listado.
        # Vamos fazê-lo no momento da chamada do "inserir_procedimento" (ou em concluir) conforme interpretado:
        # Na verdade, o enunciado diz: "O segundo capítulo (Do Planejamento) ... para cada nome de método ..."
        # Assim, iremos armazenar os métodos e só ao concluir_auditoria iremos inserir.
        # Mas parece mais coerente inserir no momento da inserção do procedimento. 
        # Entretanto, o enunciado diz: "Você deverá ler o json e, para cada nome de método, ... inserir um subtítulo e texto..."
        # A ação de inserir_procedimento refere-se ao terceiro capítulo e à matriz do quarto.
        # Por outro lado, o enunciado diz que o segundo capítulo terá um texto pré-definido (em JSON) para cada método
        # (inserido na variável “relatorio”, no método “inserir_procedimento”).
        # Para evitar confusão, faremos assim:
        # - Quando inserir_procedimento for chamado, adicionamos o método no self.lista_procedimentos.
        # - Ao concluir_auditoria, iremos inserir no segundo capítulo, antes do terceiro, todos os métodos com seus textos do JSON.
        # Assim garantimos que todos os métodos tratados em inserir_procedimento também apareçam no Capítulo II.
        
        # Para facilitar, guardaremos um placeholder no documento e ao concluir_auditoria iremos ajustar a posição.
        self.p_marcapos_planejamento = self.document.add_paragraph("") # marcador de posição.
        
    def _criar_capitulo_dos_procedimentos_executados(self):

        
        # Capítulo: DOS PROCEDIMENTOS EXECUTADOS
        p = self.document.add_paragraph("DOS PROCEDIMENTOS EXECUTADOS")
        p.runs[0].bold = True
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        # Iremos inserir os procedimentos no momento da chamada do método inserir_procedimento().
        # Para isso, também podemos inserir um marcador de posição.
        self.p_marcapos_procedimentos = self.document.add_paragraph("")
        
    def _criar_capitulo_da_matriz_achados(self):

        
        # Capítulo: DA MATRIZ DE ACHADOS
        p = self.document.add_paragraph("DA MATRIZ DE ACHADOS")
        p.runs[0].bold = True
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        self.document.add_paragraph(self.texto_fixo_da_matriz_achados)
        
        # Criar tabela da matriz de achados, com colunas: "Procedimento”, “Resultado”, “Conclusão”, “Providências”
        self.tabela_achados = self.document.add_table(rows=1, cols=4)
        self.tabela_achados.style = 'Table Grid'
        hdr_cells = self.tabela_achados.rows[0].cells
        hdr_cells[0].text = "Procedimento"
        hdr_cells[1].text = "Resultado"
        hdr_cells[2].text = "Conclusão"
        hdr_cells[3].text = "Providências"
        
        self.p_marcapos_matriz = self.document.add_paragraph("")
        
    def _criar_capitulo_conclusao_providencias(self):

        
            
        # Capítulo: CONCLUSÃO E PROVIDÊNCIAS
        p = self.document.add_paragraph("CONCLUSÃO E PROVIDÊNCIAS")
        p.runs[0].bold = True
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        # Aqui colocaremos o texto da LLM após o método concluir_auditoria()
        self.p_marcapos_conclusao = self.document.add_paragraph("")
        
    def _inserir_rodape(self):
        # Inserir nota de rodapé em cada página:
        # docx não tem suporte nativo a rodapé em todas as páginas de forma simples.
        # Uma solução simples é inserir rodapé na seção principal.
        # O texto: “Relatório Preliminar de Auditoria – Dados sujeitos a Sigilo Fiscal, conforme CTN. 
        # O descumprimento do sigilo fiscal é considerado crime.” e número da página.
        
        sections = self.document.sections
        for section in sections:
            footer = section.footer
            footer_paragraph = footer.paragraphs[0]
            footer_paragraph.text = "Relatório Preliminar de Auditoria – Dados sujeitos a Sigilo Fiscal, conforme CTN. O descumprimento do sigilo fiscal é considerado crime. - Página "
            # Inserir número da página (não é trivial com python-docx inserir numeração automática, mas tentaremos usar um field)
            
            # Criar um field para número de página:
            # Essa é uma workaround: Não oficial no python-docx, mas usando low-level OXML:
            run = footer_paragraph.add_run()
            fldChar1 = OxmlElement('w:fldChar')
            fldChar1.set(qn('w:fldCharType'), 'begin')

            instrText = OxmlElement('w:instrText')
            instrText.set(qn('xml:space'), 'preserve')
            instrText.text = "PAGE"

            fldChar2 = OxmlElement('w:fldChar')
            fldChar2.set(qn('w:fldCharType'), 'separate')

            fldChar3 = OxmlElement('w:t')
            fldChar3.text = "1"

            fldChar4 = OxmlElement('w:fldChar')
            fldChar4.set(qn('w:fldCharType'), 'end')

            run._r.append(fldChar1)
            run._r.append(instrText)
            run._r.append(fldChar2)
            run._r.append(fldChar3)
            run._r.append(fldChar4)
        
    def inserir_procedimento(self, nome_metodo:str, texto:str, resultado:str, imagem=None, texto_adicional: str = None, tabela=None):
        # Armazena procedimento
        proc = {
            "nome_metodo": nome_metodo,
            "texto": texto,
            'texto_adicional': texto_adicional,
            "imagem": imagem,
            "tabela": tabela,
            "resultado": resultado
        }
        self.lista_procedimentos.append(proc)
        
        # # Adicionar linha na matriz de achados do quarto capítulo
        # row_cells = self.tabela_achados.add_row().cells
        # row_cells[0].text = nome_metodo
        # row_cells[1].text = resultado
        # row_cells[2].text = ""  # Conclusão vazio
        # row_cells[3].text = ""  # Providências vazio
        
    def concluir_auditoria(self):

        # Criar o documento agora
        self.document = Document()
        
        # Configurar estilos ABNT
        self._configurar_estilos_documento()

        # Inserir logo e título
        self._inserir_cabecalho()

        # Criar capítulo DA AUDITORIA
        self._criar_capitulo_da_auditoria()

        # Criar capítulo DO PLANEJAMENTO
        self._criar_capitulo_do_planejamento()

        # Inserir métodos do planejamento (com base no JSON)
        metodos_unicos = []
        for p in self.lista_procedimentos:
            if p["nome_metodo"] not in metodos_unicos:
                metodos_unicos.append(p["nome_metodo"])

        for metodo in metodos_unicos:
            idx = metodos_unicos.index(metodo) + 1
            roman = self._int_to_roman(idx)
            texto_metodo = self.dict_planejamento.get(metodo, f"Texto não encontrado para '{metodo}' no JSON")
            
            p_sub = self.document.add_paragraph(f"{roman}. {metodo}")
            p_sub.runs[0].bold = True
            p_txt = self.document.add_paragraph(texto_metodo)

        # Dar espaço em relação ao capítulo posterior
        self.document.add_page_break()

        # Criar capítulo DOS PROCEDIMENTOS EXECUTADOS
        self._criar_capitulo_dos_procedimentos_executados()
        
        # Inserir procedimentos
        for p in self.lista_procedimentos:
            idx = self.lista_procedimentos.index(p) + 1
            roman = self._int_to_roman(idx)
            # Subtítulo do procedimento
            subt = self.document.add_paragraph(f"{roman}. {p['nome_metodo']}")
            subt.runs[0].bold = True

            # Texto descritivo
            self.document.add_paragraph(p['texto'])

            # Texto adicional, se houver
            if p['texto_adicional'] is not None:
                self.document.add_paragraph(p['texto_adicional'])

            p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY

            # Ajustar formatação do parágrafo para ter indentação na primeira linha
            p_format = p.paragraph_format
            p_format.first_line_indent = Inches(0.5)  # ou qualquer valor em polegadas

            # Imagem (se houver)
            if p['imagem'] is not None:
                with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmpf:
                    p['imagem'].figure.savefig(tmpf.name)
                    self.document.add_picture(tmpf.name, width=Inches(5))
                    os.remove(tmpf.name)

            # Tabela (se houver)
            if p['tabela'] is not None:
                df = p['tabela']
                t = self.document.add_table(rows=df.shape[0]+1, cols=df.shape[1])
                t.style = 'Table Grid'
                for j, col in enumerate(df.columns):
                    t.cell(0,j).text = str(col)
                for i in range(df.shape[0]):
                    for j in range(df.shape[1]):
                        t.cell(i+1,j).text = str(df.iat[i,j])

            # Resultado
            
            self.document.add_paragraph(self.texto_resultado)
            self.document.add_paragraph(p['resultado'])

        # Dar espaço em relação ao capítulo posterior
        self.document.add_page_break()

        # Criar capítulo DA MATRIZ DE ACHADOS
        self._criar_capitulo_da_matriz_achados()
        
        # Preencher a tabela de achados com base nos procedimentos
        tabela_achados = self.tabela_achados
        for p in self.lista_procedimentos:
            row_cells = tabela_achados.add_row().cells
            row_cells[0].text = p['nome_metodo']
            row_cells[1].text = p['resultado']
            row_cells[2].text = ""  # Conclusão vazio
            row_cells[3].text = ""  # Providências vazio

        # Dar espaço em relação ao capítulo posterior
        self.document.add_page_break()
        
        # Criar capítulo CONCLUSÃO E PROVIDÊNCIAS
        self._criar_capitulo_conclusao_providencias()

        # Comunicar LLM (placeholder)
        achados = []
        for i in range(1, len(self.tabela_achados.rows)):
            procedimento = self.tabela_achados.cell(i,0).text
            resultado = self.tabela_achados.cell(i,1).text
            achados.append({"Procedimento": procedimento, "Resultado": resultado})

        prompt = "Resuma os procedimentos executados, conclusões e providências a partir da seguinte matriz de achados:\n"
        for a in achados:
            prompt += f"- {a['Procedimento']}: {a['Resultado']}\n"

        texto_conclusao = "Este texto foi criado pela LLM local, que identificou que havia duas discrepâncias..."
        self.p_marcapos_conclusao.text = texto_conclusao

        # Inserir rodapé
        self._inserir_rodape()
        

    def salvar(self, tipo:str='docx'):
        if self.document is None:
            raise RuntimeError("Documento não foi criado. Chame 'concluir_auditoria()' primeiro.")
        if tipo == 'docx':
            self.document.save(f"{self.nome_arquivo}.docx")
        elif tipo == 'pdf':
            pass
    
    def _int_to_roman(self, num):
        # Função auxiliar para transformar um inteiro em numeral romano (simples)
        val = [
            1000, 900, 500, 400,
            100, 90, 50, 40,
            10, 9, 5, 4,
            1
        ]
        syb = [
            "M", "CM", "D", "CD",
            "C", "XC", "L", "XL",
            "X", "IX", "V", "IV",
            "I"
        ]
        roman_num = ''
        i = 0
        while  num > 0:
            for _ in range(num // val[i]):
                roman_num += syb[i]
                num -= val[i]
            i += 1
        return roman_num
