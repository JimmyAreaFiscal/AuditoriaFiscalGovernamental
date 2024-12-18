import pandas as pd 
import requests 
from dados import url031Str, payload031Str, headers
import requests
import xmltodict


def retrieve_cnpj_data(cnpj):
        url = f"https://open.cnpja.com/office/{cnpj}"
        
        # Fetch the JSON data from the API
        response = requests.get(url)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            json_data = response.json()
            
            return json_data
        else:
            # Handle the case where the request failed
            print(f"Failed to retrieve data for CNPJ {cnpj}. Status code: {response.status_code}")
            return None

class DadosCadastraisJUCERR:

    def __init__(self, cnpj):
        self.json = self._retrieve_jucerr_data(cnpj)

    def _retrieve_jucerr_data(self, cnpj):
        response = requests.request("POST", url031Str, headers=headers, data=payload031Str.format(cnpj))
        dados_redesim = xmltodict.parse(response.content.decode(response.apparent_encoding))['soap:Envelope']['soap:Body']['ns2:wsE031Response']['return']['registrosRedesim']['registroRedesim']['dadosRedesim']
        return dados_redesim
    
    
    def BuscarSocios(self):
        " Função que retorna nome de sócios constante na JUCERR"
        dados_socios = {
                'funcao': [],
                'cpf_cnpj': [],
                'nome': [],
                'dataInclusao':[] }

        for condicao in self.json['socios']:
            dados_socios['funcao'].append(condicao)
            dados_socios['cpf_cnpj'].append(self.json['socios'][condicao]['cnpjCpfSocio'])
            dados_socios['nome'].append(self.json['socios'][condicao]['nome'])
            dados_socios['dataInclusao'].append(self.json['socios'][condicao]['dataInclusao'])

        return pd.DataFrame(dados_socios)

    def BuscarInformacoesFiscais(self):
        
        info = {}

        info['RazaoSocial'] = self.json['cnpj']
        info['CapitalSocial'] = self.json['capitalSocial']
        info['Porte'] = self.json['porte']
        info['OptanteSimples'] = self.json['opcaoSimplesNacional']
        info['OptanteMEI'] = self.json['opcaoSimei']

        return pd.DataFrame(info, index=[0])
    

    def BuscarCadastro(self):
        
        info = {}

        info['RazaoSocial'] = self.json['nomeEmpresarial']
        info['DataCadastro'] = self.json['dataAberturaEstabelecimento']
        info['SituacaoCadastral'] = self.json['situacaoCadastralRFB']['descricao']
        
        info['CapitalSocial'] = self.json['capitalSocial']
        info['Porte'] = self.json['porte']

        info['Rua'] = self.json['endereco']['codTipoLogradouro'] + '. ' + self.json['endereco']['logradouro']
        info['Numero'] = self.json['endereco']['numLogradouro']
        info['Bairro'] = self.json['endereco']['bairro']
        info['Cidade'] = self.json['endereco']['codMunicipio']
        info['Estado'] = self.json['endereco']['uf']
        info['Complemento'] = self.json['endereco']['complemento']
        info['CEP'] = self.json['endereco']['cep']

        info['OptanteSimples'] = self.json['opcaoSimplesNacional']
        info['OptanteMEI'] = self.json['opcaoSimei']

        return pd.DataFrame(info, index = [0])


    
    def BuscarTelefones(self):
        raise NotImplementedError("Não há contato telefônico no exemplo. Ainda preciso construir uma maneira de obter")
        info = {}

        info['Area'] = [i['area'] for i in self.json['phones']]
        info['Telefone'] = [i['number'] for i in self.json['phones']]
        return pd.DataFrame(info)


    def HorarioFuncionamento(self):
        info = {}

        info['Dia'] = [i['dia'] for i in self.json['horariosFuncionamento']['horarioFuncionamento']]
        info['HoraInicio'] = [i['horaInicio'] for i in self.json['horariosFuncionamento']['horarioFuncionamento']]
        info['HoraFim'] = [i['horaFim'] for i in self.json['horariosFuncionamento']['horarioFuncionamento']]
        return pd.DataFrame(info)


    def BuscarEmails(self):

        info = {}
        info['Email'] = [self.json['contato']['correioEletronico']]
        return pd.DataFrame(info, index=[0])
    
    def BuscarAtividades(self):
        info = {'CNAE':[], 'Relacao': [], 'ExercidaNoLocal': []}

        info['CNAE'].append(self.json['atividadesEconomica']['cnaeFiscal']['codigo'])
        info['Relacao'].append('Principal')
        info['ExercidaNoLocal'].append(self.json['atividadesEconomica']['cnaeFiscal']['exercidaNoLocal'])


        info['CNAE'] += [str(i['codigo']) for i in self.json['atividadesEconomica']['cnaesSecundarias']['cnaeSecundaria']]
        info['Relacao'] += ['Secundária' for i in self.json['atividadesEconomica']['cnaesSecundarias']['cnaeSecundaria']]
        info['ExercidaNoLocal'] += [i['exercidaNoLocal'] for i in self.json['atividadesEconomica']['cnaesSecundarias']['cnaeSecundaria']]

        info_df = pd.DataFrame(info)

        info_df['CNAE'] = info_df['CNAE'].str[:4] + '-' + info_df['CNAE'].str[4] + '/' + info_df['CNAE'].str[5:]

        return info_df




class DadosCadastraisRFB:

    def __init__(self, cnpj):
        self.json = retrieve_cnpj_data(cnpj)

    def BuscarSocios(self):
        socios = pd.DataFrame()
        for dados_socios in self.json['company']['members']:
            inicio = dados_socios['since']
            funcao = dados_socios['role']['text']
            socio = pd.DataFrame(dados_socios['person'], index = [0])
            socio['Inicio'] = inicio 
            socio['Função'] = funcao 
            socios = pd.concat([socios, socio])
        return socios
    

    def BuscarInformacoesFiscais(self):
        
        info = {}

        info['RazaoSocial'] = self.json['company']['name']
        info['CapitalSocial'] = self.json['company']['equity']
        info['Porte'] = self.json['company']['size']['acronym']
        info['OptanteSimples'] = self.json['company']['simples']['optant']
        info['DataOpcaoSimples'] = self.json['company']['simples']['since']
        info['OptanteMEI'] = self.json['company']['simei']['optant']
        info['DataOpcaoMEI'] = self.json['company']['simei']['since']
        info['NaturezaJuridica'] = self.json['company']['nature']['text']

        return pd.DataFrame(info, index=[0])
    

    def BuscarCadastro(self):
        
        info = {}

        info['RazaoSocial'] = self.json['company']['name']
        info['NomeFantasia'] = self.json['alias']
        info['DataCadastro'] = self.json['founded']
        info['SituacaoCadastral'] = self.json['status']['text']
        # if self.json['status']['id'] != '2':
        #     info['RazaoSituacaoCadastral'] = self.json['reason']['text']
        # else:
        #     info['RazaoSituacaoCadastral'] = None 
        
        info['CapitalSocial'] = self.json['company']['equity']
        info['Porte'] = self.json['company']['size']['acronym']

        info['Rua'] = self.json['address']['street']
        info['Numero'] = self.json['address']['number']
        info['Bairro'] = self.json['address']['district']
        info['Cidade'] = self.json['address']['city']
        info['Estado'] = self.json['address']['state']
        info['Complemento'] = self.json['address']['details']
        info['CEP'] = self.json['address']['zip']
        # info['Latitude'] = self.json['address']['latitude']
        # info['Longitude'] = self.json['address']['longitude']

        info['OptanteSimples'] = self.json['company']['simples']['optant']
        info['DataOpcaoSimples'] = self.json['company']['simples']['since']
        info['OptanteMEI'] = self.json['company']['simei']['optant']
        info['DataOpcaoMEI'] = self.json['company']['simei']['since']
        info['NaturezaJuridica'] = self.json['company']['nature']['text']

        return pd.DataFrame(info, index = [0])
    


    def BuscarTelefones(self):
        info = {}

        info['Area'] = [i['area'] for i in self.json['phones']]
        info['Telefone'] = [i['number'] for i in self.json['phones']]
        return pd.DataFrame(info)

    def BuscarEmails(self):

        info = {}
        info['Email'] = [i['address'] for i in self.json['emails']]
        return pd.DataFrame(info)
    
    def BuscarAtividades(self):
        info = {}

        info['CNAE'] = [str(i['id']) for i in self.json['sideActivities']]
        info['Descricao'] = [i['text'] for i in self.json['sideActivities']]
        info['Relacao'] = ['Secundario' for i in self.json['sideActivities']]

        info['CNAE'].append(str(self.json['mainActivity']['id']))
        info['Descricao'].append(self.json['mainActivity']['text'])
        info['Relacao'].append('Principal')

        info_df = pd.DataFrame(info)

        info_df['CNAE'] = info_df['CNAE'].str[:4] + '-' + info_df['CNAE'].str[4] + '/' + info_df['CNAE'].str[5:]
        return info_df
