import pandas as pd 
import requests 

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
