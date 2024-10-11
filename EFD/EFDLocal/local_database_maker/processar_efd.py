import EFD.EFDLocal.local_database_maker.process as efd
import os

def processar_efd(local):
    # Procurar arquivos de acordo com a classe CMID
    for file in os.listdir(local):
        if file.endswith(f'.txt'):

            # Definir localização do arquivo.
            file_path = os.path.join(local, file)

            efd.process_efd_file(file_path)