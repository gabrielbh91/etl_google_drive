# ETL_GOOGLE_DRIVE

Este projeto tem como objetivo a criação de um script para realizar o ETL de arquivos de dados que estão no Google Drive para outros repositórios.

## Application Deployment

### GOOGLE DRIVE API
Está aplicatação utliza a API do google Drive. Portanto, primeiramente é necessário satisfazer os pré-requisito para a utilização da APÌ.
Para mais informações acesse: https://developers.google.com/drive/api/quickstart/pythonclear

## Escopo de Projeto

    - Acessar o Google Drive e exportar os "data sets" armazenados no drive.
        Tipos de "data sets": csv, google sheet e xlsx
    
    - Consolidar o "data sets" em:
        - Repositório Local
        - Banco de dados Postgres (Pendente)
        - Bucket S3  (Pendente)

## Quickstart
Configure os arquivos:
    Credenciais da API:
        Token.json e credentials.json (Vide: https://developers.google.com/drive/api/quickstart/pythonclear)

    Configurações de destino:
        config.yaml

Terminal
    Exportar todos os arquivo:
        $ python ./resources/export_drive_files.py

    Exportar arquivos declarados
        $ python ./resources/export_drive_files.py <file_name_1> <file_name_2>
    
    Exportar todos arquivos exceto:
        $ python ./resources/export_drive_files.py ----except <file_name_1> <file_name_2>



