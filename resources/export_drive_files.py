from __future__ import print_function

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from operator import itemgetter 
from os.path import join, exists

from sys import argv
import io
import variables as var
import re


# If modifying these scopes, delete the file token.json.
SCOPES = [
        'https://www.googleapis.com/auth/drive.metadata.readonly',
        'https://www.googleapis.com/auth/docs',
        'https://www.googleapis.com/auth/drive.readonly',
]

def main(except_arg = False,*files_name):

    # argumentos: selecionar arquivos que serão exportados, null = Todos arquivos
    # iniciando com --except or -e, Todos exceto <args>
    # * (Aplicar Regex)
    
    global service_files, items, EXCEPT, PATTERN_FILES_NAME
    EXCEPT = except_arg
    PATTERN_FILES_NAME = parse_files_name_to_regex(files_name)
    
    service_files, items = driver_conection()
    set_id_items_to_export()
    download_files_from_drive()

    return service_files, items


def set_id_items_to_export():
    global items
    items_index = []
    for index, item in enumerate(items):
        if item['mimeType'] in var.MIMETYPES_LIST and apply_args(item['name']):
            items_index.append(index)

    if bool(items_index):
        if len(items_index) == 1:
            items = [items[items_index[0]]]
        else:
            items = list(itemgetter(*items_index)(items))    
    else:
        items = []

def driver_conection():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if exists(var.TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(var.TOKEN_FILE, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                var.CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(var.TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('drive', 'v3', credentials=creds)

        # Call the Drive v3 API
        service_files = service.files()
        results = service_files.list(
            fields="nextPageToken, files(id, name, mimeType)").execute()
        items = results.get('files', [])

        if not items:
            print('No files found.')
            return
        
        return service_files, items
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')

def download_files_from_drive():
    # Get id files to export
    for item in items:
        if re.search('vnd.google-apps.spreadsheet',item['mimeType']):
            download_with_export_media(item['id'],item['name'])
        else:
            download_with_get_media(item['id'],item['name'])

def apply_args(file_name):
    
    if not bool(PATTERN_FILES_NAME):
        return True

    if EXCEPT:
        for pattern in PATTERN_FILES_NAME:
            match = re.search(pattern,file_name)    
            if match: return False    
        return True

    else:
        for pattern in PATTERN_FILES_NAME:
            match = re.search(pattern,file_name)    
            if match: return True    
        return False
        

def download_with_export_media(file_id,file_name):
    # Dowload files with "export_media"
    # Para arquivos editáveis no drive
    request = service_files.export_media(
        fileId=file_id,
        mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'# Parse To
        )
    fh = io.FileIO(join(var.DIR_TEMP,file_name+'.xlsx'),'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print ("Download %s %d%%." % (file_name,int(status.progress() * 100)))

def download_with_get_media(file_id,file_name):
    # Dowload files with "get_media"
    # Para arquivos não editáveis no drive
    request = service_files.get_media(fileId=file_id)
    fh = io.FileIO(join(var.DIR_TEMP,file_name),'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print ("Download %s %d%%." % (file_name,int(status.progress() * 100)))

def parse_files_name_to_regex(files_name):
    pattern_files_name = []
    for file_name in files_name:
        if not file_name[0] in ('*','^'): 
            file_name = '^' + file_name
        if not file_name[-1] in ('*','$'): 
            file_name = file_name + '$'
        file_name = file_name.replace('*','')

        pattern_files_name.append(re.compile(file_name))

    return pattern_files_name


if __name__ == '__main__':

    if len(argv) > 1:
        if argv[1].lower() in ('-e','--except'): 
            main(True, *argv[2:])
        else:
            main(False, *argv[1:])
    else:
        main(False)