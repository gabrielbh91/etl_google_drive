from os.path import dirname,join,split

DIR_PRINCIPAL = dirname(__file__)
DIR_SOURCE = split(dirname(__file__))[0]
DIR_TEMP = join(dirname(__file__),'temp')
MIMETYPES_LIST = [
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',    # xlsx
    'application/vnd.google-apps.spreadsheet',                              # google sheet
    'text/csv',                                                             # csv
]

TOKEN_FILE = join(DIR_SOURCE,'token.json')
CREDENTIALS_FILE = join(DIR_SOURCE,'credentials.json')

